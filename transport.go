package zk

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/let-z-go/toolkit/byte_stream"
)

type TransportPolicy struct {
	InitialReadBufferSize int32
	MaxPacketPayloadSize  int32
	validateOnce          sync.Once
}

func (self *TransportPolicy) Validate() {
	self.validateOnce.Do(func() {
		if self.InitialReadBufferSize < minInitialReadBufferSizeOfTransport {
			self.InitialReadBufferSize = minInitialReadBufferSizeOfTransport
		} else if self.InitialReadBufferSize > maxInitialReadBufferSizeOfTransport {
			self.InitialReadBufferSize = maxInitialReadBufferSizeOfTransport
		}

		if self.MaxPacketPayloadSize < minMaxPacketPayloadSize {
			self.MaxPacketPayloadSize = minMaxPacketPayloadSize
		}
	})
}

var TransportClosedError = errors.New("zk: transport closed")
var PacketPayloadTooLargeError = errors.New("zk: packet payload too large")

const minInitialReadBufferSizeOfTransport = 1 << 4
const maxInitialReadBufferSizeOfTransport = 1 << 16
const minMaxPacketPayloadSize = (1 << 20) - 1
const packetHeaderSize = 4

type transport struct {
	policy           *TransportPolicy
	connection       *net.TCPConn
	inputByteStream  byte_stream.ByteStream
	outputByteStream byte_stream.ByteStream
	openness         int32
}

func (self *transport) Connect(context_ context.Context, policy *TransportPolicy, serverAddress string) error {
	var connection net.Conn
	var e error

	if context_ == nil {
		connection, e = (&net.Dialer{}).Dial("tcp", serverAddress)
	} else {
		if e := context_.Err(); e != nil {
			return e
		}

		connection, e = (&net.Dialer{}).DialContext(context_, "tcp", serverAddress)
	}

	if e != nil {
		return e
	}

	self.initialize(policy, connection.(*net.TCPConn))
	return nil
}

func (self *transport) Accept(policy *TransportPolicy, connection *net.TCPConn) {
	self.initialize(policy, connection)
}

func (self *transport) Close() error {
	if self.IsClosed() {
		return TransportClosedError
	}

	e := self.connection.Close()
	self.policy = nil
	self.connection = nil
	self.inputByteStream.Collect()
	self.outputByteStream.Collect()
	self.openness = -1
	return e
}

func (self *transport) Peek(context_ context.Context, timeout time.Duration) ([]byte, error) {
	packet, e := self.doPeek(context_, timeout)

	if e != nil {
		return nil, e
	}

	packetPayload := packet[packetHeaderSize:]
	return packetPayload, nil
}

func (self *transport) Skip(packetPayload []byte) {
	packetSize := packetHeaderSize + len(packetPayload)
	self.inputByteStream.Skip(packetSize)
}

func (self *transport) PeekInBatch(context_ context.Context, timeout time.Duration) ([][]byte, error) {
	packet, e := self.doPeek(context_, timeout)

	if e != nil {
		return nil, e
	}

	packetPayloads := [][]byte{packet[packetHeaderSize:]}
	dataOffset := len(packet)

	for {
		packet, ok := self.tryPeek(dataOffset)

		if !ok {
			break
		}

		packetPayloads = append(packetPayloads, packet[packetHeaderSize:])
		dataOffset += len(packet)
	}

	return packetPayloads, nil
}

func (self *transport) SkipInBatch(packetPayloads [][]byte) {
	totalPacketSize := 0

	for _, packetPayload := range packetPayloads {
		totalPacketSize += packetHeaderSize + len(packetPayload)
	}

	self.inputByteStream.Skip(totalPacketSize)
}

func (self *transport) Write(callback func(*byte_stream.ByteStream) error) error {
	if self.IsClosed() {
		return TransportClosedError
	}

	i := self.outputByteStream.GetDataSize()

	self.outputByteStream.WriteDirectly(packetHeaderSize, func(buffer []byte) error {
		return nil
	})

	if e := callback(&self.outputByteStream); e != nil {
		self.outputByteStream.Unwrite(self.outputByteStream.GetDataSize() - i)
		return e
	}

	packet := self.outputByteStream.GetData()[i:]
	packetPayloadSize := len(packet) - packetHeaderSize

	if packetPayloadSize > int(self.policy.MaxPacketPayloadSize) {
		self.outputByteStream.Unwrite(len(packet))
		return PacketPayloadTooLargeError
	}

	binary.BigEndian.PutUint32(packet, uint32(packetPayloadSize))
	return nil
}

func (self *transport) Flush(context_ context.Context, timeout time.Duration) error {
	if self.IsClosed() {
		return TransportClosedError
	}

	deadline, e := makeDeadline(context_, timeout)

	if e != nil {
		return e
	}

	if e := self.connection.SetWriteDeadline(deadline); e != nil {
		return e
	}

	n, e := self.connection.Write(self.outputByteStream.GetData())
	self.outputByteStream.Skip(n)
	return e
}

func (self *transport) IsClosed() bool {
	return self.openness != 1
}

func (self *transport) initialize(policy *TransportPolicy, connection *net.TCPConn) {
	if self.openness != 0 {
		panic(errors.New("zk: transport already initialized"))
	}

	policy.Validate()
	self.policy = policy
	self.connection = connection
	self.inputByteStream.ReserveBuffer(int(policy.InitialReadBufferSize))
	self.openness = 1
}

func (self *transport) doPeek(context_ context.Context, timeout time.Duration) ([]byte, error) {
	if self.IsClosed() {
		return nil, TransportClosedError
	}

	deadlineIsSet := false

	if self.inputByteStream.GetDataSize() < packetHeaderSize {
		deadline, e := makeDeadline(context_, timeout)

		if e != nil {
			return nil, e
		}

		if e := self.connection.SetReadDeadline(deadline); e != nil {
			return nil, e
		}

		deadlineIsSet = true

		for {
			dataSize, e := self.connection.Read(self.inputByteStream.GetBuffer())

			if dataSize == 0 {
				return nil, e
			}

			self.inputByteStream.CommitBuffer(dataSize)

			if self.inputByteStream.GetDataSize() >= packetHeaderSize {
				if self.inputByteStream.GetBufferSize() == 0 {
					self.inputByteStream.ReserveBuffer(1)
				}

				break
			}
		}
	}

	packetHeader := self.inputByteStream.GetData()[:packetHeaderSize]
	packetPayloadSize := int(binary.BigEndian.Uint32(packetHeader))

	if packetPayloadSize > int(self.policy.MaxPacketPayloadSize) {
		self.connection.CloseRead()
		return nil, PacketPayloadTooLargeError
	}

	packetSize := packetHeaderSize + packetPayloadSize

	if bufferSize := packetSize - self.inputByteStream.GetDataSize(); bufferSize >= 1 {
		if !deadlineIsSet {
			deadline, e := makeDeadline(context_, timeout)

			if e != nil {
				return nil, e
			}

			if e := self.connection.SetReadDeadline(deadline); e != nil {
				return nil, e
			}
		}

		self.inputByteStream.ReserveBuffer(bufferSize)

		for {
			dataSize, e := self.connection.Read(self.inputByteStream.GetBuffer())

			if dataSize == 0 {
				return nil, e
			}

			self.inputByteStream.CommitBuffer(dataSize)

			if self.inputByteStream.GetDataSize() >= packetSize {
				if self.inputByteStream.GetBufferSize() == 0 {
					self.inputByteStream.ReserveBuffer(1)
				}

				break
			}
		}
	}

	packet := self.inputByteStream.GetData()[:packetSize]
	return packet, nil
}

func (self *transport) tryPeek(dataOffset int) ([]byte, bool) {
	if self.inputByteStream.GetDataSize()-dataOffset < packetHeaderSize {
		return nil, false
	}

	packetHeader := self.inputByteStream.GetData()[dataOffset : dataOffset+packetHeaderSize]
	packetPayloadSize := int(binary.BigEndian.Uint32(packetHeader))

	if packetPayloadSize > int(self.policy.MaxPacketPayloadSize) {
		return nil, false
	}

	packetSize := packetHeaderSize + packetPayloadSize

	if self.inputByteStream.GetDataSize()-dataOffset < packetSize {
		return nil, false
	}

	packet := self.inputByteStream.GetData()[dataOffset : dataOffset+packetSize]
	return packet, true
}

func makeDeadline(context_ context.Context, timeout time.Duration) (time.Time, error) {
	if e := context_.Err(); e != nil {
		return time.Time{}, e
	}

	deadline1, ok := context_.Deadline()
	deadline2 := time.Now().Add(timeout)

	if ok && deadline1.Before(deadline2) {
		return deadline1, nil
	} else {
		return deadline2, nil
	}
}
