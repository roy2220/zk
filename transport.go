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
	policy         *TransportPolicy
	connection     *net.TCPConn
	packetPayloads [][]byte
	packets        []byte
	byteStream     byte_stream.ByteStream
	openness       int32
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
	self.packetPayloads = nil
	self.packets = nil
	self.byteStream.Collect()
	self.openness = -1
	return e
}

func (self *transport) Write(packetPayload []byte) error {
	if self.IsClosed() {
		return TransportClosedError
	}

	if len(packetPayload) > int(self.policy.MaxPacketPayloadSize) {
		return PacketPayloadTooLargeError
	}

	self.packetPayloads = append(self.packetPayloads, packetPayload)
	return nil
}

func (self *transport) Flush(context_ context.Context, timeout time.Duration) error {
	if self.IsClosed() {
		return TransportClosedError
	}

	if len(self.packets) == 0 {
		totalPacketSize := 0

		for _, packetPayload := range self.packetPayloads {
			totalPacketSize += packetHeaderSize + len(packetPayload)
		}

		packets := make([]byte, totalPacketSize)
		totalPacketSize = 0

		for i, packetPayload := range self.packetPayloads {
			self.packetPayloads[i] = nil
			packet := packets[totalPacketSize:]
			binary.BigEndian.PutUint32(packet, uint32(len(packetPayload)))
			copy(packet[packetHeaderSize:], packetPayload)
			totalPacketSize += packetHeaderSize + len(packetPayload)
		}

		self.packetPayloads = self.packetPayloads[0:0]
		self.packets = packets
	}

	deadline, e := makeDeadline(context_, timeout)

	if e != nil {
		return e
	}

	if e := self.connection.SetWriteDeadline(deadline); e != nil {
		return e
	}

	n, e := self.connection.Write(self.packets)
	self.packets = self.packets[n:]
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
	self.byteStream.DiscardData(packetSize)
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

	self.byteStream.DiscardData(totalPacketSize)
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
	self.byteStream.ReserveBuffer(int(policy.InitialReadBufferSize))
	self.openness = 1
}

func (self *transport) doPeek(context_ context.Context, timeout time.Duration) ([]byte, error) {
	if self.IsClosed() {
		return nil, TransportClosedError
	}

	deadlineIsSet := false

	if self.byteStream.GetDataSize() < packetHeaderSize {
		deadline, e := makeDeadline(context_, timeout)

		if e != nil {
			return nil, e
		}

		if e := self.connection.SetReadDeadline(deadline); e != nil {
			return nil, e
		}

		deadlineIsSet = true

		for {
			dataSize, e := self.connection.Read(self.byteStream.GetBuffer())

			if dataSize == 0 {
				return nil, e
			}

			self.byteStream.CommitBuffer(dataSize)

			if self.byteStream.GetDataSize() >= packetHeaderSize {
				if self.byteStream.GetBufferSize() == 0 {
					self.byteStream.ReserveBuffer(1)
				}

				break
			}
		}
	}

	packetHeader := self.byteStream.GetData()[:packetHeaderSize]
	packetPayloadSize := int(binary.BigEndian.Uint32(packetHeader))

	if packetPayloadSize > int(self.policy.MaxPacketPayloadSize) {
		self.connection.CloseRead()
		return nil, PacketPayloadTooLargeError
	}

	packetSize := packetHeaderSize + packetPayloadSize

	if bufferSize := packetSize - self.byteStream.GetDataSize(); bufferSize >= 1 {
		if !deadlineIsSet {
			deadline, e := makeDeadline(context_, timeout)

			if e != nil {
				return nil, e
			}

			if e := self.connection.SetReadDeadline(deadline); e != nil {
				return nil, e
			}
		}

		self.byteStream.ReserveBuffer(bufferSize)

		for {
			dataSize, e := self.connection.Read(self.byteStream.GetBuffer())

			if dataSize == 0 {
				return nil, e
			}

			self.byteStream.CommitBuffer(dataSize)

			if self.byteStream.GetDataSize() >= packetSize {
				if self.byteStream.GetBufferSize() == 0 {
					self.byteStream.ReserveBuffer(1)
				}

				break
			}
		}
	}

	packet := self.byteStream.GetData()[:packetSize]
	return packet, nil
}

func (self *transport) tryPeek(dataOffset int) ([]byte, bool) {
	if self.byteStream.GetDataSize()-dataOffset < packetHeaderSize {
		return nil, false
	}

	packetHeader := self.byteStream.GetData()[dataOffset : dataOffset+packetHeaderSize]
	packetPayloadSize := int(binary.BigEndian.Uint32(packetHeader))

	if packetPayloadSize > int(self.policy.MaxPacketPayloadSize) {
		return nil, false
	}

	packetSize := packetHeaderSize + packetPayloadSize

	if self.byteStream.GetDataSize()-dataOffset < packetSize {
		return nil, false
	}

	packet := self.byteStream.GetData()[dataOffset : dataOffset+packetSize]
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
