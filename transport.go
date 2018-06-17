package zk

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/let-z-go/toolkit/byte_stream"
)

type TransportPolicy struct {
	MinReadBufferSize    int32
	MaxPacketPayloadSize int32
	validateOnce         sync.Once
}

func (self *TransportPolicy) Validate() {
	self.validateOnce.Do(func() {
		if self.MinReadBufferSize < minMinTransportReadBufferSize {
			self.MinReadBufferSize = minMinTransportReadBufferSize
		} else if self.MinReadBufferSize > maxMinTransportReadBufferSize {
			self.MinReadBufferSize = maxMinTransportReadBufferSize
		}

		if self.MaxPacketPayloadSize < minMaxPacketPayloadSize {
			self.MaxPacketPayloadSize = minMaxPacketPayloadSize
		}
	})
}

var TransportClosedError = errors.New("zk: transport closed")
var PacketPayloadTooLargeError = errors.New("zk: packet payload too large")

type transport struct {
	policy     *TransportPolicy
	connection *net.TCPConn
	buffers    net.Buffers
	byteStream byte_stream.ByteStream
	openness   int32
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
	if !atomic.CompareAndSwapInt32(&self.openness, 1, -1) {
		return TransportClosedError
	}

	e := self.connection.Close()
	self.policy = nil
	self.connection = nil
	self.buffers = nil
	self.byteStream.Collect()
	return e
}

func (self *transport) Write(packetPayload []byte) error {
	if self.IsClosed() {
		return TransportClosedError
	}

	if len(packetPayload) > int(self.policy.MaxPacketPayloadSize) {
		return PacketPayloadTooLargeError
	}

	self.buffers = append(self.buffers, nil, packetPayload)
	return nil
}

func (self *transport) Flush(context_ context.Context, timeout time.Duration) error {
	if self.IsClosed() {
		return TransportClosedError
	}

	if len(self.buffers) == 0 {
		return nil
	}

	packetHeadersSize := 0

	for i := len(self.buffers) - 2; i >= 0; i -= 2 {
		packetHeader := self.buffers[i]

		if packetHeader != nil {
			break
		}

		packetHeadersSize += packetHeaderSize
	}

	packetHeaders := make([]byte, packetHeadersSize)

	for i := len(self.buffers) - 2; i >= 0; i -= 2 {
		packetHeader := self.buffers[i]

		if packetHeader != nil {
			break
		}

		packetHeader = packetHeaders[:packetHeaderSize]
		packetHeaders = packetHeaders[packetHeaderSize:]
		packetPayload := self.buffers[i+1]
		binary.BigEndian.PutUint32(packetHeader, uint32(len(packetPayload)))
		self.buffers[i] = packetHeader
	}

	deadline, e := makeDeadline(context_, timeout)

	if e != nil {
		return e
	}

	if e := self.connection.SetWriteDeadline(deadline); e != nil {
		return e
	}

	_, e = self.buffers.WriteTo(self.connection)
	return e
}

func (self *transport) Peek(context_ context.Context, timeout time.Duration) ([]byte, error) {
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
		buffer := self.byteStream.GetBuffer()

		for {
			dataSize, e := self.connection.Read(buffer)

			if dataSize == 0 {
				return nil, e
			}

			self.byteStream.CommitBuffer(dataSize)

			if self.byteStream.GetDataSize() >= packetHeaderSize {
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
		buffer := self.byteStream.GetBuffer()

		for {
			dataSize, e := self.connection.Read(buffer)

			if dataSize == 0 {
				return nil, e
			}

			self.byteStream.CommitBuffer(dataSize)

			if self.byteStream.GetDataSize() >= packetSize {
				break
			}
		}
	}

	packetPayload := self.byteStream.GetData()[packetHeaderSize:packetSize]
	return packetPayload, nil
}

func (self *transport) Skip(packetPayloadSize int) {
	packetSize := packetHeaderSize + packetPayloadSize
	self.byteStream.DiscardData(packetSize)
}

func (self *transport) IsClosed() bool {
	return atomic.LoadInt32(&self.openness) != 1
}

func (self *transport) initialize(policy *TransportPolicy, connection *net.TCPConn) {
	if self.openness != 0 {
		panic(errors.New("zk: transport already initialized"))
	}

	policy.Validate()
	self.policy = policy
	self.connection = connection
	self.byteStream.ReserveBuffer(int(policy.MinReadBufferSize))
	self.openness = 1
}

const minMinTransportReadBufferSize = 1 << 4
const maxMinTransportReadBufferSize = 1 << 16
const minMaxPacketPayloadSize = (1 << 20) - 1
const packetHeaderSize = 4

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
