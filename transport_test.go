package zk

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/byte_stream"
)

func TestWriteAndReadTransport(t *testing.T) {
	l, e := net.Listen("tcp", "")

	if e != nil {
		t.Fatalf("%v", e)
	}

	la := l.Addr().String()
	defer l.Close()

	m1 := []byte("test")
	m2 := []byte("test222")

	go func() {
		var tp transport
		e := tp.connect(nil, &TransportPolicy{}, la)

		if e != nil {
			t.Fatalf("%v", e)
		}

		defer tp.close()

		if e := tp.write(func(byteStream *byte_stream.ByteStream) error {
			byteStream.Write(m1)
			return nil
		}); e != nil {
			t.Fatalf("%v", e)
		}

		if e := tp.flush(context.Background(), 60*time.Second); e != nil {
			t.Fatalf("%v", e)
		}

		time.Sleep(time.Second / 5)

		if e := tp.write(func(byteStream *byte_stream.ByteStream) error {
			byteStream.Write(m2)
			return nil
		}); e != nil {
			t.Fatalf("%v", e)
		}

		if e := tp.write(func(byteStream *byte_stream.ByteStream) error {
			byteStream.Write(m1)
			return nil
		}); e != nil {
			t.Fatalf("%v", e)
		}

		if e := tp.flush(context.Background(), 60*time.Second); e != nil {
			t.Fatalf("%v", e)
		}
	}()

	c, e := l.Accept()

	if e != nil {
		t.Fatalf("%v", e)
	}

	var tp transport
	tp.accept(&TransportPolicy{}, c)
	defer tp.close()

	m3, e := tp.peek(context.Background(), 60*time.Second)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !bytes.Equal(m1, m3) {
		t.Fatalf("%#v != %#v", m1, m3)
	}

	tp.skip(m3)
	m4, e := tp.peek(context.Background(), 60*time.Second)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !bytes.Equal(m2, m4) {
		t.Fatalf("%#v != %#v", m2, m4)
	}

	tp.skip(m4)
	m5, e := tp.peek(context.Background(), 60*time.Second)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !bytes.Equal(m1, m5) {
		t.Fatalf("%#v != %#v", m1, m5)
	}

	tp.skip(m5)
	_, e = tp.peek(context.Background(), 60*time.Second)

	if e == nil {
		t.Fatal("e is nil")
	}

	if e != io.EOF {
		t.Fatalf("%T is not EOF", e)
	}
}
