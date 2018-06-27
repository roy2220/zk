package zk

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"
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
		e := tp.Connect(nil, &TransportPolicy{}, la)

		if e != nil {
			t.Fatalf("%v", e)
		}

		defer tp.Close()

		if e := tp.Write(m1); e != nil {
			t.Fatalf("%v", e)
		}

		if e := tp.Flush(context.Background(), 60*time.Second); e != nil {
			t.Fatalf("%v", e)
		}

		time.Sleep(time.Second / 5)

		if e := tp.Write(m2); e != nil {
			t.Fatalf("%v", e)
		}

		if e := tp.Write(m1); e != nil {
			t.Fatalf("%v", e)
		}

		if e := tp.Flush(context.Background(), 60*time.Second); e != nil {
			t.Fatalf("%v", e)
		}
	}()

	c, e := l.Accept()

	if e != nil {
		t.Fatalf("%v", e)
	}

	var tp transport
	tp.Accept(&TransportPolicy{}, c.(*net.TCPConn))
	defer tp.Close()

	m3, e := tp.Peek(context.Background(), 60*time.Second)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !bytes.Equal(m1, m3) {
		t.Fatalf("%#v != %#v", m1, m3)
	}

	tp.Skip(m3)
	m4, e := tp.Peek(context.Background(), 60*time.Second)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !bytes.Equal(m2, m4) {
		t.Fatalf("%#v != %#v", m2, m4)
	}

	tp.Skip(m4)
	m5, e := tp.Peek(context.Background(), 60*time.Second)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !bytes.Equal(m1, m5) {
		t.Fatalf("%#v != %#v", m1, m5)
	}

	tp.Skip(m5)
	_, e = tp.Peek(context.Background(), 60*time.Second)

	if e == nil {
		t.Fatal("e is nil")
	}

	if e != io.EOF {
		t.Fatalf("%T is not EOF", e)
	}
}
