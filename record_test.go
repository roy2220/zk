package zk

import (
	"bytes"
	"runtime/debug"
	"testing"

	"github.com/let-z-go/toolkit/byte_stream"
)

func TestSerializeAnddeserializeRecord1(t *testing.T) {
	type Foo struct {
		B  bool
		I  int32
		L  int64
		Bf []byte
		S  string
	}

	inFoo := Foo{B: true, I: -99, L: 0xFFFFFFFFF, Bf: []byte("\xde\xad\xbe\xef"), S: "testtest"}
	var bs1 byte_stream.ByteStream
	serializeRecord(inFoo, &bs1)
	var bs2 byte_stream.ByteStream
	serializeRecord(&inFoo, &bs2)

	if !bytes.Equal(bs1.GetData(), bs2.GetData()) {
		t.Error("buffer1 != buffer2")
	}

	dataOffset := 0
	outFoo := Foo{}
	e := deserializeRecord(&outFoo, bs1.GetData(), &dataOffset)
	bs1.Skip(dataOffset)

	if sz := bs1.GetDataSize(); sz != 0 {
		t.Errorf("%#v", sz)
	}

	if e != nil {
		t.Fatalf("%v", e)
	}

	if outFoo.B != inFoo.B {
		t.Errorf("%#v != %#v", outFoo.B, inFoo.B)
	}

	if outFoo.I != inFoo.I {
		t.Errorf("%#v != %#v", outFoo.I, inFoo.I)
	}

	if outFoo.L != inFoo.L {
		t.Errorf("%#v != %#v", outFoo.L, inFoo.L)
	}

	if !bytes.Equal(outFoo.Bf, inFoo.Bf) {
		t.Errorf("%#v != %#v", outFoo.Bf, inFoo.Bf)
	}

	if outFoo.S != inFoo.S {
		t.Errorf("%#v != %#v", outFoo.S, inFoo.S)
	}
}

func TestSerializeAnddeserializeRecord2(t *testing.T) {
	type Foo struct {
		Vec []int32
	}
	type Bar struct {
		Fs []Foo
	}

	inBar := Bar{Fs: []Foo{
		Foo{Vec: []int32{1, 2, 3}},
		Foo{Vec: []int32{4, 5, 6}},
		Foo{Vec: []int32{7, 8, 9}},
	}}

	var bs1 byte_stream.ByteStream
	serializeRecord(inBar, &bs1)
	var bs2 byte_stream.ByteStream
	serializeRecord(&inBar, &bs2)

	if !bytes.Equal(bs1.GetData(), bs2.GetData()) {
		t.Error("buffer1 != buffer2")
	}

	dataOffset := 0
	outBar := Bar{}
	e := deserializeRecord(&outBar, bs1.GetData(), &dataOffset)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if len(outBar.Fs) != len(inBar.Fs) {
		t.Errorf("%#v != %#v", len(outBar.Fs), len(inBar.Fs))
	}

	for i := 0; i < 3; i++ {
		inF := inBar.Fs[i]
		outF := outBar.Fs[i]

		if len(outF.Vec) != len(inF.Vec) {
			t.Errorf("%#v != %#v", len(outF.Vec), len(inF.Vec))
		}

		for j := 0; j < 3; j++ {
			if outF.Vec[j] != inF.Vec[j] {
				t.Errorf("%#v != %#v", outF.Vec[j], inF.Vec[j])
			}
		}
	}
}

func TestSerializeRecord(t *testing.T) {
	type Foo struct {
		I int
	}

	defer func() {
		r := recover()

		if r == nil {
			t.Fatal("The code did not panic")
		}

		switch r.(type) {
		case *recordSerializationError:
		default:
			t.Errorf("%#v", r)
			debug.PrintStack()
		}
	}()

	inFoo := Foo{}
	var bs byte_stream.ByteStream
	serializeRecord(inFoo, &bs)
}

func TestDeserializeRecord(t *testing.T) {
	defer func() {
		r := recover()

		if r == nil {
			t.Fatal("The code did not panic")
		}

		switch r.(type) {
		case *RecordDeserializationError:
		default:
			t.Errorf("%#v", r)
			debug.PrintStack()
		}
	}()

	type Foo struct {
		I int32
	}

	outFoo := Foo{}
	data := []byte("")
	dataOffset := 0
	e := deserializeRecord(&outFoo, data, &dataOffset)

	if e == nil {
		t.Fatal("e is null")
	}

	deserializeRecord(outFoo, data, &dataOffset)
}

var fooXS = false
var fooXD = false

type fooX struct {
	D bool
}

func (f fooX) Serialize(*byte_stream.ByteStream) {
	fooXS = true
	return
}

func (f *fooX) Deserialize([]byte, *int) error {
	fooXD = true
	return nil
}

func TestSerializeAnddeserializeRecord3(t *testing.T) {
	var bs byte_stream.ByteStream
	var fx fooX
	serializeRecord(&fx, &bs)

	if !fooXS {
		t.Error()
	}

	dataOffset := 0
	e := deserializeRecord(&fx, bs.GetData(), &dataOffset)

	if e != nil {
		t.Fatalf("%v", e)
	}

	if !fooXD {
		t.Error()
	}
}
