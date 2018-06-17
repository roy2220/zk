package zk

import (
	"bytes"
	"runtime/debug"
	"testing"
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
	buffer1 := []byte(nil)
	serializeRecord(inFoo, &buffer1)
	buffer2 := []byte(nil)
	serializeRecord(&inFoo, &buffer2)

	if !bytes.Equal(buffer1, buffer2) {
		t.Error("buffer1 != buffer2")
	}

	dataOffset := 0
	outFoo := Foo{}
	e := deserializeRecord(&outFoo, buffer1, &dataOffset)

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

	if dataOffset != len(buffer1) {
		t.Errorf("%#v != %#v", dataOffset, len(buffer1))
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

	buffer1 := []byte(nil)
	serializeRecord(inBar, &buffer1)
	buffer2 := []byte(nil)
	serializeRecord(&inBar, &buffer2)

	if !bytes.Equal(buffer1, buffer2) {
		t.Error("buffer1 != buffer2")
	}

	dataOffset := 0
	outBar := Bar{}
	e := deserializeRecord(&outBar, buffer1, &dataOffset)

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
		case recordSerializationError:
		default:
			t.Errorf("%#v", r)
			debug.PrintStack()
		}
	}()

	inFoo := Foo{}
	buffer := []byte(nil)
	serializeRecord(inFoo, &buffer)
}

func TestDeserializeRecord(t *testing.T) {
	defer func() {
		r := recover()

		if r == nil {
			t.Fatal("The code did not panic")
		}

		switch r.(type) {
		case RecordDerecordSerializationError:
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
