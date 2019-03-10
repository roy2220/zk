package zk

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/let-z-go/toolkit/byte_stream"
)

type RecordDeserializationError struct {
	context string
}

func (self RecordDeserializationError) Error() string {
	result := "zk: record deserialization"

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}

type serializer interface {
	Serialize(*byte_stream.ByteStream)
}

type deserializer interface {
	Deserialize([]byte, *int) error
}

func serializeRecord(record interface{}, byteStream *byte_stream.ByteStream) {
	rvalue := reflect.ValueOf(record)

	if rvalue.Kind() == reflect.Ptr {
		rvalue = rvalue.Elem()
	}

	if rvalue.Kind() != reflect.Struct {
		panic(recordSerializationError{fmt.Sprintf("recordType=%T", record)})
	}

	doSerializeRecord(rvalue, byteStream)
}

func deserializeRecord(record interface{}, data []byte, dataOffset *int) error {
	temp := reflect.ValueOf(record)

	if temp.Kind() != reflect.Ptr {
		panic(RecordDeserializationError{fmt.Sprintf("recordType=%T", record)})
	}

	lvalue := temp.Elem()

	if lvalue.Kind() != reflect.Struct {
		panic(RecordDeserializationError{fmt.Sprintf("recordType=%T", record)})
	}

	return doDeserializeRecord(lvalue, data, dataOffset)
}

func doSerializeValue(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	switch rvalue.Kind() {
	case reflect.Bool:
		doSerializeBoolean(rvalue, byteStream)
	case reflect.Int32:
		doSerializeInt(rvalue, byteStream)
	case reflect.Int64:
		doSerializeLong(rvalue, byteStream)
	case reflect.String:
		doSerializeString(rvalue, byteStream)
	case reflect.Slice:
		if rvalue.Type().Elem().Kind() == reflect.Uint8 {
			doSerializeBuffer(rvalue, byteStream)
		} else {
			doSerializeVector(rvalue, byteStream)
		}
	case reflect.Struct:
		doSerializeRecord(rvalue, byteStream)
	default:
		panic(recordSerializationError{fmt.Sprintf("valueType=%v", rvalue.Type())})
	}
}

func doDeserializeValue(lvalue reflect.Value, data []byte, dataOffset *int) error {
	switch lvalue.Kind() {
	case reflect.Bool:
		return doDeserializeBoolean(lvalue, data, dataOffset)
	case reflect.Int32:
		return doDeserializeInt(lvalue, data, dataOffset)
	case reflect.Int64:
		return doDeserializeLong(lvalue, data, dataOffset)
	case reflect.String:
		return doDeserializeString(lvalue, data, dataOffset)
	case reflect.Slice:
		if lvalue.Type().Elem().Kind() == reflect.Uint8 {
			return doDeserializeBuffer(lvalue, data, dataOffset)
		} else {
			return doDeserializeVector(lvalue, data, dataOffset)
		}
	case reflect.Struct:
		return doDeserializeRecord(lvalue, data, dataOffset)
	default:
		panic(RecordDeserializationError{fmt.Sprintf("valueType=%v", lvalue.Type())})
	}
}

func doSerializeBoolean(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	byteStream.WriteDirectly(1, func(buffer []byte) error {
		if rvalue.Bool() {
			buffer[0] = 1
		} else {
			buffer[0] = 0
		}

		return nil
	})
}

func doDeserializeBoolean(lvalue reflect.Value, data []byte, dataOffset *int) error {
	nextDataOffset := *dataOffset + 1

	if nextDataOffset > len(data) {
		return RecordDeserializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetBool(data[*dataOffset] != 0)
	*dataOffset = nextDataOffset
	return nil
}

func doSerializeInt(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	byteStream.WriteDirectly(4, func(buffer []byte) error {
		binary.BigEndian.PutUint32(buffer, uint32(rvalue.Int()))
		return nil
	})
}

func doDeserializeInt(lvalue reflect.Value, data []byte, dataOffset *int) error {
	nextDataOffset := *dataOffset + 4

	if nextDataOffset > len(data) {
		return RecordDeserializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetInt(int64(binary.BigEndian.Uint32(data[*dataOffset:nextDataOffset])))
	*dataOffset = nextDataOffset
	return nil
}

func doSerializeLong(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	byteStream.WriteDirectly(8, func(buffer []byte) error {
		binary.BigEndian.PutUint64(buffer, uint64(rvalue.Int()))
		return nil
	})
}

func doDeserializeLong(lvalue reflect.Value, data []byte, dataOffset *int) error {
	nextDataOffset := *dataOffset + 8

	if nextDataOffset > len(data) {
		return RecordDeserializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetInt(int64(binary.BigEndian.Uint64(data[*dataOffset:nextDataOffset])))
	*dataOffset = nextDataOffset
	return nil
}

func serializeLen(l int, byteStream *byte_stream.ByteStream) {
	if l < math.MinInt32 || l > math.MaxInt32 {
		panic(recordSerializationError{fmt.Sprintf("len=%#v", l)})
	}

	byteStream.WriteDirectly(4, func(buffer []byte) error {
		binary.BigEndian.PutUint32(buffer, uint32(l))
		return nil
	})
}

func deserializeLen(data []byte, dataOffset *int) (int, error) {
	nextDataOffset := *dataOffset + 4

	if nextDataOffset > len(data) {
		return 0, RecordDeserializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	l := int(int32(binary.BigEndian.Uint32(data[*dataOffset:nextDataOffset])))
	*dataOffset = nextDataOffset
	return l, nil
}

func doSerializeBuffer(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	bytes := rvalue.Bytes()
	serializeLen(len(bytes), byteStream)
	byteStream.Write(bytes)
}

func doDeserializeBuffer(lvalue reflect.Value, data []byte, dataOffset *int) error {
	numberOfBytes, e := deserializeLen(data, dataOffset)

	if e != nil {
		return e
	}

	if numberOfBytes < 1 {
		lvalue.SetBytes(nil)
	} else {
		nextDataOffset := *dataOffset + numberOfBytes

		if nextDataOffset > len(data) {
			return RecordDeserializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
		}

		lvalue.SetBytes(append([]byte(nil), data[*dataOffset:nextDataOffset]...))
		*dataOffset = nextDataOffset
	}

	return nil
}

func doSerializeString(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	bytes := []byte(rvalue.String())
	serializeLen(len(bytes), byteStream)
	byteStream.Write(bytes)
}

func doDeserializeString(lvalue reflect.Value, data []byte, dataOffset *int) error {
	numberOfBytes, e := deserializeLen(data, dataOffset)

	if e != nil {
		return e
	}

	if numberOfBytes < 1 {
		lvalue.SetString("")
	} else {
		nextDataOffset := *dataOffset + numberOfBytes

		if nextDataOffset > len(data) {
			return RecordDeserializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
		}

		lvalue.SetString(string(data[*dataOffset:nextDataOffset]))
		*dataOffset = nextDataOffset
	}

	return nil
}

func doSerializeVector(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	numberOfElements := rvalue.Len()
	serializeLen(numberOfElements, byteStream)

	for i := 0; i < numberOfElements; i++ {
		doSerializeValue(rvalue.Index(i), byteStream)
	}
}

func doDeserializeVector(lvalue reflect.Value, data []byte, dataOffset *int) error {
	numberOfElements, e := deserializeLen(data, dataOffset)

	if e != nil {
		return e
	}

	if numberOfElements < 1 {
		lvalue.Set(reflect.Zero(lvalue.Type()))
	} else {
		lvalue.Set(reflect.MakeSlice(lvalue.Type(), numberOfElements, numberOfElements))

		for i := 0; i < numberOfElements; i++ {
			if e := doDeserializeValue(lvalue.Index(i), data, dataOffset); e != nil {
				return e
			}
		}
	}

	return nil
}

func doSerializeRecord(rvalue reflect.Value, byteStream *byte_stream.ByteStream) {
	if rvalue.CanAddr() {
		if serializer_, ok := rvalue.Addr().Interface().(serializer); ok {
			serializer_.Serialize(byteStream)
			return
		}
	} else {
		if serializer_, ok := rvalue.Interface().(serializer); ok {
			serializer_.Serialize(byteStream)
			return
		}
	}

	numberOfFields := rvalue.NumField()

	for i := 0; i < numberOfFields; i++ {
		doSerializeValue(rvalue.Field(i), byteStream)
	}
}

func doDeserializeRecord(lvalue reflect.Value, data []byte, dataOffset *int) error {
	if deserializer_, ok := lvalue.Addr().Interface().(deserializer); ok {
		return deserializer_.Deserialize(data, dataOffset)
	}

	numberOfFields := lvalue.NumField()

	for i := 0; i < numberOfFields; i++ {
		if e := doDeserializeValue(lvalue.Field(i), data, dataOffset); e != nil {
			return e
		}
	}

	return nil
}

type recordSerializationError struct {
	context string
}

func (self recordSerializationError) Error() string {
	result := "zk: record serialization"

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}
