package zk

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

type RecordDerecordSerializationError struct {
	context string
}

func (self RecordDerecordSerializationError) Error() string {
	result := "zk: record deserialization"

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}

func serializeRecord(record interface{}, buffer *[]byte) {
	rvalue := reflect.ValueOf(record)

	if rvalue.Kind() == reflect.Ptr {
		if rvalue.IsNil() {
			panic(recordSerializationError{"record=<nil>"})
		}

		rvalue = rvalue.Elem()
	}

	if rvalue.Kind() != reflect.Struct {
		panic(recordSerializationError{fmt.Sprintf("recordType=%T", record)})
	}

	doSerializeRecord(rvalue, buffer)
}

func deserializeRecord(record interface{}, data []byte, dataOffset *int) error {
	lvalue := reflect.ValueOf(record)

	if lvalue.Kind() != reflect.Ptr {
		panic(RecordDerecordSerializationError{fmt.Sprintf("recordType=%T", record)})
	}

	if lvalue.IsNil() {
		panic(RecordDerecordSerializationError{"record=<nil>"})
	}

	lvalue = lvalue.Elem()

	if lvalue.Kind() != reflect.Struct {
		panic(RecordDerecordSerializationError{fmt.Sprintf("recordType=%T", record)})
	}

	return doDeserializeRecord(lvalue, data, dataOffset)
}

func doSerializeValue(rvalue reflect.Value, buffer *[]byte) {
	switch rvalue.Kind() {
	case reflect.Bool:
		doSerializeBoolean(rvalue, buffer)
	case reflect.Int32:
		doSerializeInt(rvalue, buffer)
	case reflect.Int64:
		doSerializeLong(rvalue, buffer)
	case reflect.String:
		doSerializeString(rvalue, buffer)
	case reflect.Slice:
		if rvalue.Type().Elem().Kind() == reflect.Uint8 {
			doSerializeBuffer(rvalue, buffer)
		} else {
			doSerializeVector(rvalue, buffer)
		}
	case reflect.Struct:
		doSerializeRecord(rvalue, buffer)
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
		panic(RecordDerecordSerializationError{fmt.Sprintf("valueType=%v", lvalue.Type())})
	}
}

func doSerializeBoolean(rvalue reflect.Value, buffer *[]byte) {
	var byte_ byte

	if rvalue.Bool() {
		byte_ = 1
	} else {
		byte_ = 0
	}

	*buffer = append(*buffer, byte_)
}

func doDeserializeBoolean(lvalue reflect.Value, data []byte, dataOffset *int) error {
	nextDataOffset := *dataOffset + 1

	if nextDataOffset > len(data) {
		return RecordDerecordSerializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetBool(data[*dataOffset] != 0)
	*dataOffset = nextDataOffset
	return nil
}

func doSerializeInt(rvalue reflect.Value, buffer *[]byte) {
	i := len(*buffer)
	*buffer = append(*buffer, 0, 0, 0, 0)
	binary.BigEndian.PutUint32((*buffer)[i:], uint32(rvalue.Int()))
}

func doDeserializeInt(lvalue reflect.Value, data []byte, dataOffset *int) error {
	nextDataOffset := *dataOffset + 4

	if nextDataOffset > len(data) {
		return RecordDerecordSerializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetInt(int64(binary.BigEndian.Uint32(data[*dataOffset:nextDataOffset])))
	*dataOffset = nextDataOffset
	return nil
}

func doSerializeLong(rvalue reflect.Value, buffer *[]byte) {
	i := len(*buffer)
	*buffer = append(*buffer, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64((*buffer)[i:], uint64(rvalue.Int()))
}

func doDeserializeLong(lvalue reflect.Value, data []byte, dataOffset *int) error {
	nextDataOffset := *dataOffset + 8

	if nextDataOffset > len(data) {
		return RecordDerecordSerializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetInt(int64(binary.BigEndian.Uint64(data[*dataOffset:nextDataOffset])))
	*dataOffset = nextDataOffset
	return nil
}

func serializeLen(l int, buffer *[]byte) {
	if l < math.MinInt32 || l > math.MaxInt32 {
		panic(recordSerializationError{fmt.Sprintf("len=%#v", l)})
	}

	i := len(*buffer)
	*buffer = append(*buffer, 0, 0, 0, 0)
	binary.BigEndian.PutUint32((*buffer)[i:], uint32(l))
}

func deserializeLen(data []byte, dataOffset *int) (int, error) {
	nextDataOffset := *dataOffset + 4

	if nextDataOffset > len(data) {
		return 0, RecordDerecordSerializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	l := int(int32(binary.BigEndian.Uint32(data[*dataOffset:nextDataOffset])))
	*dataOffset = nextDataOffset
	return l, nil
}

func doSerializeBuffer(rvalue reflect.Value, buffer *[]byte) {
	if rvalue.IsNil() {
		serializeLen(-1, buffer)
	} else {
		bytes := rvalue.Bytes()
		serializeLen(len(bytes), buffer)
		*buffer = append(*buffer, bytes...)
	}
}

func doDeserializeBuffer(lvalue reflect.Value, data []byte, dataOffset *int) error {
	numberOfBytes, e := deserializeLen(data, dataOffset)

	if e != nil {
		return e
	}

	if numberOfBytes < 0 {
		lvalue.SetBytes(nil)
	} else {
		nextDataOffset := *dataOffset + numberOfBytes

		if nextDataOffset > len(data) {
			return RecordDerecordSerializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
		}

		lvalue.SetBytes(data[*dataOffset:nextDataOffset])
		*dataOffset = nextDataOffset
	}

	return nil
}

func doSerializeString(rvalue reflect.Value, buffer *[]byte) {
	bytes := []byte(rvalue.String())
	serializeLen(len(bytes), buffer)
	*buffer = append(*buffer, bytes...)
}

func doDeserializeString(lvalue reflect.Value, data []byte, dataOffset *int) error {
	numberOfBytes, e := deserializeLen(data, dataOffset)

	if e != nil {
		return e
	}

	if numberOfBytes < 0 {
		return RecordDerecordSerializationError{"null string"}
	}

	nextDataOffset := *dataOffset + numberOfBytes

	if nextDataOffset > len(data) {
		return RecordDerecordSerializationError{fmt.Sprintf("dataSize=%#v, nextDataOffset=%#v", len(data), nextDataOffset)}
	}

	lvalue.SetString(string(data[*dataOffset:nextDataOffset]))
	*dataOffset = nextDataOffset
	return nil
}

func doSerializeVector(rvalue reflect.Value, buffer *[]byte) {
	if rvalue.IsNil() {
		serializeLen(-1, buffer)
	} else {
		numberOfElements := rvalue.Len()
		serializeLen(numberOfElements, buffer)

		for i := 0; i < numberOfElements; i++ {
			doSerializeValue(rvalue.Index(i), buffer)
		}
	}
}

func doDeserializeVector(lvalue reflect.Value, data []byte, dataOffset *int) error {
	numberOfElements, e := deserializeLen(data, dataOffset)

	if e != nil {
		return e
	}

	if numberOfElements < 0 {
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

func doSerializeRecord(rvalue reflect.Value, buffer *[]byte) {
	numberOfFields := rvalue.NumField()

	for i := 0; i < numberOfFields; i++ {
		doSerializeValue(rvalue.Field(i), buffer)
	}
}

func doDeserializeRecord(lvalue reflect.Value, data []byte, dataOffset *int) error {
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
