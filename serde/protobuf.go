package serde

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

type protobufSerde[T proto.Message] struct{}

func Protobuf[T proto.Message]() Serde[T] {
	return protobufSerde[T]{}
}

func (s protobufSerde[T]) Serialise(topic string, value T) ([]byte, error) {
	data, err := proto.Marshal(value)
	return data, err
}

func (s protobufSerde[T]) Deserialise(topic string, data []byte) (T, error) {
	var zero T

	typ := reflect.TypeOf((*T)(nil)).Elem()
	if typ.Kind() != reflect.Ptr {
		return zero, fmt.Errorf("deserialize requires a pointer type, got %s", typ)
	}

	result := reflect.New(typ.Elem()).Interface().(T)
	if err := proto.Unmarshal(data, result); err != nil {
		return zero, err
	}
	return result, nil
}
