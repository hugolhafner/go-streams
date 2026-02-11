package serde

import (
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
	result := reflect.New(reflect.TypeOf(*new(T)).Elem()).Interface().(T)
	err := proto.Unmarshal(data, result)
	return result, err
}
