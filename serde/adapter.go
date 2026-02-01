package serde

import "fmt"

type deserializerAdapter[T any] struct {
	typed Deserialiser[T]
}

func (a deserializerAdapter[T]) Deserialise(topic string, data []byte) (any, error) {
	return a.typed.Deserialise(topic, data)
}

type serializerAdapter[T any] struct {
	typed Serialiser[T]
}

func (a serializerAdapter[T]) Serialise(topic string, value any) ([]byte, error) {
	typed, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("serde: expected %T, got %T", *new(T), value)
	}
	return a.typed.Serialise(topic, typed)
}

type serdeAdapter[T any] struct {
	typed Serde[T]
}

func (a serdeAdapter[T]) Deserialise(topic string, data []byte) (any, error) {
	return a.typed.Deserialise(topic, data)
}

func (a serdeAdapter[T]) Serialise(topic string, value any) ([]byte, error) {
	typed, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("serde: expected %T, got %T", *new(T), value)
	}
	return a.typed.Serialise(topic, typed)
}

func ToUntypedDeserialser[T any](d Deserialiser[T]) UntypedDeserialiser {
	return deserializerAdapter[T]{typed: d}
}

func ToUntypedSerialiser[T any](s Serialiser[T]) UntypedSerialiser {
	return serializerAdapter[T]{typed: s}
}

func ToUntyped[T any](s Serde[T]) UntypedSerde {
	return serdeAdapter[T]{typed: s}
}
