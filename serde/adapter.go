package serde

import "fmt"

type deserializerAdapter[T any] struct {
	typed Deserializer[T]
}

func (a deserializerAdapter[T]) Deserialize(topic string, data []byte) (any, error) {
	return a.typed.Deserialize(topic, data)
}

type serializerAdapter[T any] struct {
	typed Serializer[T]
}

func (a serializerAdapter[T]) Serialize(topic string, value any) ([]byte, error) {
	typed, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("serde: expected %T, got %T", *new(T), value)
	}
	return a.typed.Serialize(topic, typed)
}

type serdeAdapter[T any] struct {
	typed Serde[T]
}

func (a serdeAdapter[T]) Deserialize(topic string, data []byte) (any, error) {
	return a.typed.Deserialize(topic, data)
}

func (a serdeAdapter[T]) Serialize(topic string, value any) ([]byte, error) {
	typed, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("serde: expected %T, got %T", *new(T), value)
	}
	return a.typed.Serialize(topic, typed)
}
