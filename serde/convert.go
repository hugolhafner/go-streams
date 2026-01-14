package serde

func ToDeserializer[T any](d Deserializer[T]) ErasedDeserializer {
	return deserializerAdapter[T]{typed: d}
}

func ToSerializer[T any](s Serializer[T]) ErasedSerializer {
	return serializerAdapter[T]{typed: s}
}

func ToErased[T any](s Serde[T]) ErasedSerde {
	return serdeAdapter[T]{typed: s}
}

func FromDeserializer[T any](fn func(topic string, data []byte) (T, error)) Deserializer[T] {
	return funcDeserializer[T]{fn: fn}
}

func FromSerializer[T any](fn func(topic string, value T) ([]byte, error)) Serializer[T] {
	return funcSerializer[T]{fn: fn}
}

type funcDeserializer[T any] struct {
	fn func(topic string, data []byte) (T, error)
}

func (f funcDeserializer[T]) Deserialize(topic string, data []byte) (T, error) {
	return f.fn(topic, data)
}

type funcSerializer[T any] struct {
	fn func(topic string, value T) ([]byte, error)
}

func (f funcSerializer[T]) Serialize(topic string, value T) ([]byte, error) {
	return f.fn(topic, value)
}
