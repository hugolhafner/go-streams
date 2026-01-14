package serde

type Serde[T any] interface {
	Serializer[T]
	Deserializer[T]
}

type Serializer[T any] interface {
	Serialize(topic string, value T) ([]byte, error)
}

type Deserializer[T any] interface {
	Deserialize(topic string, data []byte) (T, error)
}

type ErasedDeserializer interface {
	Deserialize(topic string, data []byte) (any, error)
}

type ErasedSerializer interface {
	Serialize(topic string, value any) ([]byte, error)
}

type ErasedSerde interface {
	ErasedSerializer
	ErasedDeserializer
}
