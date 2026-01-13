package serde

type Serde[T any] interface {
	Serialiser[T]
	Deserialiser[T]
}

type Serialiser[T any] interface {
	Serialise(topic string, value T) ([]byte, error)
}

type Deserialiser[T any] interface {
	Deserialise(topic string, data []byte) (T, error)
}

type UntypedDeserialiser interface {
	Deserialize(topic string, data []byte) (any, error)
}

type UntypedSerialiser interface {
	Serialize(topic string, value any) ([]byte, error)
}

type UntypedSerde interface {
	UntypedSerialiser
	UntypedDeserialiser
}
