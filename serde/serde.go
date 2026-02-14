package serde

// Serde is an interface that combines both Serialiser and Deserialiser for a given type T.
type Serde[T any] interface {
	Serialiser[T]
	Deserialiser[T]
}

// Serialiser is an interface that defines a method for serialising a value of type T to a byte slice, given a topic.
type Serialiser[T any] interface {
	Serialise(topic string, value T) ([]byte, error)
}

// Deserialiser is an interface that defines a method for deserialising a byte slice to a value of type T, given a topic.
type Deserialiser[T any] interface {
	Deserialise(topic string, data []byte) (T, error)
}

// UntypedDeserialiser is an interface that defines a method for deserialising a byte slice to an untyped value, given a topic.
// It is used internally to allow deserialisers to be used without knowing the types of the values.
type UntypedDeserialiser interface {
	Deserialise(topic string, data []byte) (any, error)
}

// UntypedSerialiser is an interface that defines a method for serialising an untyped value to a byte slice, given a topic.
// It is used internally to allow serialisers to be used without knowing the types of the values.
type UntypedSerialiser interface {
	Serialise(topic string, value any) ([]byte, error)
}

// UntypedSerde is an interface that combines both UntypedSerialiser and UntypedDeserialiser for untyped values.
type UntypedSerde interface {
	UntypedSerialiser
	UntypedDeserialiser
}
