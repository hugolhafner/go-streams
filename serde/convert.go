package serde

func ToUntypedDeserialser[T any](d Deserialiser[T]) UntypedDeserialiser {
	return deserializerAdapter[T]{typed: d}
}

func ToUntypedSerialiser[T any](s Serialiser[T]) UntypedSerialiser {
	return serializerAdapter[T]{typed: s}
}

func ToUntyped[T any](s Serde[T]) UntypedSerde {
	return serdeAdapter[T]{typed: s}
}
