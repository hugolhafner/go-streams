package serde

import "encoding/json"

type jsonSerde[T any] struct{}

// JSON returns a Serde that uses JSON for serialisation and deserialisation.
func JSON[T any]() Serde[T] {
	return jsonSerde[T]{}
}

func (s jsonSerde[T]) Serialise(_ string, value T) ([]byte, error) {
	return json.Marshal(value)
}

func (s jsonSerde[T]) Deserialise(_ string, data []byte) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	return result, err
}
