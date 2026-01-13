package serde

import "encoding/json"

type jsonSerde[T any] struct{}

func JSON[T any]() Serde[T] {
	return jsonSerde[T]{}
}

func (s jsonSerde[T]) Serialise(topic string, value T) ([]byte, error) {
	return json.Marshal(value)
}

func (s jsonSerde[T]) Deserialise(topic string, data []byte) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	return result, err
}
