package serde

import "encoding/json"

type jsonSerde[T any] struct{}

func JSON[T any]() Serde[T] {
	return jsonSerde[T]{}
}

func (s jsonSerde[T]) Serialize(topic string, value T) ([]byte, error) {
	return json.Marshal(value)
}

func (s jsonSerde[T]) Deserialize(topic string, data []byte) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	return result, err
}
