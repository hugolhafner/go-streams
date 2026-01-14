package kstream

import (
	"github.com/hugolhafner/go-streams/serde"
)

// KStream represents a stream of records with key type K and value type V
type KStream[K, V any] struct {
	builder  *StreamsBuilder
	nodeName string
}

// Stream creates a KStream from a Kafka topic
func Stream(b *StreamsBuilder, topic string) KStream[[]byte, []byte] {
	return StreamWithSerde(b, topic, serde.Bytes(), serde.Bytes())
}

func StreamWithKeySerde[K any](
	b *StreamsBuilder, topic string, keySerde serde.Deserializer[K],
) KStream[K, []byte] {
	return StreamWithSerde(b, topic, keySerde, serde.Bytes())
}

func StreamWithValueSerde[V any](
	b *StreamsBuilder, topic string, valueSerde serde.Deserializer[V],
) KStream[[]byte, V] {
	return StreamWithSerde(b, topic, serde.Bytes(), valueSerde)
}

// StreamWithSerde creates a KStream from a Kafka topic with specified serdes
func StreamWithSerde[K, V any](
	b *StreamsBuilder, topic string, keySerde serde.Deserializer[K],
	valueSerde serde.Deserializer[V],
) KStream[K, V] {
	name := b.nextName("SOURCE")
	ks := serde.ToDeserializer(keySerde)
	vs := serde.ToDeserializer(valueSerde)

	b.topology.AddSource(name, topic, ks, vs)

	return KStream[K, V]{
		builder:  b,
		nodeName: name,
	}
}
