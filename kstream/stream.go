package kstream

// KStream represents a stream of records with key type K and value type V
type KStream[K, V any] struct {
	builder  *StreamsBuilder
	nodeName string
}

// Stream creates a KStream from a Kafka topic
func Stream[K, V any](b *StreamsBuilder, topic string) KStream[K, V] {
	name := b.nextName("SOURCE")
	b.topology.AddSource(name, topic)

	return KStream[K, V]{
		builder:  b,
		nodeName: name,
	}
}
