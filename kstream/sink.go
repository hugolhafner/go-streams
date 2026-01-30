package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/serde"
)

// To writes records to a Kafka topic
// terminal operation
func To(s KStream[[]byte, []byte], topic string) {
	ToWithSerde(s, topic, serde.Bytes(), serde.Bytes())
}

func ToWithKeySerde[K any](s KStream[K, []byte], topic string, keySerde serde.Serde[K]) {
	ToWithSerde(s, topic, keySerde, serde.Bytes())
}

func ToWithValueSerde[V any](s KStream[[]byte, V], topic string, valueSerde serde.Serde[V]) {
	ToWithSerde(s, topic, serde.Bytes(), valueSerde)
}

// ToWithSerde writes records to a Kafka topic with specified serdes
// terminal operation
func ToWithSerde[K, V any](s KStream[K, V], topic string, keySerde serde.Serde[K], valueSerde serde.Serde[V]) {
	name := s.builder.nextName("SINK")
	ks := serde.ToUntypedSerialiser(keySerde)
	vs := serde.ToUntypedSerialiser(valueSerde)

	s.builder.topology.AddSink(name, topic, ks, vs, s.nodeName)
}

// ForEach applies an action to each record
// terminal operation
func ForEach[K, V any](s KStream[K, V], action builtins.ForEachFunc[K, V]) {
	name := s.builder.nextName("FOREACH")

	var supplier processor.Supplier[K, V, K, V] = func() processor.Processor[K, V, K, V] {
		return builtins.NewForEachProcessor(action)
	}

	s.builder.topology.AddProcessor(name, supplier.ToUntyped(), s.nodeName)
}
