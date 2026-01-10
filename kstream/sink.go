package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

// To writes records to a Kafka topic
// terminal operation
func To[K, V any](s KStream[K, V], topic string) {
	name := s.builder.nextName("SINK")
	s.builder.topology.AddSink(name, topic, s.nodeName)
}

// ForEach applies an action to each record
// terminal operation
func ForEach[K, V any](s KStream[K, V], action func(K, V)) {
	name := s.builder.nextName("FOREACH")

	supplier := processor.ToSupplier(func() processor.Processor[K, V, K, V] {
		return builtins.NewForEachProcessor(action)
	})

	s.builder.topology.AddProcessor(name, supplier, s.nodeName)
}
