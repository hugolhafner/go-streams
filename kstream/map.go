package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

// Map transforms each record's key and value
func Map[K1, V1, K2, V2 any](
	s KStream[K1, V1],
	mapper func(K1, V1) (K2, V2),
) KStream[K2, V2] {
	name := s.builder.nextName("MAP")

	supplier := processor.ToSupplier(func() processor.Processor[K1, V1, K2, V2] {
		return builtins.NewMapProcessor(mapper)
	})

	s.builder.topology.AddProcessor(name, supplier, s.nodeName)

	return KStream[K2, V2]{
		builder:  s.builder,
		nodeName: name,
	}
}

// MapValues transforms each record's value, keeping the key unchanged
func MapValues[K, V1, V2 any](
	s KStream[K, V1],
	mapper func(V1) V2,
) KStream[K, V2] {
	return Map(s, func(k K, v V1) (K, V2) {
		return k, mapper(v)
	})
}

// MapKeys transforms each record's key, keeping the value unchanged
func MapKeys[K1, K2, V any](
	s KStream[K1, V],
	mapper func(K1) K2,
) KStream[K2, V] {
	return Map(s, func(k K1, v V) (K2, V) {
		return mapper(k), v
	})
}
