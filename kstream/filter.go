package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

// Filter returns a KStream containing only records that match the predicate
func Filter[K, V any](s KStream[K, V], predicate func(K, V) bool) KStream[K, V] {
	name := s.builder.nextName("FILTER")

	var typedSupplier processor.Supplier[K, V, K, V] = func() processor.Processor[K, V, K, V] {
		return builtins.NewFilterProcessor(predicate)
	}

	s.builder.topology.AddProcessor(name, typedSupplier.ToUntyped(), s.nodeName)

	return KStream[K, V]{
		builder:  s.builder,
		nodeName: name,
	}
}

// FilterNot returns a KStream containing only records that do NOT match the predicate
func FilterNot[K, V any](s KStream[K, V], predicate func(K, V) bool) KStream[K, V] {
	return Filter(s, func(k K, v V) bool {
		return !predicate(k, v)
	})
}
