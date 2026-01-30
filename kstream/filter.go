package kstream

import (
	"context"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

// Filter returns a KStream containing only records that match the predicate
func Filter[K, V any](s KStream[K, V], predicate builtins.PredicateFunc[K, V]) KStream[K, V] {
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
func FilterNot[K, V any](s KStream[K, V], predicate builtins.PredicateFunc[K, V]) KStream[K, V] {
	return Filter(
		s, func(ctx context.Context, k K, v V) (bool, error) {
			ok, err := predicate(ctx, k, v)
			return !ok, err
		},
	)
}
