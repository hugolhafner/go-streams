package kstream

import (
	"context"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

type MapValuesFunc[VIn, VOut any] func(context.Context, VIn) (VOut, error)
type MapKeysFunc[KIn, KOut any] func(context.Context, KIn) (KOut, error)

// Map transforms each record's key and value
func Map[KIn, VIn, KOut, VOut any](
	s KStream[KIn, VIn],
	mapper builtins.MapFunc[KIn, VIn, KOut, VOut],
) KStream[KOut, VOut] {
	name := s.builder.nextName("MAP")

	var supplier processor.Supplier[KIn, VIn, KOut, VOut] = func() processor.Processor[KIn, VIn, KOut, VOut] {
		return builtins.NewMapProcessor(mapper)
	}

	s.builder.topology.AddProcessor(name, supplier.ToUntyped(), s.nodeName)

	return KStream[KOut, VOut]{
		builder:  s.builder,
		nodeName: name,
	}
}

// MapValues transforms each record's value, keeping the key unchanged
func MapValues[K, VIn, VOut any](
	s KStream[K, VIn],
	mapper MapValuesFunc[VIn, VOut],
) KStream[K, VOut] {
	return Map(
		s, func(ctx context.Context, k K, v VIn) (K, VOut, error) {
			vOut, err := mapper(ctx, v)
			return k, vOut, err
		},
	)
}

// MapKeys transforms each record's key, keeping the value unchanged
func MapKeys[KIn, KOut, V any](
	s KStream[KIn, V],
	mapper MapKeysFunc[KIn, KOut],
) KStream[KOut, V] {
	return Map(
		s, func(ctx context.Context, k KIn, v V) (KOut, V, error) {
			kOut, err := mapper(ctx, k)
			return kOut, v, err
		},
	)
}
