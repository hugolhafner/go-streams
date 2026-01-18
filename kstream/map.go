package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

// Map transforms each record's key and value
func Map[KIn, VIn, KOut, VOut any](
	s KStream[KIn, VIn],
	mapper func(KIn, VIn) (KOut, VOut),
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
	mapper func(VIn) VOut,
) KStream[K, VOut] {
	return Map(
		s, func(k K, v VIn) (K, VOut) {
			return k, mapper(v)
		},
	)
}

// MapKeys transforms each record's key, keeping the value unchanged
func MapKeys[KIn, KOut, V any](
	s KStream[KIn, V],
	mapper func(KIn) KOut,
) KStream[KOut, V] {
	return Map(
		s, func(k KIn, v V) (KOut, V) {
			return mapper(k), v
		},
	)
}
