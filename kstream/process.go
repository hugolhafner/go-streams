package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
)

// Process applies a custom processor
func Process[KIn, VIn, KOut, VOut any](
	s KStream[KIn, VIn],
	supplier processor.Supplier[KIn, VIn, KOut, VOut],
) KStream[KOut, VOut] {
	name := s.builder.nextName("PROCESS")
	s.builder.topology.AddProcessor(name, supplier.ToUntyped(), s.nodeName)

	return KStream[KOut, VOut]{
		builder:  s.builder,
		nodeName: name,
	}
}
