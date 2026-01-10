package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
)

// Process applies a custom processor
func Process[K1, V1, K2, V2 any](
	s KStream[K1, V1],
	supplier func() processor.Processor[K1, V1, K2, V2],
) KStream[K2, V2] {
	name := s.builder.nextName("PROCESS")

	adapted := processor.ToSupplier(supplier)
	s.builder.topology.AddProcessor(name, adapted, s.nodeName)

	return KStream[K2, V2]{
		builder:  s.builder,
		nodeName: name,
	}
}
