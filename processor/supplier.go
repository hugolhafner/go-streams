package processor

import (
	"github.com/hugolhafner/go-streams/topology"
)

type Supplier[KIn, VIn, KOut, VOut any] func() Processor[KIn, VIn, KOut, VOut]

func ToSupplier[KIn, VIn, KOut, VOut any](
	factory func() Processor[KIn, VIn, KOut, VOut],
) topology.ProcessorSupplier {
	return func() topology.ProcessorNode {
		return &processorAdapter[KIn, VIn, KOut, VOut]{
			typed: factory(),
		}
	}
}
