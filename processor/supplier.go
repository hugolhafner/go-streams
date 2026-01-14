package processor

type Supplier[KIn, VIn, KOut, VOut any] func() Processor[KIn, VIn, KOut, VOut]
type UntypedSupplier func() UntypedProcessor

func (s Supplier[KIn, VIn, KOut, VOut]) ToUntyped() UntypedSupplier {
	return func() UntypedProcessor {
		return &untypedProcessorAdapter[KIn, VIn, KOut, VOut]{
			typed: s(),
		}
	}
}
