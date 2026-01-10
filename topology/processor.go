package topology

type ProcessorNode interface {
	Init(ctx ProcessorContext)
	Process(record any)
	Close() error
}

type ProcessorSupplier func() ProcessorNode

type ProcessorContext interface {
	Forward(record any)
	ForwardTo(childName string, record any)
}
