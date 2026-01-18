package topology

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/serde"
)

type ProcessorNode interface {
	Node
	Supplier() processor.UntypedSupplier
}

type SourceNode interface {
	Node
	Topic() string
	KeySerde() serde.UntypedDeserialiser
	ValueSerde() serde.UntypedDeserialiser
}

type SinkNode interface {
	Node
	Topic() string
	KeySerde() serde.UntypedSerialiser
	ValueSerde() serde.UntypedSerialiser
}
