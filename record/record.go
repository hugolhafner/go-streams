package record

import (
	"time"

	"github.com/hugolhafner/go-streams/kafka"
)

type Metadata struct {
	Timestamp time.Time
	Headers   []kafka.Header

	Topic     string
	Partition int32
	Offset    int64
}

type Record[K, V any] struct {
	Key   K
	Value V
	Metadata
}

type UntypedRecord struct {
	Key   any
	Value any
	Metadata
}
