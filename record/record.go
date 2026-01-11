package record

import (
	"time"
)

type Metadata struct {
	Timestamp time.Time
	Headers   map[string][]byte

	Topic     string
	Partition int32
	Offset    int64
}

type Record[K, V any] struct {
	Key   K
	Value V
	Metadata
}

type ErasedRecord struct {
	Key   any
	Value any
	Metadata
}
