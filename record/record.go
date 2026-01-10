package record

import (
	"time"
)

type Record[K, V any] struct {
	Key       K
	Value     V
	Timestamp time.Time
	Headers   map[string][]byte

	Topic     string
	Partition int32
	Offset    int64
}
