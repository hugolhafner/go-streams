package kafka

import (
	"strconv"
	"time"
)

// Header represents a single Kafka record header
// kafka needs to support multiple headers with duplicate keys
type Header struct {
	Key   string
	Value []byte
}

// HeaderValue returns the value of the first header matching the given key
// Returns (nil, false) if no header with that key exists
func HeaderValue(headers []Header, key string) ([]byte, bool) {
	for _, h := range headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

type ConsumerRecord struct {
	Key         []byte
	Value       []byte
	Headers     []Header
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Timestamp   time.Time
}

func (r ConsumerRecord) TopicPartition() TopicPartition {
	return TopicPartition{
		Topic:     r.Topic,
		Partition: r.Partition,
	}
}

func (r ConsumerRecord) Copy() ConsumerRecord {
	headersCopy := make([]Header, len(r.Headers))
	for i, h := range r.Headers {
		vCopy := make([]byte, len(h.Value))
		copy(vCopy, h.Value)
		headersCopy[i] = Header{Key: h.Key, Value: vCopy}
	}

	keyCopy := make([]byte, len(r.Key))
	copy(keyCopy, r.Key)

	valueCopy := make([]byte, len(r.Value))
	copy(valueCopy, r.Value)

	return ConsumerRecord{
		Key:         keyCopy,
		Value:       valueCopy,
		Headers:     headersCopy,
		Topic:       r.Topic,
		Partition:   r.Partition,
		Offset:      r.Offset,
		LeaderEpoch: r.LeaderEpoch,
		Timestamp:   r.Timestamp,
	}
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return tp.Topic + "-" + strconv.FormatInt(int64(tp.Partition), 10)
}

type Offset struct {
	LeaderEpoch int32
	Offset      int64
}
