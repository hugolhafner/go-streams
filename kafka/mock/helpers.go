package mockkafka

import (
	"time"

	"github.com/hugolhafner/go-streams/kafka"
)

// RecordBuilder provides a fluent interface for building ConsumerRecords.
type RecordBuilder struct {
	record kafka.ConsumerRecord
}

// Record creates a new RecordBuilder with the given key and value.
func Record(key, value string) *RecordBuilder {
	return &RecordBuilder{
		record: kafka.ConsumerRecord{
			Key:       []byte(key),
			Value:     []byte(value),
			Headers:   make(map[string][]byte),
			Timestamp: time.Now(),
		},
	}
}

// RecordBytes creates a new RecordBuilder with byte slices for key and value.
func RecordBytes(key, value []byte) *RecordBuilder {
	return &RecordBuilder{
		record: kafka.ConsumerRecord{
			Key:       key,
			Value:     value,
			Headers:   make(map[string][]byte),
			Timestamp: time.Now(),
		},
	}
}

// WithOffset sets the record's offset.
func (b *RecordBuilder) WithOffset(offset int64) *RecordBuilder {
	b.record.Offset = offset
	return b
}

// WithTimestamp sets the record's timestamp.
func (b *RecordBuilder) WithTimestamp(ts time.Time) *RecordBuilder {
	b.record.Timestamp = ts
	return b
}

// WithHeader adds a header to the record.
func (b *RecordBuilder) WithHeader(key string, value []byte) *RecordBuilder {
	if b.record.Headers == nil {
		b.record.Headers = make(map[string][]byte)
	}
	b.record.Headers[key] = value
	return b
}

// WithHeaders sets all headers on the record.
func (b *RecordBuilder) WithHeaders(headers map[string][]byte) *RecordBuilder {
	b.record.Headers = headers
	return b
}

// WithLeaderEpoch sets the leader epoch.
func (b *RecordBuilder) WithLeaderEpoch(epoch int32) *RecordBuilder {
	b.record.LeaderEpoch = epoch
	return b
}

// Build returns the constructed ConsumerRecord.
func (b *RecordBuilder) Build() kafka.ConsumerRecord {
	return b.record
}

// SimpleRecord creates a ConsumerRecord with just key and value as strings.
func SimpleRecord(key, value string) kafka.ConsumerRecord {
	return Record(key, value).Build()
}

// SimpleRecords creates multiple ConsumerRecords from key-value pairs.
// key, value argument pairs
func SimpleRecords(keyValuePairs ...string) []kafka.ConsumerRecord {
	if len(keyValuePairs)%2 != 0 {
		panic("SimpleRecords requires an even number of arguments (key-value pairs)")
	}

	records := make([]kafka.ConsumerRecord, 0, len(keyValuePairs)/2)
	for i := 0; i < len(keyValuePairs); i += 2 {
		records = append(records, SimpleRecord(keyValuePairs[i], keyValuePairs[i+1]))
	}
	return records
}
