package kafka

import (
	"context"
)

type Client interface {
	Producer
	Consumer

	Ping(ctx context.Context) error
}

type Producer interface {
	Send(ctx context.Context, topic string, key, value []byte, headers []Header) error
	Flush(ctx context.Context) error
	Close()
}

type Consumer interface {
	Subscribe(topics []string, rebalanceCb RebalanceCallback) error
	Poll(ctx context.Context) ([]ConsumerRecord, error)
	Commit(ctx context.Context) error
	MarkRecords(records ...ConsumerRecord)
	PausePartitions(partitions ...TopicPartition)
	ResumePartitions(partitions ...TopicPartition)
	Close()
}

type RebalanceCallback interface {
	OnAssigned(partitions []TopicPartition)
	OnRevoked(partitions []TopicPartition)
}
