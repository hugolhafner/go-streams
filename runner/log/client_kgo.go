package log

import (
	"context"
	"time"
)

var _ Client = (*KgoClient)(nil)

type KgoClient struct {
}

func NewKgoClient() *KgoClient {
	return &KgoClient{}
}

func (k KgoClient) Send(ctx context.Context, topic string, key, value []byte, headers map[string][]byte) error {
	// TODO implement me
	panic("implement me")
}

func (k KgoClient) Flush(timeout time.Duration) error {
	// TODO implement me
	panic("implement me")
}

func (k KgoClient) Close() error {
	// TODO implement me
	panic("implement me")
}

func (k KgoClient) Subscribe(topics []string, rebalanceCb RebalanceCallback) error {
	// TODO implement me
	panic("implement me")
}

func (k KgoClient) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerRecord, error) {
	// TODO implement me
	panic("implement me")
}

func (k KgoClient) Commit(offsets map[TopicPartition]int64) error {
	// TODO implement me
	panic("implement me")
}

func (k KgoClient) Ping(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}
