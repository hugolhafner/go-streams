package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hugolhafner/go-streams/logger"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ Client = (*KgoClient)(nil)

type KgoClientConfig struct {
	BootstrapServers   []string
	GroupID            string
	SessionTimeout     time.Duration
	HeartbeatInterval  time.Duration
	AutoCommitInterval time.Duration
	MaxPollRecords     int
	PollTimeout        time.Duration

	Logger logger.Logger
}

func defaultConfig() KgoClientConfig {
	return KgoClientConfig{
		BootstrapServers:   []string{"localhost:9092"},
		GroupID:            "default-group",
		SessionTimeout:     45 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		PollTimeout:        3 * time.Second,
		AutoCommitInterval: 5 * time.Second,
		MaxPollRecords:     10,
		Logger:             logger.NewNoopLogger(),
	}
}

type KgoOption func(*KgoClientConfig)

func WithBootstrapServers(servers []string) KgoOption {
	return func(cfg *KgoClientConfig) {
		cfg.BootstrapServers = servers
	}
}

func WithGroupID(id string) KgoOption {
	return func(cfg *KgoClientConfig) {
		cfg.GroupID = id
	}
}

func WithLogger(l logger.Logger) KgoOption {
	return func(cfg *KgoClientConfig) {
		cfg.Logger = l.
			With("client", "kgo")
	}
}

type KgoClient struct {
	client *kgo.Client
	config KgoClientConfig

	mu          sync.RWMutex
	subscribed  bool
	rebalanceCb RebalanceCallback
	topics      []string

	logger logger.Logger
}

func NewKgoClient(opts ...KgoOption) (*KgoClient, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	kc := &KgoClient{config: cfg, logger: cfg.Logger}

	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.OnPartitionsAssigned(kc.onAssigned),
		kgo.OnPartitionsRevoked(kc.onRevoked),
		kgo.WithLogger(newKgoLogger(kc.logger)),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.HeartbeatInterval(cfg.HeartbeatInterval),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(cfg.AutoCommitInterval),
		// TODO: Metrics support
	}

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("create kgo client: %w", err)
	}

	kc.client = client

	return kc, nil
}

func (k *KgoClient) onAssigned(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
	k.mu.RLock()
	cb := k.rebalanceCb
	k.mu.RUnlock()

	if cb == nil {
		return
	}

	partitions := mapToTopicPartitions(assigned)
	cb.OnAssigned(partitions)
}

func (k *KgoClient) onRevoked(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
	k.mu.RLock()
	cb := k.rebalanceCb
	k.mu.RUnlock()

	if cb == nil {
		return
	}

	partitions := mapToTopicPartitions(revoked)
	cb.OnRevoked(partitions)
}

func (k *KgoClient) Subscribe(topics []string, rebalanceCb RebalanceCallback) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.subscribed {
		return fmt.Errorf("already subscribed")
	}

	k.rebalanceCb = rebalanceCb
	k.topics = topics
	k.client.AddConsumeTopics(topics...)
	k.subscribed = true

	return nil
}

func (k *KgoClient) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, k.config.PollTimeout)
	defer cancel()

	fetches := k.client.PollRecords(ctx, k.config.MaxPollRecords)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, err := range errs {
			if !errors.Is(err.Err, context.DeadlineExceeded) && !errors.Is(err.Err, context.Canceled) {
				return nil, fmt.Errorf("poll: %w", err.Err)
			}
		}
	}

	return convertRecords(fetches.Records()), nil
}

func (k *KgoClient) MarkRecords(records ...ConsumerRecord) {
	k.client.MarkCommitRecords(convertRecordsToKgo(records)...)
}

func (k *KgoClient) Commit(ctx context.Context) error {
	return k.client.CommitMarkedOffsets(ctx)
}

func (k *KgoClient) Send(ctx context.Context, topic string, key, value []byte, headers []Header) error {
	record := &kgo.Record{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: convertToKgoHeaders(headers),
	}

	k.logger.Info("Sending record", "topic", topic, "key", string(key), "value", string(value))

	results := k.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

func (k *KgoClient) Flush(ctx context.Context) error {
	return k.client.Flush(ctx)
}

func (k *KgoClient) Ping(ctx context.Context) error {
	return k.client.Ping(ctx)
}

func (k *KgoClient) PausePartitions(partitions ...TopicPartition) {
	k.client.PauseFetchPartitions(topicPartitionsToMap(partitions))
}

func (k *KgoClient) ResumePartitions(partitions ...TopicPartition) {
	k.client.ResumeFetchPartitions(topicPartitionsToMap(partitions))
}

func (k *KgoClient) Close() {
	k.client.CloseAllowingRebalance()
}

func convertRecordsToKgo(records []ConsumerRecord) []*kgo.Record {
	kgoRecords := make([]*kgo.Record, len(records))
	for i, r := range records {
		kgoRecords[i] = &kgo.Record{
			Topic:       r.Topic,
			Partition:   r.Partition,
			Offset:      r.Offset,
			Key:         r.Key,
			Value:       r.Value,
			Headers:     convertToKgoHeaders(r.Headers),
			Timestamp:   r.Timestamp,
			LeaderEpoch: r.LeaderEpoch,
		}
	}

	return kgoRecords
}

func convertRecords(records []*kgo.Record) []ConsumerRecord {
	converted := make([]ConsumerRecord, len(records))
	for i, r := range records {
		converted[i] = ConsumerRecord{
			Topic:       r.Topic,
			Partition:   r.Partition,
			Offset:      r.Offset,
			Key:         r.Key,
			Value:       r.Value,
			Headers:     convertFromKgoHeaders(r.Headers),
			Timestamp:   r.Timestamp,
			LeaderEpoch: r.LeaderEpoch,
		}
	}

	return converted
}

func convertFromKgoHeaders(headers []kgo.RecordHeader) []Header {
	converted := make([]Header, len(headers))
	for i, h := range headers {
		converted[i] = Header{Key: h.Key, Value: h.Value}
	}
	return converted
}

func convertToKgoHeaders(headers []Header) []kgo.RecordHeader {
	kgoHeaders := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		kgoHeaders[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	return kgoHeaders
}

func topicPartitionsToMap(tps []TopicPartition) map[string][]int32 {
	m := make(map[string][]int32)
	for _, tp := range tps {
		m[tp.Topic] = append(m[tp.Topic], tp.Partition)
	}
	return m
}

func mapToTopicPartitions(m map[string][]int32) []TopicPartition {
	var tps []TopicPartition
	for topic, partitions := range m {
		for _, partition := range partitions {
			tps = append(
				tps, TopicPartition{
					Topic:     topic,
					Partition: partition,
				},
			)
		}
	}

	return tps
}
