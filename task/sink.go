package task

import (
	"context"
	"fmt"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/topology"
)

type sinkHandler struct {
	node     topology.SinkNode
	producer kafka.Producer
}

func (s *sinkHandler) Process(ctx context.Context, rec *record.UntypedRecord) error {
	topic := s.node.Topic()

	key, err := s.node.KeySerde().Serialise(topic, rec.Key)
	if err != nil {
		return fmt.Errorf("serialize key: %w", err)
	}

	value, err := s.node.ValueSerde().Serialise(topic, rec.Value)
	if err != nil {
		return fmt.Errorf("serialize value: %w", err)
	}

	if err := s.producer.Send(ctx, topic, key, value, rec.Headers); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}

	return nil
}
