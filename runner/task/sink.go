package task

import (
	"context"
	"fmt"

	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/topology"
)

type sinkHandler struct {
	node     topology.SinkNode
	producer log.Producer
}

func (s *sinkHandler) Process(rec *record.UntypedRecord) error {
	topic := s.node.Topic()
	fmt.Println("Producing record to topic:", topic)

	key, err := s.node.KeySerde().Serialize(topic, rec.Key)
	if err != nil {
		return fmt.Errorf("serialize key: %w", err)
	}

	value, err := s.node.ValueSerde().Serialize(topic, rec.Value)
	if err != nil {
		return fmt.Errorf("serialize value: %w", err)
	}

	var headers map[string][]byte
	if rec.Headers != nil {
		headers = rec.Headers
	}

	if err := s.producer.Send(context.Background(), topic, key, value, headers); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}

	return nil
}
