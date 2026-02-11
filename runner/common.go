package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
)

// emitError emits an error to the provided channel without blocking
func emitError(errCh chan<- error, l logger.Logger, err error) {
	select {
	case errCh <- err:
	default:
		l.Error("Error channel full, dropping error", "error", err)
	}
}

func sendToDLQ(
	ctx context.Context, producer kafka.Producer, record kafka.ConsumerRecord, ec errorhandler.ErrorContext,
	topic string,
) error {
	key := make([]byte, len(record.Key))
	copy(key, record.Key)
	value := make([]byte, len(record.Value))
	copy(value, record.Value)

	headers := make([]kafka.Header, len(record.Headers), len(record.Headers)+7)
	for i, h := range record.Headers {
		vCopy := make([]byte, len(h.Value))
		copy(vCopy, h.Value)
		headers[i] = kafka.Header{Key: h.Key, Value: vCopy}
	}

	headers = append(
		headers,
		kafka.Header{Key: "x-original-topic", Value: []byte(record.Topic)},
		kafka.Header{Key: "x-original-partition", Value: []byte(fmt.Sprintf("%d", record.Partition))},
		kafka.Header{Key: "x-original-offset", Value: []byte(fmt.Sprintf("%d", record.Offset))},
		kafka.Header{Key: "x-error-timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		kafka.Header{Key: "x-error-attempt", Value: []byte(fmt.Sprintf("%d", ec.Attempt))},
	)

	if ec.Error != nil {
		headers = append(headers, kafka.Header{Key: "x-error-message", Value: []byte(ec.Error.Error())})
	}
	if ec.NodeName != "" {
		headers = append(headers, kafka.Header{Key: "x-error-node", Value: []byte(ec.NodeName)})
	}

	return producer.Send(ctx, topic, key, value, headers)
}
