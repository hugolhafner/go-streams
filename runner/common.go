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
	topic string, logger logger.Logger,
) {
	key := make([]byte, len(record.Key))
	copy(key, record.Key)
	value := make([]byte, len(record.Value))
	copy(value, record.Value)

	headers := make(map[string][]byte, len(record.Headers)+6)
	for k, v := range record.Headers {
		headerCopy := make([]byte, len(v))
		copy(headerCopy, v)
		headers[k] = headerCopy
	}

	headers["x-original-topic"] = []byte(record.Topic)
	headers["x-original-partition"] = []byte(fmt.Sprintf("%d", record.Partition))
	headers["x-original-offset"] = []byte(fmt.Sprintf("%d", record.Offset))
	headers["x-error-timestamp"] = []byte(time.Now().Format(time.RFC3339))
	headers["x-error-attempt"] = []byte(fmt.Sprintf("%d", ec.Attempt))

	if ec.Error != nil {
		headers["x-error-message"] = []byte(ec.Error.Error())
	}
	if ec.NodeName != "" {
		headers["x-error-node"] = []byte(ec.NodeName)
	}

	if err := producer.Send(ctx, topic, key, value, headers); err != nil {
		logger.Error(
			"Failed to send record to DLQ, dropping record",
			"error", err,
			"key", string(key),
			"original_topic", record.Topic,
			"original_partition", record.Partition,
			"original_offset", record.Offset,
		)
		return
	}

	logger.Debug(
		"Sent record to DLQ",
		"key", string(key),
		"original_topic", record.Topic,
		"original_partition", record.Partition,
		"original_offset", record.Offset,
	)
}
