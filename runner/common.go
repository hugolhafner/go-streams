package runner

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
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

	headers := make([]kafka.Header, len(record.Headers), len(record.Headers)+10)
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
		kafka.Header{Key: "x-error-phase", Value: []byte(ec.Phase.String())},
	)

	if ec.Error != nil {
		headers = append(headers, kafka.Header{Key: "x-error-message", Value: []byte(ec.Error.Error())})
	}
	if ec.NodeName != "" {
		headers = append(headers, kafka.Header{Key: "x-error-node", Value: []byte(ec.NodeName)})
	}

	return producer.Send(ctx, topic, key, value, headers)
}

// processRecordWithRetry handles the error-retry loop for a single record,
// including OTel span creation, context propagation, and metric recording.
// On all non-error return paths, the record is marked as consumed.
func processRecordWithRetry(
	ctx context.Context,
	rec kafka.ConsumerRecord,
	t task.Task,
	consumer kafka.Consumer,
	producer kafka.Producer,
	handler errorhandler.Handler,
	tel *streamsotel.Telemetry,
	l logger.Logger,
) error {
	carrier := streamsotel.NewKafkaHeadersCarrier(&rec.Headers)
	ctx = tel.Propagator.Extract(ctx, carrier)

	processStart := time.Now()
	ctx, span := tel.Tracer.Start(
		ctx, rec.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(rec.Partition), 10)),
			semconv.MessagingKafkaOffsetKey.Int64(rec.Offset),
			semconv.MessagingConsumerGroupName(consumer.GroupID()),
			semconv.MessagingMessageBodySize(rec.Size()),
		),
	)
	defer span.End()

	ec := errorhandler.NewErrorContext(rec, nil)
	recordProcessStatus := func(status string) {
		span.SetAttributes(attribute.Int("stream.process.retry_count", ec.Attempt))
		tel.ProcessDuration.Record(
			ctx, time.Since(processStart).Seconds(), metric.WithAttributes(
				semconv.MessagingDestinationName(rec.Topic),
				semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(rec.Partition), 10)),
				streamsotel.AttrProcessStatus.String(status),
			),
		)
	}

	var lastErr error

	for {
		select {
		case <-ctx.Done():
			l.Warn("Context cancelled while processing record", "offset", rec.Offset, "error", ctx.Err())
			span.SetStatus(codes.Error, ctx.Err().Error())
			return ctx.Err()
		default:
		}

		err := t.Process(ctx, rec)
		if err == nil {
			l.Debug("Record processed successfully", "offset", rec.Offset)
			consumer.MarkRecords(rec)
			recordProcessStatus(streamsotel.StatusSuccess)
			return nil
		}

		var phase errorhandler.ErrorPhase
		var nodeName string

		if _, ok := task.AsSerdeError(err); ok {
			phase = errorhandler.PhaseSerde
		} else if pErr, ok := task.AsProductionError(err); ok {
			phase = errorhandler.PhaseProduction
			nodeName = pErr.Node
		} else if pErr, ok := task.AsProcessError(err); ok {
			phase = errorhandler.PhaseProcessing
			nodeName = pErr.Node
		} else {
			phase = errorhandler.PhaseProcessing
		}

		ec = ec.WithError(err).WithNodeName(nodeName).WithPhase(phase)
		lastErr = err

		span.RecordError(err)
		tel.Errors.Add(
			ctx, 1, metric.WithAttributes(
				semconv.MessagingDestinationName(rec.Topic),
				streamsotel.AttrErrorNode.String(ec.NodeName),
				streamsotel.AttrErrorPhase.String(ec.Phase.String()),
			),
		)

		action := handler.Handle(ctx, ec)

		tel.ErrorHandlerActions.Add(
			ctx, 1, metric.WithAttributes(
				streamsotel.AttrErrorAction.String(action.Type().String()),
				semconv.MessagingDestinationName(rec.Topic),
				streamsotel.AttrErrorPhase.String(ec.Phase.String()),
			),
		)

		switch action.Type() {
		case errorhandler.ActionTypeFail:
			recordProcessStatus(streamsotel.StatusFailed)
			span.SetStatus(codes.Error, err.Error())
			return err

		case errorhandler.ActionTypeRetry:
			l.Debug("Retrying record", "attempt", ec.Attempt, "offset", rec.Offset)
			ec = ec.IncrementAttempt()

			if ec.Attempt%10 == 0 {
				l.Warn(
					"Record seen high number of retry attempts, "+
						"consider sending to DLQ or allowing error handler to skip.",
					"attempt", ec.Attempt, "key", string(rec.Key), "topic", rec.Topic, "offset", rec.Offset,
					"partition", rec.Partition,
				)
			}

			continue

		case errorhandler.ActionTypeSendToDLQ:
			a, ok := action.(errorhandler.ActionSendToDLQ)
			if !ok {
				l.Error("Invalid action type, expected ActionSendToDLQ", "action", action.Type().String())
				recordProcessStatus(streamsotel.StatusFailed)
				span.SetStatus(codes.Error, "invalid action type")
				return errors.New("invalid action type, expected ActionSendToDLQ")
			}

			if err := sendToDLQ(ctx, producer, rec, ec, a.Topic()); err != nil {
				l.Error(
					"Failed to send record to DLQ.",
					"error", err,
					"key", string(rec.Key),
					"original_topic", rec.Topic,
					"original_partition", rec.Partition,
					"original_offset", rec.Offset,
				)
				recordProcessStatus(streamsotel.StatusFailed)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			consumer.MarkRecords(rec)
			recordProcessStatus(streamsotel.StatusDLQ)
			return nil

		case errorhandler.ActionTypeContinue:
			l.Debug("Skipping failed record", "offset", rec.Offset)
			consumer.MarkRecords(rec)
			recordProcessStatus(streamsotel.StatusDropped)
			return nil

		default:
			l.Error(
				"Unknown error handler action, failing record",
				"error", lastErr,
				"key", string(ec.Record.Key),
				"topic", ec.Record.Topic,
				"offset", ec.Record.Offset,
				"partition", ec.Record.Partition,
				"attempt", ec.Attempt,
				"node", ec.NodeName,
			)
			recordProcessStatus(streamsotel.StatusFailed)
			span.SetStatus(codes.Error, lastErr.Error())
			return lastErr
		}
	}
}
