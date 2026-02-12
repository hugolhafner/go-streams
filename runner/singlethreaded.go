package runner

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
)

var _ Runner = (*SingleThreaded)(nil)
var _ kafka.RebalanceCallback = (*SingleThreaded)(nil)

type SingleThreaded struct {
	consumer    kafka.Consumer
	producer    kafka.Producer
	taskManager task.Manager
	topology    *topology.Topology
	config      SingleThreadedConfig

	logger    logger.Logger
	telemetry *streamsotel.Telemetry

	// errChan is used to signal fatal errors from goroutines to the main Run loop
	errChan chan error
}

func NewSingleThreadedRunner(
	opts ...SingleThreadedOption,
) Factory {
	config := defaultSingleThreadedConfig()
	for _, opt := range opts {
		opt.applySingleThreaded(&config)
	}

	return func(
		t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer,
		telemetry *streamsotel.Telemetry,
	) (Runner, error) {
		return &SingleThreaded{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, config.Logger),
			topology:    t,
			config:      config,
			errChan:     make(chan error, 1),
			telemetry:   telemetry,
			logger: config.Logger.
				With("component", "runner").
				With("runner", "single-threaded"),
		}, nil
	}
}

func (r *SingleThreaded) shutdown() {
	r.logger.Info("Shutting down runner")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	if err := r.consumer.Commit(ctx); err != nil {
		r.logger.Error("Failed to commit offsets during shutdown", "error", err)
	}

	if err := r.producer.Flush(ctx); err != nil {
		r.logger.Error("Failed to flush producer during shutdown", "error", err)
	}

	r.logger.Info("Shutdown complete")
}

// doPoll polls for records and processes them sequentially
// returns any fatal errors that should stop the runner
func (r *SingleThreaded) doPoll(ctx context.Context) error {
	r.logger.Debug("Polling for records")

	tel := r.telemetry
	pollStart := time.Now()

	ctx, receiveSpan := tel.Tracer.Start(
		ctx, "receive",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeReceive,
		),
	)
	records, err := r.consumer.Poll(ctx)

	if err != nil {
		receiveSpan.RecordError(err)
		receiveSpan.End()
		
		tel.PollDuration.Record(
			ctx, time.Since(pollStart).Seconds(), metric.WithAttributes(
				streamsotel.AttrPollStatus.String(streamsotel.StatusError),
			),
		)
		return fmt.Errorf("failed to poll: %w", err)
	}

	tel.PollDuration.Record(
		ctx, time.Since(pollStart).Seconds(), metric.WithAttributes(
			streamsotel.AttrPollStatus.String(streamsotel.StatusSuccess),
		),
	)

	receiveSpan.SetAttributes(semconv.MessagingBatchMessageCount(len(records)))
	receiveSpan.End()

	if len(records) == 0 {
		r.logger.Debug("No records received from poll")
		return nil
	}

	r.logger.Debug("Received records", "records", len(records))

	for _, record := range records {
		r.logger.Debug(
			"Processing record",
			"key", string(record.Key),
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
		)

		tel.MessagesConsumed.Add(
			ctx, 1, metric.WithAttributes(
				semconv.MessagingDestinationName(record.Topic),
				semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(record.Partition), 10)),
			),
		)

		if err := r.processRecord(ctx, record); err != nil {
			return fmt.Errorf("fatal error processing record: %w", err)
		}
	}

	return nil
}

func (r *SingleThreaded) processRecord(ctx context.Context, record kafka.ConsumerRecord) error {
	t, ok := r.taskManager.TaskFor(record.TopicPartition())
	if !ok {
		r.logger.Warn(
			"No task found for topic partition, may have just been rebalanced, continuing...", "topic_partition",
			record.TopicPartition(),
		)
		return nil
	}

	return processRecordWithRetry(ctx, record, t, r.consumer, r.producer, r.config.ErrorHandler, r.telemetry, r.logger)
}

func (r *SingleThreaded) emitErr(err error) {
	select {
	case r.errChan <- err:
	default:
		r.logger.Error("Error channel full, dropping error", "error", err)
	}
}

func (r *SingleThreaded) OnAssigned(ctx context.Context, partitions []kafka.TopicPartition) {
	r.logger.Debug("Assigned partitions", "partitions", partitions)
	r.logger.Debug("Creating tasks for partitions")
	if err := r.taskManager.CreateTasks(partitions); err != nil {
		r.logger.Error("Failed to create tasks on assigned partitions", "error", err)
		r.emitErr(err)
		return
	}
	r.telemetry.TasksActive.Add(
		ctx, int64(len(partitions)), metric.WithAttributes(
			streamsotel.AttrRunnerType.String(streamsotel.RunnerTypeSingleThreaded),
		),
	)
}

func (r *SingleThreaded) OnRevoked(ctx context.Context, partitions []kafka.TopicPartition) {
	// Close tasks first to stop processing new records
	r.logger.Debug("Revoking partitions", "partitions", partitions)
	r.logger.Debug("Closing tasks for revoked partitions")

	if err := r.taskManager.CloseTasks(partitions); err != nil {
		r.logger.Error("Failed to close tasks on revoked partitions", "error", err)
		r.emitErr(err)
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	r.logger.Debug("Forcing a commit prior to revoking partitions")
	if err := r.consumer.Commit(commitCtx); err != nil {
		r.logger.Error("Failed to commit offsets on revoked partitions", "error", err)
	}

	r.logger.Debug("Deleting tasks for revoked partitions")
	if err := r.taskManager.DeleteTasks(partitions); err != nil {
		r.logger.Error("Failed to delete tasks on revoked partitions", "error", err)
		r.emitErr(err)
	}

	r.telemetry.TasksActive.Add(
		ctx, -int64(len(partitions)), metric.WithAttributes(
			streamsotel.AttrRunnerType.String(streamsotel.RunnerTypeSingleThreaded),
		),
	)
}

func (r *SingleThreaded) Run(ctx context.Context) error {
	topics := r.topology.SourceTopics()
	if err := r.consumer.Subscribe(topics, r); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	defer r.shutdown()

	var errAttempts uint = 0
	for {
		select {
		case err := <-r.errChan:
			r.logger.Error("Fatal error received in Run()", "error", err)
			return err

		case <-ctx.Done():
			r.logger.Info("Context cancelled, shutting down")
			return nil

		default:
			if err := r.doPoll(ctx); err != nil {
				r.logger.Warn("Poll error", "error", err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(r.config.PollErrorBackoff.Next(errAttempts)):
				}
				errAttempts++
			} else {
				errAttempts = 0
			}
		}
	}
}
