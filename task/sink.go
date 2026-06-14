package task

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/topology"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
)

type sinkHandler struct {
	name      string
	node      topology.SinkNode
	producer  kafka.Producer
	telemetry *streamsotel.Telemetry

	// precomputed to reduce overhead
	sinkAttrs           metric.MeasurementOption // node name + type=sink
	producedAttrs       metric.MeasurementOption // destination name
	produceSuccessAttrs metric.MeasurementOption // destination name + status=success
	publishSpanName     string                   // "<topic> publish"
}

func (s *sinkHandler) Process(ctx context.Context, rec *record.UntypedRecord) error {
	topic := s.node.Topic()
	tel := s.telemetry

	nodeStart := time.Now()

	key, err := s.node.KeySerde().Serialise(topic, rec.Key)
	if err != nil {
		return NewSerdeError(fmt.Errorf("serialize key for topic %s: %w", topic, err))
	}

	value, err := s.node.ValueSerde().Serialise(topic, rec.Value)
	if err != nil {
		return NewSerdeError(fmt.Errorf("serialize value for topic %s: %w", topic, err))
	}

	ctx, span := tel.Tracer.Start(
		ctx, s.publishSpanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeSend,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingMessageBodySize(len(key)+len(value)),
		),
	)
	defer span.End()

	headers := make([]kafka.Header, len(rec.Headers))
	copy(headers, rec.Headers)
	carrier := streamsotel.NewKafkaHeadersCarrier(&headers)
	tel.Propagator.Inject(ctx, carrier)

	produceStart := time.Now()
	produceAttrs := s.produceSuccessAttrs

	defer func() {
		tel.ProduceDuration.Record(ctx, time.Since(produceStart).Seconds(), produceAttrs)
	}()

	produceErr := s.producer.Send(ctx, topic, key, value, headers)
	if produceErr != nil {
		span.RecordError(produceErr)
		span.SetStatus(codes.Error, produceErr.Error())
		produceAttrs = metric.WithAttributes(
			semconv.MessagingDestinationName(topic),
			streamsotel.AttrProduceStatus.String(streamsotel.StatusError),
		)
		return NewProductionError(fmt.Errorf("produce to %s: %w", topic, produceErr), s.name)
	}

	tel.MessagesProduced.Add(ctx, 1, s.producedAttrs)

	tel.NodeRecords.Add(ctx, 1, s.sinkAttrs)
	tel.NodeLatency.Record(ctx, time.Since(nodeStart).Seconds(), s.sinkAttrs)

	return nil
}
