# Observability

go-streams has built-in support for [OpenTelemetry](https://opentelemetry.io/) (OTel) to provide distributed tracing and metrics for your stream processing applications. When no providers are configured, all instrumentation is noop with zero overhead.

## Install

The OTel dependencies are included in the go-streams module. You only need the OTel SDK packages to configure exporters:

```bash
go get go.opentelemetry.io/otel/sdk \
       go.opentelemetry.io/otel/sdk/metric
```

## Enabling Telemetry

Pass a `TracerProvider` and/or `MeterProvider` when creating the application:

```go
app, err := streams.NewApplication(client, topology,
    streams.WithTracerProvider(tracerProvider),
    streams.WithMeterProvider(meterProvider),
)
```

Both are optional. If you only want tracing, pass just `WithTracerProvider`. If you only want metrics, pass just `WithMeterProvider`. Omitting both results in zero-overhead noop instrumentation.

## Quick Setup Example

```go
package main

import (
    "context"
    "log"

    streams "github.com/hugolhafner/go-streams"
    "github.com/hugolhafner/go-streams/kafka"
    "github.com/hugolhafner/go-streams/kstream"
    "github.com/hugolhafner/go-streams/serde"

    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
    ctx := context.Background()

    // Set up trace exporter (e.g. to an OTLP collector)
    traceExporter, err := otlptracehttp.New(ctx)
    if err != nil {
        log.Fatal(err)
    }
    tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter))
    defer tp.Shutdown(ctx)

    // Set up metrics exporter
    metricExporter, err := otlpmetrichttp.New(ctx)
    if err != nil {
        log.Fatal(err)
    }
    mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(
        sdkmetric.NewPeriodicReader(metricExporter),
    ))
    defer mp.Shutdown(ctx)

    // Build topology
    builder := kstream.NewStreamsBuilder()
    orders := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[map[string]any]())
    kstream.To(orders, "orders-out")
    t := builder.Build()

    // Create Kafka client
    client, err := kafka.NewKgoClient(
        kafka.WithBootstrapServers([]string{"localhost:9092"}),
        kafka.WithGroupID("my-app"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create application with OTel providers
    app, err := streams.NewApplication(client, t,
        streams.WithTracerProvider(tp),
        streams.WithMeterProvider(mp),
    )
    if err != nil {
        log.Fatal(err)
    }

    app.Run(ctx)
}
```

## Distributed Tracing

### Trace Context Propagation

go-streams automatically propagates W3C `traceparent`/`tracestate` headers through Kafka record headers. This means:

- **On consume:** Trace context is extracted from incoming record headers and used as the parent for processing spans.
- **On produce:** Trace context is injected into outgoing record headers so downstream consumers can continue the trace.

This enables end-to-end distributed traces across multiple services connected via Kafka.

### Span Hierarchy

Each record produces the following span tree:

```
receive              (SpanKind: Consumer)
  └── orders process        (SpanKind: Consumer)
       ├── FILTER-000001 execute   (SpanKind: Internal)
       ├── MAP-000002 execute      (SpanKind: Internal)
       └── orders-out publish      (SpanKind: Producer)
```

| Span              | Kind     | Description                                     |
|-------------------|----------|-------------------------------------------------|
| `receive`         | Consumer | Covers the entire poll batch                    |
| `{topic} process` | Consumer | Per-record processing through the full topology |
| `{node} execute`  | Internal | Execution of a single processor or sink node    |
| `{topic} publish` | Producer | Producing a record to a sink topic              |

### Span Attributes

Spans include standard [OTel messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/):

| Attribute                            | Example                      | Spans                     |
|--------------------------------------|------------------------------|---------------------------|
| `messaging.system`                   | `kafka`                      | receive, process, publish |
| `messaging.operation.type`           | `receive`, `process`, `send` | receive, process, publish |
| `messaging.destination.name`         | `orders`                     | process, publish          |
| `messaging.destination.partition.id` | `0`                          | process                   |
| `messaging.kafka.offset`             | `42`                         | process                   |
| `messaging.consumer.group.name`      | `my-app`                     | process                   |
| `messaging.message.body.size`        | `256`                        | process, publish          |
| `messaging.batch.message_count`      | `10`                         | receive                   |
| `stream.node.name`                   | `FILTER-000001`              | execute                   |
| `stream.node.type`                   | `processor`, `sink`          | execute                   |

Error events are recorded on the `process` span as `exception` events with `exception.type` and `exception.message` attributes.

## Metrics

All metrics are registered under the `github.com/hugolhafner/go-streams` instrumentation scope.

### Available Metrics

| Metric                         | Type          | Unit | Description                         |
|--------------------------------|---------------|------|-------------------------------------|
| `messaging.consumer.messages`  | Counter       | —    | Total records consumed              |
| `messaging.producer.messages`  | Counter       | —    | Total records produced              |
| `stream.poll.duration`         | Histogram     | s    | Time per `Poll()` call              |
| `stream.process.duration`      | Histogram     | s    | End-to-end record processing time   |
| `stream.produce.duration`      | Histogram     | s    | Time per `Send()` call to Kafka     |
| `stream.errors`                | Counter       | —    | Processing errors encountered       |
| `stream.error_handler.actions` | Counter       | —    | Error handler decisions             |
| `stream.tasks.active`          | UpDownCounter | —    | Currently active tasks (partitions) |

### Metric Attributes

| Attribute                            | Metrics                                                                                                 | Values                                         |
|--------------------------------------|---------------------------------------------------------------------------------------------------------|------------------------------------------------|
| `messaging.destination.name`         | consumer.messages, producer.messages, process.duration, produce.duration, errors, error_handler.actions | Topic name                                     |
| `messaging.destination.partition.id` | consumer.messages, process.duration                                                                     | Partition ID                                   |
| `stream.poll.status`                 | poll.duration                                                                                           | `success`, `error`                             |
| `stream.process.status`              | process.duration                                                                                        | `success`, `dropped`, `dlq`, `failed`, `error` |
| `stream.produce.status`              | produce.duration                                                                                        | `success`, `error`                             |
| `stream.error.action`                | error_handler.actions                                                                                   | `continue`, `retry`, `fail`, `send_to_dlq`     |
| `stream.error.node`                  | errors                                                                                                  | Node name where the error occurred             |
| `stream.runner.type`                 | tasks.active                                                                                            | `single_threaded`, `partitioned`               |

### Process Status Values

The `stream.process.status` attribute tracks the outcome of each record:

| Status    | Meaning                                                |
|-----------|--------------------------------------------------------|
| `success` | Record processed without error                         |
| `dropped` | Error handler returned `Continue` — record was skipped |
| `dlq`     | Record was sent to a dead letter queue                 |
| `failed`  | Error handler returned `Fail` — runner stopped         |
| `error`   | An error occurred (used for poll/produce status)       |

## Integration with Error Handling

Observability works alongside the [error handling](error-handling.md) system. When a processing error occurs:

1. An `exception` event is recorded on the `process` span
2. The `stream.errors` counter increments
3. The error handler decides an action
4. The `stream.error_handler.actions` counter increments with the action type
5. The `stream.process.status` attribute reflects the final outcome (`dropped`, `dlq`, or `failed`)

This lets you correlate error handler behavior with traces and metrics — for example, alerting when the DLQ rate exceeds a threshold.
