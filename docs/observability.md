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
| `stream.process.retry_count`         | `0`, `1`, `2`                | process                   |

`stream.process.retry_count` records the raw processing attempt count (0-based) reached for the
record. It is a span attribute only - it is **not** an attribute on the `stream.process.duration`
metric.

Error events are recorded on the `process` span as `exception` events with `exception.type` and `exception.message` attributes.

## Metrics

All metrics are registered under the `github.com/hugolhafner/go-streams` instrumentation scope.

### Available Metrics

| Metric                           | Type          | Unit        | Description                                        |
|----------------------------------|---------------|-------------|----------------------------------------------------|
| `messaging.consumer.messages`    | Counter       | {message}   | Total records consumed                             |
| `messaging.producer.messages`    | Counter       | {message}   | Total records produced                             |
| `stream.poll.duration`           | Histogram     | s           | Time per `Poll()` call                             |
| `stream.poll.records`            | Histogram     | {message}   | Records per poll batch                             |
| `stream.consumer.lag`            | Histogram     | s           | Time since record was produced (saturation signal) |
| `stream.process.duration`        | Histogram     | s           | End-to-end record processing time                  |
| `stream.process.retries`         | Counter       | {retry}     | Individual retry attempts                          |
| `stream.produce.duration`        | Histogram     | s           | Time per `Send()` call to Kafka                    |
| `stream.errors`                  | Counter       | {error}     | Processing errors with handler action              |
| `stream.tasks.active`            | UpDownCounter | {task}      | Currently active tasks (partitions)                |
| `stream.rebalance.count`         | Counter       | {rebalance} | Rebalance events (assigned/revoked)                |
| `stream.node.records`            | Counter       | {message}   | Records processed per node (incl. source)          |
| `stream.node.latency`            | Histogram     | s           | Processing time per node                           |
| `stream.edge.records`            | Counter       | {message}   | Records flowing between nodes                      |
| `stream.partitioned.queue.depth` | Gauge         | {message}   | Total records queued across partition workers      |
| `stream.partitioned.paused.depth`| Gauge         | {message}   | Total records queued in paused partitions          |
| `stream.partitioned.backpressure.events` | Counter | {event}   | Partition pause/resume events due to backpressure  |

### Metric Attributes

| Attribute                            | Metrics                                                                                                         | Values                                         |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------|
| `messaging.destination.name`         | consumer.messages, producer.messages, process.duration, produce.duration, errors, consumer.lag, process.retries, backpressure.events | Topic name                                     |
| `messaging.destination.partition.id` | consumer.messages, process.duration, consumer.lag, backpressure.events                                          | Partition ID                                   |
| `stream.poll.status`                 | poll.duration                                                                                                   | `success`, `error`                             |
| `stream.process.status`              | process.duration                                                                                                | `success`, `dropped`, `dlq`, `failed`, `error` |
| `stream.produce.status`              | produce.duration                                                                                                | `success`, `error`                             |
| `stream.error.action`                | errors                                                                                                          | `continue`, `retry`, `fail`, `send_to_dlq`     |
| `stream.error.node`                  | errors                                                                                                          | Node name where the error occurred             |
| `stream.error.phase`                 | errors, process.retries                                                                                         | `unknown`, `serde`, `processing`, `production` |
| `stream.runner.type`                 | tasks.active                                                                                                    | `single_threaded`, `partitioned`               |
| `stream.rebalance.type`              | rebalance.count                                                                                                 | `assigned`, `revoked`                          |
| `stream.node.name`                   | node.records, node.latency                                                                                      | Node name                                      |
| `stream.node.type`                   | node.records, node.latency                                                                                      | `source`, `processor`, `sink`                  |
| `stream.edge.source`                 | edge.records                                                                                                    | Source node name                               |
| `stream.edge.target`                 | edge.records                                                                                                    | Target node name                               |
| `stream.backpressure.event`          | backpressure.events                                                                                             | `paused`, `resumed`                            |

### Process Status Values

The `stream.process.status` attribute tracks the outcome of each record:

| Status    | Meaning                                                |
|-----------|--------------------------------------------------------|
| `success` | Record processed without error                         |
| `dropped` | Error handler returned `Continue` - record was skipped |
| `dlq`     | Record was sent to a dead letter queue                 |
| `failed`  | Error handler returned `Fail` - runner stopped         |
| `error`   | An error occurred (used for poll/produce status)       |

## Integration with Error Handling

Observability works alongside the [error handling](error-handling.md) system. When a processing error occurs:

1. An `exception` event is recorded on the `process` span
2. The error handler decides an action (phase-specific handlers are routed automatically)
3. The `stream.errors` counter increments with combined attributes: `stream.error.phase`, `stream.error.node`, and `stream.error.action`
4. If the action is `retry`, `stream.process.retries` also increments
5. The `stream.process.status` attribute reflects the final outcome (`dropped`, `dlq`, or `failed`)

The `stream.error.phase` attribute lets you distinguish between deserialization, processing, and production errors in your dashboards and alerts - for example, alerting on poison pills separately from sink failures.

### DLQ Headers

When a record is sent to a dead letter queue, the following headers are added:

| Header                 | Description                                                               |
|------------------------|---------------------------------------------------------------------------|
| `x-original-topic`     | Source topic                                                              |
| `x-original-partition` | Source partition                                                          |
| `x-original-offset`    | Source offset                                                             |
| `x-error-timestamp`    | ISO 8601 timestamp of the error                                           |
| `x-error-attempt`      | Number of processing attempts                                             |
| `x-error-message`      | Error message (if present)                                                |
| `x-error-node`         | Topology node name (if present)                                           |
| `x-error-phase`        | Error phase: `unknown`, `serde`, `processing`, or `production` (if known) |

## Service Graph (Topology Visualization)

go-streams provides per-node and per-edge metrics that enable visualizing the stream topology as a service graph in Grafana's [Node Graph panel](https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/node-graph/).

### Topology Descriptor

Use `Topology.Describe()` to get a static description of the topology graph:

```go
desc := t.Describe()
for _, node := range desc.Nodes {
    fmt.Printf("Node: %s (type=%s, topic=%s)\n", node.ID, node.Type, node.Topic)
}
for _, edge := range desc.Edges {
    fmt.Printf("Edge: %s -> %s\n", edge.Source, edge.Target)
}
```

`Describe()` returns a `topology.Description` holding `[]NodeInfo` and `[]EdgeInfo`. `NodeInfo` has
`ID`, `Type`, `Name`, and `Topic` (topic is set for source/sink nodes, empty otherwise); `EdgeInfo`
has `ID`, `Source`, and `Target`. These map onto Grafana's node/edge data format: node `id` = `ID`,
`title` = `Name`, `subtitle` = `Type`; edge `source` = `Source`, `target` = `Target`.

### Grafana Node Graph Setup

To visualize the topology in Grafana's Node Graph panel, create two Prometheus queries:

**Nodes dataset:**
```promql
sum by (stream_node_name, stream_node_type) (rate(stream_node_records_total[5m]))
```

Configure field mappings: `id` = `stream_node_name`, `title` = `stream_node_name`, `subtitle` = `stream_node_type`, `mainstat` = `Value` (throughput).

**Edges dataset:**
```promql
sum by (stream_edge_source, stream_edge_target) (rate(stream_edge_records_total[5m]))
```

Configure field mappings: `source` = `stream_edge_source`, `target` = `stream_edge_target`, `mainstat` = `Value` (message flow rate).

You can also use `stream.node.latency` for the node `secondarystat` to show average processing time, and `stream.errors` with `stream.error.node` for error overlays. `stream.node.latency` is exclusive per-node self-time: a processor's latency excludes the downstream nodes it forwards to, so the values sum rather than nest.

> **Note:** `stream.node.records`, `stream.edge.records`, and `stream.node.latency` are recorded **per processing attempt**. When a record is retried, the full topology is re-traversed, so these counters increment again on each attempt - whereas `messaging.consumer.messages` increments once per consumed record. During a retry storm, service-graph throughput will exceed the consumed-message rate by the retry factor.

## Alerting Examples

> **Tip:** A complete, ready-to-load rule set built from these examples (plus traffic and latency
> coverage, two-tier severities and per-application grouping) ships in
> [`docs/prometheus/rules/go-streams-alerts.yaml`](prometheus/rules/go-streams-alerts.yaml) — see
> [its README](prometheus/README.md) for the catalog and how to regenerate or tune it.

### Error Rate

Alert when the error rate exceeds 1% of consumed messages over 5 minutes:

```promql
sum(rate(stream_errors_total[5m]))
  /
sum(rate(messaging_consumer_messages_total[5m]))
  > 0.01
```

### Consumer Lag p99

Alert when the 99th percentile consumer lag exceeds 30 seconds:

```promql
histogram_quantile(0.99, sum(rate(stream_consumer_lag_bucket[5m])) by (le))
  > 30
```

### Retry Rate

Alert on excessive retries (more than 5 retries/sec averaged over 5 minutes):

```promql
sum(rate(stream_process_retries_total[5m])) > 5
```

### Queue Depth (Partitioned Runner)

Alert when total queue depth exceeds a threshold:

```promql
stream_partitioned_queue_depth > 1000
```

### Backpressure Events (Partitioned Runner)

Alert when partitions are being paused frequently (more than 1 pause/sec averaged over 5 minutes), a sign that workers can't keep up with the poll rate:

```promql
sum(rate(stream_partitioned_backpressure_events_total{stream_backpressure_event="paused"}[5m])) > 1
```

### Rebalance Frequency

Alert on frequent rebalances (more than 5 in 10 minutes):

```promql
sum(increase(stream_rebalance_count_total[10m])) > 5
```
