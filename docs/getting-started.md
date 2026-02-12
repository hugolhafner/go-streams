# Getting Started

This guide walks through building your first stream processing topology with go-streams.

## Prerequisites

- Go 1.22+
- A running Kafka broker (or [Redpanda](https://redpanda.com/))

## Install

```bash
go get github.com/hugolhafner/go-streams
```

## Building a Topology

A topology is a directed acyclic graph (DAG) that defines how records flow from source topics, through processors, to sink topics. You build one using the `StreamsBuilder` and KStream DSL.

### 1. Create a Builder

```go
builder := kstream.NewStreamsBuilder()
```

### 2. Define a Source

Read from a Kafka topic. Use a serde to deserialize values into Go types:

```go
type Order struct {
    ID     string  `json:"id"`
    Amount float64 `json:"amount"`
    UserID string  `json:"user_id"`
}

orders := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[Order]())
```

This creates a `KStream[[]byte, Order]` — keys remain raw bytes, values are deserialized as JSON.

### 3. Add Processing Steps

Filter out invalid records:

```go
valid := kstream.Filter(orders, func(ctx context.Context, k []byte, v Order) (bool, error) {
    return v.ID != "" && v.Amount > 0, nil
})
```

Transform each record:

```go
type OrderSummary struct {
    OrderID string  `json:"order_id"`
    Amount  float64 `json:"amount"`
}

summary := kstream.Map(valid, func(ctx context.Context, k []byte, v Order) (string, OrderSummary, error) {
    return v.UserID, OrderSummary{OrderID: v.ID, Amount: v.Amount}, nil
})
```

### 4. Write to a Sink

Send the transformed records to an output topic:

```go
kstream.ToWithSerde(summary, "order-summaries", serde.String(), serde.JSON[OrderSummary]())
```

### 5. Build the Topology

```go
t := builder.Build()
```

Use `PrintTree()` to inspect the topology structure during development:

```go
t.PrintTree()
// Output:
// - SOURCE-000001 (Source, topic=orders)
//   - FILTER-000002 (Processor)
//     - MAP-000003 (Processor)
//       - SINK-000004 (Sink, topic=order-summaries)
```

## Creating the Kafka Client

go-streams uses [franz-go](https://github.com/twmb/franz-go) under the hood. Create a client with the options you need:

```go
client, err := kafka.NewKgoClient(
    kafka.WithBootstrapServers([]string{"localhost:9092"}),
    kafka.WithGroupID("my-app"),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

## Running the Application

Create the application and run it:

```go
app, err := streams.NewApplication(client, t)
if err != nil {
    log.Fatal(err)
}

if err := app.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

`app.Run()` uses the `SingleThreadedRunner` by default. To choose a different runner, use `app.RunWith()`:

```go
app.RunWith(ctx, runner.NewSingleThreadedRunner(
    runner.WithLogger(myLogger),
    runner.WithErrorHandler(myHandler),
))
```

See [Runners](runners.md) for details on runner options.

## Graceful Shutdown

Use OS signals to shut down cleanly. Call `app.Close()` to stop the runner, commit offsets, and flush the producer:

```go
go func() {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
    <-ch
    app.Close()
}()

if err := app.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

## Full Example

See [`examples/kgo_complete.go`](https://github.com/hugolhafner/go-streams/blob/main/examples/kgo_complete.go) for a production-ready example with logging, error handling, and graceful shutdown.

## Next Steps

- [KStream DSL](kstream-dsl.md) — all available stream operations
- [Serialization](serialization.md) — built-in serdes and writing custom ones
- [Error Handling](error-handling.md) — retries, dead letter queues, and handler composition
