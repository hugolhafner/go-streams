# go-streams

A Go library for building Kafka stream processing applications, inspired by Java Kafka Streams. Built on [franz-go](https://github.com/twmb/franz-go).

> **Pre-v1 Warning:** This library is under active development. APIs may change between minor versions.

## Install

```bash
go get github.com/hugolhafner/go-streams
```

## Quick Example

```go
package main

import (
    "context"

    streams "github.com/hugolhafner/go-streams"
    "github.com/hugolhafner/go-streams/kafka"
    "github.com/hugolhafner/go-streams/kstream"
    "github.com/hugolhafner/go-streams/serde"
)

type Order struct {
    ID     string  `json:"id"`
    Amount float64 `json:"amount"`
    UserID string  `json:"user_id"`
}

func main() {
    builder := kstream.NewStreamsBuilder()

    orders := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[Order]())

    valid := kstream.Filter(orders, func(ctx context.Context, k []byte, v Order) (bool, error) {
        return v.ID != "" && v.Amount > 0, nil
    })

    mapped := kstream.MapValues(valid, func(ctx context.Context, v Order) (string, error) {
        return v.UserID, nil
    })

    kstream.ToWithKeySerde(mapped, "order-users", serde.String())

    t := builder.Build()
    client, _ := kafka.NewKgoClient(
        kafka.WithBootstrapServers([]string{"localhost:9092"}),
        kafka.WithGroupID("my-app"),
    )
    defer client.Close()

    app, _ := streams.NewApplication(client, t)
    app.Run(context.Background())
}
```

## Where to Go Next

- [Getting Started](getting-started.md) - step-by-step walkthrough of your first topology
- [KStream DSL](kstream-dsl.md) - all stream operations (`Filter`, `Map`, `Branch`, `To`, etc.)
- [Serialization](serialization.md) - built-in and custom serdes
- [Error Handling](error-handling.md) - retry, DLQ, and handler composition
- [Runners](runners.md) - `SingleThreadedRunner` vs `PartitionedRunner`
- [Observability](observability.md) - OpenTelemetry tracing and metrics
- [Custom Processors](custom-processors.md) - implementing the `Processor` interface
