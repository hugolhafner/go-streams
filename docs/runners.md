# Runners

A runner is the execution engine that polls records from Kafka, dispatches them to tasks for processing, handles errors, and manages partition assignments during rebalances.

## Run vs RunWith

`app.Run(ctx)` uses the `SingleThreadedRunner` with default settings:

```go
app.Run(context.Background())
```

`app.RunWith(ctx, factory)` lets you choose the runner and configure it:

```go
app.RunWith(ctx, runner.NewPartitionedRunner(
    runner.WithLogger(myLogger),
    runner.WithErrorHandler(myHandler),
))
```

Both methods block until the context is cancelled or a fatal error occurs.

## SingleThreadedRunner

Processes all partitions sequentially in a single goroutine. This is the default runner.

```go
runner.NewSingleThreadedRunner(opts ...SingleThreadedOption) Factory
```

**How it works:**

1. Subscribes to source topics
2. Polls records in a loop
3. Processes each record sequentially through the topology
4. Marks records for commit after successful processing
5. On shutdown: commits offsets and flushes the producer

**When to use:** Simple applications, low throughput, or when ordering across partitions matters.

### Options

| Option                                 | Description                                                                     |
|----------------------------------------|---------------------------------------------------------------------------------|
| `runner.WithLogger(l)`                 | Set the logger                                                                  |
| `runner.WithErrorHandler(h)`           | Set the error handler (default: `SilentFail`)                                   |
| `runner.WithProcessingErrorHandler(h)` | Handler for processing errors (default: nil, falls back to `ErrorHandler`)      |
| `runner.WithSerdeErrorHandler(h)`      | Handler for deserialization errors (default: nil, falls back to `ErrorHandler`) |
| `runner.WithProductionErrorHandler(h)` | Handler for production/sink errors (default: nil, falls back to `ErrorHandler`) |
| `runner.WithPollErrorBackoff(b)`       | Backoff strategy for poll errors (default: 1s fixed)                            |

## PartitionedRunner

Processes partitions in parallel with one goroutine per partition. Each partition gets a dedicated worker with a buffered channel.

```go
runner.NewPartitionedRunner(opts ...PartitionedOption) Factory
```

**How it works:**

1. Subscribes to source topics
2. On partition assignment: creates a worker goroutine for each partition
3. Polls records and dispatches them to the appropriate partition worker via channels
4. When a worker's channel is full (backpressure): pauses that partition at the Kafka consumer level
5. Resumes partitions once their pending records are drained
6. On shutdown: stops all workers, waits for drain, commits offsets, flushes producer

**When to use:** High throughput, CPU-bound processing, or when partition-level parallelism improves performance.

### Options

All `SingleThreadedRunner` options are also available, plus:

| Option                                 | Default      | Description                                                       |
|----------------------------------------|--------------|-------------------------------------------------------------------|
| `runner.WithLogger(l)`                 | noop         | Set the logger                                                    |
| `runner.WithErrorHandler(h)`           | `SilentFail` | Set the error handler                                             |
| `runner.WithProcessingErrorHandler(h)` | nil          | Handler for processing errors (falls back to `ErrorHandler`)      |
| `runner.WithSerdeErrorHandler(h)`      | nil          | Handler for deserialization errors (falls back to `ErrorHandler`) |
| `runner.WithProductionErrorHandler(h)` | nil          | Handler for production/sink errors (falls back to `ErrorHandler`) |
| `runner.WithPollErrorBackoff(b)`       | 1s fixed     | Backoff for poll errors                                           |
| `runner.WithChannelBufferSize(n)`      | 100          | Buffer size for partition record channels                         |
| `runner.WithWorkerShutdownTimeout(d)`  | 30s          | Max time a single worker spends draining on shutdown              |
| `runner.WithDrainTimeout(d)`           | 60s          | Max time to wait for all workers to finish draining               |

### Backpressure

The `PartitionedRunner` uses channel-based backpressure:

1. Each worker has a buffered channel of size `ChannelBufferSize`
2. When a record can't be submitted (channel full), it's queued in a pending buffer
3. The partition is **paused** at the Kafka consumer - no more records are fetched for it
4. On each poll cycle, pending records are retried
5. When the pending buffer is fully drained, the partition is **resumed**

This prevents unbounded memory growth under load.

## Comparison

| | SingleThreadedRunner | PartitionedRunner |
|-|---------------------|-------------------|
| Concurrency | Single goroutine | One goroutine per partition |
| Ordering | Sequential across all partitions | Sequential within each partition |
| Backpressure | N/A (inline processing) | Channel-based with partition pausing |
| Complexity | Simple | More moving parts |
| Best for | Low throughput, simplicity | High throughput, CPU-bound work |

## Example

```go
app.RunWith(ctx, runner.NewPartitionedRunner(
    runner.WithLogger(myLogger),
    runner.WithErrorHandler(
        errorhandler.WithMaxAttempts(3, backoff.NewFixed(time.Second),
            errorhandler.LogAndFail(myLogger),
        ),
    ),
    runner.WithChannelBufferSize(200),
    runner.WithWorkerShutdownTimeout(15 * time.Second),
    runner.WithDrainTimeout(30 * time.Second),
))
```
