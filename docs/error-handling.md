# Error Handling

go-streams uses a composable error handler system. When a processor returns an error, the runner passes it to the configured error handler, which decides what action to take.

## Error Flow

```
Processor returns error
    → Runner creates ErrorContext
    → Handler.Handle(ctx, errorContext) returns Action
    → Runner executes the Action
```

## Actions

An error handler returns one of four actions:

| Action                   | Behavior                                                    |
|--------------------------|-------------------------------------------------------------|
| `ActionContinue{}`       | Skip the record and continue processing                     |
| `ActionRetry{}`          | Retry processing the same record                            |
| `ActionFail{}`           | Stop the runner with the error                              |
| `ActionSendToDLQ{topic}` | Send the record to a dead letter queue topic, then continue |

## ErrorContext

The handler receives an `ErrorContext` with full details about the failure:

```go
type ErrorContext struct {
    Record   kafka.ConsumerRecord  // The record that caused the error
    Error    error                 // The error from the processor
    Attempt  int                   // Current attempt number (1-indexed)
    NodeName string                // Topology node where the error occurred
    Phase    ErrorPhase            // Pipeline phase where the error occurred
}
```

## Error Phases

Every error is classified into a phase that indicates where in the pipeline it occurred:

| Phase             | Value | Description                                          |
|-------------------|-------|------------------------------------------------------|
| `PhaseSerde`      | `0`   | Error during key/value deserialization at the source |
| `PhaseProcessing` | `1`   | Error during processor execution                     |
| `PhaseProduction` | `2`   | Error during sink serialization or Kafka production  |

The phase is set automatically by the runner and available on `ErrorContext.Phase`. You can use `phase.String()` to 
get a human-readable name (e.g. `"serde"`, `"processing"`, `"production"`).

## Built-in Handlers

### LogAndContinue

Logs the error and skips the record:

```go
errorhandler.LogAndContinue(myLogger)
```

### LogAndFail

Logs the error and stops processing:

```go
errorhandler.LogAndFail(myLogger)
```

### WithMaxAttempts

Retries processing up to N times with backoff. When the limit is reached, delegates to a fallback handler:

```go
errorhandler.WithMaxAttempts(3, backoff.NewFixed(time.Second), fallbackHandler)
```

The `backoff.Backoff` parameter controls delay between retries. `backoff.NewFixed(duration)` uses a constant delay.

### WithDLQ

Intercepts `Continue` actions from the inner handler and replaces them with `SendToDLQ`. Other actions pass through unchanged:

```go
errorhandler.WithDLQ("my-dlq-topic", innerHandler)
```

### ActionLogger

Wraps a handler and logs the action it decides:

```go
errorhandler.ActionLogger(myLogger, logger.InfoLevel, innerHandler)
```

## Composing Handlers

Handlers are designed to be composed. Here is the error handler from the `kgo_complete` example:

```go
handler := errorhandler.ActionLogger(
    myLogger,
    logger.InfoLevel,
    errorhandler.WithMaxAttempts(
        3, backoff.NewFixed(time.Second),
        errorhandler.WithDLQ("dlq", errorhandler.LogAndContinue(myLogger)),
    ),
)
```

This creates the following pipeline:

1. **ActionLogger** — logs every handler decision at `Info` level
2. **WithMaxAttempts** — retries up to 3 times with 1-second backoff
3. When retries are exhausted, **WithDLQ** — sends to the `"dlq"` topic
4. **LogAndContinue** — logs the error (this would normally return `Continue`, but `WithDLQ` intercepts it and returns `SendToDLQ` instead)

## Custom Handlers

Implement the `Handler` interface:

```go
type Handler interface {
    Handle(ctx context.Context, ec ErrorContext) Action
}
```

Or use `HandlerFunc` for a quick inline handler:

```go
handler := errorhandler.HandlerFunc(func(ctx context.Context, ec ErrorContext) errorhandler.Action {
    if errors.Is(ec.Error, ErrTransient) {
        return errorhandler.ActionRetry{}
    }
    return errorhandler.ActionFail{}
})
```

## Configuring the Handler

Pass the handler when creating a runner:

```go
app.RunWith(ctx, runner.NewSingleThreadedRunner(
    runner.WithErrorHandler(handler),
))
```

The default error handler is `LogAndContinue` with a noop logger.

## Phase-Specific Error Handlers

By default, `WithErrorHandler` handles errors from all phases. You can optionally set separate handlers for serde and production errors:

```go
app.RunWith(ctx, runner.NewPartitionedRunner(
    runner.WithErrorHandler(defaultHandler),                          // fallback for all phases
    runner.WithSerdeErrorHandler(poisonPillHandler),                  // serialization / deserialization errors only
    runner.WithProductionErrorHandler(productionHandler),             // production/sink errors only
))
```

The routing logic:

| Error Phase | Handler Used                                              |
|-------------|-----------------------------------------------------------|
| Serde       | `SerdeErrorHandler` if set, otherwise `ErrorHandler`      |
| Processing  | `ProcessingErrorHandler` if set, otherwise `ErrorHandler` |
| Production  | `ProductionErrorHandler` if set, otherwise `ErrorHandler` |

All phase-specific handlers default to `nil`, which means the general `ErrorHandler` is used as fallback.

### Example: Skip Poison Pills, Fail on Production Errors

```go
app.RunWith(ctx, runner.NewPartitionedRunner(
    // Default: retry up to 3 times, then skip
    runner.WithErrorHandler(
        errorhandler.WithMaxAttempts(3, backoff.NewFixed(time.Second),
            errorhandler.LogAndContinue(myLogger),
        ),
    ),
    // Serialization / Deserialization: always skip (poison pill / bad data)
    runner.WithSerdeErrorHandler(
        errorhandler.LogAndContinue(myLogger),
    ),
    // Production: always fail immediately
    runner.WithProductionErrorHandler(
        errorhandler.LogAndFail(myLogger),
    ),
))
```

### Inspecting Error Phase in Custom Handlers

Phase information is available on `ErrorContext` for custom routing logic:

```go
handler := errorhandler.HandlerFunc(func(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
    switch ec.Phase {
    case errorhandler.PhaseSerde:
        return errorhandler.ActionContinue{}  // skip bad records
    case errorhandler.PhaseProduction:
        return errorhandler.ActionFail{}      // fail on sink errors
    default:
        return errorhandler.ActionRetry{}     // retry processing errors
    }
})
```
