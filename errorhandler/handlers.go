package errorhandler

import (
	"context"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/logger"
)

// LogAndContinue logs error and continues processing
func LogAndContinue(logger logger.Logger) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			logger.Error(
				"error processing record, skipping",
				"error", ec.Error,
				"key", ec.Record.Key,
				"topic", ec.Record.Topic,
				"offset", ec.Record.Offset,
				"partition", ec.Record.Partition,
				"attempt", ec.Attempt,
				"node", ec.NodeName,
			)
			return ActionContinue{}
		},
	)
}

// LogAndFail logs error and stops processing
func LogAndFail(logger logger.Logger) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			logger.Error(
				"error processing record, failing",
				"error", ec.Error,
				"key", ec.Record.Key,
				"topic", ec.Record.Topic,
				"offset", ec.Record.Offset,
				"partition", ec.Record.Partition,
				"attempt", ec.Attempt,
				"node", ec.NodeName,
			)
			return ActionFail{}
		},
	)
}

// WithMaxAttempts wraps a handler with retry logic
// When the max attempts is reached, the fallback handler is called
func WithMaxAttempts(maxAttempts int, b backoff.Backoff, fallback Handler) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			select {
			case <-ctx.Done():
				return ActionFail{}
			case <-time.After(b.Next(uint(ec.Attempt))):
			}

			if ec.Attempt < maxAttempts {
				return ActionRetry{}
			}

			return fallback.Handle(ctx, ec)
		},
	)
}

// WithDLQ returns SendToDLQ action when inner would Continue
// Useful for: WithMaxAttempts(3, backoff, WithDLQ(inner))
func WithDLQ(topic string, inner Handler) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			var action Action = ActionContinue{}
			if inner != nil {
				action = inner.Handle(ctx, ec)
			}

			if action.Type() == ActionTypeContinue {
				return ActionSendToDLQ{topic: topic}
			}

			return action
		},
	)
}

// ActionLogger logs the action decided by the next handler
func ActionLogger(l logger.Logger, level logger.LogLevel, next Handler) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			action := next.Handle(ctx, ec)

			l.Log(
				level,
				"Error handler decision",
				"action", action.Type().String(),
				"error", ec.Error,
				"key", ec.Record.Key,
				"topic", ec.Record.Topic,
				"offset", ec.Record.Offset,
				"partition", ec.Record.Partition,
				"attempt", ec.Attempt,
				"node", ec.NodeName,
			)
			return action
		},
	)
}
