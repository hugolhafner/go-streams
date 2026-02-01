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
			return ActionContinue
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
			return ActionFail
		},
	)
}

// WithRetry wraps a handler with retry logic
// When retries exhausted, delegates to fallback
func WithRetry(maxRetries int, b backoff.Backoff, fallback Handler) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			select {
			case <-ctx.Done():
				return ActionFail
			case <-time.After(b.Next(uint(ec.Attempt))):
			}

			if ec.Attempt <= maxRetries {
				return ActionRetry
			}

			return fallback.Handle(ctx, ec)
		},
	)
}

// WithDLQ returns SendToDLQ action when inner would Continue
// Useful for: WithRetry(3, backoff, WithDLQ(inner))
func WithDLQ(inner Handler) Handler {
	return HandlerFunc(
		func(ctx context.Context, ec ErrorContext) Action {
			action := ActionContinue
			if inner != nil {
				action = inner.Handle(ctx, ec)
			}

			if action == ActionContinue {
				return ActionSendToDLQ
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
				"action", action.String(),
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
