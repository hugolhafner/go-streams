package errorhandler

import (
	"context"
)

type Action int

const (
	ActionContinue  Action = iota // Skip record, continue and commit
	ActionRetry                   // Retry this record
	ActionFail                    // Stop processing and don't commit
	ActionSendToDLQ               // Send to DLQ, then continue
)

func (a Action) String() string {
	switch a {
	case ActionContinue:
		return "Continue"
	case ActionRetry:
		return "Retry"
	case ActionFail:
		return "Fail"
	case ActionSendToDLQ:
		return "SendToDLQ"
	default:
		return "Unknown"
	}
}

type Handler interface {
	Handle(ctx context.Context, ec ErrorContext) Action
}

type HandlerFunc func(ctx context.Context, ec ErrorContext) Action

func (f HandlerFunc) Handle(ctx context.Context, ec ErrorContext) Action {
	return f(ctx, ec)
}
