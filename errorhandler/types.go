package errorhandler

import (
	"context"
)

type ActionType int

const (
	ActionTypeContinue  ActionType = iota // Skip record, continue and commit
	ActionTypeRetry                       // Retry this record
	ActionTypeFail                        // Stop processing and don't commit
	ActionTypeSendToDLQ                   // Send to DLQ, then continue
)

func (a ActionType) String() string {
	switch a {
	case ActionTypeContinue:
		return "Continue"
	case ActionTypeRetry:
		return "Retry"
	case ActionTypeFail:
		return "Fail"
	case ActionTypeSendToDLQ:
		return "SendToDLQ"
	default:
		return "Unknown"
	}
}

var _ Action = ActionContinue{}
var _ Action = ActionRetry{}
var _ Action = ActionFail{}
var _ Action = ActionSendToDLQ{}

type Action interface {
	Type() ActionType
}

type ActionContinue struct{}

func (a ActionContinue) Type() ActionType {
	return ActionTypeContinue
}

type ActionRetry struct{}

func (a ActionRetry) Type() ActionType {
	return ActionTypeRetry
}

type ActionFail struct{}

func (a ActionFail) Type() ActionType {
	return ActionTypeFail
}

type ActionSendToDLQ struct {
	topic string
}

func (a ActionSendToDLQ) Type() ActionType {
	return ActionTypeSendToDLQ
}

func (a ActionSendToDLQ) Topic() string {
	return a.topic
}

type Handler interface {
	Handle(ctx context.Context, ec ErrorContext) Action
}

type HandlerFunc func(ctx context.Context, ec ErrorContext) Action

func (f HandlerFunc) Handle(ctx context.Context, ec ErrorContext) Action {
	return f(ctx, ec)
}
