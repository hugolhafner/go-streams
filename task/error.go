package task

import (
	"errors"
)

type ProcessError struct {
	Cause error
	Node  string
}

func (e *ProcessError) Error() string {
	return e.Cause.Error()
}

func (e *ProcessError) Unwrap() error {
	return e.Cause
}

func NewProcessError(cause error, node string) error {
	return &ProcessError{
		Cause: cause,
		Node:  node,
	}
}

func AsProcessError(err error) (*ProcessError, bool) {
	var pe *ProcessError
	if errors.As(err, &pe) {
		return pe, true
	}

	return nil, false
}
