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

// SerdeError wraps errors that occur during key/value serialization or deserialization.
type SerdeError struct {
	Cause error
}

func (e *SerdeError) Error() string {
	return e.Cause.Error()
}

func (e *SerdeError) Unwrap() error {
	return e.Cause
}

func NewSerdeError(cause error) error {
	return &SerdeError{Cause: cause}
}

func AsSerdeError(err error) (*SerdeError, bool) {
	var de *SerdeError
	if errors.As(err, &de) {
		return de, true
	}
	return nil, false
}

// ProductionError wraps errors that occur during sink production.
type ProductionError struct {
	Cause error
	Node  string
}

func (e *ProductionError) Error() string {
	return e.Cause.Error()
}

func (e *ProductionError) Unwrap() error {
	return e.Cause
}

func NewProductionError(cause error, node string) error {
	return &ProductionError{Cause: cause, Node: node}
}

func AsProductionError(err error) (*ProductionError, bool) {
	var pe *ProductionError
	if errors.As(err, &pe) {
		return pe, true
	}
	return nil, false
}
