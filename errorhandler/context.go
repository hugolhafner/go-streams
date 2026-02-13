package errorhandler

import (
	"github.com/hugolhafner/go-streams/kafka"
)

// ErrorContext provides context about an error that occurred during processing.
// It contains all the information a handler needs to make a decision about
// how to handle the error.
type ErrorContext struct {
	// Record is the Kafka record that caused the error.
	Record kafka.ConsumerRecord

	// Error is the error that occurred during processing.
	Error error

	// Attempt is current attempt number, 1 indexed.
	Attempt int

	// NodeName is the name of the topology node where the error occurred.
	// empty if not during node processing (eg. serialization/deserialization).
	NodeName string

	// Phase indicates where in the pipeline the error occurred
	Phase ErrorPhase
}

func NewErrorContext(record kafka.ConsumerRecord, err error) ErrorContext {
	return ErrorContext{
		Record:  record.Copy(),
		Error:   err,
		Attempt: 1,
	}
}

func (ec ErrorContext) WithError(err error) ErrorContext {
	ec.Error = err
	return ec
}

func (ec ErrorContext) WithAttempt(attempt int) ErrorContext {
	ec.Attempt = attempt
	return ec
}

func (ec ErrorContext) WithNodeName(name string) ErrorContext {
	ec.NodeName = name
	return ec
}

func (ec ErrorContext) WithPhase(phase ErrorPhase) ErrorContext {
	ec.Phase = phase
	return ec
}

func (ec ErrorContext) IncrementAttempt() ErrorContext {
	ec.Attempt++
	return ec
}
