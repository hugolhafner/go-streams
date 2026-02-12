//go:build unit

package otel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNewTelemetry_WithProviders(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	mp := sdkmetric.NewMeterProvider()
	defer tp.Shutdown(nil)
	defer mp.Shutdown(nil)

	tel, err := NewTelemetry(tp, mp, nil)
	require.NoError(t, err)
	assert.NotNil(t, tel.Tracer)
	assert.NotNil(t, tel.Propagator)
	assert.NotNil(t, tel.MessagesConsumed)
	assert.NotNil(t, tel.PollDuration)
	assert.NotNil(t, tel.ProcessDuration)
	assert.NotNil(t, tel.MessagesProduced)
	assert.NotNil(t, tel.ProduceDuration)
	assert.NotNil(t, tel.Errors)
	assert.NotNil(t, tel.ErrorHandlerActions)
	assert.NotNil(t, tel.TasksActive)
}

func TestNewTelemetry_NilProviders(t *testing.T) {
	tel, err := NewTelemetry(nil, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, tel.Tracer)
	assert.NotNil(t, tel.Propagator)
}

func TestNoop(t *testing.T) {
	tel := Noop()
	assert.NotNil(t, tel)
	assert.NotNil(t, tel.Tracer)
}
