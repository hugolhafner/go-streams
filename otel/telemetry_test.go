//go:build unit

package otel

import (
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNewTelemetry_WithProviders(t *testing.T) {
	t.Parallel()
	tp := sdktrace.NewTracerProvider()
	mp := sdkmetric.NewMeterProvider()
	defer tp.Shutdown(nil)
	defer mp.Shutdown(nil)

	tel, err := NewTelemetry(tp, mp, nil)
	require.NoError(t, err)
	require.NotNil(t, tel.Tracer)
	require.NotNil(t, tel.Propagator)
	require.NotNil(t, tel.MessagesConsumed)
	require.NotNil(t, tel.PollDuration)
	require.NotNil(t, tel.ProcessDuration)
	require.NotNil(t, tel.MessagesProduced)
	require.NotNil(t, tel.ProduceDuration)
	require.NotNil(t, tel.Errors)
	require.NotNil(t, tel.ErrorHandlerActions)
	require.NotNil(t, tel.TasksActive)
}

func TestNewTelemetry_NilProviders(t *testing.T) {
	t.Parallel()
	tel, err := NewTelemetry(nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, tel.Tracer)
	require.NotNil(t, tel.Propagator)
}

func TestNoop(t *testing.T) {
	t.Parallel()
	tel := Noop()
	require.NotNil(t, tel)
	require.NotNil(t, tel.Tracer)
}
