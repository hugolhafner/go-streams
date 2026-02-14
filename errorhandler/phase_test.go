//go:build unit

package errorhandler_test

import (
	"context"
	"testing"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorPhase_String(t *testing.T) {
	tests := []struct {
		phase    errorhandler.ErrorPhase
		expected string
	}{
		{errorhandler.PhaseUnknown, "unknown"},
		{errorhandler.PhaseSerde, "serde"},
		{errorhandler.PhaseProcessing, "processing"},
		{errorhandler.PhaseProduction, "production"},
		{errorhandler.ErrorPhase(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(
			tt.expected, func(t *testing.T) {
				require.Equal(t, tt.expected, tt.phase.String())
			},
		)
	}
}

// actionHandler returns a handler that always returns the given action.
func actionHandler(a errorhandler.Action) errorhandler.Handler {
	return errorhandler.HandlerFunc(
		func(_ context.Context, _ errorhandler.ErrorContext) errorhandler.Action {
			return a
		},
	)
}

func ecWithPhase(phase errorhandler.ErrorPhase) errorhandler.ErrorContext {
	return errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil).WithPhase(phase)
}

func TestPhaseRouter_RoutesToSerdeHandler(t *testing.T) {
	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionFail{}),
		actionHandler(errorhandler.ActionContinue{}), // serde
		nil,
		nil,
	)

	action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseSerde))
	assert.IsType(t, errorhandler.ActionContinue{}, action)
}

func TestPhaseRouter_RoutesToProcessingHandler(t *testing.T) {
	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionFail{}),
		nil,
		actionHandler(errorhandler.ActionRetry{}), // processing
		nil,
	)

	action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseProcessing))
	assert.IsType(t, errorhandler.ActionRetry{}, action)
}

func TestPhaseRouter_RoutesToProductionHandler(t *testing.T) {
	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionFail{}),
		nil,
		nil,
		actionHandler(errorhandler.ActionContinue{}), // production
	)

	action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseProduction))
	assert.IsType(t, errorhandler.ActionContinue{}, action)
}

func TestPhaseRouter_FallsBackToDefaultHandler(t *testing.T) {
	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionRetry{}), // default
		nil, nil, nil,
	)

	tests := []errorhandler.ErrorPhase{
		errorhandler.PhaseUnknown,
		errorhandler.PhaseSerde,
		errorhandler.PhaseProcessing,
		errorhandler.PhaseProduction,
	}

	for _, phase := range tests {
		t.Run(
			phase.String(), func(t *testing.T) {
				action := router.Handle(context.Background(), ecWithPhase(phase))
				assert.IsType(t, errorhandler.ActionRetry{}, action)
			},
		)
	}
}

func TestPhaseRouter_UnknownPhaseFallsBackToDefault(t *testing.T) {
	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionContinue{}),
		actionHandler(errorhandler.ActionFail{}),     // serde - should NOT be selected
		actionHandler(errorhandler.ActionRetry{}),    // processing
		actionHandler(errorhandler.ActionContinue{}), // production
	)

	action := router.Handle(context.Background(), ecWithPhase(errorhandler.ErrorPhase(99)))
	assert.IsType(t, errorhandler.ActionContinue{}, action)
}

func TestPhaseRouter_NilDefaultUsesSilentFail(t *testing.T) {
	router := errorhandler.NewPhaseRouter(nil, nil, nil, nil)

	action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseProcessing))
	assert.IsType(t, errorhandler.ActionFail{}, action)
}

func TestPhaseRouter_SpecificHandlerTakesPrecedenceOverDefault(t *testing.T) {
	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionFail{}),     // default
		actionHandler(errorhandler.ActionContinue{}), // serde
		actionHandler(errorhandler.ActionRetry{}),    // processing
		actionHandler(errorhandler.ActionContinue{}), // production
	)

	t.Run(
		"serde gets serde handler", func(t *testing.T) {
			action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseSerde))
			assert.IsType(t, errorhandler.ActionContinue{}, action)
		},
	)

	t.Run(
		"processing gets processing handler", func(t *testing.T) {
			action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseProcessing))
			assert.IsType(t, errorhandler.ActionRetry{}, action)
		},
	)

	t.Run(
		"production gets production handler", func(t *testing.T) {
			action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseProduction))
			assert.IsType(t, errorhandler.ActionContinue{}, action)
		},
	)

	t.Run(
		"unknown phase gets default handler", func(t *testing.T) {
			action := router.Handle(context.Background(), ecWithPhase(errorhandler.PhaseUnknown))
			assert.IsType(t, errorhandler.ActionFail{}, action)
		},
	)

	t.Run(
		"unrecognized phase gets default handler", func(t *testing.T) {
			action := router.Handle(context.Background(), ecWithPhase(errorhandler.ErrorPhase(42)))
			assert.IsType(t, errorhandler.ActionFail{}, action)
		},
	)
}

func TestPhaseRouter_PassesErrorContextToHandler(t *testing.T) {
	var captured errorhandler.ErrorContext
	captureHandler := errorhandler.HandlerFunc(
		func(_ context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
			captured = ec
			return errorhandler.ActionContinue{}
		},
	)

	router := errorhandler.NewPhaseRouter(
		actionHandler(errorhandler.ActionFail{}),
		captureHandler, // serde
		nil, nil,
	)

	ec := errorhandler.NewErrorContext(
		kafka.ConsumerRecord{Topic: "test-topic", Partition: 3, Offset: 42}, nil,
	).WithPhase(errorhandler.PhaseSerde).WithNodeName("source-1")

	router.Handle(context.Background(), ec)

	assert.Equal(t, "test-topic", captured.Record.Topic)
	assert.Equal(t, int32(3), captured.Record.Partition)
	assert.Equal(t, int64(42), captured.Record.Offset)
	assert.Equal(t, "source-1", captured.NodeName)
	assert.Equal(t, errorhandler.PhaseSerde, captured.Phase)
}
