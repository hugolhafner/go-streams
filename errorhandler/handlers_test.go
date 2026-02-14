//go:build unit

package errorhandler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	mocklogger "github.com/hugolhafner/go-streams/logger/mock"
	"github.com/stretchr/testify/require"
)

func TestLogAndContinue(t *testing.T) {
	t.Parallel()
	var testErr = errors.New("processing failed")

	tests := []struct {
		name string
		err  error
	}{
		{"simple error", testErr},
		{"nil error", nil},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil)

				l := mocklogger.New()
				h := errorhandler.LogAndContinue(l)
				action := h.Handle(context.Background(), ec.WithError(tt.err))

				require.Equal(t, errorhandler.ActionContinue{}, action)
				l.AssertCalledWithLevelAndMessage(t, logger.ErrorLevel, "error processing record, skipping")
			},
		)
	}
}

func TestLogAndFail(t *testing.T) {
	t.Parallel()
	var testErr = errors.New("processing failed")

	tests := []struct {
		name string
		err  error
	}{
		{"simple error", testErr},
		{"nil error", nil},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil)

				l := mocklogger.New()
				h := errorhandler.LogAndFail(l)
				action := h.Handle(context.Background(), ec.WithError(tt.err))

				require.Equal(t, errorhandler.ActionFail{}, action)
				l.AssertCalledWithLevelAndMessage(t, logger.ErrorLevel, "error processing record, failing")
			},
		)
	}
}

func TestWithMaxAttempts(t *testing.T) {
	t.Parallel()
	t.Run(
		"should call fallback after max attempts", func(t *testing.T) {
			t.Parallel()
			var testErr = errors.New("processing failed")
			var maxAttempts = 3

			ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, testErr)

			fallbackCalled := false
			fallback := errorhandler.HandlerFunc(
				func(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
					fallbackCalled = true
					return errorhandler.ActionFail{}
				},
			)

			h := errorhandler.WithMaxAttempts(
				maxAttempts,
				backoff.NewFixed(0),
				fallback,
			)

			for i := 1; i < maxAttempts; i++ {
				action := h.Handle(context.Background(), ec.WithAttempt(i))
				require.False(t, fallbackCalled, "fallback should not be called yet on attempt %d", i)
				require.Equal(t, errorhandler.ActionRetry{}, action)
			}

			action := h.Handle(context.Background(), ec.WithAttempt(maxAttempts+1))
			require.True(t, fallbackCalled, "fallback should have been called")
			require.Equal(t, errorhandler.ActionFail{}, action)
		},
	)

	t.Run(
		"should wait on attempts", func(t *testing.T) {
			t.Parallel()
			var testErr = errors.New("processing failed")
			var maxAttempts = 3

			ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, testErr)

			fallbackCalled := false
			fallback := errorhandler.HandlerFunc(
				func(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
					fallbackCalled = true
					return errorhandler.ActionFail{}
				},
			)

			h := errorhandler.WithMaxAttempts(
				maxAttempts,
				backoff.NewFixed(100*time.Millisecond),
				fallback,
			)

			start := time.Now()
			action := h.Handle(context.Background(), ec.WithAttempt(2))
			elapsed := time.Since(start)

			require.False(t, fallbackCalled, "fallback should not be called yet")
			require.Equal(t, errorhandler.ActionRetry{}, action)
			require.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "should have waited on retry attempt")
		},
	)

	t.Run(
		"should respect context cancellation", func(t *testing.T) {
			t.Parallel()
			var testErr = errors.New("processing failed")
			var maxRetries = 3

			ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, testErr)

			fallbackCalled := false
			fallback := errorhandler.HandlerFunc(
				func(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
					fallbackCalled = true
					return errorhandler.ActionFail{}
				},
			)

			h := errorhandler.WithMaxAttempts(
				maxRetries,
				backoff.NewFixed(time.Millisecond),
				fallback,
			)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			action := h.Handle(ctx, ec)
			require.False(t, fallbackCalled, "fallback should not be called yet")
			require.Equal(
				t, errorhandler.ActionFail{}, action, "expected ActionTypeFail on context cancellation, got: %v",
				action.Type().String(),
			)
		},
	)
}
