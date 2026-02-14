//go:build unit

package builtins_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var errUserFunction = errors.New("user function error")

func TestFilterProcessor_Process(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		predicate     builtins.PredicateFunc[int, int]
		input         *record.Record[int, int]
		shouldForward bool
	}{
		{
			name:          "predicate true",
			predicate:     func(ctx context.Context, k, v int) (bool, error) { return k+v > 0, nil },
			input:         &record.Record[int, int]{Key: 1, Value: 2},
			shouldForward: true,
		},
		{
			name:          "predicate false",
			predicate:     func(ctx context.Context, k, v int) (bool, error) { return k+v < 0, nil },
			input:         &record.Record[int, int]{Key: 1, Value: 2},
			shouldForward: false,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				p := builtins.NewFilterProcessor(tt.predicate)
				ctx := processor.NewMockContext[int, int]()
				ctx.Mock.On("Forward", mock.Anything, mock.Anything).Return(nil)
				p.Init(ctx)

				err := p.Process(context.Background(), tt.input)
				require.NoError(t, err)

				if tt.shouldForward {
					ctx.AssertCalled(
						t, "Forward", mock.Anything,
						&record.Record[int, int]{
							Key:   tt.input.Key,
							Value: tt.input.Value,
						},
					)
				} else {
					ctx.AssertNotCalled(t, "Forward", mock.Anything)
				}
			},
		)
	}
}

func TestFilterProcessor_PredicateError(t *testing.T) {
	t.Parallel()
	t.Run(
		"predicate error is propagated", func(t *testing.T) {
			t.Parallel()
			predicate := func(ctx context.Context, k, v int) (bool, error) {
				return false, errUserFunction
			}

			p := builtins.NewFilterProcessor(predicate)
			ctx := processor.NewMockContext[int, int]()
			p.Init(ctx)

			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, errUserFunction)
			ctx.AssertNotCalled(t, "Forward", mock.Anything, mock.Anything)
		},
	)

	t.Run(
		"predicate error takes precedence over forward", func(t *testing.T) {
			t.Parallel()
			// Even if the predicate would return true, an error should stop processing
			predicate := func(ctx context.Context, k, v int) (bool, error) {
				return true, errUserFunction // returns true but also error
			}

			p := builtins.NewFilterProcessor(predicate)
			ctx := processor.NewMockContext[int, int]()
			p.Init(ctx)

			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, errUserFunction)
			ctx.AssertNotCalled(t, "Forward", mock.Anything, mock.Anything)
		},
	)
}

func TestFilterProcessor_ForwardError(t *testing.T) {
	t.Parallel()
	t.Run(
		"forward error is propagated", func(t *testing.T) {
			t.Parallel()
			forwardErr := errors.New("forward failed")

			predicate := func(ctx context.Context, k, v int) (bool, error) {
				return true, nil
			}

			p := builtins.NewFilterProcessor(predicate)
			ctx := processor.NewMockContext[int, int]()
			ctx.On("Forward", mock.Anything, mock.Anything).Return(forwardErr)
			p.Init(ctx)

			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, forwardErr)
		},
	)
}
