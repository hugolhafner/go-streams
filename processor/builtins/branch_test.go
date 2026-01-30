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

func TestBranchProcessor_Process(t *testing.T) {
	tests := []struct {
		name             string
		processor        builtins.BranchProcessor[int, int]
		record           *record.Record[int, int]
		expectedBranches []string
	}{
		{
			name: "single branch match",
			processor: *builtins.NewBranchProcessor(
				[]builtins.PredicateFunc[int, int]{
					func(ctx context.Context, k, v int) (bool, error) { return v%2 == 0, nil },
					func(ctx context.Context, k, v int) (bool, error) { return v > 10, nil },
				},
				[]string{"even", "greater_than_10"},
			),
			record:           &record.Record[int, int]{Key: 1, Value: 2},
			expectedBranches: []string{"even"},
		},
		{
			name: "single branch match greater than 10",
			processor: *builtins.NewBranchProcessor(
				[]builtins.PredicateFunc[int, int]{
					func(ctx context.Context, k, v int) (bool, error) { return v%2 == 0, nil },
					func(ctx context.Context, k, v int) (bool, error) { return v > 10, nil },
				},
				[]string{"even", "greater_than_10"},
			),
			record:           &record.Record[int, int]{Key: 2, Value: 12},
			expectedBranches: []string{"even"},
		},
		{
			name: "no branch match",
			processor: *builtins.NewBranchProcessor(
				[]builtins.PredicateFunc[int, int]{
					func(ctx context.Context, k, v int) (bool, error) { return v%2 == 0, nil },
					func(ctx context.Context, k, v int) (bool, error) { return v > 10, nil },
				},
				[]string{"even", "greater_than_10"},
			),
			record:           &record.Record[int, int]{Key: 3, Value: 7},
			expectedBranches: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				ctx := processor.NewMockContext[int, int]()
				ctx.On("ForwardTo", mock.Anything, mock.AnythingOfType("string"), tt.record).Return(nil)
				tt.processor.Init(ctx)

				err := tt.processor.Process(context.Background(), tt.record)
				require.NoError(t, err)

				for _, branch := range tt.expectedBranches {
					ctx.AssertCalled(t, "ForwardTo", mock.Anything, branch, tt.record)
				}

				if len(tt.expectedBranches) == 0 {
					ctx.AssertNotCalled(t, "ForwardTo", mock.Anything, tt.record)
				}
			},
		)
	}
}

func TestBranchProcessor_PredicateError(t *testing.T) {
	t.Run(
		"first predicate error is propagated", func(t *testing.T) {
			predicates := []builtins.PredicateFunc[int, int]{
				func(ctx context.Context, k, v int) (bool, error) {
					return false, errUserFunction
				},
				func(ctx context.Context, k, v int) (bool, error) {
					return true, nil
				},
			}
			branches := []string{"branch1", "branch2"}

			p := builtins.NewBranchProcessor(predicates, branches)
			ctx := processor.NewMockContext[int, int]()
			p.Init(ctx)

			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, errUserFunction)
			ctx.AssertNotCalled(t, "ForwardTo", mock.Anything, mock.Anything, mock.Anything)
		},
	)

	t.Run(
		"second predicate error after first passes", func(t *testing.T) {
			predicates := []builtins.PredicateFunc[int, int]{
				func(ctx context.Context, k, v int) (bool, error) {
					return false, nil // first predicate passes but returns false
				},
				func(ctx context.Context, k, v int) (bool, error) {
					return false, errUserFunction // second predicate errors
				},
			}
			branches := []string{"branch1", "branch2"}

			p := builtins.NewBranchProcessor(predicates, branches)
			ctx := processor.NewMockContext[int, int]()
			p.Init(ctx)

			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, errUserFunction)
			ctx.AssertNotCalled(t, "ForwardTo", mock.Anything, mock.Anything, mock.Anything)
		},
	)
}

func TestBranchProcessor_ForwardToError(t *testing.T) {
	t.Run(
		"forwardTo error is propagated", func(t *testing.T) {
			forwardErr := errors.New("forwardTo failed")

			predicates := []builtins.PredicateFunc[int, int]{
				func(ctx context.Context, k, v int) (bool, error) {
					return true, nil
				},
			}
			branches := []string{"branch1"}

			p := builtins.NewBranchProcessor(predicates, branches)
			ctx := processor.NewMockContext[int, int]()
			ctx.On("ForwardTo", mock.Anything, "branch1", mock.Anything).Return(forwardErr)
			p.Init(ctx)

			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, forwardErr)
		},
	)
}
