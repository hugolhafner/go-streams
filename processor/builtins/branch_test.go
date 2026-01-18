package builtins_test

import (
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
				[]func(int, int) bool{
					func(k, v int) bool { return v%2 == 0 },
					func(k, v int) bool { return v > 10 },
				},
				[]string{"even", "greater_than_10"},
			),
			record:           &record.Record[int, int]{Key: 1, Value: 2},
			expectedBranches: []string{"even"},
		},
		{
			name: "multiple branch matches",
			processor: *builtins.NewBranchProcessor(
				[]func(int, int) bool{
					func(k, v int) bool { return v%2 == 0 },
					func(k, v int) bool { return v > 10 },
				},
				[]string{"even", "greater_than_10"},
			),
			record:           &record.Record[int, int]{Key: 2, Value: 12},
			expectedBranches: []string{"even", "greater_than_10"},
		},
		{
			name: "no branch match",
			processor: *builtins.NewBranchProcessor(
				[]func(int, int) bool{
					func(k, v int) bool { return v%2 == 0 },
					func(k, v int) bool { return v > 10 },
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
				ctx.On("ForwardTo", mock.AnythingOfType("string"), tt.record).Return(nil)
				tt.processor.Init(ctx)

				err := tt.processor.Process(tt.record)
				require.NoError(t, err)

				for _, branch := range tt.expectedBranches {
					ctx.AssertCalled(t, "ForwardTo", branch, tt.record)
				}

				if len(tt.expectedBranches) == 0 {
					ctx.AssertNotCalled(t, "ForwardTo", mock.Anything, tt.record)
				}
			},
		)
	}
}
