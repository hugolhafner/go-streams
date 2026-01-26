package builtins_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestFilterProcessor_Process(t *testing.T) {
	tests := []struct {
		name          string
		predicate     func(int, int) bool
		input         *record.Record[int, int]
		shouldForward bool
	}{
		{
			name:          "predicate true",
			predicate:     func(k, v int) bool { return k+v > 0 },
			input:         &record.Record[int, int]{Key: 1, Value: 2},
			shouldForward: true,
		},
		{
			name:          "predicate false",
			predicate:     func(k, v int) bool { return k+v < 0 },
			input:         &record.Record[int, int]{Key: 1, Value: 2},
			shouldForward: false,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				p := builtins.NewFilterProcessor(tt.predicate)
				ctx := processor.NewMockContext[int, int]()
				ctx.Mock.On("Forward", mock.Anything).Return(nil)
				p.Init(ctx)

				err := p.Process(tt.input)
				require.NoError(t, err)

				if tt.shouldForward {
					ctx.AssertCalled(
						t, "Forward",
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
