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

func TestMapProcessor_Process(t *testing.T) {
	t.Run(
		"map primitive types", func(t *testing.T) {
			tests := []struct {
				name     string
				mapper   builtins.MapFunc[int, int, int, int]
				input    *record.Record[int, int]
				expected *record.Record[int, int]
			}{
				{
					name:   "double key and value",
					mapper: func(ctx context.Context, k, v int) (int, int, error) { return k * 2, v * 2, nil },
					input: &record.Record[int, int]{
						Key:   1,
						Value: 2,
					},
					expected: &record.Record[int, int]{
						Key:   2,
						Value: 4,
					},
				},
				{
					name:   "increment key and value",
					mapper: func(ctx context.Context, k, v int) (int, int, error) { return k + 1, v + 1, nil },
					input: &record.Record[int, int]{
						Key:   3,
						Value: 4,
					},
					expected: &record.Record[int, int]{
						Key:   4,
						Value: 5,
					},
				},
			}

			for _, tt := range tests {
				t.Run(
					tt.name, func(t *testing.T) {
						p := builtins.NewMapProcessor(tt.mapper)
						ctx := processor.NewMockContext[int, int]()
						ctx.Mock.On("Forward", mock.Anything, mock.Anything).Return(nil)
						p.Init(ctx)

						err := p.Process(context.Background(), tt.input)
						require.NoError(t, err)
						ctx.AssertCalled(
							t, "Forward",
							mock.Anything,
							&record.Record[int, int]{
								Key:      tt.expected.Key,
								Value:    tt.expected.Value,
								Metadata: tt.input.Metadata,
							},
						)
					},
				)
			}
		},
	)

	t.Run(
		"map to different struct types", func(t *testing.T) {
			type Input struct {
				A int
				B string
			}
			type Output struct {
				X string
				Y int
			}

			tests := []struct {
				name     string
				mapper   builtins.MapFunc[string, Input, string, Output]
				input    *record.Record[string, Input]
				expected *record.Record[string, Output]
			}{
				{
					name: "map Input to Output",
					mapper: func(ctx context.Context, k string, v Input) (string, Output, error) {
						return k + "_mapped", Output{
							X: v.B,
							Y: v.A * 10,
						}, nil
					},
					input: &record.Record[string, Input]{
						Key: "key1",
						Value: Input{
							A: 5,
							B: "value",
						},
					},
					expected: &record.Record[string, Output]{
						Key: "key1_mapped",
						Value: Output{
							X: "value",
							Y: 50,
						},
					},
				},
			}

			for _, tt := range tests {
				t.Run(
					tt.name, func(t *testing.T) {
						p := builtins.NewMapProcessor(tt.mapper)
						ctx := processor.NewMockContext[string, Output]()
						ctx.Mock.On("Forward", mock.Anything, mock.Anything).Return(nil)
						p.Init(ctx)

						err := p.Process(context.Background(), tt.input)
						require.NoError(t, err)
						ctx.AssertCalled(
							t, "Forward", mock.Anything,
							&record.Record[string, Output]{
								Key:      tt.expected.Key,
								Value:    tt.expected.Value,
								Metadata: tt.input.Metadata,
							},
						)
					},
				)
			}
		},
	)
}

func TestMapProcessor_MapperError(t *testing.T) {
	t.Run(
		"mapper error is propagated", func(t *testing.T) {
			mapper := func(ctx context.Context, k, v int) (int, int, error) {
				return 0, 0, errUserFunction
			}

			p := builtins.NewMapProcessor(mapper)
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
		"mapper error with type transformation", func(t *testing.T) {
			mapper := func(ctx context.Context, k string, v int) (string, string, error) {
				if v < 0 {
					return "", "", errors.New("negative values not allowed")
				}
				return k, "transformed", nil
			}

			p := builtins.NewMapProcessor(mapper)
			ctx := processor.NewMockContext[string, string]()
			p.Init(ctx)

			input := &record.Record[string, int]{Key: "key", Value: -1}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.Contains(t, err.Error(), "negative values not allowed")
			ctx.AssertNotCalled(t, "Forward", mock.Anything, mock.Anything)
		},
	)
}

func TestMapProcessor_ForwardError(t *testing.T) {
	t.Run(
		"forward error is propagated", func(t *testing.T) {
			forwardErr := errors.New("forward failed")

			mapper := func(ctx context.Context, k, v int) (int, int, error) {
				return k * 2, v * 2, nil
			}

			p := builtins.NewMapProcessor(mapper)
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
