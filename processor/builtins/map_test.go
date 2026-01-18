package builtins_test

import (
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
				mapper   func(int, int) (int, int)
				input    *record.Record[int, int]
				expected *record.Record[int, int]
			}{
				{
					name:   "double key and value",
					mapper: func(k, v int) (int, int) { return k * 2, v * 2 },
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
					mapper: func(k, v int) (int, int) { return k + 1, v + 1 },
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
						ctx.Mock.On("Forward", mock.Anything).Return(nil)
						p.Init(ctx)

						err := p.Process(tt.input)
						require.NoError(t, err)
						ctx.AssertCalled(
							t, "Forward",
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
				mapper   func(string, Input) (string, Output)
				input    *record.Record[string, Input]
				expected *record.Record[string, Output]
			}{
				{
					name: "map Input to Output",
					mapper: func(k string, v Input) (string, Output) {
						return k + "_mapped", Output{
							X: v.B,
							Y: v.A * 10,
						}
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
						ctx.Mock.On("Forward", mock.Anything).Return(nil)
						p.Init(ctx)

						err := p.Process(tt.input)
						require.NoError(t, err)
						ctx.AssertCalled(
							t, "Forward",
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
