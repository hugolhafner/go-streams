package builtins_test

import (
	"fmt"
	"testing"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPassthroughProcessor_Process(t *testing.T) {
	t.Run(
		"should forward record unchanged", func(t *testing.T) {
			ctx := processor.NewMockContext[[]byte, []byte]()
			ctx.Mock.On("Forward", mock.Anything).Return(nil)

			p := builtins.NewPassthroughProcessor[[]byte, []byte]()
			p.Init(ctx)

			r := &record.Record[[]byte, []byte]{
				Key:   []byte("key"),
				Value: []byte("value"),
			}

			err := p.Process(r)
			require.NoError(t, err)
			ctx.AssertCalled(t, "Forward", r)
		},
	)

	t.Run(
		"should return forward error", func(t *testing.T) {
			ctx := processor.NewMockContext[[]byte, []byte]()
			expectedErr := fmt.Errorf("forward error")
			ctx.Mock.On("Forward", mock.Anything).Return(expectedErr)
			p := builtins.NewPassthroughProcessor[[]byte, []byte]()
			p.Init(ctx)

			r := &record.Record[[]byte, []byte]{
				Key:   []byte("key"),
				Value: []byte("value"),
			}

			err := p.Process(r)
			require.Equal(t, expectedErr, err)
			ctx.AssertCalled(t, "Forward", r)
		},
	)
}
