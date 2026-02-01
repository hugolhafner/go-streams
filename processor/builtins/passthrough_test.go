//go:build unit

package builtins_test

import (
	"context"
	"errors"
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
			ctx.Mock.On("Forward", mock.Anything, mock.Anything).Return(nil)

			p := builtins.NewPassthroughProcessor[[]byte, []byte]()
			p.Init(ctx)

			r := &record.Record[[]byte, []byte]{
				Key:   []byte("key"),
				Value: []byte("value"),
			}

			err := p.Process(context.Background(), r)
			require.NoError(t, err)
			ctx.AssertCalled(t, "Forward", mock.Anything, r)
		},
	)

	t.Run(
		"should return forward error", func(t *testing.T) {
			ctx := processor.NewMockContext[[]byte, []byte]()
			expectedErr := fmt.Errorf("forward error")
			ctx.Mock.On("Forward", mock.Anything, mock.Anything).Return(expectedErr)
			p := builtins.NewPassthroughProcessor[[]byte, []byte]()
			p.Init(ctx)

			r := &record.Record[[]byte, []byte]{
				Key:   []byte("key"),
				Value: []byte("value"),
			}

			err := p.Process(context.Background(), r)
			require.Equal(t, expectedErr, err)
			ctx.AssertCalled(t, "Forward", mock.Anything, r)
		},
	)
}

func TestPassthroughProcessor_ForwardError(t *testing.T) {
	t.Run(
		"forward error is propagated", func(t *testing.T) {
			forwardErr := errors.New("forward failed")

			p := builtins.NewPassthroughProcessor[int, int]()
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
