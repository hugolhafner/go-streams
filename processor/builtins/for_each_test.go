//go:build unit

package builtins_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/stretchr/testify/require"
)

func TestForEachProcessor_Process(t *testing.T) {
	t.Parallel()
	records := []*record.Record[int, int]{
		{Key: 1, Value: 2},
		{Key: 3, Value: 4},
		{Key: 5, Value: 6},
	}

	for _, r := range records {
		expectedKey := r.Key
		expectedValue := r.Value

		action := func(_ context.Context, k, v int) error {
			if k != expectedKey {
				t.Errorf("Expected key %d, got %d", expectedKey, k)
			}
			if v != expectedValue {
				t.Errorf("Expected value %d, got %d", expectedValue, v)
			}
			return nil
		}

		p := builtins.NewForEachProcessor(action)
		err := p.Process(context.Background(), r)
		require.NoError(t, err)
	}
}

func TestForEachProcessor_ActionError(t *testing.T) {
	t.Parallel()
	t.Run(
		"action error is propagated", func(t *testing.T) {
			t.Parallel()
			action := func(ctx context.Context, k, v int) error {
				return errUserFunction
			}

			p := builtins.NewForEachProcessor(action)
			input := &record.Record[int, int]{Key: 1, Value: 2}
			err := p.Process(context.Background(), input)

			require.Error(t, err)
			require.ErrorIs(t, err, errUserFunction)
		},
	)

	t.Run(
		"action error with specific condition", func(t *testing.T) {
			t.Parallel()
			action := func(ctx context.Context, k, v int) error {
				if v > 100 {
					return errors.New("value too large")
				}
				return nil
			}

			p := builtins.NewForEachProcessor(action)

			input1 := &record.Record[int, int]{Key: 1, Value: 50}
			err := p.Process(context.Background(), input1)
			require.NoError(t, err)

			input2 := &record.Record[int, int]{Key: 1, Value: 150}
			err = p.Process(context.Background(), input2)
			require.Error(t, err)
			require.Contains(t, err.Error(), "value too large")
		},
	)
}
