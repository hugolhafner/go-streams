package builtins_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/stretchr/testify/require"
)

func TestForEachProcessor_Process(t *testing.T) {
	records := []*record.Record[int, int]{
		{Key: 1, Value: 2},
		{Key: 3, Value: 4},
		{Key: 5, Value: 6},
	}

	for _, r := range records {
		expectedKey := r.Key
		expectedValue := r.Value

		action := func(k, v int) error {
			if k != expectedKey {
				t.Errorf("Expected key %d, got %d", expectedKey, k)
			}
			if v != expectedValue {
				t.Errorf("Expected value %d, got %d", expectedValue, v)
			}
			return nil
		}

		p := builtins.NewForEachProcessor(action)
		err := p.Process(r)
		require.NoError(t, err)
	}
}
