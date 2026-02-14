//go:build unit

package task_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hugolhafner/go-streams/task"
	"github.com/stretchr/testify/require"
)

func TestProcessError(t *testing.T) {
	cause := errors.New("processing failed")
	err := task.NewProcessError(cause, "proc-1")

	require.Equal(t, "processing failed", err.Error())
	require.ErrorIs(t, err, cause)

	pe, ok := task.AsProcessError(err)
	require.True(t, ok)
	require.Equal(t, "proc-1", pe.Node)
	require.Equal(t, cause, pe.Cause)
}

func TestDeserializationError(t *testing.T) {
	cause := fmt.Errorf("deserialize key: %w", errors.New("invalid utf8"))
	err := task.NewSerdeError(cause)

	require.Contains(t, err.Error(), "deserialize key")
	require.ErrorIs(t, err, cause)

	de, ok := task.AsSerdeError(err)
	require.True(t, ok)
	require.Equal(t, cause, de.Cause)

	// Should not match other error types
	_, ok = task.AsProcessError(err)
	require.False(t, ok)
	_, ok = task.AsProductionError(err)
	require.False(t, ok)
}

func TestProductionError(t *testing.T) {
	cause := fmt.Errorf("produce to output: %w", errors.New("broker down"))
	err := task.NewProductionError(cause, "sink-1")

	require.Contains(t, err.Error(), "produce to output")
	require.ErrorIs(t, err, cause)

	pe, ok := task.AsProductionError(err)
	require.True(t, ok)
	require.Equal(t, "sink-1", pe.Node)
	require.Equal(t, cause, pe.Cause)

	// Should not match other error types
	_, ok = task.AsProcessError(err)
	require.False(t, ok)
	_, ok = task.AsSerdeError(err)
	require.False(t, ok)
}

func TestAsHelpers_Nil(t *testing.T) {
	_, ok := task.AsSerdeError(nil)
	require.False(t, ok)

	_, ok = task.AsProductionError(nil)
	require.False(t, ok)

	_, ok = task.AsProcessError(nil)
	require.False(t, ok)
}

func TestAsHelpers_PlainError(t *testing.T) {
	err := errors.New("plain error")

	_, ok := task.AsSerdeError(err)
	require.False(t, ok)

	_, ok = task.AsProductionError(err)
	require.False(t, ok)

	_, ok = task.AsProcessError(err)
	require.False(t, ok)
}
