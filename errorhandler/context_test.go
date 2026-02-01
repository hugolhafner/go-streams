package errorhandler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/stretchr/testify/require"
)

func TestNewErrorContext(t *testing.T) {
	headers := make(map[string][]byte)
	headers["header-1"] = []byte("value-1")
	headers["header-2"] = []byte("value-2")

	record := kafka.ConsumerRecord{
		Key:         []byte("key-123"),
		Value:       []byte("value-123"),
		Headers:     headers,
		Topic:       "test-topic",
		Partition:   1,
		Offset:      10,
		LeaderEpoch: 2,
		Timestamp:   time.Now(),
	}

	err := errorhandler.NewErrorContext(record, nil)

	require.Equal(t, record, err.Record)
	require.Nil(t, err.Error)
	require.Equal(t, 1, err.Attempt)
	require.Empty(t, err.NodeName)
}

func TestNewErrorContext_Copy(t *testing.T) {
	headers := make(map[string][]byte)
	headers["header-1"] = []byte("value-1")
	headers["header-2"] = []byte("value-2")

	key := []byte("key-123")
	value := []byte("value-123")

	record := kafka.ConsumerRecord{
		Key:         key,
		Value:       value,
		Headers:     headers,
		Topic:       "test-topic",
		Partition:   1,
		Offset:      10,
		LeaderEpoch: 2,
		Timestamp:   time.Now(),
	}

	err := errorhandler.NewErrorContext(record, nil)

	record.Headers["header-1"] = []byte("modified-value")
	record.Key = []byte("modified-key")
	record.Value = []byte("modified-value")

	require.Equal(t, []byte("value-1"), err.Record.Headers["header-1"])
	require.Equal(t, []byte("key-123"), err.Record.Key)
	require.Equal(t, []byte("value-123"), err.Record.Value)
}

func TestErrorContext_IncrementAttempt(t *testing.T) {
	ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil)
	require.Equal(t, 1, ec.Attempt)

	ec = ec.IncrementAttempt()
	require.Equal(t, 2, ec.Attempt)

	ec = ec.IncrementAttempt()
	require.Equal(t, 3, ec.Attempt)
}

func TestErrorContext_WithError(t *testing.T) {
	ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil)
	require.Nil(t, ec.Error)

	sampleErr := errors.New("sample error")
	ec = ec.WithError(sampleErr)
	require.Equal(t, sampleErr, ec.Error)
}

func TestErrorContext_WithAttempt(t *testing.T) {
	ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil)
	require.Equal(t, 1, ec.Attempt)

	ec = ec.WithAttempt(5)
	require.Equal(t, 5, ec.Attempt)
}

func TestErrorContext_WithNodeName(t *testing.T) {
	ec := errorhandler.NewErrorContext(kafka.ConsumerRecord{}, nil)
	require.Empty(t, ec.NodeName)

	ec = ec.WithNodeName("TestNode")
	require.Equal(t, "TestNode", ec.NodeName)
}
