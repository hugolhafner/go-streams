//go:build unit

package otel

import (
	"testing"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/stretchr/testify/require"
)

func TestKafkaHeadersCarrier_Get(t *testing.T) {
	t.Parallel()
	headers := []kafka.Header{
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: "other", Value: []byte("value")},
	}
	carrier := NewKafkaHeadersCarrier(&headers)

	require.Equal(t, "00-abc-def-01", carrier.Get("traceparent"))
	require.Equal(t, "value", carrier.Get("other"))
	require.Equal(t, "", carrier.Get("missing"))
}

func TestKafkaHeadersCarrier_Set_New(t *testing.T) {
	t.Parallel()
	headers := []kafka.Header{
		{Key: "existing", Value: []byte("val")},
	}
	carrier := NewKafkaHeadersCarrier(&headers)

	carrier.Set("traceparent", "00-abc-def-01")

	require.Len(t, headers, 2)
	require.Equal(t, "traceparent", headers[1].Key)
	require.Equal(t, []byte("00-abc-def-01"), headers[1].Value)
}

func TestKafkaHeadersCarrier_Set_Replace(t *testing.T) {
	t.Parallel()
	headers := []kafka.Header{
		{Key: "traceparent", Value: []byte("old-value")},
	}
	carrier := NewKafkaHeadersCarrier(&headers)

	carrier.Set("traceparent", "new-value")

	require.Len(t, headers, 1)
	require.Equal(t, []byte("new-value"), headers[0].Value)
}

func TestKafkaHeadersCarrier_Keys(t *testing.T) {
	t.Parallel()
	headers := []kafka.Header{
		{Key: "traceparent", Value: []byte("val1")},
		{Key: "tracestate", Value: []byte("val2")},
	}
	carrier := NewKafkaHeadersCarrier(&headers)

	keys := carrier.Keys()
	require.Equal(t, []string{"traceparent", "tracestate"}, keys)
}

func TestKafkaHeadersCarrier_Empty(t *testing.T) {
	t.Parallel()
	headers := []kafka.Header{}
	carrier := NewKafkaHeadersCarrier(&headers)

	require.Equal(t, "", carrier.Get("anything"))
	require.Empty(t, carrier.Keys())
}
