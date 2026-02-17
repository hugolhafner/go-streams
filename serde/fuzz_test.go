//go:build fuzz

package serde_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Fuzz JSON serde round-trip consistency.
func FuzzJSONSerde_RoundTrip(f *testing.F) {
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(`42`))

	s := serde.JSON[any]()

	f.Fuzz(
		func(t *testing.T, data []byte) {
			val, err := s.Deserialise("topic", data)
			if err != nil {
				return // invalid JSON is expected
			}

			// Round-trip: serialize then deserialize again
			encoded, err := s.Serialise("topic", val)
			if err != nil {
				return // some deserialized values may not re-serialize cleanly
			}

			val2, err := s.Deserialise("topic", encoded)
			require.NoError(t, err, "re-deserialization must not fail after successful serialization")
			_ = val2
		},
	)
}

// Fuzz String serde identity round-trip.
func FuzzStringSerde_Identity(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte(""))
	f.Add([]byte{0x00, 0xff, 0xfe})

	s := serde.String()

	f.Fuzz(
		func(t *testing.T, data []byte) {
			val, err := s.Deserialise("topic", data)
			require.NoError(t, err, "String.Deserialise must never error")

			encoded, err := s.Serialise("topic", val)
			require.NoError(t, err, "String.Serialise must never error")

			require.Equal(t, data, encoded, "round-trip must be identity")
		},
	)
}

// Fuzz Bytes serde identity round-trip.
func FuzzBytesSerde_Identity(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte(""))
	f.Add([]byte{0x00, 0xff, 0xfe})

	s := serde.Bytes()

	f.Fuzz(
		func(t *testing.T, data []byte) {
			val, err := s.Deserialise("topic", data)
			require.NoError(t, err, "Bytes.Deserialise must never error")

			encoded, err := s.Serialise("topic", val)
			require.NoError(t, err, "Bytes.Serialise must never error")

			require.Equal(t, data, encoded, "round-trip must be identity")
		},
	)
}

// Fuzz Protobuf serde - Deserialise must never panic.
func FuzzProtobufSerde_NoDeserializePanic(f *testing.F) {
	// Valid proto bytes for wrapperspb.StringValue("hello")
	validBytes, _ := proto.Marshal(wrapperspb.String("hello"))
	f.Add(validBytes)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xfe, 0x00, 0x01, 0x02, 0x80})

	s := serde.Protobuf[*wrapperspb.StringValue]()

	f.Fuzz(
		func(t *testing.T, data []byte) {
			// Must not panic - errors are acceptable
			val, err := s.Deserialise("topic", data)
			if err != nil {
				return
			}

			// If deserialization succeeded, round-trip should work
			encoded, err := s.Serialise("topic", val)
			require.NoError(t, err, "Serialise must not fail after successful Deserialise")

			val2, err := s.Deserialise("topic", encoded)
			require.NoError(t, err, "re-Deserialise must not fail after Serialise")
			require.True(t, proto.Equal(val, val2), "round-trip must preserve value")
		},
	)
}
