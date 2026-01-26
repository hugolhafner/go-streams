package serde_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

func TestStringSerde_Serialise(t *testing.T) {
	s := serde.String()
	input := "hello world"
	output, err := s.Serialise("test-topic", input)
	require.NoError(t, err)
	require.Equal(t, input, string(output))
}

func TestStringSerde_Deserialise(t *testing.T) {
	s := serde.String()
	input := []byte("hello world")
	output, err := s.Deserialise("test-topic", input)
	require.NoError(t, err)
	require.Equal(t, "hello world", output)
}
