package serde_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

func TestBytesSerde_Serialise(t *testing.T) {
	s := serde.Bytes()
	input := []byte{0x01, 0x02, 0x03}
	output, err := s.Serialise("test-topic", input)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestBytesSerde_Deserialise(t *testing.T) {
	s := serde.Bytes()
	input := []byte{0x04, 0x05, 0x06}
	output, err := s.Deserialise("test-topic", input)
	require.NoError(t, err)
	require.Equal(t, input, output)
}
