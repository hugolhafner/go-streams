//go:build unit

package serde_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtobufSerde_Serialise(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input *wrapperspb.StringValue
	}{
		{
			name:  "simple string value",
			input: wrapperspb.String("hello world"),
		},
		{
			name:  "empty string value",
			input: wrapperspb.String(""),
		},
		{
			name:  "nil message",
			input: nil,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				s := serde.Protobuf[*wrapperspb.StringValue]()
				output, err := s.Serialise("test-topic", tt.input)
				require.NoError(t, err)

				expected, err := proto.Marshal(tt.input)
				require.NoError(t, err)
				require.Equal(t, expected, output)
			},
		)
	}
}

func TestProtobufSerde_Deserialise(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   *wrapperspb.StringValue
		wantErr bool
	}{
		{
			name:  "simple string value",
			input: wrapperspb.String("hello world"),
		},
		{
			name:  "empty string value",
			input: wrapperspb.String(""),
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				s := serde.Protobuf[*wrapperspb.StringValue]()

				data, err := proto.Marshal(tt.input)
				require.NoError(t, err)

				output, err := s.Deserialise("test-topic", data)
				if tt.wantErr {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.True(t, proto.Equal(tt.input, output))
			},
		)
	}
}

func TestProtobufSerde_Deserialise_InvalidData(t *testing.T) {
	t.Parallel()
	s := serde.Protobuf[*wrapperspb.StringValue]()
	_, err := s.Deserialise("test-topic", []byte("not valid protobuf \xff\xfe"))
	require.Error(t, err)
}

func TestProtobufSerde_Roundtrip(t *testing.T) {
	t.Parallel()
	s := serde.Protobuf[*wrapperspb.StringValue]()
	original := wrapperspb.String("roundtrip test")

	data, err := s.Serialise("test-topic", original)
	require.NoError(t, err)

	result, err := s.Deserialise("test-topic", data)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, result))
}

func TestProtobufSerde_Timestamp(t *testing.T) {
	t.Parallel()
	s := serde.Protobuf[*timestamppb.Timestamp]()
	original := timestamppb.Now()

	data, err := s.Serialise("test-topic", original)
	require.NoError(t, err)

	result, err := s.Deserialise("test-topic", data)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, result))
}
