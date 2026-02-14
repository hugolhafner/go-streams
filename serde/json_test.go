//go:build unit

package serde_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

func TestJsonSerde_Serialise(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		input  any
		expect string
	}{
		{
			name: "simple struct",
			input: struct {
				Name string `json:"name"`
				Age  int    `json:"age"`
			}{Name: "Alice", Age: 30},
			expect: `{"name":"Alice","age":30}`,
		},
		{
			name:   "map",
			input:  map[string]int{"one": 1, "two": 2},
			expect: `{"one":1,"two":2}`,
		},
		{
			name:   "invalid input",
			input:  func() {},
			expect: ``,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				s := serde.JSON[any]()
				output, err := s.Serialise("test-topic", tt.input)
				if tt.expect == `` {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.JSONEq(t, tt.expect, string(output))
			},
		)
	}
}

func TestJsonSerde_Deserialise(t *testing.T) {
	t.Parallel()
	type Person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	tests := []struct {
		name    string
		input   string
		expect  Person
		wantErr bool
	}{
		{
			name:  "valid json",
			input: `{"name":"Bob","age":25}`,
			expect: Person{
				Name: "Bob",
				Age:  25,
			},
			wantErr: false,
		},
		{
			name:    "invalid json",
			input:   `{"name":"Charlie","age":"not-a-number"}`,
			expect:  Person{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()
				s := serde.JSON[Person]()
				output, err := s.Deserialise("test-topic", []byte(tt.input))
				if tt.wantErr {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.Equal(t, tt.expect, output)
			},
		)
	}
}
