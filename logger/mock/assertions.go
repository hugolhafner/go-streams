package mocklogger

import (
	"testing"

	"github.com/hugolhafner/go-streams/logger"
)

func (m *MockLogger) AssertCalledWithMessage(tb testing.TB, message string) {
	for _, entry := range m.Entries {
		if entry.Message == message {
			return
		}
	}

	tb.Errorf("expected log message '%s' to be called", message)
}

func (m *MockLogger) AssertCalledWithLevel(tb testing.TB, level logger.LogLevel) {
	for _, entry := range m.Entries {
		if entry.Level == level {
			return
		}
	}

	tb.Errorf("expected log level '%s' to be called", level.String())
}

func (m *MockLogger) AssertCalledWithLevelAndMessage(tb testing.TB, level logger.LogLevel, message string) {
	for _, entry := range m.Entries {
		if entry.Level == level && entry.Message == message {
			return
		}
	}

	tb.Errorf("expected log with level '%s' and message '%s' to be called", level.String(), message)
}

func (m *MockLogger) AssertNotCalledWithMessage(tb testing.TB, message string) {
	for _, entry := range m.Entries {
		if entry.Message == message {
			tb.Errorf("expected log message '%s' to NOT be called", message)
			return
		}
	}
}

func (m *MockLogger) AssertNotCalledWithLevel(tb testing.TB, level logger.LogLevel) {
	for _, entry := range m.Entries {
		if entry.Level == level {
			tb.Errorf("expected log level '%s' to NOT be called", level.String())
			return
		}
	}
}

func (m *MockLogger) AssertCalled(tb testing.TB, level logger.LogLevel, message string, kv ...any) {
	for _, entry := range m.Entries {
		if entry.Level != level || entry.Message != message {
			continue
		}

		if len(entry.KV) != len(kv) {
			continue
		}

		match := true
		for i := range kv {
			if entry.KV[i] != kv[i] {
				match = false
				break
			}
		}

		if match {
			return
		}
	}

	tb.Errorf("expected log with level '%s' and message '%s' to be called", level.String(), message)
}
