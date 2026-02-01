package mocklogger

import (
	"github.com/hugolhafner/go-streams/logger"
)

var _ logger.Logger = (*MockLogger)(nil)

type LogEntry struct {
	Level   logger.LogLevel
	Message string
	KV      []any
}

type MockLogger struct {
	Entries []LogEntry
	args    []any
}

func New() *MockLogger {
	return &MockLogger{}
}

func (m *MockLogger) Log(level logger.LogLevel, msg string, kv ...any) {
	m.Entries = append(
		m.Entries, LogEntry{
			Level:   level,
			Message: msg,
			KV:      kv,
		},
	)
}

func (m *MockLogger) Level() logger.LogLevel {
	return logger.DebugLevel
}

func (m *MockLogger) With(kv ...any) logger.Logger {
	return &MockLogger{
		Entries: m.Entries,
		args:    append(m.args, kv...),
	}
}

func (m *MockLogger) Debug(msg string, kv ...any) {
	m.Log(logger.DebugLevel, msg, kv...)
}

func (m *MockLogger) Info(msg string, kv ...any) {
	m.Log(logger.InfoLevel, msg, kv...)
}

func (m *MockLogger) Warn(msg string, kv ...any) {
	m.Log(logger.WarnLevel, msg, kv...)
}

func (m *MockLogger) Error(msg string, kv ...any) {
	m.Log(logger.ErrorLevel, msg, kv...)
}
