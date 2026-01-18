package logger

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Base interface {
	Level() LogLevel
	Log(level LogLevel, msg string, kv ...any)
}

type Logger interface {
	Base
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type NoopLogger struct{}

func (n *NoopLogger) Log(level LogLevel, msg string, kv ...any) {
	// no operation
}

func (n *NoopLogger) Level() LogLevel {
	return InfoLevel
}

func NewNoopLogger() Logger {
	return WrapLogger(&NoopLogger{})
}
