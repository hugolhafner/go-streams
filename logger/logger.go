package logger

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func (level LogLevel) String() string {
	switch level {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

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
	With(kv ...any) Logger
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
