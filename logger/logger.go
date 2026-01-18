package logger

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Logger interface {
	Level() LogLevel
	Log(level LogLevel, msg string, kv ...any)
}

type NoopLogger struct{}

func (n *NoopLogger) Log(level LogLevel, msg string, kv ...any) {
	// no operation
}

func (n *NoopLogger) Level() LogLevel {
	return InfoLevel
}

func NewNoopLogger() Logger {
	return &NoopLogger{}
}
