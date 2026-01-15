package streams

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Logger interface {
	Log(level LogLevel, msg string, kv ...any)
}

type noopLogger struct{}

func (n *noopLogger) Log(level LogLevel, msg string, kv ...any) {
	// no operation
}
