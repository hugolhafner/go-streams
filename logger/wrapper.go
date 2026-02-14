package logger

type LevelWrapper struct {
	Base
	args []any
}

func WrapLogger(l Base) Logger {
	return &LevelWrapper{Base: l, args: []any{}}
}

// Log logs a message at the specified level with the provided key-value pairs, including any additional key-value pairs from the wrapper.
func (w *LevelWrapper) Log(level LogLevel, msg string, kv ...any) {
	w.Base.Log(level, msg, append(w.args, kv...)...)
}

// Debug logs a message at the Debug level with the provided key-value pairs.
func (w *LevelWrapper) Debug(msg string, kv ...any) {
	w.Log(DebugLevel, msg, kv...)
}

// Info logs a message at the Info level with the provided key-value pairs.
func (w *LevelWrapper) Info(msg string, kv ...any) {
	w.Log(InfoLevel, msg, kv...)
}

// Warn logs a message at the Warn level with the provided key-value pairs.
func (w *LevelWrapper) Warn(msg string, kv ...any) {
	w.Log(WarnLevel, msg, kv...)
}

// Error logs a message at the Error level with the provided key-value pairs.
func (w *LevelWrapper) Error(msg string, kv ...any) {
	w.Log(ErrorLevel, msg, kv...)
}

// With returns a new Logger that includes the provided key-value pairs in all subsequent log entries.
func (w *LevelWrapper) With(kv ...any) Logger {
	return &LevelWrapper{
		Base: w,
		args: append(w.args, kv...),
	}
}
