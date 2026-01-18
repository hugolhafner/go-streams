package logger

type LevelWrapper struct {
	Base
	args []any
}

func WrapLogger(l Base) Logger {
	return &LevelWrapper{Base: l, args: []any{}}
}

func (w *LevelWrapper) Log(level LogLevel, msg string, kv ...any) {
	w.Base.Log(level, msg, append(w.args, kv...)...)
}

func (w *LevelWrapper) Debug(msg string, kv ...any) {
	w.Log(DebugLevel, msg, kv...)
}

func (w *LevelWrapper) Info(msg string, kv ...any) {
	w.Log(InfoLevel, msg, kv...)
}

func (w *LevelWrapper) Warn(msg string, kv ...any) {
	w.Log(WarnLevel, msg, kv...)
}

func (w *LevelWrapper) Error(msg string, kv ...any) {
	w.Log(ErrorLevel, msg, kv...)
}

func (w *LevelWrapper) With(kv ...any) Logger {
	return &LevelWrapper{
		Base: w,
		args: append(w.args, kv...),
	}
}
