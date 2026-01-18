package logger

type LevelWrapper struct {
	Base
}

func WrapLogger(l Base) Logger {
	return &LevelWrapper{l}
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
