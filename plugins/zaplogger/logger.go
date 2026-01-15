package zaplogger

import (
	"github.com/hugolhafner/go-streams"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ streams.Logger = (*ZapLogger)(nil)

type ZapLogger struct {
	l *zap.Logger
}

func New(l *zap.Logger) *ZapLogger {
	return &ZapLogger{
		l,
	}
}

func (z *ZapLogger) Log(level streams.LogLevel, msg string, kv ...any) {
	fields := make([]zap.Field, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			continue
		}
		fields[i/2] = zap.Any(key, kv[i+1])
	}
	
	z.l.Log(mapToZapLevel(level), msg, fields...)
}

func mapToZapLevel(level streams.LogLevel) zapcore.Level {
	switch level {
	case streams.DebugLevel:
		return zap.DebugLevel
	case streams.InfoLevel:
		return zap.InfoLevel
	case streams.WarnLevel:
		return zap.WarnLevel
	case streams.ErrorLevel:
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}

func mapFromZapLevel(level zapcore.Level) streams.LogLevel {
	switch level {
	case zap.DebugLevel:
		return streams.DebugLevel
	case zap.InfoLevel:
		return streams.InfoLevel
	case zap.WarnLevel:
		return streams.WarnLevel
	case zap.ErrorLevel, zap.DPanicLevel, zap.PanicLevel, zap.FatalLevel:
		return streams.ErrorLevel
	default:
		return streams.InfoLevel
	}
}
