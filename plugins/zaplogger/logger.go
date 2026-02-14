package zaplogger

import (
	"github.com/hugolhafner/go-streams/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ logger.Base = (*ZapLogger)(nil)

// ZapLogger is a logger that wraps a zap.Logger and implements the logger.Base interface.
type ZapLogger struct {
	l *zap.Logger
}

func New(l *zap.Logger) logger.Logger {
	return logger.WrapLogger(
		&ZapLogger{
			l,
		},
	)
}

func (z *ZapLogger) Level() logger.LogLevel {
	return mapFromZapLevel(z.l.Level())
}

func (z *ZapLogger) Log(level logger.LogLevel, msg string, kv ...any) {
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

func mapToZapLevel(level logger.LogLevel) zapcore.Level {
	switch level {
	case logger.DebugLevel:
		return zapcore.DebugLevel
	case logger.InfoLevel:
		return zapcore.InfoLevel
	case logger.WarnLevel:
		return zapcore.WarnLevel
	case logger.ErrorLevel:
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func mapFromZapLevel(level zapcore.Level) logger.LogLevel {
	switch level {
	case zapcore.DebugLevel:
		return logger.DebugLevel
	case zapcore.InfoLevel:
		return logger.InfoLevel
	case zapcore.WarnLevel:
		return logger.WarnLevel
	case zapcore.InvalidLevel, zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return logger.ErrorLevel
	default:
		return logger.InfoLevel
	}
}
