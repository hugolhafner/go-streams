package kafka

import (
	"github.com/hugolhafner/go-streams/logger"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ kgo.Logger = (*kgoLogger)(nil)

type kgoLogger struct {
	l logger.Logger
}

func newKgoLogger(l logger.Logger) *kgoLogger {
	return &kgoLogger{l: l}
}

func (kl *kgoLogger) Level() kgo.LogLevel {
	return mapToKgoLevel(kl.l.Level())
}

func (kl *kgoLogger) Log(level kgo.LogLevel, msg string, args ...interface{}) {
	zapLevel := mapFromKgoLevel(level)
	kl.l.Log(zapLevel, msg, args...)
}

func mapToKgoLevel(level logger.LogLevel) kgo.LogLevel {
	switch level {
	case logger.DebugLevel:
		return kgo.LogLevelDebug
	case logger.InfoLevel:
		return kgo.LogLevelInfo
	case logger.WarnLevel:
		return kgo.LogLevelWarn
	case logger.ErrorLevel:
		return kgo.LogLevelError
	default:
		return kgo.LogLevelWarn
	}
}

func mapFromKgoLevel(level kgo.LogLevel) logger.LogLevel {
	switch level {
	case kgo.LogLevelDebug:
		return logger.DebugLevel
	case kgo.LogLevelInfo:
		return logger.InfoLevel
	case kgo.LogLevelWarn:
		return logger.WarnLevel
	case kgo.LogLevelError:
		return logger.ErrorLevel
	default:
		return logger.WarnLevel
	}
}
