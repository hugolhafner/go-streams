package log

import (
	"context"
	"time"
)

type Producer interface {
	Send(ctx context.Context, topic string, key, value []byte, headers map[string][]byte) error
	Flush(timeout time.Duration) error
	Close() error
}
