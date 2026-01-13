package log

import (
	"context"
)

type Client interface {
	Producer
	Consumer

	Ping(ctx context.Context) error
}
