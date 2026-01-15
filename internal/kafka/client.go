package kafka

import (
	"context"
)

type Client interface {
	Producer
	Consumer

	Ping(ctx context.Context) error
}
