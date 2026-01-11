package client

type Consumer interface {
	Subscribe(topics []string) error
	Close() error
}
