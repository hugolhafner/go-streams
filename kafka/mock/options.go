package mockkafka

import (
	"time"
)

// Option is a functional option for configuring a MockClient.
type Option func(*Client)

// WithMaxPollRecords sets the maximum number of records returned per Poll call.
// Default is 10.
func WithMaxPollRecords(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxPollRecords = n
		}
	}
}

// WithPollDelay adds an artificial delay to Poll calls.
// This can be useful for testing timeout behavior or rate limiting.
func WithPollDelay(d time.Duration) Option {
	return func(c *Client) {
		c.pollDelay = d
	}
}

// WithSendError configures an error to be returned by all Send calls.
func WithSendError(err error) Option {
	return func(c *Client) {
		c.sendErr = func(string, []byte, []byte) error { return err }
	}
}

// WithPollError configures an error to be returned by all Poll calls.
func WithPollError(err error) Option {
	return func(c *Client) {
		c.pollErr = func() error { return err }
	}
}

// WithCommitError configures an error to be returned by all Commit calls.
func WithCommitError(err error) Option {
	return func(c *Client) {
		c.commitErr = func() error { return err }
	}
}

// WithPingError configures an error to be returned by Ping.
func WithPingError(err error) Option {
	return func(c *Client) {
		c.pingErr = err
	}
}
