//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// startDedicatedBroker creates a fresh standalone Redpanda container for broker restart tests.
// The container is terminated via t.Cleanup.
func startDedicatedBroker(t *testing.T) (*redpanda.Container, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := redpanda.Run(
		ctx,
		"docker.redpanda.com/redpandadata/redpanda:v24.2.1",
		redpanda.WithAutoCreateTopics(),
	)
	require.NoError(t, err)

	addr, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = container.Terminate(cleanupCtx)
	})

	return container, addr
}
