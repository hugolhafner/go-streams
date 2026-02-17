//go:build unit

package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
)

// failingValueDeserializer always returns an error on deserialize.
type failingValueDeserializer struct{}

func (f failingValueDeserializer) Deserialise(_ string, _ []byte) (any, error) {
	return nil, errors.New("intentional deserialization failure")
}

// createTopologyWithFailingSerde creates a topology where value deserialization always fails.
func createTopologyWithFailingSerde() *topology.Topology {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		failingValueDeserializer{},
	)

	var supplier processor.Supplier[string, any, string, any] = func() processor.Processor[string, any, string, any] {
		return builtins.NewPassthroughProcessor[string, any]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.Bytes()),
		"proc",
	)

	return topo
}

// conditionalFailDeserializer fails deserialization only for specific key values.
type conditionalFailDeserializer struct {
	failKeys map[string]bool
}

func (d conditionalFailDeserializer) Deserialise(_ string, data []byte) (any, error) {
	// We receive the raw value bytes; check if the value matches a poison pattern
	val := string(data)
	if d.failKeys[val] {
		return nil, fmt.Errorf("intentional deserialization failure for value: %s", val)
	}
	return val, nil
}

func createTopologyWithConditionalSerde(failValues ...string) *topology.Topology {
	failKeys := make(map[string]bool)
	for _, v := range failValues {
		failKeys[v] = true
	}

	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		conditionalFailDeserializer{failKeys: failKeys},
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	return topo
}

// Test 12 (corrected): Poison pill serde error with Continue handler.
func TestChaos_PoisonPill_SerdeError_Continue_Selective(t *testing.T) {
	topo := createTopologyWithConditionalSerde("poison-value")

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("poison", "poison-value"),
		mockkafka.SimpleRecord("good", "good-value"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithSerdeErrorHandler(errorhandler.LogAndContinue(l)),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Only good record should be produced
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("output")) == 1
		}, 3*time.Second, 50*time.Millisecond, "good record should be produced",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	client.AssertProducedString(t, "output", "good", "good-value")
	client.AssertNotProduced(t, "output", []byte("poison"))
}

// Test 13: Poison pill serde error with DLQ handler - poison goes to DLQ.
func TestChaos_PoisonPill_SerdeError_DLQ(t *testing.T) {
	topo := createTopologyWithConditionalSerde("poison-value")

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("poison", "poison-value"),
		mockkafka.SimpleRecord("good", "good-value"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithSerdeErrorHandler(errorhandler.WithDLQ("dlq", errorhandler.LogAndContinue(l))),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Wait for both records to be processed
	require.Eventually(
		t, func() bool {
			output := len(client.ProducedRecordsForTopic("output"))
			dlq := len(client.ProducedRecordsForTopic("dlq"))
			return output == 1 && dlq == 1
		}, 3*time.Second, 50*time.Millisecond, "good record to output, poison to DLQ",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify good record reached output
	client.AssertProducedString(t, "output", "good", "good-value")

	// Verify poison record went to DLQ with correct headers
	dlqRecords := client.ProducedRecordsForTopic("dlq")
	require.Len(t, dlqRecords, 1)
	require.Equal(t, "poison", string(dlqRecords[0].Key))

	// Check DLQ headers
	headers := dlqRecords[0].Headers
	assertHeaderValue(t, headers, "x-error-phase", "serde")
	assertHeaderValue(t, headers, "x-original-topic", "input")
	assertHeaderExists(t, headers, "x-error-message")
}

// Test 14: Process error with retry then DLQ.
func TestChaos_PoisonPill_ProcessError_RetryThenDLQ(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var attemptCount atomic.Int32

	var failOnPoison processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				if k == "poison" {
					attemptCount.Add(1)
					return "", "", errors.New("processing failed for poison key")
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", failOnPoison.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("poison", "bad"),
		mockkafka.SimpleRecord("good", "ok"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithProcessingErrorHandler(
			errorhandler.WithMaxAttempts(
				3, backoff.NewFixed(0),
				errorhandler.WithDLQ("dlq", errorhandler.LogAndContinue(l)),
			),
		),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			output := len(client.ProducedRecordsForTopic("output"))
			dlq := len(client.ProducedRecordsForTopic("dlq"))
			return output == 1 && dlq == 1
		}, 3*time.Second, 50*time.Millisecond, "good to output, poison to DLQ after retries",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify retry count: 3 retries then DLQ
	require.Equal(t, int32(3), attemptCount.Load(), "poison should be attempted 3 times")

	// Verify DLQ headers
	dlqRecords := client.ProducedRecordsForTopic("dlq")
	require.Len(t, dlqRecords, 1)
	assertHeaderValue(t, dlqRecords[0].Headers, "x-error-phase", "processing")
}

// Test 15: Production error with phase-based DLQ.
func TestChaos_PoisonPill_ProductionError_PhaseDLQ(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("fail-produce", "value1"),
		mockkafka.SimpleRecord("ok", "value2"),
	)

	// Fail sends only for key "fail-produce" on topic "output"
	client.SetSendErrorFunc(
		func(topic string, key, value []byte) error {
			if topic == "output" && string(key) == "fail-produce" {
				return errors.New("production error for fail-produce")
			}
			return nil
		},
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithProductionErrorHandler(
			errorhandler.WithDLQ("dlq", errorhandler.LogAndContinue(l)),
		),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			output := len(client.ProducedRecordsForTopic("output"))
			dlq := len(client.ProducedRecordsForTopic("dlq"))
			return output == 1 && dlq == 1
		}, 3*time.Second, 50*time.Millisecond, "ok to output, fail-produce to DLQ",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify DLQ headers show production phase
	dlqRecords := client.ProducedRecordsForTopic("dlq")
	require.Len(t, dlqRecords, 1)
	assertHeaderValue(t, dlqRecords[0].Headers, "x-error-phase", "production")
	assertHeaderExists(t, dlqRecords[0].Headers, "x-error-node")
}

// Test 16: Panic recovery sends to DLQ, processing continues.
func TestChaos_PoisonPill_PanicRecovery_DLQ(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var panicSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				if k == "panic" {
					panic("intentional panic for testing")
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", panicSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("panic", "crash"),
		mockkafka.SimpleRecord("normal", "ok"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithProcessingErrorHandler(
			errorhandler.WithDLQ("dlq", errorhandler.LogAndContinue(l)),
		),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			output := len(client.ProducedRecordsForTopic("output"))
			dlq := len(client.ProducedRecordsForTopic("dlq"))
			return output == 1 && dlq == 1
		}, 3*time.Second, 50*time.Millisecond, "normal to output, panic to DLQ",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	client.AssertProducedString(t, "output", "normal", "ok")

	dlqRecords := client.ProducedRecordsForTopic("dlq")
	require.Len(t, dlqRecords, 1)
	require.Equal(t, "panic", string(dlqRecords[0].Key))
}

// Test 17: Mixed phases routed to distinct DLQ topics.
func TestChaos_PoisonPill_MixedPhases_RoutedCorrectly(t *testing.T) {
	topo := topology.New()

	// Use conditional serde that fails on specific value
	failKeys := map[string]bool{"serde-poison": true}
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		conditionalFailDeserializer{failKeys: failKeys},
	)

	var procSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				if k == "process-poison" {
					return "", "", errors.New("processing error")
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", procSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("serde-key", "serde-poison"), // serde error
		mockkafka.SimpleRecord("process-poison", "value"),   // process error
		mockkafka.SimpleRecord("normal", "normal-value"),    // normal record
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithSerdeErrorHandler(
			errorhandler.WithDLQ("dlq-serde", errorhandler.LogAndContinue(l)),
		),
		WithProcessingErrorHandler(
			errorhandler.WithDLQ("dlq-processing", errorhandler.LogAndContinue(l)),
		),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			output := len(client.ProducedRecordsForTopic("output"))
			dlqSerde := len(client.ProducedRecordsForTopic("dlq-serde"))
			dlqProcessing := len(client.ProducedRecordsForTopic("dlq-processing"))
			return output == 1 && dlqSerde == 1 && dlqProcessing == 1
		}, 3*time.Second, 50*time.Millisecond, "records should be routed to correct DLQs",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify output
	client.AssertProducedString(t, "output", "normal", "normal-value")

	// Verify serde DLQ
	serdeDLQ := client.ProducedRecordsForTopic("dlq-serde")
	require.Len(t, serdeDLQ, 1)
	assertHeaderValue(t, serdeDLQ[0].Headers, "x-error-phase", "serde")

	// Verify processing DLQ
	procDLQ := client.ProducedRecordsForTopic("dlq-processing")
	require.Len(t, procDLQ, 1)
	assertHeaderValue(t, procDLQ[0].Headers, "x-error-phase", "processing")
}

// --- Helpers ---

func assertHeaderValue(t *testing.T, headers []kafka.Header, key, expectedValue string) {
	t.Helper()
	val, ok := kafka.HeaderValue(headers, key)
	require.True(t, ok, "header %q not found", key)
	require.Equal(t, expectedValue, string(val), "header %q value mismatch", key)
}

func assertHeaderExists(t *testing.T, headers []kafka.Header, key string) {
	t.Helper()
	_, ok := kafka.HeaderValue(headers, key)
	require.True(t, ok, "header %q not found", key)
}
