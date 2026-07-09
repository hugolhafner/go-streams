//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The TestE2E_Chaos_ tests drive failure scenarios — flaky and poison records,
// retry storms with real backoff latency, runner death and restart, and slow
// consumers through rebalances — against a real Redpanda broker.
// Run in isolation with `mise run test-e2e-chaos`.

// keyAttempts tracks per-key processing attempts across concurrent partition
// workers.
type keyAttempts struct {
	mu     sync.Mutex
	counts map[string]int
}

func newKeyAttempts() *keyAttempts {
	return &keyAttempts{counts: make(map[string]int)}
}

func (a *keyAttempts) Inc(key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts[key]++
	return a.counts[key]
}

// committedSum returns the total committed offset for a topic across all of a
// group's partitions.
func committedSum(t *testing.T, broker, groupID, topic string) int64 {
	t.Helper()

	var sum int64
	for _, offset := range getCommittedOffsets(t, broker, groupID)[topic] {
		sum += offset
	}
	return sum
}

func TestE2E_Chaos_FlakyAndPoisonWithDLQ(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "chaos-input")
	outputTopic := testTopicName(t, "chaos-output")
	dlqTopic := testTopicName(t, "chaos-dlq")
	groupID := testGroupID(t, "chaos")

	createTopics(t, broker, 3, inputTopic, outputTopic, dlqTopic)

	// 40 ok, 12 flaky (fail first 2 attempts), 8 poison (always fail)
	testData := make(map[string]string)
	poisonKeys := make(map[string]bool)
	healthyKeys := make(map[string]bool)
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("ok-%d", i)
		testData[k] = "v"
		healthyKeys[k] = true
	}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("flaky-%d", i)
		testData[k] = "v"
		healthyKeys[k] = true
	}
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("poison-%d", i)
		testData[k] = "v"
		poisonKeys[k] = true
	}

	attempts := newKeyAttempts()
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			n := attempts.Inc(key)
			switch {
			case poisonKeys[key]:
				return "", "", fmt.Errorf("poison record %s", key)
			case healthyKeys[key] && key[0] == 'f' && n <= 2:
				return "", "", fmt.Errorf("flaky record %s attempt %d", key, n)
			}
			return key, value, nil
		},
	)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, builder.Build())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(
			ctx,
			runner.NewPartitionedRunner(
				runner.WithErrorHandler(
					errorhandler.WithMaxAttempts(
						3, backoff.NewFixed(50*time.Millisecond), errorhandler.WithDLQ(dlqTopic, nil),
					),
				),
			),
		)
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)
	produceRecords(t, broker, inputTopic, testData)

	// healthy records (flaky ones after retries) reach the output exactly once
	consumed := consumeRecords(t, broker, outputTopic, testGroupID(t, "out-verifier"), len(healthyKeys), consumeWait)
	outputSeen := make(map[string]int)
	for _, r := range consumed {
		outputSeen[r.Key]++
	}
	require.Len(t, outputSeen, len(healthyKeys), "every healthy key should be produced")
	for k := range healthyKeys {
		require.Equal(t, 1, outputSeen[k], "healthy key %s should be produced exactly once", k)
	}

	// poison records land in the DLQ with the failure metadata headers
	dlqRecords := consumeRecordsFull(t, broker, dlqTopic, testGroupID(t, "dlq-verifier"), len(poisonKeys), consumeWait)
	dlqSeen := make(map[string]bool)
	for _, r := range dlqRecords {
		dlqSeen[r.Key] = true

		origTopic, ok := headerValue(r.Headers, "x-original-topic")
		require.True(t, ok, "DLQ record %s missing x-original-topic header", r.Key)
		require.Equal(t, inputTopic, origTopic)

		attempt, ok := headerValue(r.Headers, "x-error-attempt")
		require.True(t, ok, "DLQ record %s missing x-error-attempt header", r.Key)
		require.Equal(t, "3", attempt, "poison records should exhaust all attempts before the DLQ")

		phase, ok := headerValue(r.Headers, "x-error-phase")
		require.True(t, ok, "DLQ record %s missing x-error-phase header", r.Key)
		require.Equal(t, "processing", phase)
	}
	require.Equal(t, poisonKeys, dlqSeen, "exactly the poison keys must reach the DLQ")

	cancel()
	waitForShutdown(t, errCh, shutdownWait)

	eventually(
		t, func() bool {
			return committedSum(t, broker, groupID, inputTopic) == int64(len(testData))
		}, eventualWait, "all records, including DLQ'd ones, should be committed",
	)
}

func TestE2E_Chaos_HighRetryBackoffLatency(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "retry-input")
	outputTopic := testTopicName(t, "retry-output")
	groupID := testGroupID(t, "retry")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	const recordCount = 8
	const failuresPerRecord = 5
	const retryBackoff = 100 * time.Millisecond

	testData := make(map[string]string, recordCount)
	for i := 0; i < recordCount; i++ {
		testData[fmt.Sprintf("key-%d", i)] = "v"
	}

	attempts := newKeyAttempts()
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			if n := attempts.Inc(key); n <= failuresPerRecord {
				return "", "", fmt.Errorf("record %s attempt %d", key, n)
			}
			return key, value, nil
		},
	)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, builder.Build())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(
			ctx,
			runner.NewSingleThreadedRunner(
				runner.WithErrorHandler(
					errorhandler.WithMaxAttempts(
						10, backoff.NewFixed(retryBackoff), errorhandler.LogAndFail(logger.NewNoopLogger()),
					),
				),
			),
		)
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	start := time.Now()
	produceRecords(t, broker, inputTopic, testData)

	consumed := consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), recordCount, consumeWait)
	elapsed := time.Since(start)

	// every retry pays the real backoff, sequentially in this runner
	minElapsed := time.Duration(recordCount*failuresPerRecord) * retryBackoff
	require.GreaterOrEqual(
		t, elapsed, minElapsed,
		"%d records x %d retries must accumulate at least %v of backoff",
		recordCount, failuresPerRecord, minElapsed,
	)

	seen := make(map[string]int)
	for _, r := range consumed {
		seen[r.Key]++
	}
	require.Len(t, seen, recordCount)
	for k := range testData {
		require.Equal(t, 1, seen[k], "key %s should be produced exactly once despite retries", k)
	}

	cancel()
	waitForShutdown(t, errCh, shutdownWait)

	eventually(
		t, func() bool {
			return committedSum(t, broker, groupID, inputTopic) == int64(recordCount)
		}, eventualWait, "all records should be committed after shutdown",
	)
}

func TestE2E_Chaos_PermanentFailureStopsRunnerAndRestartResumes(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "restart-input")
	outputTopic := testTopicName(t, "restart-output")
	dlqTopic := testTopicName(t, "restart-dlq")
	groupID := testGroupID(t, "restart")

	createTopics(t, broker, 1, inputTopic, outputTopic, dlqTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.Map(
			source, func(_ context.Context, key, value string) (string, string, error) {
				if key == "poison" {
					return "", "", errors.New("poison record")
				}
				return key, value, nil
			},
		)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// phase 1: default error handler (SilentFail) - the poison record kills
	// the runner after the two records before it were processed
	client1, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)

	app1, err := streams.NewApplication(client1, buildTopology().Build())
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- app1.RunWith(ctx1, runner.NewSingleThreadedRunner())
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	produceOrderedRecords(
		t, broker, inputTopic, []kgo.Record{
			{Key: []byte("good-1"), Value: []byte("v1")},
			{Key: []byte("good-2"), Value: []byte("v2")},
			{Key: []byte("poison"), Value: []byte("v3")},
			{Key: []byte("good-3"), Value: []byte("v4")},
		},
	)

	select {
	case err := <-errCh1:
		require.Error(t, err, "the poison record should stop the runner")
		require.ErrorContains(t, err, "poison record")
	case <-time.After(consumeWait):
		t.Fatal("timeout waiting for the runner to die on the poison record")
	}
	client1.Close()

	// the records processed before the failure were committed on shutdown;
	// the poison record was not
	eventually(
		t, func() bool {
			return committedSum(t, broker, groupID, inputTopic) == 2
		}, eventualWait, "the two records before the poison record should be committed",
	)

	// phase 2: restart the same group with a DLQ handler - processing resumes
	// from the committed offset, dead-letters the poison record, and continues
	client2, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client2.Close()

	app2, err := streams.NewApplication(client2, buildTopology().Build())
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- app2.RunWith(
			ctx2,
			runner.NewSingleThreadedRunner(
				runner.WithErrorHandler(
					errorhandler.WithMaxAttempts(
						2, backoff.NewFixed(50*time.Millisecond), errorhandler.WithDLQ(dlqTopic, nil),
					),
				),
			),
		)
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	dlqRecords := consumeRecordsFull(t, broker, dlqTopic, testGroupID(t, "dlq-verifier"), 1, consumeWait)
	require.Equal(t, "poison", dlqRecords[0].Key)
	attempt, ok := headerValue(dlqRecords[0].Headers, "x-error-attempt")
	require.True(t, ok)
	require.Equal(t, "2", attempt)

	// at-least-once: every good record surfaces on the output; duplicates are
	// permitted (phase 1's commit could in principle be lost) but logged
	goodKeys := map[string]bool{"good-1": true, "good-2": true, "good-3": true}
	consumed := consumeUntilKeys(t, broker, outputTopic, testGroupID(t, "out-verifier"), goodKeys, consumeWait)
	if dups := len(consumed) - len(goodKeys); dups > 0 {
		t.Logf("observed %d duplicate output records (at-least-once redelivery)", dups)
	}

	cancel2()
	waitForShutdown(t, errCh2, shutdownWait)

	eventually(
		t, func() bool {
			return committedSum(t, broker, groupID, inputTopic) == 4
		}, eventualWait, "the restarted runner should commit through the end of the input",
	)
}

func TestE2E_Chaos_SlowConsumerBackpressureAcrossRebalance(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "slow-input")
	outputTopic := testTopicName(t, "slow-output")
	groupID := testGroupID(t, "slow")

	createTopics(t, broker, 3, inputTopic, outputTopic)

	const recordCount = 300
	const perRecordDelay = 15 * time.Millisecond

	testData := make(map[string]string, recordCount)
	allKeys := make(map[string]bool, recordCount)
	for i := 0; i < recordCount; i++ {
		k := fmt.Sprintf("key-%03d", i)
		testData[k] = "v"
		allKeys[k] = true
	}

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.Map(
			source, func(_ context.Context, key, value string) (string, string, error) {
				time.Sleep(perRecordDelay)
				return key, value, nil
			},
		)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	slowRunner := func() runner.Factory {
		// a small buffer keeps the workers backpressured while records pile up
		return runner.NewPartitionedRunner(runner.WithChannelBufferSize(8))
	}

	client1, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client1.Close()

	app1, err := streams.NewApplication(client1, buildTopology().Build())
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- app1.RunWith(ctx1, slowRunner())
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)
	produceRecords(t, broker, inputTopic, testData)

	// let the first instance work through roughly a third of the backlog
	eventually(
		t, func() bool {
			return getEndOffsetSum(t, broker, outputTopic) >= 100
		}, consumeWait, "the first instance should make progress against the backlog",
	)

	// scale out mid-backlog: a second instance joins the group (rebalance 1)
	client2, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)

	app2, err := streams.NewApplication(client2, buildTopology().Build())
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- app2.RunWith(ctx2, slowRunner())
	}()

	waitForGroupMembers(t, broker, groupID, 2, 30*time.Second)

	// and leaves again while the backlog is still draining (rebalance 2);
	// closing the client is what actually leaves the group
	cancel2()
	waitForShutdown(t, errCh2, shutdownWait)
	client2.Close()
	waitForGroupMembers(t, broker, groupID, 1, 30*time.Second)

	// no record may be lost across the rebalances; duplicates are permitted
	// (at-least-once) but logged
	eventually(
		t, func() bool {
			return getEndOffsetSum(t, broker, outputTopic) >= recordCount
		}, 2*consumeWait, "the backlog should fully drain after the rebalances",
	)

	consumed := consumeUntilKeys(t, broker, outputTopic, testGroupID(t, "verifier"), allKeys, 2*consumeWait)
	if dups := len(consumed) - recordCount; dups > 0 {
		t.Logf("observed %d duplicate output records across rebalances (at-least-once redelivery)", dups)
	}

	cancel1()
	waitForShutdown(t, errCh1, shutdownWait)

	eventually(
		t, func() bool {
			return committedSum(t, broker, groupID, inputTopic) == recordCount
		}, eventualWait, "the group should commit through the end of the input",
	)
}
