//go:build unit

package runner

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The TestChaos_ tests drive mixed adversarial workloads — flaky, poison, and
// slow records plus intermittent poll errors — through a runner and assert the
// end-to-end guarantees: no record lost, no record duplicated, per-partition
// ordering preserved, poison records dead-lettered, offsets committed.
// Run in isolation with `mise run test-chaos`.

// chaosSeed makes the generated workload reproducible; log output includes it
// so a failure can be replayed by pinning the same seed.
const chaosSeed int64 = 20260709

var (
	errChaosFlaky  = errors.New("chaos: transient failure")
	errChaosPoison = errors.New("chaos: poison record")
)

// chaosWorkload is a deterministic pseudo-random record set. Keys encode the
// partition and record class ("p2-flaky-17") so assertions and the processor
// need no shared mutable state beyond the invocation counter.
type chaosWorkload struct {
	partitions   int32
	perPartition int

	// keysByPartition preserves input order per partition
	keysByPartition map[int32][]string
	flakyFails      map[string]int
	slowDelay       map[string]time.Duration
	poisonKeys      map[string]bool

	totalFlakyFailures int
}

func generateChaosWorkload(seed int64, partitions int32, perPartition int) *chaosWorkload {
	rng := rand.New(rand.NewSource(seed))

	w := &chaosWorkload{
		partitions:      partitions,
		perPartition:    perPartition,
		keysByPartition: make(map[int32][]string),
		flakyFails:      make(map[string]int),
		slowDelay:       make(map[string]time.Duration),
		poisonKeys:      make(map[string]bool),
	}

	for p := int32(0); p < partitions; p++ {
		for i := 0; i < perPartition; i++ {
			class := "ok"
			switch roll := rng.Float64(); {
			case roll < 0.15:
				class = "flaky"
			case roll < 0.23:
				class = "poison"
			case roll < 0.31:
				class = "slow"
			}

			key := fmt.Sprintf("p%d-%s-%d", p, class, i)
			w.keysByPartition[p] = append(w.keysByPartition[p], key)

			switch class {
			case "flaky":
				fails := 1 + rng.Intn(2)
				w.flakyFails[key] = fails
				w.totalFlakyFailures += fails
			case "poison":
				w.poisonKeys[key] = true
			case "slow":
				w.slowDelay[key] = time.Duration(2+rng.Intn(3)) * time.Millisecond
			}
		}
	}

	return w
}

func (w *chaosWorkload) totalRecords() int {
	return int(w.partitions) * w.perPartition
}

func (w *chaosWorkload) allKeys() map[string]bool {
	keys := make(map[string]bool, w.totalRecords())
	for _, partitionKeys := range w.keysByPartition {
		for _, k := range partitionKeys {
			keys[k] = true
		}
	}
	return keys
}

// expectedOutputOrder returns the input order of a partition minus its poison
// keys, which end up in the DLQ instead of the output topic.
func (w *chaosWorkload) expectedOutputOrder(partition int32) []string {
	var expected []string
	for _, k := range w.keysByPartition[partition] {
		if !w.poisonKeys[k] {
			expected = append(expected, k)
		}
	}
	return expected
}

// topology builds the chaos processor: poison keys always fail, flaky keys
// fail their first N invocations, slow keys sleep their per-key delay.
func (w *chaosWorkload) topology(counter *invocationCounter) *topology.Topology {
	return createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		switch {
		case w.poisonKeys[k]:
			return "", "", errChaosPoison
		case w.flakyFails[k] > 0:
			if counter.Inc(k) <= w.flakyFails[k] {
				return "", "", errChaosFlaky
			}
		case w.slowDelay[k] > 0:
			time.Sleep(w.slowDelay[k])
		}
		return k, v, nil
	})
}

// addToClient seeds the mock with every record, in order, per partition.
func (w *chaosWorkload) addToClient(client *mockkafka.Client) {
	for p := int32(0); p < w.partitions; p++ {
		for _, k := range w.keysByPartition[p] {
			client.AddRecords("input", p, mockkafka.SimpleRecord(k, "v-"+k))
		}
	}
}

// assertNoLossNoDuplicates verifies every key surfaced exactly once across the
// output and DLQ topics, and that only poison keys reached the DLQ.
func (w *chaosWorkload) assertNoLossNoDuplicates(t *testing.T, client *mockkafka.Client) {
	t.Helper()

	seen := make(map[string]int)
	for _, k := range producedKeys(client, "output") {
		seen[k]++
	}
	dlqKeys := make(map[string]bool)
	for _, k := range producedKeys(client, "dlq") {
		seen[k]++
		dlqKeys[k] = true
	}

	all := w.allKeys()
	for k := range all {
		require.Equal(t, 1, seen[k], "key %s must surface exactly once across output and DLQ", k)
	}
	require.Len(t, seen, len(all), "no unexpected keys may be produced")
	require.Equal(t, w.poisonKeys, dlqKeys, "exactly the poison keys must reach the DLQ")
}

// assertOrderingAndCommits verifies per-partition output ordering and that the
// commit for each partition covers all of its records.
func (w *chaosWorkload) assertOrderingAndCommits(t *testing.T, client *mockkafka.Client) {
	t.Helper()

	outputKeys := producedKeys(client, "output")
	for p := int32(0); p < w.partitions; p++ {
		prefix := fmt.Sprintf("p%d-", p)
		require.Equal(
			t, w.expectedOutputOrder(p), keysWithPrefix(outputKeys, prefix),
			"partition %d output must preserve input order", p,
		)
		client.AssertCommittedOffset(
			t, kafka.TopicPartition{Topic: "input", Partition: p}, int64(w.perPartition),
		)
	}
}

// assertDLQMetadata verifies every DLQ record carries the failure headers.
func assertDLQMetadata(t *testing.T, client *mockkafka.Client, attempts string) {
	t.Helper()

	for _, rec := range client.ProducedRecordsForTopic("dlq") {
		client.AssertHeader(t, "dlq", rec.Key, "x-original-topic", []byte("input"))
		client.AssertHeader(t, "dlq", rec.Key, "x-error-phase", []byte("processing"))
		client.AssertHeader(t, "dlq", rec.Key, "x-error-attempt", []byte(attempts))
	}
}

// flakyPollErrors makes every nth poll fail, exercising the ErrPoll retry loop
// underneath the record-level chaos.
func flakyPollErrors(client *mockkafka.Client, n int32) {
	var polls atomic.Int32
	client.SetPollErrorFunc(func() error {
		if polls.Add(1)%n == 0 {
			return errors.New("chaos: poll failure")
		}
		return nil
	})
}

// requireMixedClasses guards against a seed that generates a vacuous
// workload — every record class must actually be present.
func requireMixedClasses(t *testing.T, w *chaosWorkload) {
	t.Helper()
	require.NotEmpty(t, w.poisonKeys, "seed must generate poison records")
	require.NotEmpty(t, w.flakyFails, "seed must generate flaky records")
	require.NotEmpty(t, w.slowDelay, "seed must generate slow records")
}

func TestChaos_MixedWorkload_Partitioned(t *testing.T) {
	t.Logf("chaos seed: %d", chaosSeed)
	w := generateChaosWorkload(chaosSeed, 4, 50)
	requireMixedClasses(t, w)
	counter := newInvocationCounter()

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(16))
	w.addToClient(client)
	flakyPollErrors(client, 7)

	r := buildRunner(
		t,
		NewPartitionedRunner(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					4, backoff.NewFixed(time.Millisecond), errorhandler.WithDLQ("dlq", nil),
				),
			),
			// a small buffer forces backpressure pause/resume cycles under load
			WithChannelBufferSize(4),
			WithPollErrorBackoff(backoff.NewFixed(time.Millisecond)),
		),
		w.topology(counter), client, nil,
	)
	pr := r.(*PartitionedRunner)
	rr := startRunner(t, r)

	expectedOutput := w.totalRecords() - len(w.poisonKeys)
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("output")) == expectedOutput &&
				len(client.ProducedRecordsForTopic("dlq")) == len(w.poisonKeys) &&
				len(pr.PausedPartitions()) == 0
		}, 10*time.Second, 25*time.Millisecond,
		"all records should surface on output or DLQ and all partitions should resume",
	)

	require.NoError(t, rr.CancelAndWait(t))

	w.assertNoLossNoDuplicates(t, client)
	w.assertOrderingAndCommits(t, client)
	assertDLQMetadata(t, client, "4")
}

func TestChaos_MixedWorkload_SingleThreaded(t *testing.T) {
	t.Logf("chaos seed: %d", chaosSeed)
	w := generateChaosWorkload(chaosSeed, 2, 40)
	requireMixedClasses(t, w)
	counter := newInvocationCounter()

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(16))
	w.addToClient(client)
	flakyPollErrors(client, 7)

	r := buildRunner(
		t,
		singleThreadedFactory(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					4, backoff.NewFixed(time.Millisecond), errorhandler.WithDLQ("dlq", nil),
				),
			),
			WithPollErrorBackoff(backoff.NewFixed(time.Millisecond)),
		),
		w.topology(counter), client, nil,
	)
	rr := startRunner(t, r)

	expectedOutput := w.totalRecords() - len(w.poisonKeys)
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("output")) == expectedOutput &&
				len(client.ProducedRecordsForTopic("dlq")) == len(w.poisonKeys)
		}, 10*time.Second, 25*time.Millisecond,
		"all records should surface on output or DLQ",
	)

	require.NoError(t, rr.CancelAndWait(t))

	w.assertNoLossNoDuplicates(t, client)
	w.assertOrderingAndCommits(t, client)
	assertDLQMetadata(t, client, "4")
}

func TestChaos_RetryStorm_Partitioned(t *testing.T) {
	t.Logf("chaos seed: %d", chaosSeed)

	// every record is flaky: 8 partitions x 25 records, each failing 1-3 times
	const partitions, perPartition = 8, 25
	rng := rand.New(rand.NewSource(chaosSeed))

	w := &chaosWorkload{
		partitions:      partitions,
		perPartition:    perPartition,
		keysByPartition: make(map[int32][]string),
		flakyFails:      make(map[string]int),
		slowDelay:       make(map[string]time.Duration),
		poisonKeys:      make(map[string]bool),
	}
	for p := int32(0); p < partitions; p++ {
		for i := 0; i < perPartition; i++ {
			key := fmt.Sprintf("p%d-flaky-%d", p, i)
			w.keysByPartition[p] = append(w.keysByPartition[p], key)
			fails := 1 + rng.Intn(3)
			w.flakyFails[key] = fails
			w.totalFlakyFailures += fails
		}
	}

	counter := newInvocationCounter()
	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(16))
	w.addToClient(client)

	_, metricReader, tel := setupOtelTest(t)

	// LogAndFail fallback: if any record exhausts its retries the runner dies
	// and the test fails loudly via CancelAndWait
	r := buildRunner(
		t,
		NewPartitionedRunner(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					6, backoff.NewFixed(time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
				),
			),
		),
		w.topology(counter), client, tel,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("output")) == w.totalRecords()
		}, 10*time.Second, 25*time.Millisecond, "every record should eventually succeed",
	)

	require.NoError(t, rr.CancelAndWait(t))

	w.assertNoLossNoDuplicates(t, client)
	w.assertOrderingAndCommits(t, client)

	var rm metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(context.Background(), &rm))
	require.EqualValues(
		t, w.totalFlakyFailures, sumInt64(t, rm, "stream.process.retries", "", ""),
		"the retry count must equal the exact number of injected failures",
	)
}
