//go:build unit

package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/stretchr/testify/require"
)

func TestSingleThreaded_BasicProcessing(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
		mockkafka.SimpleRecord("k3", "v3"),
	)

	r := buildRunner(t, singleThreadedFactory(), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 3
		}, 3*time.Second, 10*time.Millisecond, "all records should be produced",
	)

	require.NoError(t, rr.CancelAndWait(t))

	client.AssertProducedString(t, "output", "k1", "v1")
	client.AssertProducedString(t, "output", "k2", "v2")
	client.AssertProducedString(t, "output", "k3", "v3")
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 3)
}

func TestSingleThreaded_SlowRecordStallsSubsequentRecords(t *testing.T) {
	gate := make(chan struct{})
	topo := createGatedTopology(gate, "slow")

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("slow-k", "v"))
	client.AddRecords(
		"input", 1,
		mockkafka.SimpleRecord("fast-k1", "v1"),
		mockkafka.SimpleRecord("fast-k2", "v2"),
	)

	r := buildRunner(t, singleThreadedFactory(), topo, client, nil)
	rr := startRunner(t, r)

	// the single poll batch is processed sequentially, so the gated record
	// stalls every record polled after it — including partition 1's second
	// record, which round-robin polling always places after the gated one
	require.Never(
		t, func() bool {
			for _, rec := range client.ProducedRecordsForTopic("output") {
				if string(rec.Key) == "fast-k2" {
					return true
				}
			}
			return false
		}, 300*time.Millisecond, 25*time.Millisecond,
		"records polled after the slow record must not be processed while it blocks",
	)

	close(gate)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 3
		}, 3*time.Second, 10*time.Millisecond, "all records should be produced once the slow record completes",
	)

	require.NoError(t, rr.CancelAndWait(t))

	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 1}, 2)
}

func TestSingleThreaded_HungProcessorBlocksShutdown(t *testing.T) {
	// documents current behavior: no per-record timeout exists, so a processor
	// that ignores its context blocks shutdown until it finishes on its own
	const hang = 300 * time.Millisecond

	started := make(chan struct{})
	topo := createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		close(started)
		time.Sleep(hang) // deliberately ignores ctx
		return k, v, nil
	})

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))

	r := buildRunner(t, singleThreadedFactory(), topo, client, nil)

	start := time.Now()
	rr := startRunner(t, r)

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for processing to start")
	}

	require.NoError(t, rr.CancelAndWait(t))
	require.GreaterOrEqual(
		t, time.Since(start), hang,
		"Run should not return until the hung processor finishes",
	)

	// the record completed after cancellation, so it is still committed
	client.AssertProducedString(t, "output", "k1", "v1")
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_SlowRecordDelaysCommit(t *testing.T) {
	gate := make(chan struct{})
	topo := createGatedTopology(gate, "slow")

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("slow-k", "v"))

	r := buildRunner(t, singleThreadedFactory(), topo, client, nil)
	rr := startRunner(t, r)

	require.Never(
		t, func() bool {
			return len(client.MarkedOffsets()) > 0 || len(client.CommittedOffsets()) > 0
		}, 300*time.Millisecond, 25*time.Millisecond,
		"an in-flight record must not be marked or committed",
	)

	close(gate)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "record should be produced once released",
	)

	require.NoError(t, rr.CancelAndWait(t))
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_PollErrorRetriesWithBackoff(t *testing.T) {
	topo := createTestTopology()

	errPollDown := errors.New("broker unavailable")
	var polls atomic.Int32

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
	)
	client.SetPollErrorFunc(func() error {
		if polls.Add(1) <= 3 {
			return errPollDown
		}
		return nil
	})

	recLogger, logRec := newRecordingLogger()
	r := buildRunner(
		t,
		singleThreadedFactory(
			WithLogger(recLogger),
			WithPollErrorBackoff(backoff.NewFixed(10*time.Millisecond)),
		),
		topo, client, nil,
	)

	start := time.Now()
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2
		}, 3*time.Second, 10*time.Millisecond, "records should be produced once polling recovers",
	)

	require.GreaterOrEqual(
		t, time.Since(start), 30*time.Millisecond,
		"three poll failures must each incur the poll error backoff",
	)
	require.GreaterOrEqual(
		t, logRec.CountLevelAndPrefix(logger.WarnLevel, "Poll error"), 3,
		"each poll failure should be logged as a warning",
	)

	require.NoError(t, rr.CancelAndWait(t))
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 2)
}

func TestSingleThreaded_CommitErrorOnShutdownIsLoggedNotFatal(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
	)

	recLogger, logRec := newRecordingLogger()
	r := buildRunner(t, singleThreadedFactory(WithLogger(recLogger)), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2
		}, 3*time.Second, 10*time.Millisecond, "records should be produced",
	)

	client.SetCommitError(errors.New("commit refused"))

	require.NoError(t, rr.CancelAndWait(t), "a failed shutdown commit must not fail Run")
	require.Equal(
		t, 1, logRec.CountLevelAndPrefix(logger.ErrorLevel, "Failed to commit offsets during shutdown"),
		"the failed shutdown commit should be logged",
	)
	require.Empty(t, client.CommittedOffsets(), "nothing should be committed when the shutdown commit fails")
}

func TestSingleThreaded_FlushErrorOnShutdownIsLoggedNotFatal(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))

	recLogger, logRec := newRecordingLogger()
	r := buildRunner(t, singleThreadedFactory(WithLogger(recLogger)), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "record should be produced",
	)

	client.SetFlushError(errors.New("flush refused"))

	require.NoError(t, rr.CancelAndWait(t), "a failed shutdown flush must not fail Run")
	require.Equal(
		t, 1, logRec.CountLevelAndPrefix(logger.ErrorLevel, "Failed to flush producer during shutdown"),
		"the failed shutdown flush should be logged",
	)
	// the commit ran before the flush failure, so offsets are still committed
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_RunnerStopsOnFatalAndDoesNotCommitFailedRecord(t *testing.T) {
	procErr := errors.New("permanent processing failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("good1", "v1"),
		mockkafka.SimpleRecord("fail", "v2"),
		mockkafka.SimpleRecord("good2", "v3"),
	)

	// default error handler is SilentFail: the first failure is fatal
	r := buildRunner(t, singleThreadedFactory(), topo, client, nil)
	rr := startRunner(t, r)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorIs(t, err, procErr, "Run should return the processing error")

	require.Equal(t, 1, counter.Count("fail"), "the failing record should be attempted exactly once")
	client.AssertProducedCount(t, 1)
	client.AssertProducedString(t, "output", "good1", "v1")

	// shutdown commits the record processed before the failure, but never the
	// failed record or anything after it
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}
