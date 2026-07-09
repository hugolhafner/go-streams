//go:build unit

package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The tests in this file exercise the shared record failure handling in
// processRecordWithRetry (runner/common.go) through both runner types.
// Timing assertions are lower bounds only; upper bounds are coarse liveness
// ceilings so scheduling jitter cannot flake them.

// weirdAction reports an action type the runner does not know about.
type weirdAction struct{}

func (weirdAction) Type() errorhandler.ActionType { return errorhandler.ActionType(99) }

// forgedDLQAction claims to be a DLQ action without being ActionSendToDLQ.
type forgedDLQAction struct{}

func (forgedDLQAction) Type() errorhandler.ActionType { return errorhandler.ActionTypeSendToDLQ }

func testRetryEventuallySucceeds(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("transient failure")
	counter := newInvocationCounter()
	topo := createFlakyTopology(counter, 5, procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))

	_, metricReader, tel := setupOtelTest(t)

	rec := newRecordingHandler(
		errorhandler.WithMaxAttempts(
			8, backoff.NewFixed(10*time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
		),
	)

	r := buildRunner(t, makeFactory(WithErrorHandler(rec)), topo, client, tel)

	start := time.Now()
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "record should be produced after retries",
	)
	elapsed := time.Since(start)

	require.NoError(t, rr.CancelAndWait(t))

	require.GreaterOrEqual(
		t, elapsed, 50*time.Millisecond,
		"five retries must each incur the 10ms backoff",
	)
	require.Equal(t, 6, counter.Count("k1"), "five failures plus the final success")
	require.Equal(t, []int{1, 2, 3, 4, 5}, rec.Attempts(), "handler should see each attempt in order")
	for _, c := range rec.Calls() {
		require.Equal(t, errorhandler.PhaseProcessing, c.EC.Phase)
		require.Equal(t, "proc", c.EC.NodeName)
		require.Equal(t, errorhandler.ActionTypeRetry, c.Action.Type())
		require.ErrorIs(t, c.EC.Error, procErr)
	}

	client.AssertProducedCountForTopic(t, "output", 1)
	client.AssertProducedString(t, "output", "k1", "v1")
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(context.Background(), &rm))
	require.EqualValues(t, 5, sumInt64(t, rm, "stream.process.retries", "", ""))
	require.EqualValues(t, 5, sumInt64(t, rm, "stream.errors", "stream.error.action", "retry"))
}

func TestSingleThreaded_RetryEventuallySucceeds(t *testing.T) {
	testRetryEventuallySucceeds(t, singleThreadedFactory)
}

func TestPartitionedRunner_RetryEventuallySucceeds(t *testing.T) {
	testRetryEventuallySucceeds(t, partitionedFactory)
}

func testHighRetryCountWarnsAtThreshold(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("transient failure")
	counter := newInvocationCounter()
	topo := createFlakyTopology(counter, 10, procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))

	recLogger, logRec := newRecordingLogger()
	r := buildRunner(
		t,
		makeFactory(
			WithLogger(recLogger),
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					15, backoff.NewFixed(time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
				),
			),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "record should be produced after retries",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 11, counter.Count("k1"), "ten failures plus the final success")
	require.Equal(
		t, 1, logRec.CountLevelAndPrefix(logger.WarnLevel, "Record seen high number of retry attempts"),
		"the high-retry warning should fire exactly once, when the attempt count reaches 10",
	)
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_HighRetryCountWarnsAtThreshold(t *testing.T) {
	testHighRetryCountWarnsAtThreshold(t, singleThreadedFactory)
}

func TestPartitionedRunner_HighRetryCountWarnsAtThreshold(t *testing.T) {
	testHighRetryCountWarnsAtThreshold(t, partitionedFactory)
}

func testRetryExhaustionFallsBackToContinue(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("permanent failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("fail", "v1"),
		mockkafka.SimpleRecord("ok", "v2"),
	)

	r := buildRunner(
		t,
		makeFactory(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					3, backoff.NewFixed(time.Millisecond), errorhandler.LogAndContinue(logger.NewNoopLogger()),
				),
			),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "the record after the failing one should be produced",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 3, counter.Count("fail"), "the failing record should be attempted exactly maxAttempts times")
	client.AssertNotProduced(t, "output", []byte("fail"))
	client.AssertProducedString(t, "output", "ok", "v2")

	// the exhausted record is skipped, so the commit advances past it
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 2)
}

func TestSingleThreaded_RetryExhaustionFallsBackToContinue(t *testing.T) {
	testRetryExhaustionFallsBackToContinue(t, singleThreadedFactory)
}

func TestPartitionedRunner_RetryExhaustionFallsBackToContinue(t *testing.T) {
	testRetryExhaustionFallsBackToContinue(t, partitionedFactory)
}

func testRetryExhaustionFallsBackToFail(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("permanent failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))

	r := buildRunner(
		t,
		makeFactory(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					3, backoff.NewFixed(time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
				),
			),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorIs(t, err, procErr, "Run should return the processing error once retries are exhausted")

	require.Equal(t, 3, counter.Count("fail"), "the failing record should be attempted exactly maxAttempts times")
	client.AssertNoProducedRecords(t)
	require.Empty(t, client.CommittedOffsets(), "a failed record must not be committed")
}

func TestSingleThreaded_RetryExhaustionFallsBackToFail(t *testing.T) {
	testRetryExhaustionFallsBackToFail(t, singleThreadedFactory)
}

func TestPartitionedRunner_RetryExhaustionFallsBackToFail(t *testing.T) {
	testRetryExhaustionFallsBackToFail(t, partitionedFactory)
}

func testFailStopsRunnerWithoutCommit(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("permanent failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("fail", "v1"),
		mockkafka.SimpleRecord("ok", "v2"),
	)

	// default error handler is SilentFail
	r := buildRunner(t, makeFactory(), topo, client, nil)
	rr := startRunner(t, r)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorIs(t, err, procErr, "Run should return the processing error")

	require.Equal(t, 1, counter.Count("fail"), "the failing record should be attempted exactly once")
	client.AssertNoProducedRecords(t)
	require.Empty(t, client.CommittedOffsets(), "neither the failed record nor anything after it may be committed")
}

func TestSingleThreaded_FailStopsRunnerWithoutCommit(t *testing.T) {
	testFailStopsRunnerWithoutCommit(t, singleThreadedFactory)
}

func TestPartitionedRunner_FailStopsRunnerWithoutCommit(t *testing.T) {
	testFailStopsRunnerWithoutCommit(t, partitionedFactory)
}

func testDLQRouteSendsRecordWithHeadersAndCommits(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("poison record")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.Record("fail", "v-poison").WithHeader("orig-h", []byte("orig-v")).Build(),
		mockkafka.SimpleRecord("ok", "v-ok"),
	)

	r := buildRunner(
		t,
		makeFactory(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					3, backoff.NewFixed(time.Millisecond), errorhandler.WithDLQ("dlq", nil),
				),
			),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("dlq")) == 1 &&
				len(client.ProducedRecordsForTopic("output")) == 1
		}, 3*time.Second, 10*time.Millisecond, "poison record should reach the DLQ and the next record the output",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 3, counter.Count("fail"), "the poison record should exhaust its retries before the DLQ")

	dlqKey := []byte("fail")
	client.AssertProduced(t, "dlq", dlqKey, []byte("v-poison"))
	client.AssertHeader(t, "dlq", dlqKey, "orig-h", []byte("orig-v"))
	client.AssertHeader(t, "dlq", dlqKey, "x-original-topic", []byte("input"))
	client.AssertHeader(t, "dlq", dlqKey, "x-original-partition", []byte("0"))
	client.AssertHeader(t, "dlq", dlqKey, "x-original-offset", []byte("0"))
	client.AssertHeader(t, "dlq", dlqKey, "x-error-attempt", []byte("3"))
	client.AssertHeader(t, "dlq", dlqKey, "x-error-phase", []byte("processing"))
	client.AssertHeader(t, "dlq", dlqKey, "x-error-node", []byte("proc"))

	dlqRecords := client.ProducedRecordsForTopic("dlq")
	msg, ok := kafka.HeaderValue(dlqRecords[0].Headers, "x-error-message")
	require.True(t, ok, "DLQ record should carry the error message header")
	require.Contains(t, string(msg), "poison record")

	client.AssertNotProduced(t, "output", dlqKey)
	client.AssertProducedString(t, "output", "ok", "v-ok")

	// DLQ'd records are committed, so processing moves on
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 2)
}

func TestSingleThreaded_DLQRouteSendsRecordWithHeadersAndCommits(t *testing.T) {
	testDLQRouteSendsRecordWithHeadersAndCommits(t, singleThreadedFactory)
}

func TestPartitionedRunner_DLQRouteSendsRecordWithHeadersAndCommits(t *testing.T) {
	testDLQRouteSendsRecordWithHeadersAndCommits(t, partitionedFactory)
}

func testDLQImmediate(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("poison record")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))

	// WithDLQ without retries: the first failure goes straight to the DLQ
	r := buildRunner(t, makeFactory(WithErrorHandler(errorhandler.WithDLQ("dlq", nil))), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("dlq")) == 1
		}, 3*time.Second, 10*time.Millisecond, "record should reach the DLQ on the first failure",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 1, counter.Count("fail"))
	client.AssertHeader(t, "dlq", []byte("fail"), "x-error-attempt", []byte("1"))
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_DLQImmediate(t *testing.T) {
	testDLQImmediate(t, singleThreadedFactory)
}

func TestPartitionedRunner_DLQImmediate(t *testing.T) {
	testDLQImmediate(t, partitionedFactory)
}

func testDLQSendFailureIsFatal(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("poison record")
	errDLQDown := errors.New("dlq topic unavailable")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))
	client.SetSendErrorFunc(func(topic string, _, _ []byte) error {
		if topic == "dlq" {
			return errDLQDown
		}
		return nil
	})

	r := buildRunner(t, makeFactory(WithErrorHandler(errorhandler.WithDLQ("dlq", nil))), topo, client, nil)
	rr := startRunner(t, r)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorIs(t, err, errDLQDown, "a failed DLQ produce must stop the runner")

	client.AssertProducedCountForTopic(t, "dlq", 0)
	require.Empty(t, client.MarkedOffsets(), "the record must not be marked when its DLQ produce fails")
	require.Empty(t, client.CommittedOffsets(), "the record must not be committed when its DLQ produce fails")
}

func TestSingleThreaded_DLQSendFailureIsFatal(t *testing.T) {
	testDLQSendFailureIsFatal(t, singleThreadedFactory)
}

func TestPartitionedRunner_DLQSendFailureIsFatal(t *testing.T) {
	testDLQSendFailureIsFatal(t, partitionedFactory)
}

func testSerdeErrorRoutedToSerdeHandler(t *testing.T, makeFactory func(...commonOption) Factory) {
	topo := createFailingSerdeTopology("bad")

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "bad-payload"),
		mockkafka.SimpleRecord("k2", "good-payload"),
	)

	serdeH := newRecordingHandler(errorhandler.LogAndContinue(logger.NewNoopLogger()))
	defaultH := newRecordingHandler(errorhandler.LogAndFail(logger.NewNoopLogger()))

	r := buildRunner(
		t, makeFactory(WithErrorHandler(defaultH), WithSerdeErrorHandler(serdeH)), topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "the record with the healthy payload should be produced",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 1, serdeH.Len(), "the serde handler should receive the deserialisation error")
	call := serdeH.Calls()[0]
	require.Equal(t, errorhandler.PhaseSerde, call.EC.Phase)
	require.Empty(t, call.EC.NodeName, "serde errors occur outside topology nodes")
	require.Equal(t, 1, call.EC.Attempt)
	require.Zero(t, defaultH.Len(), "the default handler must not be consulted when a serde handler is set")

	client.AssertNotProduced(t, "output", []byte("k1"))
	client.AssertProducedString(t, "output", "k2", "good-payload")
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 2)
}

func TestSingleThreaded_SerdeErrorRoutedToSerdeHandler(t *testing.T) {
	testSerdeErrorRoutedToSerdeHandler(t, singleThreadedFactory)
}

func TestPartitionedRunner_SerdeErrorRoutedToSerdeHandler(t *testing.T) {
	testSerdeErrorRoutedToSerdeHandler(t, partitionedFactory)
}

func testProductionErrorRoutedToProductionHandler(t *testing.T, makeFactory func(...commonOption) Factory) {
	topo := createTestTopology()
	errSend := errors.New("output topic unavailable")

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))
	client.SetSendErrorFunc(func(topic string, _, _ []byte) error {
		if topic == "output" {
			return errSend
		}
		return nil
	})

	prodH := newRecordingHandler(errorhandler.LogAndContinue(logger.NewNoopLogger()))
	defaultH := newRecordingHandler(errorhandler.LogAndFail(logger.NewNoopLogger()))

	r := buildRunner(
		t, makeFactory(WithErrorHandler(defaultH), WithProductionErrorHandler(prodH)), topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return prodH.Len() == 1
		}, 3*time.Second, 10*time.Millisecond, "the production handler should receive the produce error",
	)

	require.NoError(t, rr.CancelAndWait(t))

	call := prodH.Calls()[0]
	require.Equal(t, errorhandler.PhaseProduction, call.EC.Phase)
	require.Equal(t, "sink", call.EC.NodeName)
	require.ErrorIs(t, call.EC.Error, errSend)
	require.Zero(t, defaultH.Len(), "the default handler must not be consulted when a production handler is set")

	client.AssertProducedCountForTopic(t, "output", 0)
	// the record was skipped by the Continue action, so the commit advances
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_ProductionErrorRoutedToProductionHandler(t *testing.T) {
	testProductionErrorRoutedToProductionHandler(t, singleThreadedFactory)
}

func TestPartitionedRunner_ProductionErrorRoutedToProductionHandler(t *testing.T) {
	testProductionErrorRoutedToProductionHandler(t, partitionedFactory)
}

func testProductionErrorRetriedThenSucceeds(t *testing.T, makeFactory func(...commonOption) Factory) {
	topo := createTestTopology()
	errSend := errors.New("output topic unavailable")

	var sends atomic.Int32
	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))
	client.SetSendErrorFunc(func(topic string, _, _ []byte) error {
		if topic == "output" && sends.Add(1) <= 2 {
			return errSend
		}
		return nil
	})

	r := buildRunner(
		t,
		makeFactory(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					5, backoff.NewFixed(time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
				),
			),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecordsForTopic("output")) == 1
		}, 3*time.Second, 10*time.Millisecond, "retries should re-drive the sink until the produce succeeds",
	)

	require.NoError(t, rr.CancelAndWait(t))

	client.AssertProducedCountForTopic(t, "output", 1)
	client.AssertProducedString(t, "output", "k1", "v1")
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 1)
}

func TestSingleThreaded_ProductionErrorRetriedThenSucceeds(t *testing.T) {
	testProductionErrorRetriedThenSucceeds(t, singleThreadedFactory)
}

func TestPartitionedRunner_ProductionErrorRetriedThenSucceeds(t *testing.T) {
	testProductionErrorRetriedThenSucceeds(t, partitionedFactory)
}

func testProcessingErrorRoutedToProcessingHandler(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("processing failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("fail", "v1"),
		mockkafka.SimpleRecord("ok", "v2"),
	)

	procH := newRecordingHandler(errorhandler.LogAndContinue(logger.NewNoopLogger()))
	defaultH := newRecordingHandler(errorhandler.LogAndFail(logger.NewNoopLogger()))

	r := buildRunner(
		t, makeFactory(WithErrorHandler(defaultH), WithProcessingErrorHandler(procH)), topo, client, nil,
	)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "the healthy record should be produced",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 1, procH.Len(), "the processing handler should receive the error")
	call := procH.Calls()[0]
	require.Equal(t, errorhandler.PhaseProcessing, call.EC.Phase)
	require.Equal(t, "proc", call.EC.NodeName)
	require.ErrorIs(t, call.EC.Error, procErr)
	require.Zero(t, defaultH.Len(), "the default handler must not be consulted when a processing handler is set")

	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 2)
}

func TestSingleThreaded_ProcessingErrorRoutedToProcessingHandler(t *testing.T) {
	testProcessingErrorRoutedToProcessingHandler(t, singleThreadedFactory)
}

func TestPartitionedRunner_ProcessingErrorRoutedToProcessingHandler(t *testing.T) {
	testProcessingErrorRoutedToProcessingHandler(t, partitionedFactory)
}

func testPanicRecoveredAndRoutedAsProcessingError(t *testing.T, makeFactory func(...commonOption) Factory) {
	topo := createPanickyTopology("boom")

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("boom", "v1"),
		mockkafka.SimpleRecord("ok", "v2"),
	)

	h := newRecordingHandler(errorhandler.LogAndContinue(logger.NewNoopLogger()))
	r := buildRunner(t, makeFactory(WithErrorHandler(h)), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 10*time.Millisecond, "the runner should survive the panic and process the next record",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(t, 1, h.Len(), "the handler should receive the recovered panic")
	call := h.Calls()[0]
	require.Equal(t, errorhandler.PhaseProcessing, call.EC.Phase)
	require.Empty(t, call.EC.NodeName, "recovered panics are not attributed to a topology node")
	require.ErrorContains(t, call.EC.Error, "panic recovered")

	client.AssertProducedString(t, "output", "ok", "v2")
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 2)
}

func TestSingleThreaded_PanicRecoveredAndRoutedAsProcessingError(t *testing.T) {
	testPanicRecoveredAndRoutedAsProcessingError(t, singleThreadedFactory)
}

func TestPartitionedRunner_PanicRecoveredAndRoutedAsProcessingError(t *testing.T) {
	testPanicRecoveredAndRoutedAsProcessingError(t, partitionedFactory)
}

func testUnknownActionTypeIsFatal(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("processing failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))

	handler := errorhandler.HandlerFunc(
		func(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
			return weirdAction{}
		},
	)

	r := buildRunner(t, makeFactory(WithErrorHandler(handler)), topo, client, nil)
	rr := startRunner(t, r)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorIs(t, err, procErr, "an unknown action should fail the record with its processing error")
	require.Empty(t, client.CommittedOffsets())
}

func TestSingleThreaded_UnknownActionTypeIsFatal(t *testing.T) {
	testUnknownActionTypeIsFatal(t, singleThreadedFactory)
}

func TestPartitionedRunner_UnknownActionTypeIsFatal(t *testing.T) {
	testUnknownActionTypeIsFatal(t, partitionedFactory)
}

func testForgedDLQActionTypeIsFatal(t *testing.T, makeFactory func(...commonOption) Factory) {
	procErr := errors.New("processing failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))

	handler := errorhandler.HandlerFunc(
		func(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
			return forgedDLQAction{}
		},
	)

	r := buildRunner(t, makeFactory(WithErrorHandler(handler)), topo, client, nil)
	rr := startRunner(t, r)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorContains(t, err, "invalid action type")
	require.Empty(t, client.CommittedOffsets())
}

func TestSingleThreaded_ForgedDLQActionTypeIsFatal(t *testing.T) {
	testForgedDLQActionTypeIsFatal(t, singleThreadedFactory)
}

func TestPartitionedRunner_ForgedDLQActionTypeIsFatal(t *testing.T) {
	testForgedDLQActionTypeIsFatal(t, partitionedFactory)
}

func TestSingleThreaded_CancelDuringBackoffShutsDownPromptly(t *testing.T) {
	procErr := errors.New("permanent failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))

	rec := newRecordingHandler(
		errorhandler.WithMaxAttempts(
			1_000_000, backoff.NewFixed(50*time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
		),
	)

	r := buildRunner(t, singleThreadedFactory(WithErrorHandler(rec)), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return rec.Len() >= 2
		}, 3*time.Second, 10*time.Millisecond, "the record should be in the retry loop",
	)

	start := time.Now()
	require.NoError(t, rr.CancelAndWait(t), "cancellation during backoff is a clean shutdown")
	require.Less(t, time.Since(start), 2*time.Second, "shutdown must not wait out the remaining retries")
	require.Empty(t, client.CommittedOffsets(), "the still-failing record must not be committed")
}

func TestPartitionedRunner_CancelDuringBackoffShutsDownPromptly(t *testing.T) {
	procErr := errors.New("permanent failure")
	counter := newInvocationCounter()
	topo := createFailingTopology(counter, "fail", procErr)

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))

	rec := newRecordingHandler(
		errorhandler.WithMaxAttempts(
			1_000_000, backoff.NewFixed(50*time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
		),
	)

	r := buildRunner(t, partitionedFactory(WithErrorHandler(rec)), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return rec.Len() >= 2
		}, 3*time.Second, 10*time.Millisecond, "the record should be in the retry loop",
	)

	start := time.Now()
	err := rr.CancelAndWait(t)
	require.Less(t, time.Since(start), 3*time.Second, "shutdown must not wait out the remaining retries")

	// cancellation makes the in-flight handler return ActionFail, and the
	// resulting worker error races the context-cancelled branch in
	// runPollLoop, so Run may return either nil or the processing error
	if err != nil {
		require.ErrorIs(t, err, procErr)
	}
	require.Empty(t, client.CommittedOffsets(), "the still-failing record must not be committed")
}

func TestPartitionedRunner_RetryPreservesOrdering(t *testing.T) {
	procErr := errors.New("still failing")
	gate := make(chan struct{})
	topo := createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if k == "p0-flaky" {
			select {
			case <-gate:
				return k, v, nil
			default:
				return "", "", procErr
			}
		}
		return k, v, nil
	})

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("p0-flaky", "v1"),
		mockkafka.SimpleRecord("p0-k2", "v2"),
		mockkafka.SimpleRecord("p0-k3", "v3"),
	)
	client.AddRecords(
		"input", 1,
		mockkafka.SimpleRecord("p1-k1", "v1"),
		mockkafka.SimpleRecord("p1-k2", "v2"),
	)

	r := buildRunner(
		t,
		partitionedFactory(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					1_000_000, backoff.NewFixed(time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
				),
			),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	// partition 1 finishes while partition 0's head record is still retrying
	require.Eventually(
		t, func() bool {
			return len(keysWithPrefix(producedKeys(client, "output"), "p1-")) == 2
		}, 3*time.Second, 10*time.Millisecond, "the healthy partition should not be blocked by the retrying one",
	)
	require.Empty(
		t, keysWithPrefix(producedKeys(client, "output"), "p0-"),
		"records behind the retrying record must wait",
	)

	close(gate)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 5
		}, 3*time.Second, 10*time.Millisecond, "all records should be produced once the retrying record succeeds",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(
		t, []string{"p0-flaky", "p0-k2", "p0-k3"},
		keysWithPrefix(producedKeys(client, "output"), "p0-"),
		"retries must not reorder records within the partition",
	)
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 3)
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 1}, 2)
}

func TestPartitionedRunner_WorkerFatalErrorStopsRunner(t *testing.T) {
	procErr := errors.New("permanent failure")
	failGate := make(chan struct{})
	topo := createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if k == "fail" {
			<-failGate // hold the failure until the healthy partition finishes
			return "", "", procErr
		}
		return k, v, nil
	})

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("fail", "v1"))
	client.AddRecords(
		"input", 1,
		mockkafka.SimpleRecord("ok1", "v1"),
		mockkafka.SimpleRecord("ok2", "v2"),
	)

	// default error handler is SilentFail
	r := buildRunner(t, partitionedFactory(), topo, client, nil)
	rr := startRunner(t, r)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2
		}, 3*time.Second, 10*time.Millisecond, "the healthy partition should finish first",
	)

	close(failGate)

	err := rr.WaitErr(t, 5*time.Second)
	require.ErrorIs(t, err, procErr)
	require.ErrorContains(t, err, "fatal processing error", "worker failures should identify the worker")

	// the healthy partition's work is committed on shutdown, the failed
	// partition's is not
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 1}, 2)
	_, committed := client.CommittedOffset(kafka.TopicPartition{Topic: "input", Partition: 0})
	require.False(t, committed, "the failed record must not be committed")
}

func TestPartitionedRunner_ShutdownWithHungWorkerRespectsDrainTimeout(t *testing.T) {
	started := make(chan struct{})
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) }) // release the hung goroutine after the test

	topo := createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		close(started)
		<-gate // deliberately ignores ctx: simulates a truly hung processor
		return k, v, nil
	})

	client := mockkafka.NewClient()
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))

	r := buildRunner(
		t,
		NewPartitionedRunner(
			WithWorkerShutdownTimeout(200*time.Millisecond),
			WithDrainTimeout(500*time.Millisecond),
		),
		topo, client, nil,
	)
	rr := startRunner(t, r)

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for processing to start")
	}

	start := time.Now()
	require.NoError(t, rr.CancelAndWait(t))
	require.Less(
		t, time.Since(start), 2*time.Second,
		"shutdown must give up on the hung worker after the configured timeouts",
	)
	require.Empty(t, client.CommittedOffsets(), "the hung record must not be committed")
}

func TestPartitionedRunner_BackpressureWithRetryingRecord(t *testing.T) {
	procErr := errors.New("still failing")
	gate := make(chan struct{})
	topo := createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if k == "flaky" {
			select {
			case <-gate:
				return k, v, nil
			default:
				return "", "", procErr
			}
		}
		return k, v, nil
	})

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(10))
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("flaky", "v0"),
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
		mockkafka.SimpleRecord("k3", "v3"),
		mockkafka.SimpleRecord("k4", "v4"),
		mockkafka.SimpleRecord("k5", "v5"),
	)

	r := buildRunner(
		t,
		NewPartitionedRunner(
			WithErrorHandler(
				errorhandler.WithMaxAttempts(
					1_000_000, backoff.NewFixed(5*time.Millisecond), errorhandler.LogAndFail(logger.NewNoopLogger()),
				),
			),
			WithChannelBufferSize(2),
		),
		topo, client, nil,
	)
	pr := r.(*PartitionedRunner)
	rr := startRunner(t, r)

	// the retrying head record fills the small buffer behind it and pauses
	// the partition
	require.Eventually(
		t, func() bool {
			return len(pr.PausedPartitions()) == 1
		}, 3*time.Second, 10*time.Millisecond, "backpressure should pause the partition while its head retries",
	)

	close(gate)

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 6 && len(pr.PausedPartitions()) == 0
		}, 3*time.Second, 10*time.Millisecond, "the partition should drain and resume once the head succeeds",
	)

	require.NoError(t, rr.CancelAndWait(t))

	require.Equal(
		t, []string{"flaky", "k1", "k2", "k3", "k4", "k5"}, producedKeys(client, "output"),
		"backpressure plus retries must not reorder the partition",
	)
	client.AssertCommittedOffset(t, kafka.TopicPartition{Topic: "input", Partition: 0}, 6)
}
