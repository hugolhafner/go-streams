//go:build unit

package runner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
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

// invocationCounter counts per-key processor invocations across goroutines.
type invocationCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newInvocationCounter() *invocationCounter {
	return &invocationCounter{counts: make(map[string]int)}
}

// Inc increments the count for key and returns the new per-key count.
func (c *invocationCounter) Inc(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key]++
	return c.counts[key]
}

func (c *invocationCounter) Count(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

func (c *invocationCounter) Total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.counts {
		total += n
	}
	return total
}

// createFlakyTopology returns a topology whose processor fails the first
// failTimes invocations per key with procErr, then passes the record through.
func createFlakyTopology(counter *invocationCounter, failTimes int, procErr error) *topology.Topology {
	return createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if counter.Inc(k) <= failTimes {
			return "", "", procErr
		}
		return k, v, nil
	})
}

// createFailingTopology returns a topology whose processor permanently fails
// records with the given key and passes all others through.
func createFailingTopology(counter *invocationCounter, failKey string, procErr error) *topology.Topology {
	return createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if k == failKey {
			counter.Inc(k)
			return "", "", procErr
		}
		return k, v, nil
	})
}

// createGatedTopology returns a topology whose processor blocks records with
// the given key prefix until the gate channel is closed (or the context is
// cancelled), giving tests a deterministic "very slow record" without sleeps.
func createGatedTopology(gate <-chan struct{}, gatedPrefix string) *topology.Topology {
	return createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if strings.HasPrefix(k, gatedPrefix) {
			select {
			case <-gate:
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
		}
		return k, v, nil
	})
}

// createPanickyTopology returns a topology whose processor panics for records
// with the given key and passes all others through.
func createPanickyTopology(panicKey string) *topology.Topology {
	return createMapTopology(func(ctx context.Context, k, v string) (string, string, error) {
		if k == panicKey {
			panic("processor exploded on key " + k)
		}
		return k, v, nil
	})
}

// failingDeserialiser fails to deserialise payloads with the given prefix.
type failingDeserialiser struct {
	badPrefix string
}

func (d failingDeserialiser) Deserialise(topic string, data []byte) (string, error) {
	if strings.HasPrefix(string(data), d.badPrefix) {
		return "", fmt.Errorf("deserialise value %q: corrupt payload", string(data))
	}
	return string(data), nil
}

// createFailingSerdeTopology returns a passthrough topology whose source value
// deserialiser fails for values with the given prefix, producing serde-phase
// errors through the runner.
func createFailingSerdeTopology(badValuePrefix string) *topology.Topology {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(failingDeserialiser{badPrefix: badValuePrefix}),
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

// handledCall captures a single error handler invocation.
type handledCall struct {
	EC     errorhandler.ErrorContext
	Action errorhandler.Action
}

// recordingHandler wraps an inner handler and records every invocation so
// tests can assert exactly what the runner delivered (attempt sequence,
// phase, node) and what came back.
type recordingHandler struct {
	mu    sync.Mutex
	inner errorhandler.Handler
	calls []handledCall
}

func newRecordingHandler(inner errorhandler.Handler) *recordingHandler {
	return &recordingHandler{inner: inner}
}

func (h *recordingHandler) Handle(ctx context.Context, ec errorhandler.ErrorContext) errorhandler.Action {
	action := h.inner.Handle(ctx, ec)
	h.mu.Lock()
	h.calls = append(h.calls, handledCall{EC: ec, Action: action})
	h.mu.Unlock()
	return action
}

func (h *recordingHandler) Calls() []handledCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	calls := make([]handledCall, len(h.calls))
	copy(calls, h.calls)
	return calls
}

func (h *recordingHandler) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

func (h *recordingHandler) Attempts() []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	attempts := make([]int, len(h.calls))
	for i, c := range h.calls {
		attempts[i] = c.EC.Attempt
	}
	return attempts
}

func (h *recordingHandler) Phases() []errorhandler.ErrorPhase {
	h.mu.Lock()
	defer h.mu.Unlock()
	phases := make([]errorhandler.ErrorPhase, len(h.calls))
	for i, c := range h.calls {
		phases[i] = c.EC.Phase
	}
	return phases
}

// logRecorder is a goroutine-safe logger.Base that records every entry.
// Wrap it via newRecordingLogger; derived loggers (With) share the same store,
// so entries logged by the runner's derived loggers remain visible to tests.
type logRecorder struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	Level   logger.LogLevel
	Message string
}

func (r *logRecorder) Level() logger.LogLevel { return logger.DebugLevel }

func (r *logRecorder) Log(level logger.LogLevel, msg string, kv ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, logEntry{Level: level, Message: msg})
}

// CountLevelAndPrefix returns how many recorded entries match the level and
// message prefix.
func (r *logRecorder) CountLevelAndPrefix(level logger.LogLevel, msgPrefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, e := range r.entries {
		if e.Level == level && strings.HasPrefix(e.Message, msgPrefix) {
			count++
		}
	}
	return count
}

func newRecordingLogger() (logger.Logger, *logRecorder) {
	rec := &logRecorder{}
	return logger.WrapLogger(rec), rec
}

// runnerRun tracks a runner started in the background by startRunner.
type runnerRun struct {
	cancel context.CancelFunc
	errCh  chan error
}

// startRunner runs r.Run in a goroutine, returning a handle to cancel it and
// collect its result.
func startRunner(t *testing.T, r Runner) *runnerRun {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	rr := &runnerRun{cancel: cancel, errCh: make(chan error, 1)}
	go func() {
		rr.errCh <- r.Run(ctx)
	}()
	return rr
}

// CancelAndWait cancels the run context and returns Run's result.
func (rr *runnerRun) CancelAndWait(t *testing.T) error {
	t.Helper()

	rr.cancel()
	select {
	case err := <-rr.errCh:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
		return nil
	}
}

// WaitErr waits for Run to return on its own, for tests where a fatal error
// is expected to stop the runner without cancellation.
func (rr *runnerRun) WaitErr(t *testing.T, timeout time.Duration) error {
	t.Helper()

	select {
	case err := <-rr.errCh:
		return err
	case <-time.After(timeout):
		t.Fatal("timeout waiting for runner to exit")
		return nil
	}
}

// producedKeys returns the keys of records produced to a topic, in produce order.
func producedKeys(client *mockkafka.Client, topic string) []string {
	records := client.ProducedRecordsForTopic(topic)
	keys := make([]string, len(records))
	for i, r := range records {
		keys[i] = string(r.Key)
	}
	return keys
}

// keysWithPrefix filters keys to those with the given prefix, preserving order.
func keysWithPrefix(keys []string, prefix string) []string {
	var filtered []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			filtered = append(filtered, k)
		}
	}
	return filtered
}

// commonOption is satisfied by options that apply to both runner types,
// letting failure tests run the same body against each runner.
type commonOption interface {
	SingleThreadedOption
	PartitionedOption
}

func singleThreadedFactory(opts ...commonOption) Factory {
	stOpts := make([]SingleThreadedOption, len(opts))
	for i, o := range opts {
		stOpts[i] = o
	}
	return NewSingleThreadedRunner(stOpts...)
}

func partitionedFactory(opts ...commonOption) Factory {
	pOpts := make([]PartitionedOption, len(opts))
	for i, o := range opts {
		pOpts[i] = o
	}
	return NewPartitionedRunner(pOpts...)
}

// buildRunner wires a topology and mock client into a runner from the given
// factory. Pass a nil telemetry for noop.
func buildRunner(
	t *testing.T, factory Factory, topo *topology.Topology, client *mockkafka.Client, tel *streamsotel.Telemetry,
) Runner {
	t.Helper()

	if tel == nil {
		tel = streamsotel.Noop()
	}

	taskFactory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	r, err := factory(topo, taskFactory, client, client, tel)
	require.NoError(t, err)
	return r
}
