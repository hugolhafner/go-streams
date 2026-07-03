# go-streams Grafana Dashboard Set

Four dashboards implementing the **Four Golden Signals** (latency, traffic, errors, saturation) for
applications built with [go-streams](https://github.com/hugolhafner/go-streams), driven entirely by the
library's built-in OpenTelemetry metrics.

## How the set fits together

The set is a funnel — each dashboard answers one question and hands off to the next when you need
more depth. All four share the `go-streams` tag; the **"go-streams dashboards"** dropdown in the top-right
of every dashboard navigates between them **carrying your variables and time range**.

| # | Dashboard (uid)                                  | Question it answers                                                    | Scope / variables           |
|---|--------------------------------------------------|------------------------------------------------------------------------|-----------------------------|
| 1 | Fleet Overview (`gostreams-overview`)            | Is anything wrong, and in which application?                           | `job`                       |
| 2 | Application Drill-Down (`gostreams-application`) | What is unhealthy inside this app — which topic, error phase, outcome? | `job`, `topic`              |
| 3 | Topology Explorer (`gostreams-topology`)         | Which topology node or edge is hot, slow, or failing?                  | `job` (single), `node`      |
| 4 | Runner & Saturation (`gostreams-runner`)         | Is it keeping up — where is backpressure building?                     | `job`, `topic`, `partition` |

Signal → metric mapping used throughout:

- **Traffic** — `messaging.consumer.messages`, `messaging.producer.messages`, `stream.poll.records`,
  `stream.node.records`, `stream.edge.records`
- **Errors** — `stream.errors` (by `phase`/`action`/`node`), `stream.process.retries`,
  `stream.process.duration` non-`success` statuses, poll/produce `status="error"`
- **Latency** — `stream.process.duration`, `stream.node.latency` (exclusive self-time),
  `stream.produce.duration`, `stream.poll.duration`
- **Saturation** — `stream.consumer.lag`, `stream.partitioned.queue.depth`,
  `stream.partitioned.paused.depth`, `stream.partitioned.backpressure.events`, `stream.tasks.active`,
  `stream.rebalance.count`

## Import

1. Grafana → **Dashboards → New → Import**, upload each JSON file (Grafana 10+).
2. Pick your Prometheus data source in the **Data source** variable — every query uses the
   `$datasource` template variable, so no hard-coded data source UIDs.
3. Open *Fleet Overview* first; the other three are reached from the dashboard-links dropdown.

## Assumptions & adapting to your stack

These dashboards aim to be portable, but two things vary between stacks:

**Metric names.** Queries assume the standard OTel → Prometheus translation used in the go-streams
docs' own alerting examples: dots become underscores, counters gain `_total`, histograms appends unit suffixes
and expose `_bucket` / `_sum` / `_count` (e.g. `stream.process.duration` → `stream_process_duration_seconds_bucket`).

**The application label.** Dashboards identify applications by the Prometheus `job` label. If your
stack uses `service_name`, `app`, or similar instead, replace `job` in the template-variable queries
and in the `job=~"$job"` selectors.

Other conventions baked in:

- `$__rate_interval` everywhere, so the dashboards work with any scrape interval.
- **Refresh is 1 m**, matching the OTel SDK's default 60 s periodic export — a faster refresh only adds
  backend load without showing new data. Lower it only if you export more often.
- **No stacked series** — outcomes/phases are drawn as separate lines so a single series can't hide
  another.
- Dual Y-axes are used only where units genuinely differ (e.g. *Consume rate vs error ratio* on the
  Overview: msg/s left, ratio right).
- Stat thresholds mirror the alerting examples in `observability.md`: error ratio > 1 %, lag p99 > 30 s,
  queue depth > 1000, backpressure pauses > 1/s, retries > 5/s, > 5 rebalances / 10 min. Tune to your SLOs.

## Ownership & housekeeping

- The dashboard JSON files are **generated** by `generate_dashboards.py` — edit the script and
  re-run it (`cd docs/grafana && uv run generate_dashboards.py`; stdlib-only, Python ≥ 3.12) rather
  than editing the JSON by hand.
- Keep the shared `go-streams` tag if you add dashboards to the set — that is what powers the
  cross-navigation dropdown.

## Reading notes (gotchas surfaced in the panels too)

- `stream.node.latency` is **exclusive per-node self-time** — node values sum to the pipeline total
  rather than nesting.
- `stream.node.records` / `stream.edge.records` / `stream.node.latency` are recorded **per processing
  attempt**. During a retry storm the Topology Explorer's throughput exceeds the consumed rate by the
  retry factor; the *Processing amplification* stat (source-node rate ÷ consumed rate) makes this
  visible at a glance.
- The **Backpressure** row on dashboard 4 only has data for the `PartitionedRunner`; the
  `SingleThreadedRunner` processes inline and exposes no queue metrics.
- On the *Backpressure pause/resume events* panel, `paused` ≈ `resumed` at high frequency is
  pause/resume thrash — lower `WithResumeThreshold` so a paused partition drains further before
  resuming (more hysteresis). `paused` persistently above `resumed` means partitions are
  accumulating in the paused state.
- The Node Graph panel builds its `nodes`/`edges` frames from two instant table queries, following the
  field mappings in the go-streams observability docs (`id` = node name, `subtitle` = node type,
  `source`/`target` = edge endpoints, `mainstat` = throughput). The required field names are produced
  with `label_replace` in PromQL plus an *Add field from calculation* transformation for `mainstat` —
  **not** with rename transformations, because Node Graph matches on raw field names and ignores
  display-name renames (grafana/grafana#54844). If you edit these queries, keep that constraint in mind.
