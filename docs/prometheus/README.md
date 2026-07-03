# go-streams Prometheus Alerting Rules

A generated Prometheus alerting-rule set covering the **Four Golden Signals** (traffic, errors,
latency, saturation) for applications built with [go-streams](https://github.com/hugolhafner/go-streams),
driven entirely by the library's built-in OpenTelemetry metrics. It is the alerting counterpart of
the [Grafana dashboard set](../grafana/README.md) — the stat thresholds on those dashboards mirror
these alerts.

## Loading

Add the rules file to your Prometheus config:

```yaml
rule_files:
  - /etc/prometheus/rules/go-streams-alerts.yaml
```

Route on the `severity` label (`warning` / `critical`) in Alertmanager. Extra routing labels
(e.g. `team`) can be stamped on every rule via the `EXTRA_LABELS` constant in the generator.

## Alert catalog

| Alert | Signal | Severity | Fires when |
|---|---|---|---|
| GoStreamsConsumptionStalled | Traffic | warning | Active tasks but zero records consumed for 10m+ |
| GoStreamsNoActiveTasks | Traffic | warning | Running but owning no partitions for 15m |
| GoStreamsPollErrors | Traffic | warning | `Poll()` returning errors for 10m |
| GoStreamsHighErrorRatio | Errors | warning | Errors > 1% of consumed records for 10m |
| GoStreamsCriticalErrorRatio | Errors | critical | Errors > 5% of consumed records for 5m |
| GoStreamsSerdeErrors | Errors | warning | Sustained (de)serialization failures (poison pills) |
| GoStreamsProductionErrors | Errors | warning | Sustained sink/producer failures |
| GoStreamsFailActionTriggered | Errors | critical | An error handler returned `Fail` — the runner is stopping |
| GoStreamsDLQTraffic | Errors | warning | Records being routed to the DLQ for 10m |
| GoStreamsHighRetryRate | Errors | warning | > 5 retries/sec for 10m |
| GoStreamsHighProcessingLatency | Latency | warning | p99 record processing > 2.5s for 10m |
| GoStreamsHighProduceLatency | Latency | warning | p99 produce > 1s for 10m |
| GoStreamsConsumerLagHigh | Saturation | warning | p99 record age at consumption > 30s for 10m |
| GoStreamsConsumerLagCritical | Saturation | critical | p99 record age at consumption > 120s for 10m |
| GoStreamsQueueDepthHigh | Saturation | warning | > 1000 records queued in partition workers for 5m |
| GoStreamsPausedPartitionsStuck | Saturation | warning | Records stuck in backpressure-paused partitions for 15m |
| GoStreamsBackpressureThrash | Saturation | warning | Partitions paused > 1/sec for 10m |
| GoStreamsRebalanceStorm | Saturation | warning | > 5 rebalance events in 10m |

Posture: the symptom alerts (error ratios, consumer lag, `Fail` action, stalled consumption) are
the page-worthy ones; the cause-oriented alerts (error phases, retries, backpressure, rebalances)
stay at `warning` for triage. Multi-window burn-rate alerting on an SLO is a natural upgrade path
once you have an error budget to burn — these rules deliberately stay threshold-based so they work
without one.

## Regenerating & tuning

The rules YAML is **generated** by `generate_alerts.py` — edit the script and re-run it
(`cd docs/prometheus && uv run generate_alerts.py`; stdlib-only, Python ≥ 3.12) rather than editing
the YAML by hand. All thresholds, the rate window, and the application-identity label are constants
at the top of the script.

## Validation

`promtool` validates syntax and PromQL, and `tests/alerts_test.yaml` unit-tests the key rules
(error-ratio tiering, the zero-traffic Inf guard, lag-quantile tiering, `Fail`-action paging):

```bash
docker run --rm --entrypoint /bin/promtool -v "$PWD/rules":/rules \
  prom/prometheus:latest check rules /rules/go-streams-alerts.yaml

docker run --rm --entrypoint /bin/promtool -v "$PWD":/w -w /w/tests \
  prom/prometheus:latest test rules alerts_test.yaml
```

(Or `brew install prometheus` and run `promtool` directly.)

## Assumptions & adapting to your stack

Same portability assumptions as the Grafana dashboards:

**Metric names.** Standard OTel → Prometheus translation: dots become underscores, counters gain
`_total`, duration histograms gain `_seconds` and expose `_bucket`/`_sum`/`_count`. Note
`stream_consumer_lag` and `stream_node_latency` carry no unit suffix.

**The application label.** Alerts group and fire per Prometheus `job` label. If your stack uses
`service_name`, `app`, or similar, change the `JOB` constant and regenerate.

Other things to know:

- **Rate windows are ≥ 5m** because the OTel SDK exports every 60s by default; don't shrink them
  below a few export periods.
- The **`stream_partitioned_*` alerts** (queue depth, paused partitions, backpressure) only ever
  fire for the `PartitionedRunner` — the `SingleThreadedRunner` emits no queue metrics. Harmless
  but inert for single-threaded apps.
- **GoStreamsConsumptionStalled** assumes a steadily-produced source topic. For legitimately
  intermittent topics, lengthen its window or drop it.
- The **latency thresholds** (2.5s process / 1s produce) are editorial defaults sitting one bucket
  below each histogram's ceiling (`histogram_quantile` saturates at the top finite bucket — 5s and
  2.5s respectively). Tune them to your workload first.
- **No "application down" alert** is included on purpose: after a `Fail` action the app stops
  exporting and `GoStreamsFailActionTriggered` goes stale rather than resolving. Sustained-outage
  detection (`up == 0` / `absent()`) depends on your scrape setup and belongs with your
  deployment-level alerts.
