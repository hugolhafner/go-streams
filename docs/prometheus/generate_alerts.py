#!/usr/bin/env python3
"""Generate the go-streams Prometheus alerting rules (Four Golden Signals)."""
import os
import re
import textwrap

OUT = "./rules"
FILENAME = "go-streams-alerts.yaml"

# ---------------------------------------------------------------- configuration
# Edit these and re-run the script rather than editing the generated YAML.

JOB = "job"  # application-identity label; replace with service_name/app if your stack differs
RATE = "5m"  # rate() window; keep >= 5m — the OTel SDK exports every 60s by default

EXTRA_LABELS = {}  # routing labels stamped on every alert, e.g. {"team": "streaming"}

ERROR_RATIO_WARN = 0.01  # errors as a fraction of consumed records
ERROR_RATIO_CRIT = 0.05
LAG_P99_WARN = 30    # seconds; consumer-lag buckets top out at 300s
LAG_P99_CRIT = 120   # seconds
PROCESS_P99 = 2.5    # seconds; process-duration buckets top out at 5s
PRODUCE_P99 = 1.0    # seconds; produce-duration buckets top out at 2.5s
RETRY_RATE = 5       # retries/sec
QUEUE_DEPTH = 1000   # records buffered across partition workers
PAUSE_RATE = 1       # backpressure pauses/sec
REBALANCES = 5       # rebalance events per 10 minutes

# ---------------------------------------------------------------- helpers

L = f"{{{{ $labels.{JOB} }}}}"
V = "{{ $value | humanize }}"
VPCT = "{{ $value | humanizePercentage }}"


def by(expr):
    return f"sum by ({JOB}) ({expr})"


def p99(bucket_metric):
    return f"histogram_quantile(0.99, sum by ({JOB}, le) (rate({bucket_metric}[{RATE}])))"


def rule(alert, expr, *, comment, summary, description, severity="warning", for_="10m"):
    return {
        "alert": alert,
        "expr": textwrap.dedent(expr).strip(),
        "for": for_,
        "severity": severity,
        "summary": summary,
        "description": description,
        "comment": comment,
    }


def group(name, comment, rules):
    return {"name": name, "comment": comment, "rules": rules}


# ---------------------------------------------------------------- rule groups

def build_traffic():
    return group("go-streams.traffic", "Traffic — is data flowing?", [
        rule("GoStreamsConsumptionStalled",
            f"""
            {by('rate(messaging_consumer_messages_total[10m])')} == 0
            and
            {by('stream_tasks_active')} > 0
            """,
            comment="Active tasks but nothing consumed. Lag alerts go silent when consumption\n"
                    "stops (lag is only observed on consumed records), so this is the stall signal.\n"
                    "Relax or drop it for legitimately intermittent source topics.",
            summary=f"go-streams {L} stopped consuming",
            description=f"{L} owns partitions but has consumed no records for over 10 minutes. "
                        "Check broker connectivity and the poll loop; if the source topic is "
                        "legitimately intermittent, relax or drop this alert."),
        rule("GoStreamsNoActiveTasks",
            f"{by('stream_tasks_active')} == 0",
            for_="15m",
            comment="Application exporting metrics but owning no partitions.",
            summary=f"go-streams {L} owns no partitions",
            description=f"{L} has been running with zero active tasks for 15 minutes. Check "
                        "consumer-group membership, topic existence, and whether other group "
                        "members hold every partition."),
        rule("GoStreamsPollErrors",
            f"{by(f'rate(stream_poll_duration_seconds_count{{stream_poll_status=\"error\"}}[{RATE}])')} > 0",
            comment="Poll() calls failing. No alert on poll *duration*: Poll blocks while idle,\n"
                    "so a slow poll means no traffic, not trouble.",
            summary=f"go-streams {L} poll loop returning errors",
            description=f"{V} failed polls/sec for 10 minutes. Check broker connectivity, "
                        "authentication/ACLs and consumer-group health."),
    ])


def error_ratio(threshold):
    consumed = by(f"rate(messaging_consumer_messages_total[{RATE}])")
    return f"""
    (
        {by(f'rate(stream_errors_total[{RATE}])')}
      /
        {consumed}
    ) > {threshold}
    and
    {consumed} > 0
    """


def build_errors():
    return group("go-streams.errors", "Errors — is work failing?", [
        rule("GoStreamsHighErrorRatio",
            error_ratio(ERROR_RATIO_WARN),
            comment=f"More than {ERROR_RATIO_WARN:.0%} of consumed records producing errors. The `and`\n"
                    "clause suppresses the Inf ratio a zero-traffic application would produce.",
            summary=f"go-streams {L} error ratio above {ERROR_RATIO_WARN:.0%}",
            description=f"{VPCT} of consumed records errored over the last {RATE} (threshold "
                        f"{ERROR_RATIO_WARN:.0%}). Break it down by stream_error_phase and "
                        "stream_error_node on the Application Drill-Down dashboard."),
        rule("GoStreamsCriticalErrorRatio",
            error_ratio(ERROR_RATIO_CRIT),
            severity="critical", for_="5m",
            comment="Same signal at page-worthy volume.",
            summary=f"go-streams {L} error ratio above {ERROR_RATIO_CRIT:.0%}",
            description=f"{VPCT} of consumed records errored over the last {RATE} (threshold "
                        f"{ERROR_RATIO_CRIT:.0%}). A significant share of traffic is failing — "
                        "check stream_error_phase for whether serde, processing or production "
                        "is responsible."),
        rule("GoStreamsSerdeErrors",
            f"{by(f'rate(stream_errors_total{{stream_error_phase=\"serde\"}}[{RATE}])')} > 0",
            comment="Poison pills: records failing (de)serialization.",
            summary=f"go-streams {L} hitting serde errors (poison pills)",
            description=f"{V} records/sec failing (de)serialization for 10 minutes — usually "
                        "schema drift or corrupt input. Route these to a DLQ via "
                        "WithSerdeErrorHandler instead of retrying."),
        rule("GoStreamsProductionErrors",
            f"{by(f'rate(stream_errors_total{{stream_error_phase=\"production\"}}[{RATE}])')} > 0",
            comment="Sink-side failures — the produce path to Kafka is unhealthy.",
            summary=f"go-streams {L} failing to produce to Kafka",
            description=f"{V} sink/producer errors/sec for 10 minutes. Check broker health, "
                        "topic existence/ACLs and message-size limits."),
        rule("GoStreamsFailActionTriggered",
            f"{by('increase(stream_errors_total{stream_error_action=\"fail\"}[10m])')} > 0",
            severity="critical", for_=None,
            comment="An error handler returned Fail: the runner is shutting down. Metric export\n"
                    "stops with it, so this alert goes stale rather than resolving — pair it with\n"
                    "deployment-level up/absent alerting for sustained outage detection.",
            summary=f"go-streams {L} hit a fatal error and is stopping",
            description=f"An error handler in {L} returned the Fail action within the last "
                        "10 minutes; the runner stops processing. Find the failing node via the "
                        "stream_error_node label and the application logs."),
        rule("GoStreamsDLQTraffic",
            f"{by(f'rate(stream_errors_total{{stream_error_action=\"send_to_dlq\"}}[{RATE}])')} > 0",
            comment="Pipeline healthy but diverting records to the dead-letter queue.",
            summary=f"go-streams {L} routing records to the DLQ",
            description=f"{V} records/sec sent to the DLQ for 10 minutes. Processing continues, "
                        "but diverted data needs attention — inspect the DLQ topic and upstream "
                        "data quality."),
        rule("GoStreamsHighRetryRate",
            f"{by(f'rate(stream_process_retries_total[{RATE}])')} > {RETRY_RATE}",
            comment="Sustained retries burn capacity and inflate the per-attempt topology metrics\n"
                    "(node/edge throughput exceeds the consumed rate by the retry factor).",
            summary=f"go-streams {L} retry rate above {RETRY_RATE}/sec",
            description=f"{V} retries/sec for 10 minutes. Check stream_error_phase to see which "
                        "stage is retrying; consider tighter retry limits or DLQ routing for "
                        "persistent failures."),
    ])


def build_latency():
    return group("go-streams.latency", "Latency — is work slow?", [
        rule("GoStreamsHighProcessingLatency",
            f"{p99('stream_process_duration_seconds_bucket')} > {PROCESS_P99}",
            comment="p99 end-to-end record processing time. Threshold sits one bucket below the\n"
                    "histogram ceiling (5s) — quantile estimates saturate at the top finite bucket.",
            summary=f"go-streams {L} p99 processing latency above {PROCESS_P99}s",
            description=f"p99 record processing takes {V}s. Use the Topology Explorer dashboard "
                        "to find the slow node (stream_node_latency is exclusive self-time)."),
        rule("GoStreamsHighProduceLatency",
            f"{p99('stream_produce_duration_seconds_bucket')} > {PRODUCE_P99}",
            comment="p99 Send() time; the histogram ceiling is 2.5s.",
            summary=f"go-streams {L} p99 produce latency above {PRODUCE_P99}s",
            description=f"p99 produce to Kafka takes {V}s. Check broker health, acks/linger "
                        "settings and network latency."),
    ])


def build_saturation():
    return group("go-streams.saturation", "Saturation — is the runner keeping up?", [
        rule("GoStreamsConsumerLagHigh",
            f"{p99('stream_consumer_lag_bucket')} > {LAG_P99_WARN}",
            comment="Record age at consumption (event-time lag); the metric has no _seconds\n"
                    "suffix. Only observed on consumed records — GoStreamsConsumptionStalled\n"
                    "covers the total-stall case.",
            summary=f"go-streams {L} p99 consumer lag above {LAG_P99_WARN}s",
            description=f"Records are {V}s old at p99 when consumed — processing is falling "
                        "behind production. Check the Runner & Saturation dashboard and consider "
                        "scaling out."),
        rule("GoStreamsConsumerLagCritical",
            f"{p99('stream_consumer_lag_bucket')} > {LAG_P99_CRIT}",
            severity="critical",
            comment="Lag buckets top out at 300s, so the estimate saturates there.",
            summary=f"go-streams {L} p99 consumer lag above {LAG_P99_CRIT}s",
            description=f"Records are {V}s old at p99 when consumed and the backlog is growing. "
                        "Scale out or fix the bottleneck now — lag buckets top out at 300s, so "
                        "the true lag may be far higher."),
        rule("GoStreamsQueueDepthHigh",
            f"{by('stream_partitioned_queue_depth')} > {QUEUE_DEPTH}",
            for_="5m",
            comment="PartitionedRunner only: records buffered in worker channels plus pending overflow.",
            summary=f"go-streams {L} worker queues holding over {QUEUE_DEPTH} records",
            description=f"{V} records buffered across partition workers for 5 minutes. Workers "
                        "are slower than the poll loop — check processing latency and "
                        "WithChannelBufferSize, and consider more partitions or instances."),
        rule("GoStreamsPausedPartitionsStuck",
            f"{by('stream_partitioned_paused_depth')} > 0",
            for_="15m",
            comment="PartitionedRunner only. Backpressure pauses should be transient; 15 minutes\n"
                    "paused means a worker is stuck or badly underprovisioned.",
            summary=f"go-streams {L} partitions paused by backpressure for 15m+",
            description=f"{V} records are sitting in paused partitions and not draining. Look "
                        "for a stuck or very slow processor on the paused partitions."),
        rule("GoStreamsBackpressureThrash",
            f"{by(f'rate(stream_partitioned_backpressure_events_total{{stream_backpressure_event=\"paused\"}}[{RATE}])')} > {PAUSE_RATE}",
            comment="PartitionedRunner only. Frequent pauses mean workers can't keep up with the poll rate.",
            summary=f"go-streams {L} partitions being paused over {PAUSE_RATE}/sec",
            description=f"{V} backpressure pauses/sec for 10 minutes. If the resume rate is "
                        "similar this is pause/resume thrash — lower WithResumeThreshold for more "
                        "hysteresis; otherwise increase WithChannelBufferSize or speed up "
                        "processors."),
        rule("GoStreamsRebalanceStorm",
            f"{by('increase(stream_rebalance_count_total[10m])')} > {REBALANCES}",
            for_=None,
            comment="Frequent rebalances drain workers and pause processing; the 10m increase\n"
                    "window is the flap dampener.",
            summary=f"go-streams {L} rebalancing frequently",
            description=f"{V} rebalance events in 10 minutes (threshold {REBALANCES}). Look for "
                        "crash-looping instances, deployment churn, or slow poll loops exceeding "
                        "the session timeout."),
    ])


# ---------------------------------------------------------------- YAML emission

HEADER = f"""\
# go-streams Prometheus alerting rules — Four Golden Signals.
#
# GENERATED by generate_alerts.py — edit the script and re-run it
# (cd docs/prometheus && uv run generate_alerts.py); do not edit this file by hand.
#
# Assumptions (see README.md for how to adapt):
#   - Standard OTel -> Prometheus metric-name translation, matching the Grafana
#     dashboards in ../grafana: dots become underscores, counters gain _total,
#     duration histograms gain _seconds (stream_consumer_lag keeps no unit suffix).
#   - Applications are identified by the `{JOB}` label.
#   - stream_partitioned_* alerts only ever fire for the PartitionedRunner; the
#     SingleThreadedRunner emits no queue/backpressure metrics.
#   - Rate windows assume the OTel SDK's default 60s export period."""


def quote(value):
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def render(groups):
    lines = [HEADER, "", "groups:"]
    for grp in groups:
        lines.append("")
        lines.extend(f"# {c}" for c in grp["comment"].splitlines())
        lines.append(f"- name: {grp['name']}")
        lines.append("  rules:")
        for r in grp["rules"]:
            lines.append("")
            lines.extend(f"  # {c}" for c in r["comment"].splitlines())
            lines.append(f"  - alert: {r['alert']}")
            lines.append("    expr: |")
            lines.extend(f"      {e}" for e in r["expr"].splitlines())
            if r["for"]:
                lines.append(f"    for: {r['for']}")
            lines.append("    labels:")
            lines.append(f"      severity: {quote(r['severity'])}")
            lines.extend(f"      {k}: {quote(EXTRA_LABELS[k])}" for k in sorted(EXTRA_LABELS))
            lines.append("    annotations:")
            lines.append(f"      summary: {quote(r['summary'])}")
            lines.append(f"      description: {quote(r['description'])}")
    return "\n".join(lines) + "\n"


def sanity(groups):
    names = [r["alert"] for g in groups for r in g["rules"]]
    dupes = {n for n in names if names.count(n) > 1}
    assert not dupes, f"duplicate alert names: {dupes}"
    for g in groups:
        for r in g["rules"]:
            assert r["expr"], f"{r['alert']}: empty expr"
            assert r["severity"] in ("warning", "critical"), f"{r['alert']}: bad severity"
            assert r["for"] is None or re.fullmatch(r"\d+[smh]", r["for"]), f"{r['alert']}: bad for"
            assert r["comment"] and r["summary"] and r["description"], f"{r['alert']}: missing text"


def write(name, content):
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        f.write(content)
    print("wrote", path)


if __name__ == "__main__":
    all_groups = [build_traffic(), build_errors(), build_latency(), build_saturation()]
    sanity(all_groups)
    os.makedirs(OUT, exist_ok=True)
    write(FILENAME, render(all_groups))
