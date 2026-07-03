#!/usr/bin/env python3
"""Generate the go-streams Grafana dashboard set (Four Golden Signals)."""
import json
import os

OUT = "./dashboards"
DS = {"type": "prometheus", "uid": "${datasource}"}
TAGS = ["go-streams", "kafka", "golden-signals", "owner:go-streams"]

# ---------------------------------------------------------------- helpers

class Grid:
    """Simple top-to-bottom grid placer."""
    def __init__(self):
        self.y = 0
        self.next_id = 1

    def pid(self):
        i = self.next_id
        self.next_id += 1
        return i


def thresholds(steps):
    return {"mode": "absolute", "steps": steps}


GREEN = {"color": "green", "value": None}


def target(expr, legend="", refid="A", instant=False, fmt=None):
    t = {"expr": expr, "legendFormat": legend, "refId": refid, "datasource": DS}
    if instant:
        t["instant"] = True
        t["range"] = False
    if fmt:
        t["format"] = fmt
    return t


def row(g, title):
    p = {
        "id": g.pid(),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": g.y},
        "panels": [],
    }
    g.y += 1
    return p


def text_panel(g, content, h=5):
    p = {
        "id": g.pid(),
        "type": "text",
        "title": "About this dashboard",
        "gridPos": {"h": h, "w": 24, "x": 0, "y": g.y},
        "options": {"mode": "markdown", "content": content},
        "transparent": True,
    }
    g.y += h
    return p


def timeseries(g, title, desc, targets, unit="short", x=0, w=12, h=8,
               overrides=None, draw="line", fill=8, legend_calcs=None,
               same_row=False, thresh=None):
    p = {
        "id": g.pid(),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": g.y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "min": 0,
                "color": {"mode": "palette-classic"},
                "thresholds": thresh or thresholds([GREEN]),
                "custom": {
                    "drawStyle": draw,
                    "lineWidth": 1,
                    "fillOpacity": fill,
                    "pointSize": 4,
                    "showPoints": "never",
                    "spanNulls": False,
                    "stacking": {"mode": "none", "group": "A"},
                    "axisPlacement": "auto",
                    "gradientMode": "none",
                    "thresholdsStyle": {"mode": "off"},
                },
            },
            "overrides": overrides or [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": True,
                "calcs": legend_calcs or [],
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }
    if not same_row:
        pass
    return p


def stat(g, title, desc, targets, unit, x, w=4, h=5, thresh=None,
         decimals=None, graph="area"):
    p = {
        "id": g.pid(),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": g.y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "thresholds"},
                "thresholds": thresh or thresholds([GREEN]),
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": graph,
            "justifyMode": "auto",
        },
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    return p


def heatmap(g, title, desc, expr, x=0, w=12, h=8, unit="s"):
    return {
        "id": g.pid(),
        "type": "heatmap",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": g.y},
        "targets": [target(expr, legend="{{le}}", fmt="heatmap")],
        "options": {
            "calculate": False,
            "cellGap": 1,
            "color": {"mode": "scheme", "scheme": "Spectral", "steps": 64,
                      "reverse": False, "exponent": 0.5, "fill": "dark-orange"},
            "yAxis": {"unit": unit, "axisPlacement": "left"},
            "legend": {"show": True},
            "tooltip": {"mode": "single", "showColorScale": True},
            "exemplars": {"color": "rgba(255,0,255,0.7)"},
            "filterValues": {"le": 1e-9},
            "rowsFrame": {"layout": "auto"},
        },
        "fieldConfig": {"defaults": {"custom": {"hideFrom": {"legend": False, "tooltip": False, "viz": False}}}, "overrides": []},
    }


def bargauge(g, title, desc, targets, unit, x=0, w=12, h=8, thresh=None):
    return {
        "id": g.pid(),
        "type": "bargauge",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": g.y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "min": 0,
                "color": {"mode": "thresholds"},
                "thresholds": thresh or thresholds([GREEN, {"color": "yellow", "value": None}]) if thresh else thresholds([GREEN]),
            },
            "overrides": [],
        },
        "options": {
            "displayMode": "gradient",
            "orientation": "horizontal",
            "valueMode": "color",
            "showUnfilled": True,
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        },
    }


def right_axis_override(name, unit, color="red"):
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [
            {"id": "unit", "value": unit},
            {"id": "custom.axisPlacement", "value": "right"},
            {"id": "custom.axisLabel", "value": name},
            {"id": "color", "value": {"mode": "fixed", "fixedColor": color}},
            {"id": "custom.fillOpacity", "value": 0},
        ],
    }


def var_datasource():
    return {
        "name": "datasource",
        "label": "Data source",
        "type": "datasource",
        "query": "prometheus",
        "current": {},
        "hide": 0,
        "refresh": 1,
        "regex": "",
    }


def var_query(name, label, query, multi=True, include_all=True, all_value=".+"):
    v = {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DS,
        "definition": query,
        "query": {"query": query, "refId": f"var-{name}"},
        "refresh": 2,
        "sort": 1,
        "multi": multi,
        "includeAll": include_all,
        "current": {},
        "options": [],
        "regex": "",
        "hide": 0,
    }
    if include_all:
        v["allValue"] = all_value
    return v


def dashboard(uid, title, description, panels, templating, time_from="now-1h",
              refresh="1m"):
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": TAGS,
        "timezone": "browser",
        "editable": True,
        "graphTooltip": 1,
        "schemaVersion": 39,
        "version": 1,
        "refresh": refresh,
        "time": {"from": time_from, "to": "now"},
        "timepicker": {
            "refresh_intervals": ["1m", "5m", "15m", "30m", "1h"],
        },
        "templating": {"list": templating},
        "annotations": {
            "list": [{
                "builtIn": 1,
                "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                "enable": True,
                "hide": True,
                "iconColor": "rgba(0, 211, 255, 1)",
                "name": "Annotations & Alerts",
                "type": "dashboard",
            }]
        },
        "links": [{
            "asDropdown": True,
            "icon": "external link",
            "includeVars": True,
            "keepTime": True,
            "tags": ["go-streams"],
            "targetBlank": False,
            "title": "go-streams dashboards",
            "type": "dashboards",
        }],
        "panels": panels,
    }


def write(name, dash):
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        json.dump(dash, f, indent=2, sort_keys=False)
        f.write("\n")
    print("wrote", path)


J = 'job=~"$job"'                       # job selector fragment
JT = 'job=~"$job", messaging_destination_name=~"$topic"'

# =========================================================== 1. OVERVIEW

def build_overview():
    g = Grid()
    panels = []

    panels.append(text_panel(g, (
        "## go-streams · Fleet Overview\n"
        "**Question this dashboard answers:** *Is any go-streams application in trouble right now — and which one?*\n\n"
        "Organised by the **Four Golden Signals** (Traffic, Errors, Latency, Saturation). Every graph is split by "
        "`job` only — once you know *which* application is unhealthy, drill down via the **go-streams dashboards** "
        "link (top right); your `job` selection and time range carry over.\n\n"
        "**Drill-down path:** Fleet Overview → *Application Drill-Down* (per topic) → *Topology Explorer* (per node/edge) "
        "→ *Runner & Saturation* (per partition / backpressure).\n\n"
        "Thresholds mirror the alerting examples in the go-streams observability docs (error ratio > 1 %, lag p99 > 30 s, "
        "queue depth > 1000, backpressure pauses > 1/s, > 5 rebalances / 10 min). Refresh is 1 m to match the default OTel periodic export interval — "
        "faster refresh adds backend load without new data. Replace the `owner:go-streams` tag with your own team's tag."
    ), h=6))

    # --- At a glance stats
    panels.append(row(g, "At a glance"))
    panels.append(stat(g, "Consume rate", "Total records consumed per second across selected jobs (messaging.consumer.messages).",
        [target(f'sum(rate(messaging_consumer_messages_total{{{J}}}[$__rate_interval]))')],
        "mps", x=0))
    panels.append(stat(g, "Produce rate", "Total records produced to sink topics per second (messaging.producer.messages).",
        [target(f'sum(rate(messaging_producer_messages_total{{{J}}}[$__rate_interval]))')],
        "mps", x=4))
    panels.append(stat(g, "Error ratio", "Errors as a fraction of consumed records over the rate interval. Docs suggest alerting above 1 %.",
        [target(f'sum(rate(stream_errors_total{{{J}}}[$__rate_interval])) / sum(rate(messaging_consumer_messages_total{{{J}}}[$__rate_interval]))')],
        "percentunit", x=8,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 0.001}, {"color": "red", "value": 0.01}]),
        decimals=2))
    panels.append(stat(g, "Consumer lag p99", "99th percentile of time-since-produced for consumed records (stream.consumer.lag). Docs suggest alerting above 30 s.",
        [target(f'histogram_quantile(0.99, sum by (le) (rate(stream_consumer_lag_bucket{{{J}}}[$__rate_interval])))')],
        "s", x=12,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 10}, {"color": "red", "value": 30}])))
    panels.append(stat(g, "Records not succeeding", "Rate of records whose final outcome was dropped, sent to DLQ, or failed (stream.process.duration count by status).",
        [target(f'sum(rate(stream_process_duration_seconds_count{{{J}, stream_process_status=~"dropped|dlq|failed"}}['
                f'$__rate_interval])) or vector(0)')],
        "mps", x=16,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 0.01}, {"color": "red", "value": 1}]),
        decimals=2))
    panels.append(stat(g, "Rebalances (range)", "Rebalance events (assigned + revoked) over the selected time range. Frequent rebalances indicate group instability; docs suggest alerting on > 5 in 10 min.",
        [target(f'sum(increase(stream_rebalance_count_total{{{J}}}[$__range])) or vector(0)')],
        "short", x=20, graph="none",
        thresh=thresholds([GREEN, {"color": "yellow", "value": 3}, {"color": "red", "value": 6}])))
    g.y += 5

    # --- Traffic
    panels.append(row(g, "Traffic"))
    panels.append(timeseries(g, "Consumed records/s by application",
        "rate(messaging.consumer.messages) summed per job. A sudden drop to zero usually means the app is stalled or partitions were revoked.",
        [target(f'sum by (job) (rate(messaging_consumer_messages_total{{{J}}}[$__rate_interval]))', "{{job}}")],
        unit="mps", x=0))
    panels.append(timeseries(g, "Produced records/s by application",
        "rate(messaging.producer.messages) summed per job. Compare with the consume rate: a growing gap means records are being filtered, dropped, or stuck.",
        [target(f'sum by (job) (rate(messaging_producer_messages_total{{{J}}}[$__rate_interval]))', "{{job}}")],
        unit="mps", x=12))
    g.y += 8

    # --- Errors
    panels.append(row(g, "Errors"))
    panels.append(timeseries(g, "Error rate by application",
        "rate(stream.errors) per job — every processor/serde/production error passed to an error handler.",
        [target(f'sum by (job) (rate(stream_errors_total{{{J}}}[$__rate_interval]))', "{{job}}")],
        unit="mps", x=0,
        thresh=thresholds([GREEN, {"color": "red", "value": 80}])))
    panels.append(timeseries(g, "Consume rate vs error ratio",
        "Total consume rate (left axis, msg/s) against the fleet-wide error ratio (right axis, %). Separate axes are used because the units and ranges differ.",
        [
            target(f'sum(rate(messaging_consumer_messages_total{{{J}}}[$__rate_interval]))', "Consume rate", refid="A"),
            target(f'sum(rate(stream_errors_total{{{J}}}[$__rate_interval])) / sum(rate(messaging_consumer_messages_total{{{J}}}[$__rate_interval]))', "Error ratio", refid="B"),
        ],
        unit="mps", x=12,
        overrides=[right_axis_override("Error ratio", "percentunit")]))
    g.y += 8

    # --- Latency
    panels.append(row(g, "Latency"))
    panels.append(timeseries(g, "Processing latency p99 by application",
        "99th percentile end-to-end record processing time (stream.process.duration) per job.",
        [target(f'histogram_quantile(0.99, sum by (le, job) (rate(stream_process_duration_seconds_bucket{{{J}}}['
                f'$__rate_interval])))', "{{job}}")],
        unit="s", x=0))
    panels.append(timeseries(g, "Produce latency p99 by application",
        "99th percentile time per Send() call to Kafka (stream.produce.duration) per job. Rising values point at broker or sink-topic problems.",
        [target(f'histogram_quantile(0.99, sum by (le, job) (rate(stream_produce_duration_seconds_bucket{{{J}}}['
                f'$__rate_interval])))', "{{job}}")],
        unit="s", x=12))
    g.y += 8

    # --- Saturation
    panels.append(row(g, "Saturation"))
    panels.append(timeseries(g, "Consumer lag p99 by application",
        "99th percentile record age at consume time (stream.consumer.lag) per job. Sustained growth means the app is not keeping up with input.",
        [target(f'histogram_quantile(0.99, sum by (le, job) (rate(stream_consumer_lag_bucket{{{J}}}[$__rate_interval])))', "{{job}}")],
        unit="s", x=0, w=8))
    panels.append(timeseries(g, "Partitioned-runner queue depth by application",
        "Records buffered in partition worker channels (stream.partitioned.queue.depth) and records held in paused partitions (stream.partitioned.paused.depth). Only populated for the PartitionedRunner. Docs suggest alerting above 1000.",
        [
            target(f'sum by (job) (stream_partitioned_queue_depth{{{J}}})', "{{job}} queued", refid="A"),
            target(f'sum by (job) (stream_partitioned_paused_depth{{{J}}})', "{{job}} paused", refid="B"),
        ],
        unit="short", x=8, w=8))
    panels.append(timeseries(g, "Backpressure pauses by application",
        "Partitions paused per second because their worker queue filled (stream.partitioned.backpressure.events with event=paused). Only populated for the PartitionedRunner. Sustained values above 1/s mean workers can't keep up with the poll rate — drill into Runner & Saturation.",
        [target(f'sum by (job) (rate(stream_partitioned_backpressure_events_total{{{J}, stream_backpressure_event="paused"}}[$__rate_interval]))', "{{job}}")],
        unit="cps", x=16, w=8,
        thresh=thresholds([GREEN, {"color": "red", "value": 1}])))
    g.y += 8

    # --- Stability
    panels.append(row(g, "Stability"))
    panels.append(timeseries(g, "Active tasks by application",
        "Currently active tasks / assigned partitions (stream.tasks.active). Sudden drops line up with rebalances or shutdowns.",
        [target(f'sum by (job) (stream_tasks_active{{{J}}})', "{{job}}")],
        unit="short", x=0, fill=0))
    panels.append(timeseries(g, "Rebalance events by application",
        "Rebalance events per interval (increase(stream.rebalance.count)). Spikes here explain gaps or duplicates elsewhere.",
        [target(f'sum by (job) (increase(stream_rebalance_count_total{{{J}}}[$__interval]))', "{{job}}")],
        unit="short", x=12, draw="bars", fill=60))
    g.y += 8

    return dashboard(
        "go-streams-overview",
        "[go-streams] Fleet Overview",
        "Four Golden Signals overview for all go-streams applications. Start here; drill down per application, topology node, or partition via the linked dashboards.",
        panels,
        [var_datasource(), var_query("job", "Application (job)", "label_values(messaging_consumer_messages_total, job)")],
        time_from="now-3h",
    )


# ================================================= 2. APPLICATION DRILL-DOWN

def build_application():
    g = Grid()
    panels = []

    panels.append(text_panel(g, (
        "## go-streams · Application Drill-Down\n"
        "**Question this dashboard answers:** *What exactly is unhealthy inside this application — which topic, "
        "which error phase, which outcome?*\n\n"
        "Pick a single `job` for the clearest picture (multi-select works but mixes topologies). Panels are grouped by "
        "golden signal. When errors point at a specific topology node, continue to **Topology Explorer**; when lag or "
        "backpressure is the problem, continue to **Runner & Saturation** — use the dashboard dropdown (top right), "
        "variables and time range carry over.\n\n"
        "Error *phase* values come straight from the library: `serde` (bad/poison records), `processing` (your code), "
        "`production` (sink/broker). Error *action* is what the configured error handler decided: continue, retry, fail, send_to_dlq."
    ), h=6))

    # Traffic
    panels.append(row(g, "Traffic"))
    panels.append(timeseries(g, "Consumed records/s by topic",
        "rate(messaging.consumer.messages) per source topic.",
        [target(f'sum by (messaging_destination_name) (rate(messaging_consumer_messages_total{{{JT}}}[$__rate_interval]))', "{{messaging_destination_name}}")],
        unit="mps", x=0))
    panels.append(timeseries(g, "Produced records/s by topic",
        "rate(messaging.producer.messages) per sink topic (includes DLQ topics).",
        [target(f'sum by (messaging_destination_name) (rate(messaging_producer_messages_total{{{JT}}}[$__rate_interval]))', "{{messaging_destination_name}}")],
        unit="mps", x=12))
    g.y += 8

    # Latency
    panels.append(row(g, "Latency"))
    panels.append(timeseries(g, "Processing latency percentiles",
        "p50 / p95 / p99 of end-to-end record processing time (stream.process.duration) across the selected topics.",
        [
            target(f'histogram_quantile(0.50, sum by (le) (rate(stream_process_duration_seconds_bucket{{{JT}}}['
                   f'$__rate_interval])))', "p50", refid="A"),
            target(f'histogram_quantile(0.95, sum by (le) (rate(stream_process_duration_seconds_bucket{{{JT}}}['
                   f'$__rate_interval])))', "p95", refid="B"),
            target(f'histogram_quantile(0.99, sum by (le) (rate(stream_process_duration_seconds_bucket{{{JT}}}['
                   f'$__rate_interval])))', "p99", refid="C"),
        ],
        unit="s", x=0))
    panels.append(heatmap(g, "Processing latency distribution",
        "Heatmap of stream.process.duration. Bi-modal bands usually mean one code path (e.g. an external call) is much slower than the rest — use Topology Explorer to find the node.",
        f'sum by (le) (rate(stream_process_duration_seconds_bucket{{{JT}}}[$__rate_interval]))', x=12))
    g.y += 8
    panels.append(timeseries(g, "Processing latency p95 by topic",
        "p95 of stream.process.duration split per source topic — isolates which input stream is slow.",
        [target(f'histogram_quantile(0.95, sum by (le, messaging_destination_name) (rate('
                f'stream_process_duration_seconds_bucket{{{JT}}}[$__rate_interval])))',
                "{{messaging_destination_name}}")],
        unit="s", x=0))
    panels.append(timeseries(g, "Produce latency by sink topic (p95 / p99)",
        "Percentiles of stream.produce.duration per sink topic. High values with normal processing latency point at the broker or sink topic, not your code.",
        [
            target(f'histogram_quantile(0.95, sum by (le, messaging_destination_name) (rate('
                   f'stream_produce_duration_seconds_bucket{{{JT}}}[$__rate_interval])))',
                   "{{messaging_destination_name}} "
                                                                                     "p95", refid="A"),
            target(f'histogram_quantile(0.99, sum by (le, messaging_destination_name) (rate('
                   f'stream_produce_duration_seconds_bucket{{{JT}}}[$__rate_interval])))',
                   "{{messaging_destination_name}} "
                                                                                     "p99", refid="B"),
        ],
        unit="s", x=12))
    g.y += 8

    # Errors
    panels.append(row(g, "Errors"))
    panels.append(timeseries(g, "Errors by phase",
        "rate(stream.errors) split by stream.error.phase: serde = deserialization/poison pills, processing = your processors, production = sink/broker failures.",
        [target(f'sum by (stream_error_phase) (rate(stream_errors_total{{{J}}}[$__rate_interval]))', "{{stream_error_phase}}")],
        unit="mps", x=0, w=8))
    panels.append(timeseries(g, "Errors by handler action",
        "rate(stream.errors) split by stream.error.action — what the configured error handler decided (continue / retry / fail / send_to_dlq).",
        [target(f'sum by (stream_error_action) (rate(stream_errors_total{{{J}}}[$__rate_interval]))', "{{stream_error_action}}")],
        unit="mps", x=8, w=8))
    panels.append(timeseries(g, "Record outcomes (non-success)",
        "Final record outcomes other than success, from stream.process.duration's status attribute: dropped (handler continued), dlq (sent to dead-letter topic), failed (runner stopped).",
        [target(f'sum by (stream_process_status) (rate(stream_process_duration_seconds_count{{{J}, '
                f'stream_process_status!="success"}}[$__rate_interval]))', "{{stream_process_status}}")],
        unit="mps", x=16, w=8))
    g.y += 8
    panels.append(timeseries(g, "Retry rate by phase",
        "rate(stream.process.retries) split by error phase. Docs suggest alerting above 5 retries/s. A retry storm re-traverses the topology per attempt, inflating node/edge metrics.",
        [target(f'sum by (stream_error_phase) (rate(stream_process_retries_total{{{J}}}[$__rate_interval]))', "{{stream_error_phase}}")],
        unit="mps", x=0,
        thresh=thresholds([GREEN, {"color": "red", "value": 80}])))
    panels.append(bargauge(g, "Top failing nodes (errors/s)",
        "Topology nodes producing the most errors right now (stream.errors by stream.error.node). Jump to Topology Explorer for the node's latency and throughput.",
        [target(f'topk(10, sum by (stream_error_node) (rate(stream_errors_total{{{J}}}[$__rate_interval])))', "{{stream_error_node}}", instant=True)],
        "mps", x=12,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 0.1}, {"color": "red", "value": 1}])))
    g.y += 8

    # Saturation
    panels.append(row(g, "Saturation & stability"))
    panels.append(timeseries(g, "Consumer lag p99 by topic",
        "p99 record age at consume time (stream.consumer.lag) per topic. For per-partition lag and backpressure, continue to the Runner & Saturation dashboard.",
        [target(f'histogram_quantile(0.99, sum by (le, messaging_destination_name) (rate(stream_consumer_lag_bucket{{{JT}}}[$__rate_interval])))', "{{messaging_destination_name}}")],
        unit="s", x=0, w=8))
    panels.append(timeseries(g, "Active tasks",
        "stream.tasks.active by runner type — the number of partitions this application is actively processing.",
        [target(f'sum by (stream_runner_type) (stream_tasks_active{{{J}}})', "{{stream_runner_type}}")],
        unit="short", x=8, w=8, fill=0))
    panels.append(timeseries(g, "Rebalance events",
        "increase(stream.rebalance.count) by type. assigned/revoked pairs are normal during deploys; continuous churn is not.",
        [target(f'sum by (stream_rebalance_type) (increase(stream_rebalance_count_total{{{J}}}[$__interval]))', "{{stream_rebalance_type}}")],
        unit="short", x=16, w=8, draw="bars", fill=60))
    g.y += 8

    tpl = [
        var_datasource(),
        var_query("job", "Application (job)", "label_values(messaging_consumer_messages_total, job)"),
        var_query("topic", "Topic", 'label_values(stream_process_duration_seconds_count{job=~"$job"}, '
                                    'messaging_destination_name)'),
    ]
    return dashboard(
        "go-streams-application",
        "[go-streams] Application Drill-Down",
        "Per-application golden signals for a go-streams app: per-topic traffic and latency, error phases/actions/outcomes, retries and lag. Reached from the Fleet Overview.",
        panels, tpl,
    )


# ==================================================== 3. TOPOLOGY EXPLORER

def build_topology():
    g = Grid()
    panels = []

    panels.append(text_panel(g, (
        "## go-streams · Topology Explorer\n"
        "**Question this dashboard answers:** *Which topology node or edge is hot, slow, or failing?*\n\n"
        "Select **one** `job` — the service graph mixes nodes from multiple topologies otherwise. Node IDs "
        "(e.g. `FILTER-000002`) match the output of `topology.Describe().Print()`.\n\n"
        "**Read carefully:**\n"
        "- `stream.node.latency` is *exclusive self-time*: a node's latency excludes downstream nodes, so values sum rather than nest.\n"
        "- Node/edge metrics are recorded **per processing attempt**. During a retry storm the graph's throughput exceeds "
        "the consumed rate by the retry factor — the *Processing amplification* stat makes this visible.\n"
    ), h=6))

    # amplification stat + graph
    panels.append(stat(g, "Processing amplification",
        "Source-node record rate divided by consumed-record rate ≈ processing attempts per consumed record. ~1 is healthy; sustained values above 1 indicate retries re-traversing the topology.",
        [target(f'sum(rate(stream_node_records_total{{{J}, stream_node_type="source"}}[$__rate_interval])) / sum(rate(messaging_consumer_messages_total{{{J}}}[$__rate_interval]))')],
        "none", x=0, w=6, h=5, decimals=2,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 1.2}, {"color": "red", "value": 2}])))
    panels.append(stat(g, "Busiest node (records/s)",
        "The topology node currently processing the most records per second.",
        [target(f'topk(1, sum by (stream_node_name) (rate(stream_node_records_total{{{J}}}[$__rate_interval])))', "{{stream_node_name}}", instant=True)],
        "mps", x=6, w=6, h=5))
    panels.append(stat(g, "Slowest node p95",
        "Highest p95 self-time across topology nodes (stream.node.latency).",
        [target(f'topk(1, histogram_quantile(0.95, sum by (le, stream_node_name) (rate(stream_node_latency_bucket{{{J}}}[$__rate_interval]))))', "{{stream_node_name}}", instant=True)],
        "s", x=12, w=6, h=5))
    panels.append(stat(g, "Node errors/s",
        "Total error rate attributed to topology nodes (stream.errors).",
        [target(f'sum(rate(stream_errors_total{{{J}}}[$__rate_interval])) or vector(0)')],
        "mps", x=18, w=6, h=5, decimals=2,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 0.1}, {"color": "red", "value": 1}])))
    g.y += 5

    node_graph = {
        "id": g.pid(),
        "type": "nodeGraph",
        "title": "Topology service graph",
        "description": ("Live service graph of the stream topology. Node stat = records/s (stream.node.records); "
                        "edge stat = records/s flowing between nodes (stream.edge.records). Field mappings follow the "
                        "go-streams observability docs (id = node name, subtitle = node type, source/target = edge "
                        "endpoints) but are produced with label_replace in PromQL plus a calculateField transformation, "
                        "because Node Graph matches raw field names and ignores rename transformations "
                        "(grafana/grafana#54844). Remember: recorded per attempt, so retries inflate these numbers."),
        "datasource": DS,
        "gridPos": {"h": 13, "w": 24, "x": 0, "y": g.y},
        "targets": [
            # Node Graph matches on raw field names and ignores rename transformations
            # (grafana/grafana#54844), so id/subtitle/source/target are created as real
            # Prometheus labels via label_replace. label_replace can't rename the Value
            # field, so mainstat is created by the calculateField transformation below.
            target(
                f'label_replace(label_replace(sum by (stream_node_name, stream_node_type) '
                f'(rate(stream_node_records_total{{{J}}}[$__rate_interval])), '
                f'"id", "$1", "stream_node_name", "(.*)"), '
                f'"subtitle", "$1", "stream_node_type", "(.*)")',
                refid="nodes", instant=True, fmt="table"),
            target(
                f'label_replace(label_replace(sum by (stream_edge_source, stream_edge_target) '
                f'(rate(stream_edge_records_total{{{J}}}[$__rate_interval])), '
                f'"source", "$1", "stream_edge_source", "(.*)"), '
                f'"target", "$1", "stream_edge_target", "(.*)")',
                refid="edges", instant=True, fmt="table"),
        ],
        "transformations": [
            # Creates a real field named "mainstat" per frame; the only numeric field
            # in each frame is the query Value, so row-sum equals it.
            {"id": "calculateField", "options": {
                "mode": "reduceRow",
                "reduce": {"reducer": "sum"},
                "alias": "mainstat",
                "replaceFields": False,
            }},
            {"id": "filterFieldsByName", "options": {"include": {"names": ["id", "subtitle", "mainstat", "source", "target"]}}},
        ],
        "options": {"nodes": {"mainStatUnit": "mps"}, "edges": {"mainStatUnit": "mps"}},
        "fieldConfig": {"defaults": {}, "overrides": []},
    }
    panels.append(node_graph)
    g.y += 13

    panels.append(row(g, "Per-node detail"))
    panels.append(timeseries(g, "Node throughput (records/s)",
        "rate(stream.node.records) per node, filtered by the Node variable. Recorded per attempt — compare with consumer messages when retries are suspected.",
        [target(f'sum by (stream_node_name) (rate(stream_node_records_total{{{J}, stream_node_name=~"$node"}}[$__rate_interval]))', "{{stream_node_name}}")],
        unit="mps", x=0))
    panels.append(timeseries(g, "Node self-time p95",
        "p95 of stream.node.latency per node (exclusive self-time; values across nodes sum to the pipeline total). The node with the tallest line is your bottleneck.",
        [target(f'histogram_quantile(0.95, sum by (le, stream_node_name) (rate(stream_node_latency_bucket{{{J}, stream_node_name=~"$node"}}[$__rate_interval])))', "{{stream_node_name}}")],
        unit="s", x=12))
    g.y += 8
    panels.append(timeseries(g, "Edge flow (records/s)",
        "rate(stream.edge.records) per edge — how many records flow between each pair of connected nodes. A branch node's outgoing edges show the split ratio.",
        [target(f'sum by (stream_edge_source, stream_edge_target) (rate(stream_edge_records_total{{{J}}}[$__rate_interval]))', "{{stream_edge_source}} → {{stream_edge_target}}")],
        unit="mps", x=0))
    panels.append(timeseries(g, "Errors by node and phase",
        "rate(stream.errors) split by originating node and phase. Serde errors attach to source nodes, production errors to sinks.",
        [target(f'sum by (stream_error_node, stream_error_phase) (rate(stream_errors_total{{{J}}}[$__rate_interval]))', "{{stream_error_node}} ({{stream_error_phase}})")],
        unit="mps", x=12))
    g.y += 8

    panels.append(row(g, "Hotspots"))
    panels.append(bargauge(g, "Top 10 nodes by self-time p95",
        "Instant snapshot of the slowest nodes (stream.node.latency p95). Optimise from the top down.",
        [target(f'topk(10, histogram_quantile(0.95, sum by (le, stream_node_name) (rate(stream_node_latency_bucket{{{J}}}[$__rate_interval]))))', "{{stream_node_name}}", instant=True)],
        "s", x=0,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 0.05}, {"color": "red", "value": 0.5}])))
    panels.append(bargauge(g, "Top 10 nodes by throughput",
        "Instant snapshot of the busiest nodes (stream.node.records rate).",
        [target(f'topk(10, sum by (stream_node_name) (rate(stream_node_records_total{{{J}}}[$__rate_interval])))', "{{stream_node_name}}", instant=True)],
        "mps", x=12,
        thresh=thresholds([GREEN])))
    g.y += 8

    tpl = [
        var_datasource(),
        var_query("job", "Application (job)", "label_values(stream_node_records_total, job)", multi=False, include_all=False),
        var_query("node", "Node", 'label_values(stream_node_records_total{job=~"$job"}, stream_node_name)'),
    ]
    return dashboard(
        "go-streams-topology",
        "[go-streams] Topology Explorer",
        "Per-node and per-edge view of a single go-streams topology: service graph, node self-time, edge flow and error hotspots. Reached from the Application Drill-Down.",
        panels, tpl,
    )


# ================================================ 4. RUNNER & SATURATION

def build_runner():
    g = Grid()
    panels = []

    panels.append(text_panel(g, (
        "## go-streams · Runner & Saturation Deep Dive\n"
        "**Question this dashboard answers:** *Is the application keeping up — and if not, where is pressure building: "
        "input (lag), workers (queues/backpressure), poll loop, or sink (produce)?*\n\n"
        "The **Backpressure** row only has data for the `PartitionedRunner`; the `SingleThreadedRunner` processes inline "
        "and has no queues. When a partition's worker channel fills, go-streams pauses that partition at the consumer — "
        "rising *paused depth* is the earliest backpressure signal. Pause/resume *events* show backpressure churn "
        "(how often partitions cycle); paused *depth* shows how much data is stuck behind it.\n\n"
        "Reading order: lag tells you *whether* you're behind; queue/paused depth tells you the workers are the bottleneck; "
        "poll panels rule the broker fetch path in or out; produce panels rule the sink out. Use the Topology Explorer to "
        "find *which node* makes workers slow."
    ), h=6))

    # Input saturation
    panels.append(row(g, "Input saturation · consumer lag"))
    panels.append(timeseries(g, "Consumer lag percentiles",
        "p50 / p95 / p99 record age at consume time (stream.consumer.lag) across selected topics. Docs suggest alerting when p99 exceeds 30 s.",
        [
            target(f'histogram_quantile(0.50, sum by (le) (rate(stream_consumer_lag_bucket{{{JT}}}[$__rate_interval])))', "p50", refid="A"),
            target(f'histogram_quantile(0.95, sum by (le) (rate(stream_consumer_lag_bucket{{{JT}}}[$__rate_interval])))', "p95", refid="B"),
            target(f'histogram_quantile(0.99, sum by (le) (rate(stream_consumer_lag_bucket{{{JT}}}[$__rate_interval])))', "p99", refid="C"),
        ],
        unit="s", x=0, w=8))
    panels.append(heatmap(g, "Consumer lag distribution",
        "Heatmap of stream.consumer.lag. A drifting band means falling steadily behind; a split band means specific partitions are lagging — check the per-partition panel.",
        f'sum by (le) (rate(stream_consumer_lag_bucket{{{JT}}}[$__rate_interval]))', x=8, w=8))
    panels.append(timeseries(g, "Lag p99 by partition (top 10)",
        "Worst partitions by p99 lag (stream.consumer.lag by partition id). One hot partition usually means key skew; all partitions lagging means general under-capacity.",
        [target(f'topk(10, histogram_quantile(0.99, sum by (le, messaging_destination_name, messaging_destination_partition_id) (rate(stream_consumer_lag_bucket{{{JT}, messaging_destination_partition_id=~"$partition"}}[$__rate_interval]))))', "{{messaging_destination_name}}/p{{messaging_destination_partition_id}}")],
        unit="s", x=16, w=8))
    g.y += 8

    # Backpressure
    panels.append(row(g, "Backpressure · PartitionedRunner only"))
    panels.append(timeseries(g, "Worker queue depth vs paused depth",
        "stream.partitioned.queue.depth = records buffered in worker channels; stream.partitioned.paused.depth = records held for partitions paused at the consumer. Paused > 0 means backpressure has kicked in. Docs suggest alerting on queue depth > 1000.",
        [
            target(f'sum(stream_partitioned_queue_depth{{{J}}})', "queued", refid="A"),
            target(f'sum(stream_partitioned_paused_depth{{{J}}})', "paused", refid="B"),
        ],
        unit="short", x=0,
        thresh=thresholds([GREEN, {"color": "red", "value": 80}])))
    panels.append(timeseries(g, "Paused fraction of queued records",
        "paused depth ÷ queue depth. Values approaching 1 mean most buffered records sit behind paused partitions — throughput is fully backpressure-limited.",
        [target(f'sum(stream_partitioned_paused_depth{{{J}}}) / sum(stream_partitioned_queue_depth{{{J}}})', "paused fraction")],
        unit="percentunit", x=12))
    g.y += 8
    panels.append(timeseries(g, "Backpressure pause/resume events",
        "rate(stream.partitioned.backpressure.events) by event type. A sustained pause rate means workers can't keep up with the poll rate — find the slow node in Topology Explorer. paused ≈ resumed at high frequency is pause/resume thrash: lower WithResumeThreshold for more hysteresis (the partition then drains further before resuming). paused persistently above resumed means partitions are accumulating in the paused state. Docs suggest alerting above 1 pause/s.",
        [target(f'sum by (stream_backpressure_event) (rate(stream_partitioned_backpressure_events_total{{{JT}}}[$__rate_interval]))', "{{stream_backpressure_event}}")],
        unit="cps", x=0,
        thresh=thresholds([GREEN, {"color": "red", "value": 1}])))
    panels.append(bargauge(g, "Top partitions by pauses",
        "Partitions paused most often over the selected time range (stream.partitioned.backpressure.events with event=paused). One hot partition usually means key skew or one slow key; all partitions pausing means general under-capacity — same reading as the lag-by-partition panel above.",
        [target(f'topk(10, sum by (messaging_destination_name, messaging_destination_partition_id) (increase(stream_partitioned_backpressure_events_total{{{JT}, messaging_destination_partition_id=~"$partition", stream_backpressure_event="paused"}}[$__range])))', "{{messaging_destination_name}}/p{{messaging_destination_partition_id}}", instant=True)],
        "short", x=12,
        thresh=thresholds([GREEN, {"color": "yellow", "value": 10}, {"color": "red", "value": 100}])))
    g.y += 8

    # Poll health
    panels.append(row(g, "Poll loop health"))
    panels.append(timeseries(g, "Poll duration p95 by status",
        "p95 time per Poll() call (stream.poll.duration) split by success/error status.",
        [target(f'histogram_quantile(0.95, sum by (le, stream_poll_status) (rate(stream_poll_duration_seconds_bucket{{{
        J}}}['
                f'$__rate_interval])))', "{{stream_poll_status}}")],
        unit="s", x=0, w=8))
    panels.append(timeseries(g, "Poll error rate",
        "Failed polls per second (stream.poll.duration count with status=error). The runner retries polls with the configured PollErrorBackoff.",
        [target(f'sum(rate(stream_poll_duration_seconds_count{{{J}, stream_poll_status="error"}}[$__rate_interval])) '
                f'or '
                f'vector(0)', "poll errors/s")],
        unit="mps", x=8, w=8,
        thresh=thresholds([GREEN, {"color": "red", "value": 80}])))
    panels.append(timeseries(g, "Records per poll batch (p50 / p95)",
        "stream.poll.records percentiles. Batches pinned at the fetch maximum while lag grows confirm input saturation; near-empty batches with lag mean the broker fetch path is the bottleneck.",
        [
            target(f'histogram_quantile(0.50, sum by (le) (rate(stream_poll_records_bucket{{{J}}}[$__rate_interval])))', "p50", refid="A"),
            target(f'histogram_quantile(0.95, sum by (le) (rate(stream_poll_records_bucket{{{J}}}[$__rate_interval])))', "p95", refid="B"),
        ],
        unit="short", x=16, w=8))
    g.y += 8

    # Sink health
    panels.append(row(g, "Sink health · produce path"))
    panels.append(timeseries(g, "Produce duration p95 / p99",
        "Time per Send() call (stream.produce.duration). Slow produce backs up the whole pipeline because processing waits on forwarding.",
        [
            target(f'histogram_quantile(0.95, sum by (le) (rate(stream_produce_duration_seconds_bucket{{{JT}}}['
                   f'$__rate_interval])))', "p95", refid="A"),
            target(f'histogram_quantile(0.99, sum by (le) (rate(stream_produce_duration_seconds_bucket{{{JT}}}['
                   f'$__rate_interval])))', "p99", refid="B"),
        ],
        unit="s", x=0))
    panels.append(timeseries(g, "Produce error rate by topic",
        "Failed Send() calls per second (stream.produce.duration count with status=error). These surface as production-phase errors in the error-handling system.",
        [target(f'sum by (messaging_destination_name) (rate(stream_produce_duration_seconds_count{{{JT}, '
                f'stream_produce_status="error"}}[$__rate_interval])) or vector(0)', "{{messaging_destination_name}}")],
        unit="mps", x=12,
        thresh=thresholds([GREEN, {"color": "red", "value": 80}])))
    g.y += 8

    # Stability
    panels.append(row(g, "Stability"))
    panels.append(timeseries(g, "Active tasks by runner type",
        "stream.tasks.active — partitions currently owned and processed, labelled single_threaded or partitioned.",
        [target(f'sum by (stream_runner_type) (stream_tasks_active{{{J}}})', "{{stream_runner_type}}")],
        unit="short", x=0, fill=0))
    panels.append(timeseries(g, "Rebalance events by type",
        "increase(stream.rebalance.count) split assigned/revoked. Every revoke drains workers (bounded by WorkerShutdownTimeout / DrainTimeout) — frequent rebalances directly cause queue-depth sawtooths above.",
        [target(f'sum by (stream_rebalance_type) (increase(stream_rebalance_count_total{{{J}}}[$__interval]))', "{{stream_rebalance_type}}")],
        unit="short", x=12, draw="bars", fill=60))
    g.y += 8

    tpl = [
        var_datasource(),
        var_query("job", "Application (job)", "label_values(messaging_consumer_messages_total, job)"),
        var_query("topic", "Topic", 'label_values(stream_consumer_lag_count{job=~"$job"}, messaging_destination_name)'),
        var_query("partition", "Partition", 'label_values(stream_consumer_lag_count{job=~"$job", messaging_destination_name=~"$topic"}, messaging_destination_partition_id)'),
    ]
    return dashboard(
        "go-streams-runner",
        "[go-streams] Runner & Saturation",
        "Deep dive into go-streams runner saturation: per-partition consumer lag, PartitionedRunner backpressure (queue/paused depth, pause/resume events), poll-loop health, produce path and rebalance stability.",
        panels, tpl,
    )


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    write("go-streams-1-fleet-overview.json", build_overview())
    write("go-streams-2-application-drilldown.json", build_application())
    write("go-streams-3-topology-explorer.json", build_topology())
    write("go-streams-4-runner-saturation.json", build_runner())
