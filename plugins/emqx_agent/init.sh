#!/usr/bin/env bash
# =============================================================================
# init.sh — Bootstrap the HVAC anomaly incident-response scenario
#
# Registers three skills, one session profile, and one pipeline via the
# Agent REST API, using only curl.
#
# Scenario
# --------
# When a telemetry anomaly arrives on evt/hvac/anomaly the pipeline:
#   1. Deterministically fetches 60 min of history for the affected
#      device + metric from PostgreSQL.
#   2. Hands the event + history to a bounded LLM session (llm_loop).
#      The model may issue further PostgreSQL queries, then must:
#        • Create an incident in the incident tracker.
#        • Post a summary to #facilities-alerts on Slack.
#   3. Publishes the LLM analysis result to pipe/hvac/summary/<device_id>
#      so downstream consumers can react or audit the outcome.
#
# Skills registered
# -----------------
#   postgresql.query    pg-hvac            — PostgreSQL telemetry history
#   http                slack-facilities   — Slack #facilities-alerts webhook
#   http                incident-tracker   — Incident tracker (stub)
#   message.publish     hvac-summary       — Internal summary bus (pipe/hvac/summary)
#
# Session profile: hvac-triage-v1
# Pipeline:        hvac-anomaly-response  (trigger: evt/hvac/anomaly)
#
# Usage
# -----
#   ./init.sh
#
# Prerequisites
# -------------
#   • EMQX running on localhost:18083 with API key "key" and secret "secret".
#   • Stub HTTP services at http://slack-bridge:8080 and
#     http://incident-tracker:8080 (started separately, needed at runtime only).
# =============================================================================

set -euo pipefail

BASE="http://localhost:18083/api/v5/plugin_api/emqx_agent"
CREDS="key:secret"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

post() {
    local path="$1"
    shift
    curl -sf -u "${CREDS}" \
         -X POST "${BASE}${path}" \
         -H "Content-Type: application/json" \
         "$@"
    echo
}

section() { echo; echo "=== $* ==="; }

# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------

section "Registering skills"

# ── 1. PostgreSQL: 60-minute telemetry history ──────────────────────────────
echo "  • postgresql.query    pg-hvac"
post /skills --data-binary @- <<'JSON'
{
  "type": "postgresql.query",
  "id":   "pg-hvac",
  "desc": "Query recent telemetry history for an HVAC device and metric from PostgreSQL. Returns time-bucketed avg/min/max/count rows.",
  "query": "SELECT date_trunc('minute', ts) AS t, avg(value) AS avg, min(value) AS min, max(value) AS max, count(*) AS cnt FROM historical_telemetry WHERE site = $1 AND device_id = $2 AND metric = $3 AND ts >= now() - ($4 || ' minute')::interval GROUP BY t ORDER BY t ASC",
  "arg_keys": ["site", "device_id", "metric", "window_min"],
  "input_schema": {
    "type": "object",
    "properties": {
      "site":           { "type": "string",  "description": "Site code, e.g. mad01" },
      "device_id":      { "type": "string",  "description": "HVAC asset ID, e.g. ahu-17" },
      "metric":         { "type": "string",  "description": "Telemetry metric name, e.g. supply_air_temp" },
      "window_min":     { "type": "integer", "minimum": 1, "maximum": 1440, "description": "Look-back window in minutes" },
      "downsample_sec": { "type": "integer", "minimum": 1, "maximum": 3600, "description": "Bucket size in seconds" }
    },
    "required": ["site", "device_id", "metric", "window_min", "downsample_sec"]
  },
  "output_schema": {
    "type": "object",
    "properties": {
      "rows": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "t":   { "type": "string",  "format": "date-time" },
            "avg": { "type": "number" },
            "min": { "type": "number" },
            "max": { "type": "number" },
            "cnt": { "type": "integer" }
          },
          "required": ["t", "avg", "min", "max", "cnt"]
        }
      }
    },
    "required": ["rows"]
  }
}
JSON

# ── 2. Slack: post to #facilities-alerts ────────────────────────────────────
echo "  • http                slack-facilities"
post /skills --data-binary @- <<'JSON'
{
  "type":   "http",
  "id":     "slack-facilities",
  "desc":   "Post a notification message to the #facilities-alerts Slack channel. Use this to inform the facilities team of incidents, updates, or resolved issues.",
  "method": "post",
  "url":    "http://slack-bridge:8080/webhook",
  "headers": { "Content-Type": "application/json" },
  "input_schema": {
    "type": "object",
    "properties": {
      "channel": {
        "type": "string",
        "description": "Slack channel name without #, e.g. facilities-alerts"
      },
      "text": {
        "type": "string",
        "description": "Message body; supports Slack mrkdwn formatting"
      },
      "severity": {
        "type": "integer",
        "minimum": 1,
        "maximum": 5,
        "description": "Incident severity for colour-coding: 1=critical … 5=low"
      }
    },
    "required": ["channel", "text"]
  },
  "output_schema": {
    "type": "object",
    "properties": {
      "ok":        { "type": "boolean" },
      "ts":        { "type": "string",  "description": "Slack message timestamp" },
      "channel":   { "type": "string" },
      "error":     { "type": "string",  "description": "Error message if ok is false" }
    },
    "required": ["ok"]
  }
}
JSON

# ── 3. Incident tracker: create incident ────────────────────────────────────
echo "  • http                incident-tracker"
post /skills --data-binary @- <<'JSON'
{
  "type":   "http",
  "id":     "incident-tracker",
  "desc":   "Create a new incident in the incident tracker (ServiceNow). Returns the incident ID and URL. Use this whenever a telemetry anomaly requires human follow-up or on-site intervention.",
  "method": "post",
  "url":    "http://incident-tracker:8080/api/incidents",
  "headers": { "Content-Type": "application/json" },
  "input_schema": {
    "type": "object",
    "properties": {
      "title": {
        "type": "string",
        "description": "Short one-line summary, e.g. 'AHU-17 supply air temp 5°C above setpoint'"
      },
      "description": {
        "type": "string",
        "description": "Full description: what happened, historical trend summary, recommended action"
      },
      "severity": {
        "type": "integer",
        "minimum": 1,
        "maximum": 5,
        "description": "1 = critical / safety risk, 2 = high, 3 = medium, 4 = low, 5 = informational"
      },
      "device_id": {
        "type": "string",
        "description": "HVAC asset ID"
      },
      "site": {
        "type": "string",
        "description": "Site code"
      },
      "metric": {
        "type": "string",
        "description": "Affected metric name"
      },
      "assigned_group": {
        "type": "string",
        "description": "Team or queue to route the incident to, e.g. facilities-mad01"
      }
    },
    "required": ["title", "description", "severity", "device_id", "site"]
  },
  "output_schema": {
    "type": "object",
    "properties": {
      "incident_id": { "type": "string",  "description": "Tracker-assigned incident ID, e.g. INC0012345" },
      "url":         { "type": "string",  "description": "Deep-link to the incident in the tracker UI" },
      "status":      { "type": "string",  "description": "Initial status, e.g. open" }
    },
    "required": ["incident_id"]
  }
}
JSON

echo "  • hvac-summary  (message.publish → pipe/hvac/summary/)"
post /skills --data-binary @- <<'JSON'
{
  "type":         "message.publish",
  "id":           "hvac-summary",
  "desc":         "Publish the final pipeline summary to the internal HVAC summary topic. Use this as the last action after all escalation steps are complete.",
  "topic_prefix": "pipe/hvac/summary/"
}
JSON

# ---------------------------------------------------------------------------
# Session profile
# ---------------------------------------------------------------------------

section "Registering session profile"

echo "  • hvac-triage-v1"
post /session_profiles --data-binary @- <<'JSON'
{
  "name":     "hvac-triage-v1",
  "api_key":  "sk-YOUR-LLM-API-KEY",
  "base_url": "https://api.openai.com/v1",
  "model":    "gpt-4o",
  "instructions": "You are an HVAC facility operations AI embedded in a live monitoring system.\n\nYou will receive:\n  • event   — the raw telemetry anomaly event (device, site, metric, observed value, expected value, severity)\n  • history — a table of recent 1-minute buckets for that metric on that device\n\nYour task:\n1. Analyse the anomaly and the historical trend. Identify whether this is a transient spike or a sustained deviation.\n2. If you need additional historical data (different metric, wider window, neighbouring device), call the pg-hvac tool before concluding.\n3. Determine severity on a 1–5 scale:\n   • 1 — Safety risk or equipment damage imminent (e.g. temp 10°C above setpoint for >15 min)\n   • 2 — High: sustained deviation >5°C or >20% outside normal band\n   • 3 — Medium: deviation 3–5°C or 10–20% outside band\n   • 4 — Low: transient or minor deviation\n   • 5 — Informational\n4. For severity 1–3, create an incident in the incident tracker with:\n   - A precise title referencing the asset, metric, and observed value\n   - A description that includes the trend summary and recommended first action\n   - The correct assigned_group (use 'facilities-{site}' unless you have better information)\n5. Post a concise summary to Slack channel 'facilities-alerts'. If an incident was created, include the incident ID and URL in the message.\n6. Return your final assessment as a structured object.\n\nBe concise and specific. Do not speculate beyond what the data shows.",
  "output_schema": {
    "type": "object",
    "properties": {
      "severity":      { "type": "integer", "minimum": 1, "maximum": 5 },
      "root_cause":    { "type": "string",  "description": "One-sentence root cause assessment" },
      "trend":         { "type": "string",  "description": "transient | sustained | escalating | recovering" },
      "incident_id":   { "type": "string",  "description": "Tracker incident ID if created, otherwise null" },
      "slack_ts":      { "type": "string",  "description": "Slack message timestamp if posted" },
      "actions_taken": { "type": "array",   "items": { "type": "string" } }
    },
    "required": ["severity", "root_cause", "trend", "actions_taken"]
  }
}
JSON

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

section "Registering pipeline"

echo "  • hvac-anomaly-response  (trigger: evt/hvac/anomaly)"
post /pipelines --data-binary @- <<'JSON'
{
  "pipeline_id": "hvac-anomaly-response",
  "trigger": {
    "topic": "evt/hvac/anomaly"
  },
  "steps": [
    {
      "id":   "fetch_history",
      "type": "call_skill",
      "skill": "postgresql.query@pg-hvac",
      "args": {
        "site":           "$.event.entity.site",
        "device_id":      "$.event.entity.id",
        "metric":         "$.event.data.metric",
        "window_min":     60
      },
      "result_path": "$.history"
    },
    {
      "id":              "analyze_and_escalate",
      "type":            "llm_loop",
      "session_profile": "hvac-triage-v1",
      "tools": [
        "postgresql.query@pg-hvac",
        "http@slack-facilities",
        "http@incident-tracker"
      ],
      "input": {
        "event":   "$.event",
        "history": "$.history"
      },
      "result_path": "$.analysis"
    },
    {
      "id":          "publish_summary",
      "type":        "call_skill",
      "skill":       "message.publish@hvac-summary",
      "args": {
        "topic":   "$.event.entity.id",
        "payload": "$.analysis"
      },
      "result_path": "$.summary_published"
    }
  ]
}
JSON

# ---------------------------------------------------------------------------

echo
echo "Done."
echo
echo "Pipeline 'hvac-anomaly-response' is active."
echo "Steps: fetch_history → analyze_and_escalate → publish_summary"
echo "Final summary published to: pipe/hvac/summary/<device_id>"
echo
echo "Subscribe to summaries:  mosquitto_sub -t 'pipe/hvac/summary/#'"
echo
echo "Publish an anomaly event to trigger it:"
echo
cat <<'EXAMPLE'
  mosquitto_pub -t evt/hvac/anomaly -m '{
    "id":     "evt-001",
    "type":   "hvac.anomaly",
    "source": "iot.telemetry",
    "time":   "2026-03-11T10:05:00Z",
    "entity": { "kind": "device", "id": "ahu-17", "site": "mad01" },
    "data":   { "metric": "supply_air_temp", "value": 21.7, "expected": 16.0, "severity": 3 }
  }'
EXAMPLE
