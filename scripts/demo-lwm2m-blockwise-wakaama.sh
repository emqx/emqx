#!/usr/bin/env bash
set -euo pipefail

# LwM2M Blockwise demo using Wakaama client (UDP only)
# This script:
# 1) Builds wakaama lwm2mclient
# 2) Configures EMQX LwM2M gateway blockwise
# 3) Starts a wakaama client
# 4) Sends a large write (Block1) and a read (Block2)
# 5) Shows MQTT <-> LwM2M interaction (subscribe + publish)

EMQX_API=${EMQX_API:-http://127.0.0.1:18083/api/v5}
EMQX_USER=${EMQX_USER:-admin}
EMQX_PASS=${EMQX_PASS:-public}
EMQX_HOST=${EMQX_HOST:-127.0.0.1}
MQTT_HOST=${MQTT_HOST:-127.0.0.1}
MQTT_PORT=${MQTT_PORT:-1883}
MQTT_USER=${MQTT_USER:-}
MQTT_PASS=${MQTT_PASS:-}
MQTT_SUB_CLIENT_ID=${MQTT_SUB_CLIENT_ID:-demo-mqtt-sub}
MQTT_PUB_CLIENT_ID=${MQTT_PUB_CLIENT_ID:-demo-mqtt-pub}
MQTT_SUB_TOPIC=${MQTT_SUB_TOPIC:-lwm2m/up/resp}
MQTT_PUB_TOPIC=${MQTT_PUB_TOPIC:-lwm2m/dn/dm}
MQTT_DEBUG=${MQTT_DEBUG:-0}
LWM2M_PORT=${LWM2M_PORT:-5783}
CLIENT_ID=${CLIENT_ID:-demo-lwm2m-001}
BLOCK_SIZE=${BLOCK_SIZE:-16}
BLOCK1_BYTES=${BLOCK1_BYTES:-256}
SHOW_LOGS=${SHOW_LOGS:-1}
LOG_TAIL_LINES=${LOG_TAIL_LINES:-0}
LWM2M_XML_DIR=${LWM2M_XML_DIR:-"$PWD/apps/emqx_gateway_lwm2m/lwm2m_xml"}
WAKAAMA_DIR=${WAKAAMA_DIR:-"$PWD/.cache/wakaama"}
WAKAAMA_BUILD_DIR=${WAKAAMA_BUILD_DIR:-"$WAKAAMA_DIR/examples/client/udp/build"}
LOG_FILE=${LOG_FILE:-"$PWD/.cache/wakaama-client.log"}
MQTT_LOG=${MQTT_LOG:-"$PWD/.cache/mqtt-sub.log"}

mkdir -p "$(dirname "$WAKAAMA_DIR")" "$(dirname "$LOG_FILE")" "$(dirname "$MQTT_LOG")"

usage() {
  cat <<'USAGE'
Usage:
  demo_lwm2m_blockwise_wakaama.sh [--help|help]

What it does:
  - Builds wakaama lwm2mclient (UDP)
  - Configures EMQX LwM2M gateway blockwise
  - Starts a wakaama client
  - Sends a large WRITE (Block1) and a READ (Block2)
  - Shows MQTT <-> LwM2M interaction (subscribe + publish)
  - Shows MQTT -> CoAP commands (read/write/execute)

Environment variables (override defaults):
  EMQX_API           (default: http://127.0.0.1:18083/api/v5)
  EMQX_USER          (default: admin)
  EMQX_PASS          (default: public)
  EMQX_HOST          (default: 127.0.0.1)
  MQTT_HOST          (default: 127.0.0.1)
  MQTT_PORT          (default: 1883)
  MQTT_USER          (default: empty)
  MQTT_PASS          (default: empty)
  MQTT_SUB_CLIENT_ID (default: demo-mqtt-sub)
  MQTT_PUB_CLIENT_ID (default: demo-mqtt-pub)
  MQTT_SUB_TOPIC     (default: lwm2m/up/resp)
  MQTT_PUB_TOPIC     (default: lwm2m/dn/dm)
  MQTT_DEBUG         (default: 0)  # 1 = subscribe to lwm2m/# for debug
  LWM2M_PORT         (default: 5783)
  CLIENT_ID          (default: demo-lwm2m-001)
  BLOCK_SIZE         (default: 16)
  BLOCK1_BYTES       (default: 256)
  SHOW_LOGS          (default: 1)
  LOG_TAIL_LINES     (default: 0, 0 = from new lines only)
  LWM2M_XML_DIR      (default: ./apps/emqx_gateway_lwm2m/lwm2m_xml)
  WAKAAMA_DIR        (default: ./.cache/wakaama)
  WAKAAMA_BUILD_DIR  (default: $WAKAAMA_DIR/examples/client/udp/build)
  LOG_FILE           (default: ./.cache/wakaama-client.log)
  MQTT_LOG           (default: ./.cache/mqtt-sub.log)
  OPAQUE_B64         (optional) base64 payload override (real firmware)
  OPAQUE_PREVIEW     (optional) preview text when OPAQUE_B64 is set
Note:
  - If you change mountpoint or translators, update MQTT_SUB_TOPIC / MQTT_PUB_TOPIC accordingly.
  - Set MQTT_DEBUG=1 to capture all lwm2m/# traffic for troubleshooting.
  - Set OPAQUE_B64 to send a real firmware payload (base64).

Examples:
  ./scripts/demo_lwm2m_blockwise_wakaama.sh
  EMQX_HOST=192.168.1.10 MQTT_HOST=192.168.1.10 ./scripts/demo_lwm2m_blockwise_wakaama.sh
  MQTT_USER=demo MQTT_PASS=secret ./scripts/demo_lwm2m_blockwise_wakaama.sh
USAGE
}

api_error_hint() {
  cat <<'HINT'
[demo] Hint:
  - Check EMQX_API (host/port/path)
  - Check EMQX_USER / EMQX_PASS
  - Ensure Dashboard API is enabled and reachable
  - Ensure LwM2M gateway app is enabled
HINT
}

print_flow() {
  cat <<FLOW

${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════════╗
║                 LwM2M Blockwise Transfer Demo                    ║
╚══════════════════════════════════════════════════════════════════╝${RESET}

${BOLD}What is this demo?${RESET}

  LwM2M (Lightweight M2M) is a protocol for managing IoT devices.
  When data is too large for a single CoAP packet, it must be split
  into smaller pieces — this is called ${BOLD}Blockwise Transfer${RESET}.

  This demo shows two directions:
    ${CYAN}Block1${RESET} — Client receives large data (e.g., firmware)   Server → Device
    ${CYAN}Block2${RESET} — Client sends large data in response to READ  Device → Server

${BOLD}Architecture:${RESET}

   REST API (Dashboard) → EMQX API/GW → CoAP/UDP → Device
   ┌────────────┐  POST .../write?path=/5/0/0  (Block1 firmware)
   │ Dashboard  │ ───────────────────────────────────────────────►
   │   Client   │  POST .../read?path=/3/0      (Block2 read)
   └────────────┘                         │
                                          ▼
                                   ┌──────────────┐
                                   │    EMQX      │
                                   │ API + GW     │
                                   └──────┬───────┘
                                          │ CoAP/UDP (Block1/Block2)
                                          ▼
                                    ┌───────────┐
                                    │  Wakaama  │
                                    │  LwM2M    │
                                    │  Client   │
                                    │ (port ${LWM2M_PORT}) │
                                    └───────────┘

   MQTT (commands + responses)
   ┌────────────┐  publish: lwm2m/dn/dm (read/write/execute)
   │  MQTT Pub  │ ───────────────────────────────────────────────►
   └────────────┘                                      ┌──────────────┐
                                                       │    EMQX      │
                                                       │ Broker + GW  │
                                                       └──────┬───────┘
                                                              │ CoAP/UDP to device
                                                              ▼
                                                        ┌───────────┐
                                                        │  Wakaama  │
                                                        └───────────┘
                                                              ▲
                                                              │ responses:
                                                              │ read=Block2, write/execute=2.04
   ┌────────────┐  subscribe: lwm2m/up/resp
   │  MQTT Sub  │ ◄───────────────────────────────────────────────────────────
   └────────────┘

${BOLD}What you'll see:${RESET}

  ${GREEN}Step 1-4${RESET}  Setup: check API, configure gateway, start client & subscriber
  ${GREEN}Step 5${RESET}    ${CYAN}REST API → CoAP WRITE${RESET} with Block1 (large firmware push)
  ${GREEN}Step 6${RESET}    ${CYAN}REST API → CoAP READ${RESET}  with Block2 (read device info)
  ${GREEN}Step 7-9${RESET}  ${MAGENTA}MQTT → CoAP${RESET} commands: read, write, execute via MQTT publish

  Block size: ${BOLD}${BLOCK_SIZE} bytes${RESET}  |  Write payload: ${BOLD}${BLOCK1_BYTES} bytes${RESET} (${BOLD}$((BLOCK1_BYTES / BLOCK_SIZE)) blocks${RESET})

FLOW
}

BOLD=$'\033[1m'
DIM=$'\033[2m'
CYAN=$'\033[36m'
GREEN=$'\033[32m'
YELLOW=$'\033[33m'
MAGENTA=$'\033[35m'
RESET=$'\033[0m'

step() {
  CURRENT_STEP=$1
  echo ""
  echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}  ▶ $CURRENT_STEP${RESET}"
  echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
}

explain() {
  echo -e "${DIM}  💡 $1${RESET}"
}

expect() {
  echo -e "${YELLOW}  👀 Expected: $1${RESET}"
}

ok() {
  echo -e "${GREEN}  ✅ $1${RESET}"
}

on_err() {
  local line=$1
  echo "[demo] ERROR at step: ${CURRENT_STEP:-unknown} (line $line)"
  echo "[demo] Last command: $BASH_COMMAND"
  api_error_hint
}

curl_api() {
  local method=$1
  local url=$2
  local data_file=${3:-}
  local resp_file
  local err_file
  local code
  local rc=0

  resp_file=$(mktemp)
  err_file=$(mktemp)

  if [ -n "$data_file" ]; then
    code=$(curl -sS -u "$EMQX_USER:$EMQX_PASS" \
      -H "Content-Type: application/json" \
      -X "$method" \
      --data @"$data_file" \
      -o "$resp_file" -w "%{http_code}" \
      "$url" 2>"$err_file") || rc=$?
  else
    code=$(curl -sS -u "$EMQX_USER:$EMQX_PASS" \
      -X "$method" \
      -o "$resp_file" -w "%{http_code}" \
      "$url" 2>"$err_file") || rc=$?
  fi

  if [ $rc -ne 0 ]; then
    echo "[demo] API call failed (curl rc=$rc) URL=$url"
    if [ -s "$err_file" ]; then
      echo "[demo] curl error:"
      cat "$err_file"
    fi
    api_error_hint
    rm -f "$resp_file" "$err_file"
    return $rc
  fi

  if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
    echo "[demo] API error: $url -> HTTP $code"
    if [ -s "$resp_file" ]; then
      echo "[demo] Response:"
      cat "$resp_file"
    fi
    api_error_hint
    rm -f "$resp_file" "$err_file"
    return 22
  fi

  rm -f "$resp_file" "$err_file"
  return 0
}

curl_api_status() {
  local method=$1
  local url=$2
  local data_file=${3:-}
  local resp_file
  local err_file
  local code
  local rc=0

  resp_file=$(mktemp)
  err_file=$(mktemp)

  if [ -n "$data_file" ]; then
    code=$(curl -sS -u "$EMQX_USER:$EMQX_PASS" \
      -H "Content-Type: application/json" \
      -X "$method" \
      --data @"$data_file" \
      -o "$resp_file" -w "%{http_code}" \
      "$url" 2>"$err_file") || rc=$?
  else
    code=$(curl -sS -u "$EMQX_USER:$EMQX_PASS" \
      -X "$method" \
      -o "$resp_file" -w "%{http_code}" \
      "$url" 2>"$err_file") || rc=$?
  fi

  if [ $rc -ne 0 ]; then
    echo "[demo] API call failed (curl rc=$rc) URL=$url"
    if [ -s "$err_file" ]; then
      echo "[demo] curl error:"
      cat "$err_file"
    fi
    api_error_hint
    rm -f "$resp_file" "$err_file"
    return $rc
  fi

  CURL_API_CODE=$code
  if [ -s "$resp_file" ]; then
    CURL_API_BODY=$(cat "$resp_file")
  else
    CURL_API_BODY=""
  fi

  rm -f "$resp_file" "$err_file"
  return 0
}

urlencode() {
  python3 - <<PY "$1"
import sys, urllib.parse
print(urllib.parse.quote(sys.argv[1]))
PY
}

ensure_wakaama() {
  if [ ! -d "$WAKAAMA_DIR/.git" ]; then
    explain "Cloning wakaama into $WAKAAMA_DIR (first run only)"
    git clone --depth 1 https://github.com/eclipse/wakaama.git "$WAKAAMA_DIR"
  fi

  (cd "$WAKAAMA_DIR" && git submodule update --init --recursive)

  # Avoid autotools by generating dtls_config.h via tinydtls CMake
  if [ ! -f "$WAKAAMA_DIR/transport/tinydtls/third_party/tinydtls/dtls_config.h" ]; then
    explain "Generating tinydtls config (first run only)"
    cmake -S "$WAKAAMA_DIR/transport/tinydtls/third_party/tinydtls" \
      -B "$WAKAAMA_DIR/transport/tinydtls/third_party/tinydtls/build" >/dev/null
    cp "$WAKAAMA_DIR/transport/tinydtls/third_party/tinydtls/build/dtls_config.h" \
      "$WAKAAMA_DIR/transport/tinydtls/third_party/tinydtls/dtls_config.h"
  fi

  if [ ! -x "$WAKAAMA_BUILD_DIR/lwm2mclient" ]; then
    explain "Building wakaama lwm2mclient binary (first run only)"
    cmake -S "$WAKAAMA_DIR/examples/client/udp" \
      -B "$WAKAAMA_BUILD_DIR" -DWAKAAMA_UNIT_TESTS=OFF >/dev/null
    cmake --build "$WAKAAMA_BUILD_DIR" -j 4 >/dev/null
  fi
}

ensure_paho() {
  if ! python3 - <<'PY'
import importlib.util, sys
sys.exit(0 if importlib.util.find_spec("paho") else 1)
PY
  then
    explain "Installing paho-mqtt Python library (one-time)"
    python3 -m pip -q install --user paho-mqtt >/dev/null
  fi
}

effective_mqtt_sub_topic() {
  if [ "$MQTT_DEBUG" = "1" ]; then
    echo "lwm2m/#"
  else
    echo "$MQTT_SUB_TOPIC"
  fi
}

start_mqtt_sub() {
  local sub_topic
  sub_topic=$(effective_mqtt_sub_topic)
  explain "Starting MQTT subscriber on topic: $sub_topic"
  if command -v mosquitto_sub >/dev/null 2>&1; then
    local -a AUTH_ARGS=()
    if [ -n "$MQTT_USER" ]; then
      AUTH_ARGS+=("-u" "$MQTT_USER")
    fi
    if [ -n "$MQTT_PASS" ]; then
      AUTH_ARGS+=("-P" "$MQTT_PASS")
    fi
    mosquitto_sub -h "$MQTT_HOST" -p "$MQTT_PORT" \
      -i "$MQTT_SUB_CLIENT_ID" ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} \
      -t "$sub_topic" -v > "$MQTT_LOG" 2>&1 &
    MQTT_SUB_PID=$!
  else
    ensure_paho
    cat > /tmp/mqtt_sub.py <<'PY'
import os, sys, time
import paho.mqtt.client as mqtt

host = os.environ.get("MQTT_HOST", "127.0.0.1")
port = int(os.environ.get("MQTT_PORT", "1883"))
user = os.environ.get("MQTT_USER") or None
password = os.environ.get("MQTT_PASS") or None
client_id = os.environ.get("MQTT_SUB_CLIENT_ID", "demo-mqtt-sub")
topic = os.environ.get("MQTT_SUB_TOPIC")

if not topic:
    print("MQTT_SUB_TOPIC is required", flush=True)
    sys.exit(1)

def on_connect(client, userdata, flags, rc, properties=None):
    client.subscribe(topic)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
    except Exception:
        payload = str(msg.payload)
    print(f"{msg.topic} {payload}", flush=True)

client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
if user:
    client.username_pw_set(user, password or "")
client.on_connect = on_connect
client.on_message = on_message
client.connect(host, port, 60)
client.loop_forever()
PY
    MQTT_SUB_TOPIC="$sub_topic" MQTT_HOST="$MQTT_HOST" MQTT_PORT="$MQTT_PORT" \
      MQTT_USER="$MQTT_USER" MQTT_PASS="$MQTT_PASS" MQTT_SUB_CLIENT_ID="$MQTT_SUB_CLIENT_ID" \
      python3 /tmp/mqtt_sub.py > "$MQTT_LOG" 2>&1 &
    MQTT_SUB_PID=$!
  fi
  sleep 1
}

publish_mqtt_command() {
  CMD_JSON=$1
  if command -v mosquitto_pub >/dev/null 2>&1; then
    local -a AUTH_ARGS=()
    if [ -n "$MQTT_USER" ]; then
      AUTH_ARGS+=("-u" "$MQTT_USER")
    fi
    if [ -n "$MQTT_PASS" ]; then
      AUTH_ARGS+=("-P" "$MQTT_PASS")
    fi
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" \
      -i "$MQTT_PUB_CLIENT_ID" ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} \
      -t "$MQTT_PUB_TOPIC" -m "$CMD_JSON"
  else
    ensure_paho
    MQTT_HOST="$MQTT_HOST" MQTT_PORT="$MQTT_PORT" \
      MQTT_USER="$MQTT_USER" MQTT_PASS="$MQTT_PASS" MQTT_PUB_CLIENT_ID="$MQTT_PUB_CLIENT_ID" \
      python3 - <<'PY' "$CMD_JSON" "$MQTT_PUB_TOPIC"
import os, sys
import paho.mqtt.client as mqtt

host = os.environ.get("MQTT_HOST", "127.0.0.1")
port = int(os.environ.get("MQTT_PORT", "1883"))
user = os.environ.get("MQTT_USER") or None
password = os.environ.get("MQTT_PASS") or None
client_id = os.environ.get("MQTT_PUB_CLIENT_ID", "demo-mqtt-pub")
payload = sys.argv[1]
topic = sys.argv[2]

client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
if user:
    client.username_pw_set(user, password or "")
client.connect(host, port, 60)
client.publish(topic, payload, qos=0)
client.disconnect()
PY
  fi
}

configure_gateway() {
  explain "Configuring EMQX LwM2M gateway via REST API"
  local code
  local resp

  CURL_API_CODE=""
  CURL_API_BODY=""
  if ! curl_api_status GET "$EMQX_API/gateways/lwm2m"; then
    return 22
  fi
  code=${CURL_API_CODE:-unknown}
  resp=${CURL_API_BODY:-}

  if [ "$code" = "200" ]; then
    explain "Gateway lwm2m already exists — updating configuration"
  elif [ "$code" = "404" ]; then
    explain "Gateway lwm2m not found — creating new configuration"
  elif [ "$code" = "unknown" ]; then
    echo "[demo] gateway check failed (unknown response)."
    api_error_hint
    return 22
  else
    echo "[demo] gateway check failed: HTTP $code"
    if [ -n "$resp" ]; then
      echo "[demo] Response:"
      echo "$resp"
    fi
    api_error_hint
    return 22
  fi
  cat > /tmp/lwm2m-gateway-demo.json <<JSON
{
  "name": "lwm2m",
  "enable": true,
  "xml_dir": "${LWM2M_XML_DIR}",
  "mountpoint": "lwm2m/",
  "auto_observe": false,
  "update_msg_publish_condition": "always",
  "translators": {
    "command": {"topic": "dn/#"},
    "response": {"topic": "up/resp"},
    "notify": {"topic": "up/notify"},
    "register": {"topic": "up/resp"},
    "update": {"topic": "up/resp"}
  },
  "listeners": [
    {"name": "default", "type": "udp", "bind": "${LWM2M_PORT}"}
  ],
  "blockwise": {
    "enable": true,
    "max_block_size": ${BLOCK_SIZE},
    "max_body_size": "1MB",
    "exchange_lifetime": "60s"
  }
}
JSON

  curl_api PUT "$EMQX_API/gateways/lwm2m" /tmp/lwm2m-gateway-demo.json
}

start_client() {
  explain "Starting Wakaama LwM2M client (endpoint: $CLIENT_ID)"
  PIPE=$(mktemp -u)
  mkfifo "$PIPE"
  # Open FIFO in read/write mode to avoid blocking before reader starts.
  exec 3<> "$PIPE"
  "$WAKAAMA_BUILD_DIR/lwm2mclient" -4 -n "$CLIENT_ID" -h "$EMQX_HOST" -p "$LWM2M_PORT" -S "$BLOCK_SIZE" \
    < "$PIPE" > "$LOG_FILE" 2>&1 &
  CLIENT_PID=$!
  explain "Client pid=$CLIENT_PID, log=$LOG_FILE"
  sleep 2
}

stop_client() {
  if [ -n "${CLIENT_PID:-}" ] && kill -0 "$CLIENT_PID" 2>/dev/null; then
    kill "$CLIENT_PID" || true
    wait "$CLIENT_PID" 2>/dev/null || true
  fi
  if [ -n "${MQTT_SUB_PID:-}" ] && kill -0 "$MQTT_SUB_PID" 2>/dev/null; then
    kill "$MQTT_SUB_PID" || true
    wait "$MQTT_SUB_PID" 2>/dev/null || true
  fi
  if [ -n "${LOG_TAIL_PID1:-}" ] && kill -0 "$LOG_TAIL_PID1" 2>/dev/null; then
    kill "$LOG_TAIL_PID1" || true
    wait "$LOG_TAIL_PID1" 2>/dev/null || true
  fi
  if [ -n "${LOG_TAIL_PID2:-}" ] && kill -0 "$LOG_TAIL_PID2" 2>/dev/null; then
    kill "$LOG_TAIL_PID2" || true
    wait "$LOG_TAIL_PID2" 2>/dev/null || true
  fi
  if [ -n "${PIPE:-}" ] && [ -p "$PIPE" ]; then
    rm -f "$PIPE"
  fi
}

stop_log_tail() {
  if [ -n "${LOG_TAIL_PID1:-}" ] && kill -0 "$LOG_TAIL_PID1" 2>/dev/null; then
    kill "$LOG_TAIL_PID1" || true
    wait "$LOG_TAIL_PID1" 2>/dev/null || true
    LOG_TAIL_PID1=""
  fi
  if [ -n "${LOG_TAIL_PID2:-}" ] && kill -0 "$LOG_TAIL_PID2" 2>/dev/null; then
    kill "$LOG_TAIL_PID2" || true
    wait "$LOG_TAIL_PID2" 2>/dev/null || true
    LOG_TAIL_PID2=""
  fi
}

WAIT_TIMEOUT=${WAIT_TIMEOUT:-30}

start_log_tail() {
  if [ "$SHOW_LOGS" != "1" ]; then
    return
  fi
  touch "$LOG_FILE" "$MQTT_LOG"
  explain "Live log tailing enabled — watch for [LWM2M] and [MQTT] prefixes below"
  tail -n "$LOG_TAIL_LINES" -f "$LOG_FILE" \
    | grep --line-buffered -v -E 'State: STATE_READY|^>|^$' \
    | sed 's/^/[LWM2M] /' &
  LOG_TAIL_PID1=$!
}

wait_mqtt_response() {
  local request_id=$1
  local before_lines=$2
  local waited=0
  while [ "$waited" -lt "$WAIT_TIMEOUT" ]; do
    local current_lines
    current_lines=$(wc -l < "$MQTT_LOG" 2>/dev/null || echo 0)
    if [ "$current_lines" -gt "$before_lines" ]; then
      local new_content
      new_content=$(
        tail -n +"$((before_lines + 1))" "$MQTT_LOG" \
          | grep -F "$request_id" \
          | grep -F -v "$MQTT_PUB_TOPIC " \
          | head -1
      )
      if [ -n "$new_content" ]; then
        local pretty
        pretty=$(echo "$new_content" | python3 -c "
import sys, json
line = sys.stdin.read().strip()
parts = line.split(' ', 1)
payload = parts[1] if len(parts) > 1 else parts[0]
try:
    d = json.loads(payload)
    code = d.get('data', {}).get('code', '?')
    msg_type = d.get('msgType', '?')
    req_id = d.get('data', {}).get('requestID', d.get('requestID', '?'))
    content = d.get('data', {}).get('content', None)
    summary = f'reqID={req_id}  code={code}  msgType={msg_type}'
    if content and isinstance(content, list):
        summary += f'  content=[{len(content)} items]'
    print(summary)
except:
    print(line[:120])
" 2>/dev/null || echo "$new_content" | head -c 120)
        echo -e "  ${GREEN}← [MQTT] ${pretty}${RESET}"
        return 0
      fi
    fi
    sleep 1
    waited=$((waited + 1))
  done
  echo -e "  ${YELLOW}⏳ No MQTT response within ${WAIT_TIMEOUT}s (check $MQTT_LOG)${RESET}"
  return 1
}

get_opaque_b64() {
  if [ -z "${OPAQUE_B64:-}" ]; then
    local out
    out=$(BLOCK1_BYTES="$BLOCK1_BYTES" BLOCK_SIZE="$BLOCK_SIZE" python3 - <<'PY'
import base64, os
size = int(os.environ.get("BLOCK1_BYTES", "256"))
block = int(os.environ.get("BLOCK_SIZE", "16"))
header = f"FWDEMOv1|size={size}|block={block}|".encode()
payload = bytearray(header)
while len(payload) < size:
    payload.extend(f"DATA{len(payload):04d}|".encode())
payload = payload[:size]
print(base64.b64encode(payload).decode())
print(payload[:48].decode("ascii", errors="replace"))
PY
)
    OPAQUE_B64=$(echo "$out" | head -n 1)
    OPAQUE_PREVIEW=$(echo "$out" | tail -n 1)
  else
    OPAQUE_PREVIEW=${OPAQUE_PREVIEW:-"(custom payload)"}
  fi
}

send_blockwise_write() {
  get_opaque_b64
  VALUE_Q=$(urlencode "$OPAQUE_B64")
  explain "Calling REST API: POST .../write?path=/5/0/0  (Firmware Package object)"
  explain "Payload is ${BLOCK1_BYTES} bytes opaque data, block_size=${BLOCK_SIZE}"
  explain "Payload preview (ascii, first 48B): ${OPAQUE_PREVIEW}"
  explain "EMQX will automatically split this into $((BLOCK1_BYTES / BLOCK_SIZE)) Block1 chunks"
  expect "CoAP 2.04 Changed — the device accepted the firmware payload"
  curl_api POST "$EMQX_API/gateways/lwm2m/clients/$CLIENT_ID/write?path=/5/0/0&type=Opaque&value=$VALUE_Q"
  ok "Block1 WRITE completed"
}

send_blockwise_read() {
  explain "Calling REST API: POST .../read?path=/3/0  (Device object)"
  explain "The device info is larger than ${BLOCK_SIZE} bytes, so the client"
  explain "will respond with multiple Block2 chunks that EMQX reassembles"
  expect "CoAP 2.05 Content — device info (manufacturer, model, serial, etc.)"
  curl_api POST "$EMQX_API/gateways/lwm2m/clients/$CLIENT_ID/read?path=/3/0"
  ok "Block2 READ completed"
}

send_mqtt_read() {
  explain "Publishing JSON command to MQTT topic: $MQTT_PUB_TOPIC"
  explain "EMQX receives this MQTT message → translates to CoAP GET → device"
  explain "Device response is large → triggers Block2 reassembly → MQTT response"
  local before_lines
  before_lines=$(wc -l < "$MQTT_LOG" 2>/dev/null || echo 0)
  CMD_JSON=$(python3 - <<PY
import json
cmd = {
  "requestID": 9001,
  "cacheID": 9001,
  "msgType": "read",
  "data": {"path": "/3/0"}
}
print(json.dumps(cmd))
PY
)
  echo -e "  ${DIM}→ ${CMD_JSON}${RESET}"
  publish_mqtt_command "$CMD_JSON"
  wait_mqtt_response "9001" "$before_lines" || true
}

send_mqtt_write() {
  get_opaque_b64
  explain "Publishing WRITE command via MQTT (same ${BLOCK1_BYTES}-byte firmware as Step 5)"
  explain "Path: MQTT publish → EMQX decodes JSON → CoAP PUT (${BLOCK1_BYTES}B payload)"
  explain "      → payload > ${BLOCK_SIZE}B → auto Block1 split → $((BLOCK1_BYTES / BLOCK_SIZE)) chunks to device"
  explain "Payload preview (ascii, first 48B): ${OPAQUE_PREVIEW}"
  local before_lines
  before_lines=$(wc -l < "$MQTT_LOG" 2>/dev/null || echo 0)
  CMD_JSON=$(OPAQUE_B64="$OPAQUE_B64" python3 - <<'PY'
import json, os
cmd = {
  "requestID": 9002,
  "cacheID": 9002,
  "msgType": "write",
  "data": {"path": "/5/0/0", "type": "Opaque", "value": os.environ["OPAQUE_B64"]}
}
print(json.dumps(cmd))
PY
)
  publish_mqtt_command "$CMD_JSON"
  wait_mqtt_response "9002" "$before_lines" || true
}

send_mqtt_execute() {
  explain "Publishing EXECUTE command via MQTT"
  explain "/3/0/4 = Device object → Reboot resource (no blockwise needed)"
  local before_lines
  before_lines=$(wc -l < "$MQTT_LOG" 2>/dev/null || echo 0)
  CMD_JSON=$(python3 - <<'PY'
import json
cmd = {
  "requestID": 9003,
  "cacheID": 9003,
  "msgType": "execute",
  "data": {"path": "/3/0/4", "args": "undefined"}
}
print(json.dumps(cmd))
PY
)
  echo -e "  ${DIM}→ ${CMD_JSON}${RESET}"
  publish_mqtt_command "$CMD_JSON"
  wait_mqtt_response "9003" "$before_lines" || true
}

main() {
  set -E
  trap 'on_err $LINENO' ERR

  for arg in "$@"; do
    case "$arg" in
      -h|--help|help)
        usage
        exit 0
        ;;
      *)
        echo "[demo] unknown argument: $arg"
        usage
        exit 2
        ;;
    esac
  done

  print_flow

  step "Step 1/9 — Checking EMQX API"
  explain "Verifying that EMQX Dashboard API is reachable at $EMQX_API"
  curl_api GET "$EMQX_API/status"
  ok "EMQX API is up"

  step "Step 2/9 — Build Wakaama + Configure LwM2M Gateway"
  explain "Wakaama is an open-source LwM2M client from Eclipse Foundation"
  explain "We'll configure the EMQX LwM2M gateway with blockwise enabled"
  ensure_wakaama
  configure_gateway
  ok "Gateway configured with block_size=${BLOCK_SIZE}"

  step "Step 3/9 — Start LwM2M Device (Wakaama Client)"
  explain "The client registers with EMQX as endpoint '${CLIENT_ID}'"
  explain "It exposes standard LwM2M objects: /3 (Device), /5 (Firmware)"
  start_client

  step "Step 4/9 — Start MQTT Subscriber"
  explain "We subscribe to '$(effective_mqtt_sub_topic)' to watch responses from the device"
  explain "Every CoAP response from the device is translated to MQTT by EMQX"
  start_mqtt_sub
  start_log_tail

  sleep 1
  step "Step 5/9 — REST API → Block1 WRITE (Server pushes firmware to Device)"
  explain "Block1 = server-initiated transfer: EMQX splits a large payload"
  explain "into ${BLOCK_SIZE}-byte chunks and sends them one by one via CoAP"
  send_blockwise_write

  sleep 1
  step "Step 6/9 — REST API → Block2 READ (Server reads device info)"
  explain "Block2 = client-initiated transfer: the device's response is too"
  explain "large for one packet, so it sends multiple Block2 chunks back"
  send_blockwise_read

  step "Step 7/9 — MQTT → CoAP READ (read device info via MQTT)"
  explain "Same READ as Step 6, but triggered by publishing to MQTT topic"
  explain "This shows the MQTT→CoAP bridge: publish JSON → EMQX → CoAP → device"
  send_mqtt_read

  step "Step 8/9 — MQTT → CoAP WRITE (push firmware via MQTT)"
  explain "Same WRITE as Step 5, but triggered via MQTT publish"
  explain "EMQX translates the MQTT JSON into CoAP Block1 chunks automatically"
  send_mqtt_write

  step "Step 9/9 — MQTT → CoAP EXECUTE (trigger device reboot)"
  explain "EXECUTE is a LwM2M command that triggers an action, not a data transfer"
  explain "Here we trigger /3/0/4 (Reboot) — the device acknowledges with 2.04"
  send_mqtt_execute

  sleep 2
  stop_log_tail

  echo ""
  echo -e "${BOLD}${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}${GREEN}  ✅ Demo Complete!${RESET}"
  echo -e "${BOLD}${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo ""
  echo -e "${BOLD}What just happened:${RESET}"
  echo -e "  Step 5,8: EMQX split ${BLOCK1_BYTES}B firmware → ${BOLD}$((BLOCK1_BYTES / BLOCK_SIZE)) Block1 chunks${RESET} → device (via REST API & MQTT)"
  echo -e "  Step 6,7: Device response reassembled from ${BOLD}Block2 chunks${RESET} → EMQX → MQTT"
  echo -e "  Step 9:   MQTT EXECUTE → CoAP (no blockwise, small payload)"
  echo ""
  echo -e "${DIM}Cleaning up...${RESET}"
}

trap stop_client EXIT
main "$@"
