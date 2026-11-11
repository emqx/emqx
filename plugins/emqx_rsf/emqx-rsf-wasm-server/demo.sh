#!/usr/bin/env bash
# Walk through the emqx-rsf/1 protocol (apps/emqx_rsf/PROTOCOL.md) with curl.
# Needs curl and jq.
#
#   docker run -d --name emqx-rsf-demo -p 127.0.0.1:18480:8080 -e RSF_TOKEN=demo-token \
#     --read-only --cap-drop ALL --security-opt no-new-privileges emqx-rsf-wasm-server:dev
#   ./demo.sh

set -euo pipefail

URL="${RSF_URL:-http://127.0.0.1:18480}"
TOKEN="${RSF_TOKEN:-demo-token}"
AUTH=(-H "Authorization: Bearer ${TOKEN}")

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
show() { curl -sS -w '\nHTTP %{http_code}\n' "$@"; }

# upload NAME < source.py: wrap the source as {"runtime", "source"} and PUT it.
upload() {
  jq -n --rawfile src /dev/stdin '{runtime: "python3", source: $src}' |
    show "${AUTH[@]}" -X PUT -H 'Content-Type: application/json' --data-binary @- \
      "${URL}/v1/functions/$1"
}

step "1. Health check (no token needed)"
show "${URL}/healthz"

step "2. Every other endpoint needs the token"
show "${URL}/v1/functions"

step "2b. Server info: protocol, runtimes and limits"
show "${AUTH[@]}" "${URL}/v1/info"

step "3. Upload a function: str_upper(s)"
upload str_upper <<'EOF'
def handle(s):
    return s.upper()
EOF

step "4. Call it"
show "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"args": ["hello emqx"]}' "${URL}/v1/functions/str_upper/call"

step "5. Upload a function that uses allowed imports: decode a base64 JSON payload"
upload temp_f <<'EOF'
import base64
import json

def handle(payload_b64):
    data = json.loads(base64.b64decode(payload_b64))
    return {"device": data["id"], "temp_f": round(data["temp_c"] * 9 / 5 + 32, 1)}
EOF
PAYLOAD=$(printf '{"id":"dev-1","temp_c":21.5}' | base64 -w0)
show "${AUTH[@]}" -d "{\"args\": [\"${PAYLOAD}\"]}" "${URL}/v1/functions/temp_f/call"

step "6. Pin the version: a call with a stale RSF-Function-Sha256 fails with 409"
show "${AUTH[@]}" -H 'RSF-Function-Sha256: 0000' -d '{"args": ["x"]}' \
  "${URL}/v1/functions/str_upper/call"

step "7. The check rejects a disallowed import: INVALID_SOURCE (feedback only, not the sandbox)"
upload bad_import <<'EOF'
import os
def handle():
    return os.listdir("/")
EOF

step "8. An exception in the function returns 422; the worker keeps running"
show "${AUTH[@]}" -d '{"args": [42]}' "${URL}/v1/functions/str_upper/call"

step "9. Get past the lint check, then try to reach the host from inside the sandbox"
upload escape <<'EOF'
import json

# 'os' is not in the allowlist, but it is reachable through json -> re -> enum -> sys.
os = json.decoder.re.enum.sys.modules["os"]

def attempt(f):
    try:
        return repr(f())[:100]
    except BaseException as e:
        return "BLOCKED: %s: %s" % (type(e).__name__, e)

def handle():
    return {
        "environ": attempt(lambda: dict(os.environ)),
        "list /": attempt(lambda: os.listdir("/")),
        "read /etc/passwd": attempt(lambda: open("/etc/passwd").read()),
        "write /lib": attempt(lambda: open("/lib/pwned.py", "w").write("x")),
        "spawn process": attempt(lambda: os.posix_spawn("/bin/sh", ["sh"], {})),
        "network": attempt(
            lambda: os.sys.modules["socket"].create_connection(("1.1.1.1", 80), 1)
            if "socket" in os.sys.modules
            else __import__("socket").create_connection(("1.1.1.1", 80), 1)
        ),
    }
EOF
show "${AUTH[@]}" -d '{"args": []}' "${URL}/v1/functions/escape/call"

step "10. An infinite loop is killed at the call timeout (504); the next call works"
upload spin <<'EOF'
def handle(n):
    if n < 0:
        while True:
            pass
    return n * 2
EOF
show "${AUTH[@]}" -d '{"args": [-1]}' "${URL}/v1/functions/spin/call"
show "${AUTH[@]}" -d '{"args": [21]}' "${URL}/v1/functions/spin/call"

step "11. Memory is capped per worker"
upload hog <<'EOF'
def handle():
    return len(bytearray(512 * 1024 * 1024))
EOF
show "${AUTH[@]}" -d '{"args": []}' "${URL}/v1/functions/hog/call"

step "12. A call to an unknown function returns FUNCTION_NOT_FOUND"
show "${AUTH[@]}" -d '{"args": []}' "${URL}/v1/functions/nope/call"

step "13. List, inspect and delete"
show "${AUTH[@]}" "${URL}/v1/functions"
show "${AUTH[@]}" "${URL}/v1/functions/str_upper"
for f in str_upper temp_f escape spin hog; do
  show "${AUTH[@]}" -X DELETE "${URL}/v1/functions/${f}"
done
