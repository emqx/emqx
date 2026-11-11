"""Guest-side runner. Runs inside CPython compiled to wasm32-wasip1.

The host passes this file's source to `python.wasm -c`, so the guest needs no
preopened directory other than the read-only standard library.

Protocol (NDJSON on stdin/stdout, one JSON object per line):

  host -> guest, first line:  {"name": <str>, "source": <str>}
  guest -> host:              {"ready": true} | {"load_error": <str>}
  host -> guest, then:        {"id": <int>, "args": [<json>, ...]}
  guest -> host:              {"id": <int>, "result": <json>} | {"id": <int>, "error": <str>}

The snippet must define a top-level function `handle(*args)`.
"""

import json
import sys
import traceback

# The protocol owns the real stdout. A `print()` in the snippet goes to stderr.
_proto_out = sys.stdout
sys.stdout = sys.stderr


def _send(obj):
    _proto_out.write(json.dumps(obj, separators=(",", ":")) + "\n")
    _proto_out.flush()


def _error_text(exc):
    return "".join(traceback.format_exception_only(type(exc), exc)).strip()


def _load():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    req = json.loads(line)
    name = req["name"]
    code = compile(req["source"], "<function:%s>" % name, "exec")
    namespace = {"__name__": "rsf_function_" + name}
    exec(code, namespace)
    handle = namespace.get("handle")
    if not callable(handle):
        raise TypeError("snippet must define a top-level function 'handle(*args)'")
    return handle


def _serve(handle):
    for line in sys.stdin:
        if not line.strip():
            continue
        req = json.loads(line)
        req_id = req["id"]
        try:
            result = handle(*req["args"])
            # Serialize here, so that a non-JSON result fails only this call.
            payload = json.dumps(
                {"id": req_id, "result": result}, separators=(",", ":")
            )
        except Exception as exc:  # noqa: BLE001
            _send({"id": req_id, "error": _error_text(exc)})
            continue
        _proto_out.write(payload + "\n")
        _proto_out.flush()


def _main():
    try:
        handle = _load()
    except Exception as exc:  # noqa: BLE001
        _send({"load_error": _error_text(exc)})
        return
    _send({"ready": True})
    _serve(handle)


_main()
