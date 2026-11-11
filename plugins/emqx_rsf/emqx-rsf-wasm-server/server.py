"""emqx-rsf-wasm-server: an emqx-rsf/1 function server with a WebAssembly sandbox.

It implements the protocol in `apps/emqx_rsf/PROTOCOL.md` for the `python3`
runtime. Each function is a Python snippet that defines `handle(*args)`. The server runs
each function in its own pool of `wasmtime` processes. Each process runs CPython
compiled to wasm32-wasip1. The guest gets no environment variables, no network,
no processes and only a read-only view of the Python standard library.

The import allowlist check (`lint`) gives fast feedback to snippet authors.
It is not a security control. The WebAssembly sandbox is the security boundary.

Standard library only. Needs Python 3.11 or newer on the host.
"""

import ast
import collections
import hashlib
import hmac
import json
import logging
import os
import queue
import re
import resource
import selectors
import signal
import subprocess
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = logging.getLogger("emqx-rsf-wasm-server")

SERVER_NAME = "emqx-rsf-wasm-server"
SERVER_VERSION = "0.1.0"
PROTOCOL = "emqx-rsf/1"
RUNTIME = "python3"

NAME_RE = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}\Z")

# Modules a snippet may import. This list gives authors early feedback only.
# The sandbox does not depend on it: a guest that reaches any other module
# still has no files, network, processes or environment.
ALLOWED_IMPORTS = frozenset(
    {
        "base64",
        "binascii",
        "bisect",
        "calendar",
        "cmath",
        "collections",
        "copy",
        "dataclasses",
        "datetime",
        "decimal",
        "enum",
        "fractions",
        "functools",
        "hashlib",
        "heapq",
        "hmac",
        "itertools",
        "json",
        "math",
        "operator",
        "random",
        "re",
        "statistics",
        "string",
        "struct",
        "textwrap",
        "time",
        "typing",
        "unicodedata",
        "uuid",
        "zlib",
    }
)


def _env_int(name, default):
    return int(os.environ.get(name, default))


class Config:
    def __init__(self):
        self.listen = os.environ.get("RSF_LISTEN", "0.0.0.0:8080")
        self.token = os.environ.get("RSF_TOKEN", "")
        self.insecure_no_auth = os.environ.get("RSF_INSECURE_NO_AUTH") == "1"
        self.wasmtime = os.environ.get("RSF_WASMTIME", "/opt/rsf/bin/wasmtime")
        self.python_wasm = os.environ.get(
            "RSF_PYTHON_WASM", "/opt/rsf/python/python.wasm"
        )
        self.python_lib = os.environ.get("RSF_PYTHON_LIB", "/opt/rsf/python/lib")
        self.python_cwasm_prebuilt = os.environ.get(
            "RSF_PYTHON_CWASM", "/opt/rsf/python/python.cwasm"
        )
        self.cache_dir = os.environ.get("RSF_CACHE_DIR", "/tmp/rsf-cache")
        self.pool_size = _env_int("RSF_POOL_SIZE", 2)
        self.call_timeout = _env_int("RSF_CALL_TIMEOUT_MS", 1000) / 1000
        self.load_timeout = _env_int("RSF_LOAD_TIMEOUT_MS", 10000) / 1000
        self.max_memory = _env_int("RSF_MAX_MEMORY_MB", 128) * 1024 * 1024
        self.max_source = _env_int("RSF_MAX_SOURCE_BYTES", 64 * 1024)
        self.max_request = _env_int("RSF_MAX_REQUEST_BYTES", 1024 * 1024)
        self.max_response = _env_int("RSF_MAX_RESPONSE_BYTES", 1024 * 1024)
        self.max_functions = _env_int("RSF_MAX_FUNCTIONS", 64)
        self.max_calls_per_worker = _env_int("RSF_MAX_CALLS_PER_WORKER", 10000)
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "guest_runner.py"), encoding="utf-8") as f:
            self.runner_source = f.read()
        self.python_version = os.environ.get("RSF_PYTHON_VERSION", "")
        self.python_cwasm = None


CONF = None


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ApiError(Exception):
    def __init__(self, status, code, message, extra=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra or {}


def version(runtime, source):
    """Return the emqx-rsf/1 version of a function: see PROTOCOL.md, section 4."""
    data = runtime.encode("utf-8") + b"\n" + source.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


class WorkerFailure(Exception):
    """The worker process is no longer usable. The caller must kill it."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Lint
# ---------------------------------------------------------------------------


def lint(source):
    """Return a list of problems. An empty list means the snippet passed."""
    try:
        tree = ast.parse(source, filename="<snippet>")
    except SyntaxError as exc:
        return ["line %s: syntax error: %s" % (exc.lineno, exc.msg)]
    problems = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                problems.append("line %d: relative import" % node.lineno)
                continue
            mods = [node.module or ""]
        else:
            continue
        for mod in mods:
            if mod.split(".")[0] not in ALLOWED_IMPORTS:
                problems.append(
                    "line %d: import of '%s' is not allowed" % (node.lineno, mod)
                )
    has_handle = any(
        isinstance(node, ast.FunctionDef) and node.name == "handle"
        for node in tree.body
    )
    if not has_handle:
        problems.append("snippet must define a top-level function 'handle(*args)'")
    return problems


# ---------------------------------------------------------------------------
# Worker: one wasmtime process that runs one function
# ---------------------------------------------------------------------------


def wasmtime_argv():
    return [
        CONF.wasmtime,
        "run",
        "--allow-precompiled",
        "-C", "cache=n",
        "-S", "inherit-env=n",
        "-S", "inherit-network=n",
        "-S", "tcp=n",
        "-S", "udp=n",
        "-S", "allow-ip-name-lookup=n",
        "-W", "max-memory-size=%d" % CONF.max_memory,
        "--dir", "%s::/lib" % CONF.python_lib,
        CONF.python_cwasm,
        "-c", CONF.runner_source,
    ]  # fmt: skip


class Worker:
    STDERR_TAIL = 20

    def __init__(self, name, source):
        self.name = name
        self.calls = 0
        self.next_id = 0
        self.buf = b""
        self.stderr_tail = collections.deque(maxlen=self.STDERR_TAIL)
        self.proc = subprocess.Popen(
            wasmtime_argv(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={},
            cwd="/",
            close_fds=True,
            start_new_session=True,
        )
        try:
            resource.prlimit(self.proc.pid, resource.RLIMIT_CORE, (0, 0))
            resource.prlimit(self.proc.pid, resource.RLIMIT_NOFILE, (64, 64))
        except (OSError, ValueError):
            pass
        os.set_blocking(self.proc.stdout.fileno(), False)
        self.stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self.stderr_thread.start()
        try:
            self._write({"name": name, "source": source})
            reply = self._read_reply(time.monotonic() + CONF.load_timeout)
        except WorkerFailure as exc:
            self.kill()
            if exc.code == "TIMEOUT":
                status, code = HTTPStatus.UNPROCESSABLE_ENTITY, "INVALID_SOURCE"
                message = "the snippet did not finish loading in time"
            else:
                status, code = HTTPStatus.BAD_GATEWAY, "WORKER_FAILED"
                message = "worker failed to start: %s" % exc.message
            raise ApiError(status, code, message, {"stderr": self.stderr_text()})
        if "load_error" in reply:
            self.kill()
            raise ApiError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "INVALID_SOURCE",
                reply["load_error"],
                {"problems": [reply["load_error"]]},
            )
        if reply.get("ready") is not True:
            self.kill()
            raise ApiError(
                HTTPStatus.BAD_GATEWAY,
                "WORKER_FAILED",
                "unexpected first reply from guest",
                {"stderr": self.stderr_text()},
            )

    def _drain_stderr(self):
        """Keep the last lines of guest stderr, with bounded memory."""
        partial = b""
        fd = self.proc.stderr.fileno()
        while True:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                return
            if not chunk:
                return
            partial += chunk
            *lines, partial = partial.split(b"\n")
            for line in lines:
                self.stderr_tail.append(line[:500].decode("utf-8", "replace"))
            partial = partial[-500:]

    def stderr_text(self):
        return "\n".join(self.stderr_tail)

    def _write(self, obj):
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8") + b"\n"
        try:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise WorkerFailure("WORKER_DIED", "guest stdin closed: %s" % exc)

    def _read_reply(self, deadline):
        fd = self.proc.stdout.fileno()
        with selectors.DefaultSelector() as sel:
            sel.register(fd, selectors.EVENT_READ)
            while b"\n" not in self.buf:
                if len(self.buf) > CONF.max_response:
                    raise WorkerFailure("RESPONSE_TOO_LARGE", "guest reply too large")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise WorkerFailure("TIMEOUT", "function did not reply in time")
                if not sel.select(remaining):
                    continue
                chunk = os.read(fd, 65536)
                if not chunk:
                    self.proc.wait(timeout=1)
                    raise WorkerFailure(
                        "WORKER_DIED",
                        "guest exited with status %s" % self.proc.returncode,
                    )
                self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        if len(line) > CONF.max_response:
            raise WorkerFailure("RESPONSE_TOO_LARGE", "guest reply too large")
        try:
            return json.loads(line)
        except ValueError:
            raise WorkerFailure("PROTOCOL_ERROR", "guest wrote a non-JSON line")

    def call(self, args):
        self.calls += 1
        self.next_id += 1
        req_id = self.next_id
        self._write({"id": req_id, "args": args})
        reply = self._read_reply(time.monotonic() + CONF.call_timeout)
        if reply.get("id") != req_id:
            raise WorkerFailure("PROTOCOL_ERROR", "guest reply has a wrong id")
        return reply

    def alive(self):
        return self.proc.poll() is None

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
        try:
            self.proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
        for pipe in (self.proc.stdin, self.proc.stdout):
            try:
                pipe.close()
            except OSError:
                pass
        self.stderr_thread.join(timeout=1)


# ---------------------------------------------------------------------------
# Function: a pool of workers for one snippet version
# ---------------------------------------------------------------------------


class Function:
    def __init__(self, name, runtime, source):
        self.name = name
        self.runtime = runtime
        self.source = source
        self.sha256 = version(runtime, source)
        self.created_at = time.time()
        self.closed = False
        self.slots = queue.Queue()
        # Start one worker first: a load error then fails the upload fast.
        workers = [Worker(name, source)]
        try:
            for _ in range(CONF.pool_size - 1):
                workers.append(Worker(name, source))
        except BaseException:
            for w in workers:
                w.kill()
            raise
        for w in workers:
            self.slots.put(w)

    def info(self):
        return {
            "name": self.name,
            "runtime": self.runtime,
            "sha256": self.sha256,
            "pool_size": CONF.pool_size,
            "created_at": self.created_at,
        }

    def _acquire(self):
        try:
            worker = self.slots.get(timeout=CONF.call_timeout)
        except queue.Empty:
            raise ApiError(
                HTTPStatus.SERVICE_UNAVAILABLE, "UNAVAILABLE", "all workers are busy"
            )
        if self.closed:
            self._release(worker)
            raise ApiError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "UNAVAILABLE",
                "the function was replaced or deleted, retry the call",
            )
        if worker is not None and (
            not worker.alive() or worker.calls >= CONF.max_calls_per_worker
        ):
            worker.kill()
            worker = None
        if worker is None:
            try:
                worker = Worker(self.name, self.source)
            except BaseException:
                self.slots.put(None)
                raise
        return worker

    def _release(self, worker):
        if self.closed and worker is not None:
            worker.kill()
            worker = None
        self.slots.put(worker)

    def call(self, args):
        worker = self._acquire()
        try:
            reply = worker.call(args)
        except WorkerFailure as exc:
            worker.kill()
            stderr = worker.stderr_text()
            self._release(None)
            LOG.warning("function=%s worker failure: %s", self.name, exc.code)
            if exc.code == "TIMEOUT":
                status, code = HTTPStatus.GATEWAY_TIMEOUT, "TIMEOUT"
            else:
                status, code = HTTPStatus.BAD_GATEWAY, "WORKER_FAILED"
            raise ApiError(status, code, exc.message, {"stderr": stderr})
        except BaseException:
            worker.kill()
            self._release(None)
            raise
        self._release(worker)
        return reply

    def close(self):
        self.closed = True
        while True:
            try:
                worker = self.slots.get_nowait()
            except queue.Empty:
                return
            if worker is not None:
                worker.kill()


class Registry:
    def __init__(self):
        self.lock = threading.Lock()
        # Uploads run one at a time. A burst of identical uploads, for example
        # from many clients after a server restart, then starts one pool.
        self.put_lock = threading.Lock()
        self.functions = {}

    def get(self, name):
        with self.lock:
            fn = self.functions.get(name)
        if fn is None:
            raise ApiError(
                HTTPStatus.NOT_FOUND, "FUNCTION_NOT_FOUND", "no such function"
            )
        return fn

    def list(self):
        with self.lock:
            return [fn.info() for fn in self.functions.values()]

    def put(self, name, runtime, source):
        with self.put_lock:
            return self._put(name, runtime, source)

    def _put(self, name, runtime, source):
        with self.lock:
            old = self.functions.get(name)
            if old is None and len(self.functions) >= CONF.max_functions:
                raise ApiError(
                    HTTPStatus.CONFLICT, "TOO_MANY_FUNCTIONS", "too many functions"
                )
        sha = version(runtime, source)
        if old is not None and old.sha256 == sha:
            return old, False
        # Start the new pool outside the lock. The old version keeps serving
        # calls until the new version is ready.
        new = Function(name, runtime, source)
        with self.lock:
            old = self.functions.get(name)
            if old is None and len(self.functions) >= CONF.max_functions:
                new.close()
                raise ApiError(
                    HTTPStatus.CONFLICT, "TOO_MANY_FUNCTIONS", "too many functions"
                )
            self.functions[name] = new
        if old is not None:
            old.close()
        return new, True

    def delete(self, name):
        with self.lock:
            fn = self.functions.pop(name, None)
        if fn is None:
            raise ApiError(
                HTTPStatus.NOT_FOUND, "FUNCTION_NOT_FOUND", "no such function"
            )
        fn.close()

    def close_all(self):
        with self.lock:
            fns = list(self.functions.values())
            self.functions.clear()
        for fn in fns:
            fn.close()


REGISTRY = Registry()


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    server_version = "%s/%s" % (SERVER_NAME, SERVER_VERSION)
    sys_version = ""

    def log_message(self, fmt, *args):
        LOG.info("%s %s", self.address_string(), fmt % args)

    # -- helpers ------------------------------------------------------------

    def _send_json(self, status, obj):
        body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self, limit):
        length = self.headers.get("Content-Length")
        if length is None:
            raise ApiError(
                HTTPStatus.LENGTH_REQUIRED, "LENGTH_REQUIRED", "Content-Length required"
            )
        try:
            length = int(length)
        except ValueError:
            raise ApiError(HTTPStatus.BAD_REQUEST, "BAD_REQUEST", "bad Content-Length")
        if length < 0 or length > limit:
            # Do not read the body. Close the connection instead.
            self.close_connection = True
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "TOO_LARGE",
                "body is larger than %d bytes" % limit,
            )
        self._body_read = True
        return self.rfile.read(length)

    def _read_json(self, limit):
        raw = self._read_body(limit)
        try:
            body = json.loads(raw)
        except ValueError:
            raise ApiError(HTTPStatus.BAD_REQUEST, "BAD_REQUEST", "body is not valid JSON")
        if not isinstance(body, dict):
            raise ApiError(HTTPStatus.BAD_REQUEST, "BAD_REQUEST", "body must be an object")
        return body

    def _check_auth(self):
        if CONF.insecure_no_auth:
            return
        header = self.headers.get("Authorization", "")
        expected = "Bearer " + CONF.token
        if not hmac.compare_digest(header.encode(), expected.encode()):
            raise ApiError(HTTPStatus.UNAUTHORIZED, "UNAUTHORIZED", "bad token")

    def _route(self, method):
        path = self.path.split("?", 1)[0]
        if path == "/healthz" and method == "GET":
            return self._healthz()
        self._check_auth()
        parts = path.strip("/").split("/")
        if parts == ["v1", "info"]:
            self._allow(method, "GET")
            return self._send_json(HTTPStatus.OK, self._info())
        if parts[:2] != ["v1", "functions"] or len(parts) > 4:
            raise ApiError(HTTPStatus.NOT_FOUND, "NOT_FOUND", "no such endpoint")
        rest = parts[2:]
        if not rest:
            self._allow(method, "GET")
            return self._send_json(HTTPStatus.OK, {"functions": REGISTRY.list()})
        if len(rest) == 2 and rest[1] != "call":
            raise ApiError(HTTPStatus.NOT_FOUND, "NOT_FOUND", "no such endpoint")
        name = rest[0]
        if not NAME_RE.match(name):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "BAD_REQUEST",
                "name must match %s" % NAME_RE.pattern,
            )
        if len(rest) == 2:
            self._allow(method, "POST")
            return self._call(name)
        self._allow(method, "GET", "PUT", "DELETE")
        if method == "PUT":
            return self._put(name)
        if method == "GET":
            fn = REGISTRY.get(name)
            return self._send_json(HTTPStatus.OK, dict(fn.info(), source=fn.source))
        REGISTRY.delete(name)
        return self._send_json(HTTPStatus.OK, {"deleted": name})

    @staticmethod
    def _allow(method, *allowed):
        if method not in allowed:
            raise ApiError(
                HTTPStatus.METHOD_NOT_ALLOWED,
                "METHOD_NOT_ALLOWED",
                "allowed methods: %s" % ", ".join(allowed),
            )

    # -- endpoints ----------------------------------------------------------

    def _healthz(self):
        self._send_json(HTTPStatus.OK, {"status": "ok"})

    def _info(self):
        return {
            "protocol": PROTOCOL,
            "server": "%s/%s" % (SERVER_NAME, SERVER_VERSION),
            "runtimes": [{"name": RUNTIME, "version": CONF.python_version}],
            "limits": {
                "max_source_bytes": CONF.max_source,
                "max_request_bytes": CONF.max_request,
                "max_response_bytes": CONF.max_response,
                "max_functions": CONF.max_functions,
                "call_timeout_ms": int(CONF.call_timeout * 1000),
            },
        }

    def _put(self, name):
        # The JSON encoding of a source is longer than the source itself.
        body = self._read_json(CONF.max_source * 2 + 1024)
        runtime = body.get("runtime")
        source = body.get("source")
        if not isinstance(runtime, str) or not isinstance(source, str):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "BAD_REQUEST",
                "'runtime' and 'source' must be strings",
            )
        if runtime != RUNTIME:
            raise ApiError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "UNSUPPORTED_RUNTIME",
                "supported runtimes: %s" % RUNTIME,
            )
        if len(source.encode("utf-8")) > CONF.max_source:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "TOO_LARGE",
                "source is larger than %d bytes" % CONF.max_source,
            )
        problems = lint(source)
        if problems:
            raise ApiError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "INVALID_SOURCE",
                "the snippet failed the check",
                {"problems": problems},
            )
        fn, changed = REGISTRY.put(name, runtime, source)
        LOG.info("function=%s sha256=%s changed=%s", name, fn.sha256, changed)
        self._send_json(HTTPStatus.OK, dict(fn.info(), changed=changed))

    def _call(self, name):
        body = self._read_json(CONF.max_request)
        args = body.get("args")
        if not isinstance(args, list):
            raise ApiError(HTTPStatus.BAD_REQUEST, "BAD_REQUEST", "'args' must be a list")
        fn = REGISTRY.get(name)
        want = self.headers.get("RSF-Function-Sha256")
        if want is not None and want != fn.sha256:
            raise ApiError(
                HTTPStatus.CONFLICT,
                "SHA256_MISMATCH",
                "this server has a different version of the function",
                {"sha256": fn.sha256},
            )
        reply = fn.call(args)
        if "error" in reply:
            raise ApiError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "FUNCTION_ERROR",
                reply["error"],
                {"sha256": fn.sha256},
            )
        self._send_json(HTTPStatus.OK, {"result": reply.get("result"), "sha256": fn.sha256})

    # -- dispatch -----------------------------------------------------------

    def _dispatch(self, method):
        self._body_read = False
        try:
            self._route(method)
        except ApiError as exc:
            self._close_if_body_unread()
            self._send_json(
                exc.status, dict(exc.extra, code=exc.code, message=exc.message)
            )
        except Exception:  # noqa: BLE001
            LOG.exception("unhandled error")
            self._close_if_body_unread()
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"code": "INTERNAL", "message": "internal error"},
            )

    def _close_if_body_unread(self):
        """An unread body would be parsed as the next request on this
        connection. Close the connection after the response instead."""
        if not self._body_read and self.headers.get("Content-Length", "0") != "0":
            self.close_connection = True

    def do_GET(self):
        self._dispatch("GET")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_POST(self):
        self._dispatch("POST")

    def do_DELETE(self):
        self._dispatch("DELETE")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


def prepare_python_cwasm():
    """Return the ahead-of-time compiled python.wasm. Compile it when the image
    does not include one."""
    if os.path.exists(CONF.python_cwasm_prebuilt):
        return CONF.python_cwasm_prebuilt
    with open(CONF.python_wasm, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()[:16]
    os.makedirs(CONF.cache_dir, mode=0o700, exist_ok=True)
    out = os.path.join(CONF.cache_dir, "python-%s.cwasm" % digest)
    if not os.path.exists(out):
        LOG.info("compiling %s to %s", CONF.python_wasm, out)
        tmp = out + ".tmp"
        subprocess.run(
            [CONF.wasmtime, "compile", CONF.python_wasm, "-o", tmp],
            check=True,
            env={},
        )
        os.replace(tmp, out)
    return out


def _on_sigterm(_signum, _frame):
    raise KeyboardInterrupt


def main():
    global CONF
    signal.signal(signal.SIGTERM, _on_sigterm)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    CONF = Config()
    if not CONF.token and not CONF.insecure_no_auth:
        LOG.error("RSF_TOKEN is not set. Set it, or set RSF_INSECURE_NO_AUTH=1.")
        sys.exit(2)
    CONF.python_cwasm = prepare_python_cwasm()
    host, port = CONF.listen.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port)), Handler)
    server.daemon_threads = True
    LOG.info("listening on %s", CONF.listen)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        REGISTRY.close_all()


if __name__ == "__main__":
    main()
