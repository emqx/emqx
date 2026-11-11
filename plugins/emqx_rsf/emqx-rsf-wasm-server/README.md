# emqx-rsf-wasm-server (draft)

`emqx-rsf-wasm-server` is a function server for the emqx-rsf/1 protocol
(`../PROTOCOL.md`). It runs user-defined Python functions for EMQX rule SQL. It
runs outside EMQX, as a sidecar or as a service behind a load balancer.

Each function runs in CPython compiled to WebAssembly (`wasm32-wasip1`), inside
Wasmtime. The WebAssembly sandbox is the security boundary.

## Build and run

```
docker build -t emqx-rsf-wasm-server .
docker run -d --name emqx-rsf -p 127.0.0.1:18480:8080 \
  -e RSF_TOKEN="$(openssl rand -hex 32)" \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 256 --memory 1g \
  emqx-rsf-wasm-server
```

The image builds for `amd64` and `arm64` (`docker buildx build --platform ...`).
It pins Wasmtime and the CPython WASI build by version and SHA-256.

`./demo.sh` runs every protocol call with `curl` and `jq` against `RSF_URL`
(default `http://127.0.0.1:18480`) and `RSF_TOKEN` (default `demo-token`).

## Protocol support

The server implements all of emqx-rsf/1 for the `python3` runtime
(`PROTOCOL.md`, section 7.1). These details are specific to this server:

- Each function has its own pool of `RSF_POOL_SIZE` worker processes. A worker
  serves many calls, one at a time. Module-level state stays alive between calls
  to the same worker. Do not rely on it: the server replaces workers after a
  failure and after `RSF_MAX_CALLS_PER_WORKER` calls.
- `print()` in a function writes to the worker's stderr. The server keeps the
  last 20 lines and returns them in the `stderr` field of `TIMEOUT` and
  `WORKER_FAILED` errors.
- `INVALID_SOURCE` errors have a `problems` field. The server rejects a source
  with a syntax error, without a top-level `handle`, with an import that is not
  in `ALLOWED_IMPORTS` (`server.py`), or that fails or times out while it loads.
  The import check gives fast feedback to authors. It is not a security control.
- Uploads run one at a time.
- The server keeps functions in memory only. After a restart, calls return
  `FUNCTION_NOT_FOUND`, and the client uploads the functions again
  (`PROTOCOL.md`, section 9).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `RSF_TOKEN` | (required) | Bearer token. The server does not start without it. |
| `RSF_INSECURE_NO_AUTH` | unset | Set to `1` to run without a token. Local tests only. |
| `RSF_LISTEN` | `0.0.0.0:8080` | Listen address. |
| `RSF_POOL_SIZE` | `2` | Worker processes per function. |
| `RSF_CALL_TIMEOUT_MS` | `1000` | Wall-clock limit for one call. |
| `RSF_LOAD_TIMEOUT_MS` | `10000` | Wall-clock limit for loading a function. |
| `RSF_MAX_MEMORY_MB` | `128` | WebAssembly linear memory limit per worker. CPython needs at least 40. |
| `RSF_MAX_SOURCE_BYTES` | `65536` | Largest source. |
| `RSF_MAX_REQUEST_BYTES` | `1048576` | Largest call body. |
| `RSF_MAX_RESPONSE_BYTES` | `1048576` | Largest reply from a worker. |
| `RSF_MAX_FUNCTIONS` | `64` | Largest number of functions. |
| `RSF_MAX_CALLS_PER_WORKER` | `10000` | Calls before a worker is replaced. |

## Security model

**Boundary.** Each worker is a `wasmtime run` process. The guest gets:

- no environment variables (`-S inherit-env=n`, and the process starts with an empty environment);
- no network (`-S inherit-network=n`, `tcp=n`, `udp=n`, `allow-ip-name-lookup=n`);
  the CPython WASI build also has no socket support;
- no processes: WASI has no `fork`, `exec` or `posix_spawn`;
- one preopened directory, the standard library, as `/lib`. In the image it is
  owned by root and the server runs as UID 10001, so the guest cannot change it;
- a linear-memory cap (`-W max-memory-size`).

The server enforces the call timeout. It kills the worker process when a call
runs too long or when a reply is too large.

**What the import check does not do.** A function can reach modules that are
not in the allowlist through other modules, for example
`json.decoder.re.enum.sys.modules["os"]`. Step 9 of `demo.sh` does this. The
guest then gets `os`, but every call to the host fails, because the sandbox
gives it nothing to reach.

**Isolation between functions.** Each function has its own processes. One
function cannot read the state, arguments or results of another function.

**Remaining risks:**

- A bug in Wasmtime could allow a guest to escape. Keep Wasmtime up to date.
  Run the container with a read-only root filesystem, no capabilities and
  `no-new-privileges`.
- After an escape, the attacker runs as UID 10001 in the container. They can
  read `RSF_TOKEN` and the source and traffic of every function. Treat the
  container as untrusted. Do not mount secrets or a service-account token into it.
- In a Kubernetes sidecar, the container shares the pod's network namespace.
  After an escape, the attacker can reach EMQX ports on `localhost`. A
  separate Deployment with a NetworkPolicy gives stronger isolation.
- The server does not terminate TLS. Put it behind a TLS proxy, or keep it
  on a private network.
- One function can use all of its own workers, for example with slow calls.
  Then calls to it return `503 UNAVAILABLE`. Other functions are not affected,
  but all workers share the container's CPU.
