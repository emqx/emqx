# emqx_rule_udf (beta)

`emqx_rule_udf` (user-defined functions for rules) is an EMQX plugin. It stores
user programs in EMQX and runs them on an external function server. Rule SQL
calls them with one SQL function:

```sql
SELECT udf('temp-convert.py', payload) AS t FROM "sensors/#"
```

EMQX runs no user code. The function server runs the programs and must isolate
them (`PROTOCOL.md`, section 10). EMQX and the server talk the emqx-rule-udf/1
protocol over HTTP. `PROTOCOL.md` specifies it, so a server can be written in any
language.

The reference server is the image `ghcr.io/emqx/emqx-rule-udf-wasm-server:0.2.0`
(source: [emqx/docker-images](https://github.com/emqx/docker-images), directory
`emqx-rule-udf-wasm-server`). It runs each `python3` program in CPython on
WebAssembly, with no file, network, process or environment access, and with a
time limit and a memory limit.

## Run the function server

```
docker run -d --name emqx-rule-udf-server -p 127.0.0.1:8080:8080 \
  -e UDF_TOKEN="$(openssl rand -hex 32)" \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 256 --memory 1g \
  ghcr.io/emqx/emqx-rule-udf-wasm-server:0.2.0
```

Run it as a sidecar of each EMQX node, or as a service behind a load balancer.
The instances behind a load balancer need no shared storage. The plugin needs
server 0.2.0 or later.
It keeps no state that EMQX needs (see "Programs" below).

## Configure

Set the function server in the plugin config:

```json
{"server": {"url": "http://127.0.0.1:8080", "token": "<UDF_TOKEN>", "cacertfile": "",
            "pool_size": 8, "connect_timeout_ms": 5000, "request_timeout_ms": 2000}}
```

- For an `https://` URL, the plugin verifies the server certificate and the host
  name. `cacertfile` is the path to a PEM file with the CA certificates to
  trust. The file must exist on every node. When it is empty, the plugin trusts
  the CAs in `node.trusted_certs`, or the CAs of the operating system when that
  is not set.
- EMQX sends a config change to all running nodes. A node that is down at that
  time keeps its old config: save the config again after it is back.

## Programs

- A program has a name, a runtime and a source. The function server lists the
  runtimes it offers in `GET /v1/info`, and the plugin does not depend on any
  one of them. The reference server offers `python3`: such a program defines
  `handle(*args)`.
- A name has 1 to 64 characters from `A-Z a-z 0-9 . _ -` and must not start with
  `.`. The name is the first argument of `udf(...)`.
- Programs are global: they do not belong to a namespace. Only the global
  administrator can create, change, delete or test-run them. Namespaced users
  can read them, and their rules can call them with `udf(...)`.
- EMQX stores the programs in a replicated `mria` table, so every node has all of
  them. Each node also writes them as files to
  `<data_dir>/plugins/emqx_rule_udf/programs/<name>`. The files are a copy for
  operators. EMQX reads the table, not the files.
- The function server keeps nothing that EMQX needs. When it restarts, or when a
  call reaches an instance that does not have the program, the server replies
  `FUNCTION_NOT_FOUND`. EMQX then repeats the call once with the program from
  its table in the request, and the server loads it and runs the call.

## Backup and restore

The cluster data backup (`emqx ctl data export` and the Dashboard backup) does
not include plugin tables. It does not save the programs or this plugin's
config. Back them up separately:

- Config: `GET /api/v5/plugins/emqx_rule_udf-<vsn>/config`. Restore with `PUT` to
  the same path.
- Programs: list with `GET programs` and fetch each one with `GET programs/{name}`
  (see "Web UI and API"). Restore each with `PUT programs/{name}`. The files in
  `<data_dir>/plugins/emqx_rule_udf/programs/` hold the sources too, but not the
  runtimes.

## Web UI and API

The Dashboard shows the editor page of the plugin. It is a plain text area with a
program list, Save, Delete, and a test run with JSON arguments. The page uses the
plugin API under `/api/v5/plugin_api/emqx_rule_udf/`:

| Method and path | Purpose |
|---|---|
| `GET ui` | The editor page. |
| `GET server` | State of the function server (`GET /v1/info`). |
| `GET programs` | All programs, without the source. |
| `GET programs/{name}` | One program, with the source. |
| `PUT programs/{name}` | Create or replace a program. Body: `{"runtime": "python3", "source": "..."}`. `runtime` is required. |
| `DELETE programs/{name}` | Delete a program. |
| `POST programs/{name}/call` | Run the saved program once. Body: `{"args": [...]}`. |

- `PUT` uploads the source to the function server first. EMQX saves the program
  only when the server accepts it. An `INVALID_SOURCE` reply comes back with its
  `problems` list.
- `PUT`, `DELETE` and `POST` need the global administrator. Viewers can only read.
