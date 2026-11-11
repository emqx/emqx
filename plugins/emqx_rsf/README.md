# emqx_rsf (draft)

`emqx_rsf` ("rsf" = rule SQL function) is an EMQX plugin. It stores user programs
in EMQX and runs them on an external function server, which rule SQL calls with
one SQL function:

```sql
SELECT rsf('temp-convert.py', payload) AS t FROM "sensors/#"
```

EMQX runs no user code. The function server runs the programs and must isolate
them (`PROTOCOL.md`, section 10). EMQX and the server talk the emqx-rsf/1
protocol over HTTP. `PROTOCOL.md` specifies it, so a server can be written in any
language. `emqx-rsf-wasm-server/` is a reference server that runs Python programs
in a WebAssembly sandbox.

## Build and install

```
make plugin-emqx_rsf                              # _build/plugins/emqx_rsf-0.1.0.tar.gz
cp _build/plugins/emqx_rsf-0.1.0.tar.gz <emqx>/plugins/
emqx ctl plugins allow emqx_rsf-0.1.0
emqx ctl plugins install emqx_rsf-0.1.0
emqx ctl plugins start emqx_rsf-0.1.0
```

Then set the function server in the plugin config (Dashboard, or
`PUT /api/v5/plugins/emqx_rsf-0.1.0/config`):

```json
{"server": {"url": "http://127.0.0.1:8080", "token": "...", "pool_size": 8,
            "connect_timeout_ms": 5000, "request_timeout_ms": 2000}}
```

## Programs

- A program has a name, a runtime (`python3`) and a source. A `python3` program
  defines `handle(*args)`.
- A name has 1 to 64 characters from `A-Z a-z 0-9 . _ -` and must not start with
  `.`. The name is the first argument of `rsf(...)`.
- EMQX stores the programs in a replicated `mria` table, so every node has all of
  them. Each node also writes them as files to
  `<data_dir>/plugins/emqx_rsf/programs/<name>`. The files are a copy for
  operators. EMQX reads the table, not the files.
- The function server keeps nothing that EMQX needs. When it restarts, or when a
  call reaches an instance that does not have the program, the server replies
  `FUNCTION_NOT_FOUND`. EMQX then uploads the program from its table and
  retries the call once.

## Web UI and API

The Dashboard shows the editor page of the plugin. It is a plain text area with a
program list, Save, Delete, and a test run with JSON arguments. The page uses the
plugin API under `/api/v5/plugin_api/emqx_rsf/`:

| Method and path | Purpose |
|---|---|
| `GET ui` | The editor page. |
| `GET server` | State of the function server (`GET /v1/info`). |
| `GET programs` | All programs, without the source. |
| `GET programs/{name}` | One program, with the source. |
| `PUT programs/{name}` | Create or replace a program. Body: `{"runtime": "python3", "source": "..."}`. |
| `DELETE programs/{name}` | Delete a program. |
| `POST programs/{name}/call` | Run the saved program once. Body: `{"args": [...]}`. |

- `PUT` uploads the source to the function server first. EMQX saves the program
  only when the server accepts it. An `INVALID_SOURCE` reply comes back with its
  `problems` list.
- `PUT`, `DELETE` and `POST` need the global administrator. Viewers can only read.

## Modules

- `emqx_rsf_app`: plugin callbacks.
- `emqx_rsf`: the rule SQL function `rsf`.
- `emqx_rsf_client`: the HTTP client, the hot path and the uploads.
- `emqx_rsf_store`: the program table and the file copies.
- `emqx_rsf_api`: the plugin API.
