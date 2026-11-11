# emqx-rsf protocol, version 1 (draft)

This document specifies the HTTP protocol between EMQX and a function server.
A function server runs user-defined functions that EMQX rule SQL calls by name.
Any language can implement a function server or a client from this document.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted
as described in RFC 2119.

## 1. Terms

- **Client**: the program that uploads and calls functions. EMQX is a client.
- **Server**: the program that stores and runs functions.
- **Function**: a named unit of user code. It has a name, a runtime and a source.
- **Runtime**: the identifier of the language and calling convention that the
  server uses to run a source, for example `python3`. See section 7.
- **Version**: the SHA-256 digest that identifies one source of one function.
  See section 4.

## 2. Transport

- The server MUST accept HTTP/1.1. It MAY also accept HTTP/2.
- The server SHOULD support persistent connections. A client SHOULD keep
  connections open and send many requests on each connection.
- The server SHOULD accept TLS. When the server does not, the operator must put
  it behind a TLS proxy or on a private network.
- The client is configured with a base URL, for example
  `https://rsf.example.com/prefix`. All paths in this document are relative to
  the base URL.
- Every request body and every response body is a JSON object (RFC 8259),
  encoded in UTF-8, with `Content-Type: application/json`.
- A request with a body MUST have a `Content-Length` header. The server MAY
  reject a request without it with `411 LENGTH_REQUIRED`.

## 3. Authentication

- Every endpoint except `GET /healthz` MUST require
  `Authorization: Bearer <token>`.
- The server MUST compare the token in constant time.
- When the token is missing or wrong, the server MUST return
  `401 UNAUTHORIZED`.

## 4. Names and versions

A function name MUST match this regular expression:

```
^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}$
```

- A name has 1 to 64 characters from `A-Z`, `a-z`, `0-9`, `.`, `_` and `-`.
- A name MUST NOT start with `.`. This rule excludes `.` and `..`, so a server
  can use the name as a path segment or a file name.
- Names are case-sensitive.

The server MUST reject other names with `400 BAD_REQUEST`.

The version of a function is the lowercase hexadecimal SHA-256 digest of this
byte sequence:

```
UTF-8(runtime) || 0x0A || UTF-8(source)
```

The client and the server compute the version independently. A client therefore
knows the version of its own copy of a function without asking the server.

## 5. Endpoints

### 5.1 `GET /healthz`

Returns `200` with `{"status": "ok"}` when the server can accept requests. It
needs no authentication and MUST NOT return other data.

### 5.2 `GET /v1/info`

Returns the protocol version, the runtimes and the limits of the server.

```json
{
  "protocol": "emqx-rsf/1",
  "server": "emqx-rsf-wasm-server/0.1.0",
  "runtimes": [{"name": "python3", "version": "3.14.7"}],
  "limits": {
    "max_source_bytes": 65536,
    "max_request_bytes": 1048576,
    "max_response_bytes": 1048576,
    "max_functions": 64,
    "call_timeout_ms": 1000
  }
}
```

- `protocol` MUST be `emqx-rsf/1` for this version of the protocol.
- `limits` is for information. A client MUST still handle the errors in
  section 6.

### 5.3 `PUT /v1/functions/{name}`

Creates a function, or replaces its source.

Request:

```json
{"runtime": "python3", "source": "def handle(s):\n    return s.upper()\n"}
```

Response `200`:

```json
{"name": "str_upper", "runtime": "python3", "sha256": "<version>", "changed": true}
```

- The server MUST check and load the source before it returns `200`. After a
  `200`, a call to the function MUST NOT fail because of the source alone.
- When the check or the load fails, the server MUST return an error and MUST
  keep the previous version of the function, if there is one.
- When the server already has the same version, it MUST return `200` with
  `"changed": false` and change nothing. A client can therefore send the same
  `PUT` again without side effects.
- A call that arrives while the server replaces a function MUST run either the
  previous version or the new version.

### 5.4 `GET /v1/functions`

```json
{"functions": [{"name": "str_upper", "runtime": "python3", "sha256": "<version>"}]}
```

The server MAY add other fields to each item.

### 5.5 `GET /v1/functions/{name}`

Returns the same fields as one item of `GET /v1/functions`, and `source`.

### 5.6 `DELETE /v1/functions/{name}`

Deletes the function. Returns `200` with `{"deleted": "<name>"}`, or
`404 FUNCTION_NOT_FOUND`.

### 5.7 `POST /v1/functions/{name}/call`

Calls the function.

Request:

```json
{"args": ["hello"]}
```

Optional request header:

```
RSF-Function-Sha256: <version>
```

Response `200`:

```json
{"result": "HELLO", "sha256": "<version>"}
```

- `args` MUST be a JSON array. The server passes its items to the function, in
  order, as positional arguments.
- `result` is the JSON value that the function returns. It MAY be `null`.
- `sha256` is the version that ran the call.
- When the request has `RSF-Function-Sha256` and the server has a different
  version, the server MUST NOT run the call. It MUST return
  `409 SHA256_MISMATCH`.
- The server MUST accept concurrent calls, to the same function and to
  different functions. It MAY limit concurrency and return `503 UNAVAILABLE`.
- A call MUST NOT see data from a call to a different function.

## 6. Errors

An error response has a status code from the table below and this body:

```json
{"code": "FUNCTION_ERROR", "message": "ZeroDivisionError: division by zero"}
```

- `code` MUST be one of the codes in the table.
- `message` is text for people. A client MUST NOT parse it.
- The server MAY add other fields, for example `problems` (a list of strings)
  for `INVALID_SOURCE`, or `sha256` for `SHA256_MISMATCH`.

| Status | Code | Meaning | Client action |
|---|---|---|---|
| 400 | `BAD_REQUEST` | The request is malformed. | Fail. |
| 401 | `UNAUTHORIZED` | The token is missing or wrong. | Fail. |
| 404 | `NOT_FOUND` | The path does not exist. | Fail. |
| 404 | `FUNCTION_NOT_FOUND` | The server does not have the function. | Upload, then retry once. |
| 405 | `METHOD_NOT_ALLOWED` | The path does not accept the method. | Fail. |
| 409 | `SHA256_MISMATCH` | The server has a different version. | Upload, then retry once. |
| 409 | `TOO_MANY_FUNCTIONS` | The server has its maximum number of functions. | Fail. |
| 411 | `LENGTH_REQUIRED` | `Content-Length` is missing. | Fail. |
| 413 | `TOO_LARGE` | The body is larger than the server limit. | Fail. |
| 422 | `UNSUPPORTED_RUNTIME` | The server does not have the runtime. | Fail. |
| 422 | `INVALID_SOURCE` | The source failed the check or the load. | Fail. |
| 422 | `FUNCTION_ERROR` | The function raised an error, or its result is not JSON. | Fail. |
| 500 | `INTERNAL` | A server error. | Fail. |
| 502 | `WORKER_FAILED` | The process that ran the call failed. | Fail. |
| 503 | `UNAVAILABLE` | The server cannot run the call now. | Fail. The client MAY retry later. |
| 504 | `TIMEOUT` | The call ran longer than the server limit. | Fail. |

"Fail" means that the client reports the error for this call. A client MUST NOT
retry a call that failed with `FUNCTION_ERROR`, `WORKER_FAILED` or `TIMEOUT`:
the function can have caused the failure, and a retry doubles its cost.

When the server has no function under the name, it MUST return
`FUNCTION_NOT_FOUND`, not `NOT_FOUND`.

## 7. Runtimes

A runtime identifier names a language and a calling convention. The server
lists its runtimes in `GET /v1/info`. This version of the protocol defines one
runtime.

### 7.1 `python3`

- The source is a Python 3 module.
- The module MUST define a top-level function `handle`. The server calls
  `handle(*args)`.
- JSON values map to Python values as `json.loads` maps them. The return value
  MUST be a value that `json.dumps` accepts.
- The server MAY limit the modules that the source can import. It MUST report
  such a limit as `INVALID_SOURCE` at upload.

New runtimes, for example `javascript`, get their own section in a later
version of this document. A server MAY offer other runtimes. It SHOULD name them
with a prefix, for example `x-lua`.

## 8. Arguments and results from EMQX

Rule SQL calls a function through one SQL function, `rsf`. Its first argument is
the function name. EMQX sends the other arguments in `args`:

```sql
SELECT rsf('temp-convert.py', payload) AS t FROM "sensors/#"
```

EMQX evaluates the SQL arguments, encodes them as JSON and sends them in `args`.

- An EMQX binary becomes a JSON string. A binary that is not valid UTF-8 cannot
  be sent: EMQX fails the call. Encode such data first, for example with
  `base64_encode(payload)`.
- An EMQX map becomes a JSON object. A list becomes a JSON array.
- `undefined` becomes `null`.

EMQX decodes `result` and returns it to the rule. A JSON object becomes a map
with binary keys.

## 9. Client behavior

A client SHOULD do these things:

- Upload a function when it is created or changed. Keep the source: the
  server does not need to keep it.
- Send `RSF-Function-Sha256` on every call. Behind a load balancer, this makes
  sure that a call never runs an old version on one instance of the server.
- On `FUNCTION_NOT_FOUND` or `SHA256_MISMATCH`, upload the function again and
  retry the call once. A server that restarts loses no data this way, so the
  server does not need persistent storage.
- Set a request timeout a little longer than the server call timeout.

## 10. Security

- A server runs code that the client sends. The server MUST isolate each
  function from the host: no file system, network, processes or environment
  that the function does not need.
- A server MUST isolate functions from each other. One function MUST NOT read
  or change the state, arguments or results of another function.
- A server SHOULD limit the CPU time, memory and output size of each call.
- Arguments carry message data. Use TLS between the client and the server.
- The token gives the right to run code on the server. Store it as a secret.
