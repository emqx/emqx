# EMQX Rate limiting subsystem

## General concepts

_Limiter_ is an entity that models a token bucket. It allows the consumption of tokens from the bucket via a _client_ that is _connected_ to a specific limiter.

A limiter is identified by a global id consisting of a _group_ (arbitrary term) and a _limiter_ name (atom):
`{Group, Name}`, e.g. `{{zone, default}, messages}`.

Limiters may have different token generation algorithms and consumption logic. Currently, the following limiters are supported:
* Shared limiter — all clients connected to the same limiter share the same bucket and consume tokens from it cooperatively.
* Exclusive limiter — each client connected to the limiter receives its own bucket that is consumed exclusively by the client.

A limiter is configured with
* _rate_, e.g. `1000/10s` or `10MB/h`, — the rate of token generation.
* _burst_, e.g. `10000/h` — additional tokens granted more rarely to cope with burst traffic.

### Interface modules

The entry point for the rate-limiting functionality is the `emqx_limiter` module.

It provides the following functionality:
* General initialization of the rate-limiting subsystem.
* Direct creation/update/deletion of limiter groups.
* Connecting to a limiter from a group, i.e., obtaining a limiter _client_. The client may be used to check the quota where needed.
* Helper functions to create limiters for zones and listener configurations.

The client module is `emqx_limiter_client`. It allows the consumption of tokens from a limiter and reinsertion into it.

The convenience module `emqx_limiter_client_container` holds several named clients and allows the checking of several limiters with a single request.

`emqx_esockd_limiter` — a module implementing callbacks for esockd.

### Generic usage

First, one creates a limiter group:

```erlang
emqx_limiter:create_group(
       shared,
       {zone, default},
       [{messages, #{rate => {100, 1000}}}, {bytes, #{rate => {100000, 1000}}}]
).
```

Then, in a relevant moment (e.g, MQTT Client connection), one obtains a limiter client:

```erlang
Group = {zone, default},
Limiter = messages,
LimiterId = {Group, Limiter},
Client = emqx_limiter:connect(LimiterId).
```

This is a lightweight operation backed by a persistent term lookup.

Then, when needed, one consumes tokens from the client:

```erlang
case emqx_limiter_client:try_consume(Client0, 10) of
 {true, Client} ->
        %% proceed with the operation
 {false, Client, Reason} ->
        %% handle the quota exceeded case
end.
```

### EMQX integration

`emqx_limiter` is the only module that deals with EMQX configuration. It has callbacks to (re)create, (re)configure and delete limiter groups on
* zone changes
* listener configuration changes

The callbacks are invoked in the appropriate places of the EMQX app.

### Internal structure

To provide token consumption (`emqx_limiter_client:try_consume/2`), we employ three basic entities:
* Bucket settings
* Bucket state
* Client state

#### Bucket settings
Bucket settings contain the rate, burst, etc. They are cached in persistent terms managed by the `emqx_limiter_registry` module.

#### Bucket state

The bucket state contains the number of tokens, the last update time, etc.

#### Client state

The client state is owned by the client process.

For exclusive limiters, the bucket state is a part of the client state. The client itself refills the bucket algorithmically based on the bucket settings.

For shared limiters, the bucket state is modeled via atomic values which provide consistent consumption of tokens.
