Backplane API
===

# Motivation

This directory contains modules that help defining and maintaining
EMQX broker-to-broker (backplane) protocols.

Historically, all inter-broker communication was done by the means of
remote procedure calls. This approach allowed for rapid development,
but presented some challenges for rolling cluster upgrade, since
tracking destination of the RPC could not be automated using standard
tools (such as xref and dialyzer).

Starting from EMQX v5.0.0, `emqx_bpapi` sub-application is used to
facilitate backplane API backward- and forward-compatibility. Wild
remote procedure calls are no longer allowed. Instead, every call is
decorated by a very thin wrapper function located in a versioned
"_proto_" module.

Some restrictions are put on the lifecycle of the `_proto_` modules,
and they are additionally tracked in a database created in at the
build time.

# Rolling upgrade

During rolling upgrades different versions of the code is running
side-by-side:

```txt
+--------------+                                        +---------------+
|              |                                        |               |
|   Node A     | ----- rpc:call(foo, foo, [])  ------>  |   Node B      |
|              |                                        |               |
| EMQX 5.1.2   | <---- rpc:call(foo, foo, [1]) -------  | EMQX 5.0.13   |
|              |                                        |               |
+--------------+                                        +---------------+
```

The following changes will break the backplane API:

1. removing a target function
2. adding a new method to the protocol
3. reducing the domain of the target function
4. extending the co-domain of the target function

Bullets 1 and 2 are addressed by a static check that verifies
immutability of the proto modules. 3 is checked using dialyzer
specs. 4 is not checked at this moment.

# Backplane API modules

A distributed Erlang application in EMQX is organized like this:

```txt
...
myapp/src/myapp.erl
myapp/src/myapp.app.src
myapp/src/proto/myapp_proto_v1.erl
myapp/src/proto/myapp_proto_v2.erl
```

Notice `proto` directory containing several modules that follow
`<something>_proto_v<number>` pattern.

These modules should follow the following template:

```erlang
-module(emqx_proto_v1).

-behaviour(emqx_bpapi).

%% Note: the below include is mandatory
-include_lib("emqx/include/bpapi.hrl").

-export([ introduced_in/0
        , deprecated_since/0 %% Optional
        ]).

-export([ is_running/1
        ]).

introduced_in() ->
    "5.0.0".

deprecated_since() ->
    "5.2.0".

-spec is_running(node()) -> boolean().
is_running(Node) ->
    rpc:call(Node, emqx, is_running, []).
```

The following limitations apply to these modules:

1. Once the minor EMQX release stated in `introduced_in()` callback of
   a module reaches GA, the module is frozen. No changes are allowed
   there, except for adding `deprecated_since()` callback.
2. If the backplane API was deprecated in a release `maj.min.0`, then
   it can be removed in release `maj.min+1.0`.
3. Old versions of the protocols can be dropped in the next major
   release.

This way we ensure each minor EMQX release is backward-compatible with
the previous one.

# Protocol version negotiation

TODO
