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

## Backplane module life cycle

1. Once the minor EMQX release stated in `introduced_in()` callback of
   a module reaches GA, the module is frozen. Only very specific
   changes are allowed in these modules, see next chapter.
2. If the backplane API was deprecated in a release `maj.min.0`, then
   it can be removed in release `maj.min+1.0`.
3. Old versions of the protocols can be dropped in the next major
   release.

This way we ensure each minor EMQX release is backward-compatible with
the previous one.

## Changes to BPAPI modules after GA

Once the backplane API module is frozen, only certain types of changes
can be made there.

- Adding or removing functions is _forbidden_
- Changing the RPC target function is _forbidden_
- Renaming the function parameters should be safe in theory, but
  currently the static check will complain when it happens
- Renaming the types of the function parameters and the return type is
  _allowed_
- Changing the structure of the function parameters' types is
  _forbidden_

To clarify the last statement: BPAPI static checks only verify the
structure of the type, so the following definitions are considered
equivalent, and replacing one with another is perfectly fine:

```erlang
-type foo() :: inet:ip6_address().

-type foo() :: {0..65535, 0..65535, 0..65535, 0..65535, 0..65535, 0..65535, 0..65535, 0..65535}.
```

# Protocol version negotiation

`emqx_bpapi` module provides APIs that business applications can use
to negotiate protocol version:

`emqx_bpapi:supported_version(Node, ProtocolId)` returns maximum
protocol version supported by the remote node
`Node`. `emqx_bpapi:supported_version(ProtocolId)` returns maximum
protocol version that is supported by all nodes in the cluster. It can
be useful when the protocol involves multicalls or multicasts.

The business logic can assume that the supported protocol version is
not going to change on the remote node, while it is running. So it is
free to cache it for the duration of the session.

# New minor release

After releasing, let's say, 5.1.0, the following actions should be performed to prepare for the next release:

1. Checkout 5.1.0 tag
1. Build the code
1. Rename `apps/emqx/test/emqx_static_checks_data/master.bpapi` to `apps/emqx/test/emqx_static_checks_data/5.1.bpapi`
1. Add `apps/emqx/test/emqx_static_checks_data/5.1.bpapi` to the repo
1. Delete the previous file (e.g. `5.0.bpapi`), unless there is plan to support rolling upgrade from 5.0 to 5.2
1. Merge the commit to master branch
