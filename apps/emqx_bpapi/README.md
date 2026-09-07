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
be useful when the protocol involves multi-calls or multi-casts.

The business logic can assume that the supported protocol version is
not going to change on the remote node, while it is running. So it is
free to cache it for the duration of the session.

# Freezing a release

A baseline records the backplane API surface of a released version. The static
check compares the current build against every baseline in
`apps/emqx_bpapi/test/emqx_static_checks_data/`, so a version that no baseline
describes is never checked.

One file per minor release line, named `<major>.<minor>.bpapi2` (`.bpapi3` on
OTP28 and later). The file is overwritten on each freeze, because proto versions
only ever accumulate within a line. `master.bpapi2` is generated by every build
and is git-ignored; never commit it.

## Freeze after every release cut, patch releases included

The original design assumed patch releases carried no backplane change, so only
`maj.min.0` needed a baseline. That assumption no longer holds: patch releases
do add proto versions. Freeze after every cut.

1. Check out the release tag.
2. Build, then run `make static_checks` to generate `master.bpapi2`.
3. Replace `release => "master"` in it with the minor, e.g. `release => "6.0"`.
   The static check fails if this string does not match the file name.
4. Save it over `<major>.<minor>.bpapi2` and commit it in its own PR.

Review the diff rather than taking the regeneration on trust. A freeze may add
`{API, Version}` keys, and may drop keys already listed in
`?FORCE_DELETED_APIS`. It must never *change* one — that is a frozen API being
edited, and the point of the check.

## A release cut on one branch freezes the branches upstream of it

Work lands on the lowest `dev-6*` branch and is forward-merged. A change can
therefore be released from a higher branch before the branch it originated on
cuts anything. When `6.3.0` ships an API added on `dev-60`, that API is frozen
even though `dev-60` has released nothing containing it, and `dev-60` cannot see
that: baselines only travel forward.

So after a release cut, also freeze the merge-base commit of that tag on every
`dev-6*` branch upstream of it, and commit the result there. Without this the
originating branch keeps treating the API as unreleased and lets it be edited.

## The 5.x baselines are not maintained

The 5.x lines are independently maintained and diverge from each other, so they
are frozen where they are and not refreshed. Comparisons among them can report
differences that say nothing about a 6.x branch; those belong in
`?DIVERGED_APIS` with the reason recorded, not in a fix on a 5.x branch.

# Two release lines claiming the same version

`?DIVERGED_APIS` in `emqx_bpapi_static_checks` lists `{API, Version}` pairs that
two lines gave the same number with different contents. Both sides have shipped,
so neither can be corrected, and the incompatibility has to be prevented some
other way. An entry is only legitimate when its comment says which: a run-time
guard that stops the two contracts meeting, or the reason no node on this branch
can reach the other side's version. Without one of those, the entry hides a real
break.
