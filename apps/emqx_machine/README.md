# EMQX machine

This application manages other OTP applications in EMQX and serves as the entry point when BEAM VM starts up.
It prepares the node before starting mnesia/mria, as well as EMQX business logic.
It keeps track of the business applications storing data in Mnesia, which need to be restarted when the node joins the cluster by registering `ekka` callbacks.
Also it kicks off autoclustering (EMQX cluster discovery) on the core nodes.

`emqx_machine` application doesn't do much on its own, but it facilitates the environment for running other EMQX applications.

# Features

## Global GC

`emqx_global_gc` is a gen_server that forces garbage collection of all Erlang processes running in the BEAM VM.
This is meant to save the RAM.

## Restricted shell

`emqx_restricted_shell` module prevents user from accidentally issuing Erlang shell commands that can stop the remote node.

## Signal handler

`emqx_machine_signal_handler` handles POSIX signals sent to BEAM VM process.
It helps to shut down EMQX broker gracefully when it receives `SIGTERM` signal.

## Cover

`emqx_cover` is a helper module that helps to collect coverage reports during testing.

# Limitation

Currently `emqx_machine` boots the business apps before starting autocluster, so a fresh node joining the cluster actually starts business application twice: first in the singleton mode, and then in clustered mode.

# Documentation links

Configuration: [node.global_gc_interval](https://www.emqx.io/docs/en/v5.0/configuration/configuration-manual.html#node-and-cookie)

# Configurations

The following application environment variables are used:

- `emqx_machine.global_gc_interval`: interval at which global GC is run
- `emqx_machine.custom_shard_transports`: contains a map that allows to fine tune transport (`rpc` or `gen_rpc`) used to send Mria transactions from the core node to the replicant
- `emqx_machine.backtrace_depth`: set maximum depth of Erlang stacktraces in crash reports


# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
