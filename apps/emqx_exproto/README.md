# emqx-exproto

The `emqx_exproto` extremly enhance the extensibility for EMQ X. It allow using an others programming language to **replace the protocol handling layer in EMQ X Broker**.

## Feature

- [x] Support Python, Java.
- [x] Support the `tcp`, `ssl`, `udp`, `dtls` socket.
- [x] Provide the `PUB/SUB` interface to others language.

We temporarily no plans to support other languages. Plaease open a issue if you have to use other programming languages.

## Architecture

![EMQ X ExProto Arch](./docs/images/exproto-arch.jpg)

## Drivers

### Python

***Requirements:***

- It requires the emqx hosted machine has Python3 Runtimes
- An executable commands in your shell, i,g: `python3` or `python`

***Examples:***

See [example/main.python](https://github.com/emqx/emqx-exproto/blob/master/example/main.py)

### Java

See [example/Main.java](https://github.com/emqx/emqx-exproto/blob/master/example/Main.java)


## SDK

The SDK encloses the underlying obscure data types and function interfaces. It only provides a convenience for development, it is not required.

See [sdk/README.md](https://github.com/emqx/emqx-exproto/blob/master/sdk/README.md)


## Benchmark

***Work in progress...***


## Known Issues or TODOs

- Configurable Log System.
    * The Java driver can not redirect the `stderr` stream to erlang vm on Windows platform

## Reference

- [erlport](https://github.com/hdima/erlport)
- [External Term Format](http://erlang.org/doc/apps/erts/erl_ext_dist.html)
- [The Ports Tutorial of Erlang](http://erlang.org/doc/tutorial/c_port.html)
