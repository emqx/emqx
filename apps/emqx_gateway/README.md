# Gateway

EMQX Gateway is an application framework that manages all gateways within EMQX.

It provides a set of standards to define how to implement a certain type of
protocol access on EMQX. For example:

- Frame parsing
- Access authentication
- Publish and subscribe
- Configuration & Schema
- HTTP/CLI management interfaces

The emqx_gateway application depends on `emqx`, `emqx_auth`, ``emqx_auth_*`, `emqx_ctl` that
provide the foundation for protocol access.

More introduction: [Extended Protocol Gateway](https://www.emqx.io/docs/en/v5.0/gateway/gateway.html)

## Usage

This application is just a Framework, we provide some standard implementations,
such as [Stomp](../emqx_gateway_stomp/README.md), [MQTT-SN](../emqx_gateway_mqttsn/README.md),
[CoAP](../emqx_gateway_coap/README.md) and [LwM2M](../emqx_gateway_lwm2m/README.md) gateway.

These applications are all packaged by default in the EMQX distribution. If you
need to start a certain gateway, you only need to enable it via
Dashboard, HTTP API or emqx.conf file.

For instance, enable the Stomp gateway in emqx.conf:
```hocon
gateway.stomp {

  mountpoint = "stomp/"

  listeners.tcp.default {
    bind = 61613
    acceptors = 16
    max_connections = 1024000
    max_conn_rate = 1000
  }
}
```

## How to develop your Gateway application

There are three ways to develop a Gateway application to accept your private protocol
clients.

### Raw Erlang Application

This approach is the same as in EMQX 4.x. You need to implement an Erlang application,
which is packaged in EMQX as a Plugin or as a source code dependency.
In this approach, you do not need to respect any specifications of emqx_gateway,
and you can freely implement the features you need.


### Respect emqx_gateway framework

Similar to the first approach, you still need to implement an application using Erlang
and package it into EMQX.
The only difference is that you need to follow the standard behaviors(callbacks) provided
by emqx_gateway.

This is the approach we recommend. In this approach, your implementation can be managed
by the emqx_gateway framework, even if it may require you to understand more details about it.


### Use ExProto Gateway (Non-Erlang developers)

If you want to implement your gateway using other programming languages such as
Java, Python, Go, etc.

You need to implement a gRPC service in the other programming language to parse
your device protocol and integrate it with EMQX.


## Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
