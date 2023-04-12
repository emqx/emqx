# emqx_gateway

EMQX Gateway is an application that managing all gateways in EMQX.

It provides a set of standards to define how to implement a certain type of
protocol access on EMQX. For example:

- Frame parsing
- Access authentication
- Publish and subscribe
- Configuration & Schema
- HTTP/CLI management interfaces

There are some standard implementations available, such as [Stomp](../emqx_stomp/README.md),
[MQTT-SN](../emqx_mqttsn/README.md), [CoAP](../emqx_coap/README.md),
and [LwM2M](../emqx_lwm2m/README.md) gateway.

The emqx_gateway application depends on `emqx`, `emqx_authn`, `emqx_ctl` that
provide the foundation for protocol access.

## Three ways to create your gateway

## Raw Erlang Application

This approach is the same as in EMQX 4.x. You need to implement an Erlang application,
which is packaged in EMQX as a [Plugin](todo) or as a source code dependency.
In this approach, you do not need to respect any specifications of emqx_gateway,
and you can freely implement the features you need.


Steps guide: [Implement Gateway via Raw Application](doc/implement_gateway_via_raw_appliction.md)

## Respect emqx_gateway framework

Similar to the first approach, you still need to implement an application using Erlang
and package it into EMQX.
The only difference is that you need to follow the standard behaviors(callbacks) provided
by emqx_gateway.

This is the approach we recommend. In this approach, your implementation can be managed
by the emqx_gateway framework, even if it may require you to understand more details about it.


Steps guide: [Implement Gateway via Gateway framework](doc/implement_gateway_via_gateway_framekwork.md)

## Use ExProto Gateway (Non-Erlang developers)

If you want to implement your gateway using other programming languages such as
Java, Python, Go, etc.

You need to implement a gRPC service in the other programming language to parse
your device protocol and integrate it with EMQX.

Refer to: [ExProto Gateway](../emqx_exproto/README.md)

## Cookbook for emqx_gateway framework

*WIP*
