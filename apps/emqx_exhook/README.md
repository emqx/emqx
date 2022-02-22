# emqx_exhook

The `emqx_exhook` extremly enhance the extensibility for EMQX. It allow using an others programming language to mount the hooks intead of erlang.

## Feature

- [x] Based on gRPC, it brings a very wide range of applicability
- [x] Allows you to use the return value to extend emqx behavior.

## Architecture

```
EMQX                                      Third-party Runtime
+========================+                 +========+==========+
|    ExHook              |                 |        |          |
|   +----------------+   |      gRPC       | gRPC   |  User's  |
|   |   gPRC Client  | ------------------> | Server |  Codes   |
|   +----------------+   |    (HTTP/2)     |        |          |
|                        |                 |        |          |
+========================+                 +========+==========+
```

## Usage

### gRPC service

See: `priv/protos/exhook.proto`

### CLI

## Example

## Recommended gRPC Framework

See: https://github.com/grpc-ecosystem/awesome-grpc

## Thanks

- [grpcbox](https://github.com/tsloughter/grpcbox)
