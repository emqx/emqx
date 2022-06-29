EMQX configuration file is in [HOCON](https://github.com/emqx/hocon) format.
HOCON, or Human-Optimized Config Object Notation is a format for human-readable data,
and a superset of JSON.

## Layered

EMQX configuration consists of 3 layers.
From bottom up:

1. Immutable base: `emqx.conf` + `EMQX_` prefixed environment variables.</br>
   Changes in this layer require a full node restart to take effect.
1. Cluster overrides: `$EMQX_NODE__DATA_DIR/configs/cluster-override.conf`
1. Local node overrides: `$EMQX_NODE__DATA_DIR/configs/local-override.conf`

When environment variable `$EMQX_NODE__DATA_DIR` is not set, config `node.data_dir`
is used.

The `cluster-override.conf` file is overwritten at runtime when changes
are made from dashboard UI, management HTTP API, or CLI. When clustered,
after EMQX restarts, it copies the file from the node which has the greatest `uptime`.

:::tip Tip
Some of the configs (such as `node.name`) are boot-only configs and not overridable.
Config values from `*-override.conf` are **not** mapped to boot configs for
the config fields attributed with `mapping: path.to.boot.config.key`
:::

For detailed override rules, see [Config Overlay Rules](#config-overlay-rules).

## Syntax

In config file the values can be notated as JSON like objects, such as
```
node {
    name = "emqx@127.0.0.1"
    cookie = "mysecret"
}
```

Another equivalent representation is flat, such as

```
node.name = "127.0.0.1"
node.cookie = "mysecret"
```

This flat format is almost backward compatible with EMQX's config file format
in 4.x series (the so called 'cuttlefish' format).

It is not fully compatible because the often HOCON requires strings to be quoted,
while cuttlefish treats all characters to the right of the `=` mark as the value.

e.g. cuttlefish: `node.name = emqx@127.0.0.1`, HOCON: `node.name = "emqx@127.0.0.1"`.

Strings without special characters in them can be unquoted in HOCON too,
e.g. `foo`, `foo_bar` and `foo_bar_1`.

For more HOCON syntax, please refer to the [specification](https://github.com/lightbend/config/blob/main/HOCON.md)

## Schema

To make the HOCON objects type-safe, EMQX introduced a schema for it.
The schema defines data types, and data fields' names and metadata for config value validation
and more.

::: tip Tip
The configuration document you are reading now is generated from schema metadata.
:::

### Complex Data Types

There are 4 complex data types in EMQX's HOCON config:

1. Struct: Named using an unquoted string, followed by a predefined list of fields.
   Only lowercase letters and digits are allowed in struct and field names.
   Alos, only underscore can be used as word separator.
1. Map: Map is like Struct, however the fields are not predefined.
1. Union: `MemberType1 | MemberType2 | ...`
1. Array: `[ElementType]`

::: tip Tip
If map filed name is a positive integer number, it is interpreted as an alternative representation of an `Array`.
For example:
```
myarray.1 = 74
myarray.2 = 75
```
will be interpreated as `myarray = [74, 75]`, which is handy when trying to override array elements.
:::

### Primitive Data Types

Complex types define data 'boxes' which may contain other complex data
or primitive values.
There are quite some different primitive types, to name a few:

* `atom()`
* `boolean()`
* `string()`
* `integer()`
* `float()`
* `number()`
* `binary()` # another format of string()
* `emqx_schema:duration()` # time duration, another format of integer()
* ...

::: tip Tip
The primitive types are mostly self-describing, so there is usually not a lot to document.
For types that are not so clear by their names, the field description is to be used to find the details.
:::

### Config Paths

If we consider the whole EMQX config as a tree,
to reference a primitive value, we can use a dot-separated names form string for
the path from the tree-root (always a Struct) down to the primitive values at tree-leaves.

Each segment of the dotted string is a Struct filed name or Map key.
For Array elements, 1-based index is used.

below are some examples

```
node.name = "emqx.127.0.0.1"
zone.zone1.max_packet_size = "10M"
authentication.1.enable = true
```

### Environment variables

Environment variables can be used to define or override config values.

Due to the fact that dots (`.`) are not allowed in environment variables, dots are
replaced with double-underscores (`__`).

And the `EMQX_` prefix is used as the namespace.

For example `node.name` can be represented as `EMQX_NODE__NAME`

Environment variable values are parsed as HOCON values, this allows users
to even set complex values from environment variables.

For example, this environment variable sets an array value.

```
export EMQX_LISTENERS__SSL__L1__AUTHENTICATION__SSL__CIPHERS="[\"TLS_AES_256_GCM_SHA384\"]"
```

::: tip Tip
Unknown root paths are silently discarded by EMQX, for example `EMQX_UNKNOWN_ROOT__FOOBAR` is
silently discarded because `unknown_root` is not a predefined root path.

Unknown field names in environment variables are logged as a `warning` level log, for example:

```
[warning] unknown_env_vars: ["EMQX_AUTHENTICATION__ENABLED"]
```

because the field name is `enable`, not `enabled`.
:::


### Config Overlay Rules

HOCON objects are overlaid, in general:

- Within one file, objects defined 'later' recursively override objects defined 'earlier'
- When layered, 'later' (higher layer) objects override objects defined 'earlier' (lower layer)

Below are more detailed rules.

#### Struct Fields

Later config values overwrites earlier values.
For example, in below config, the last line `debug` overwrites `error` for
console log handler's `level` config, but leaving `enable` unchanged.
```
log {
    console_handler{
        enable=true,
        level=error
    }
}

## ... more configs ...

log.console_handler.level=debug
```

#### Map Values

Maps are like structs, only the files are user-defined rather than
the config schema. For instance, `zone1` in the example below.

```
zone {
    zone1 {
        mqtt.max_packet_size = 1M
    }
}

## The maximum packet size can be defined as above,
## then overridden as below

zone.zone1.mqtt.max_packet_size = 10M
```

#### Array Elements

Arrays in EMQX config have two different representations

* list, such as: `[1, 2, 3]`
* indexed-map, such as: `{"1"=1, "2"=2, "3"=3}`

Dot-separated paths with number in it are parsed to indexed-maps
e.g. `authentication.1={...}` is parsed as `authentication={"1": {...}}`

This feature makes it easy to override array elment values. For example:

```
authentication=[{enable=true, backend="built_in_database", mechanism="password_based"}]
# we can disable this authentication provider with:
authentication.1.enable=false
```

::: warning Warning
List arrays is a full-array override, but not a recursive merge, into indexed-map arrays.
e.g.

```
authentication=[{enable=true, backend="built_in_database", mechanism="password_based"}]
## below value will replace the whole array, but not to override just one field.
authentication=[{enable=true}]
```
:::
