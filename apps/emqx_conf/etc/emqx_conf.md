EMQ X configuration file is in [HOCON](https://github.com/emqx/hocon) format.
HOCON, or Human-Optimized Config Object Notation is a format for human-readable data,
and a superset of JSON.

## Layered

EMQ X configuration consists of 3 layers.
From bottom up:

1. Immutable base: `emqx.conf` + `EMQX_` prfixed environment variables.<br>
   Changes in this layer require a full node restart to take effect.
1. Cluster override: The path of which is configured by `cluster_override_conf_file`.<br>
   Overrides made from management APIs (or dashboard) for all nodes in the cluster.
1. Local override: The path of which is configured by `local_override_conf_file`.<br>
   Special overrides applied to local node.

For detailed override rules, see [Config overlay rules](#config-overlay-rules).

## Syntax

In config file the values can be notated as JSON like ojbects, such as
```
node {
    name = "emqx@127.0.0.1"
    cookie = "mysecret"
}
```

Another equivalent representation is flat, such as

```
node.name="127.0.0.1"
node.cookie="mysecret"
```

This flat format is almost backward compatible with EMQ X's config file format
in 4.x series (the so called 'cuttlefish' format).

It is 'almost' compabile because the often HOCON requires strings to be quoted,
while cuttlefish treats all characters to the right of the `=` mark as the value.

e.g. cuttlefish: `node.name = emqx@127.0.0.1`, HOCON: `node.name = "emqx@127.0.0.1"`

Strings without special characters in them can be unquoted in HOCON too,
e.g. `foo`, `foo_bar`, `foo_bar_1`:

For more HOCON syntax, pelase refer to the [specification](https://github.com/lightbend/config/blob/main/HOCON.md)

## Schema

To make the HOCON objects type-safe, EMQ X introduded a schema for it.
The schema defines data types, and data fields' names and metadata for config value validation
and more. In fact, this config document itself is generated from schema metadata.

### Complex Data Types

There are 4 complex data types in EMQ X's HOCON config:

1. Struct: Named using an unquoted string, followed by a pre-defined list of fields,
   fields can not start with a number, and are only allowed to use
   lowercase letters and underscores as word separater.
1. Map: Map is like Struct, however the fields are not pre-defined.
   1-based index number can also be used as map keys for an alternative
   representation of an Array.
1. Union: `MemberType1 | MemberType2 | ...`
1. Array: `[ElementType]`

### Primitive Data Types

Complex types define data 'boxes' wich may contain other complex data
or primitive values.
There are quite some different primitive types, to name a fiew:

* `atom()`
* `boolean()`
* `string()`
* `integer()`
* `float()`
* `number()`
* `binary()` # another format of string()
* `emqx_schema:duration()` # time duration, another format of integer()
* ...

The primitive types are mostly self-describing, some are built-in, such
as `atom()`, some are defiend in EMQ X modules, such as `emqx_schema:duration()`.

### Config Paths

If we consider the whole EMQ X config as a tree,
to reference a primitive value, we can use a dot-separated names form string for
the path from the tree-root (always a Struct) down to the primitive values at tree-leaves.

Each segment of the dotted string is a Struct filed name or Map key.
For Array elements, 1-based index is used.

below are some examples

```
node.name="emqx.127.0.0.1"
zone.zone1.max_packet_size="10M"
authentication.1.enable=true
```

### Environment varialbes

Environment variables can be used to define or override config values.

Due to the fact that dots (`.`) are not allowed in environment variables, dots are
replaced with double-underscores (`__`).

And a the `EMQX_` prefix is used as the namespace.

For example `node.name` can be represented as `EMQX_NODE__NAME`

Environment varialbe values are parsed as hocon values, this allows users
to even set complex values from environment variables.

For example, this environment variable sets an array value.

```
export EMQX_LISTENERS__SSL__L1__AUTHENTICATION__SSL__CIPHERS="[\"TLS_AES_256_GCM_SHA384\"]"
```

Unknown environment variables are logged as a `warning` level log, for example:

```
[warning] unknown_env_vars: ["EMQX_AUTHENTICATION__ENABLED"]
```

because the field name is `enable`, not `enabled`.

<strong>NOTE:</strong> Unknown root keys are however silently discarded.

### Config overlay rules

HOCON objects are overlayed, in general:

- Within one file, objects defined 'later' recursively override objects defined 'earlier'
- When layered, 'later' (hihger lalyer) objects override objects defined 'earlier' (lower layer)

Below are more detailed rules.

#### Struct Fileds

Later config values overwrites earlier values.
For example, in below config, the last line `debug` overwrites `errro` for
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
the config schema. For instance, `zone1` in the exampele below.

```
zone {
    zone1 {
        mqtt.max_packet_size = 1M
    }
}

## The maximum packet size can be defined as above,
## then overriden as below

zone.zone1.mqtt.max_packet_size = 10M
```

#### Array Elements

Arrays in EMQ X config have two different representations

* list, such as: `[1, 2, 3]`
* indexed-map, such as: `{"1"=1, "2"=2, "3"=3}`

Dot-separated paths with number in it are parsed to indexed-maps
e.g. `authentication.1={...}` is parsed as `authentication={"1": {...}}`

Indexed-map arrays can be used to override list arrays:

```
authentication=[{enable=true, backend="built-in-database", mechanism="password-based"}]
# we can disable this authentication provider with:
authentication.1.enable=false
```
However, list arrays do not get recursively merged into indexed-map arrays.
e.g.

```
authentication=[{enable=true, backend="built-in-database", mechanism="password-based"}]
## below value will replace the whole array, but not to override just one field.
authentication=[{enable=true}]
```
