# EMQX Schema Registry

EMQX Schema Registry for managing various of schemas for
decoding/encoding messages.

To use schema in rule engine, a schema name should be passed to the
SQL functions that decode/encode data, like:

```sql
SELECT
     schema_decode('sensor_notify', payload) as payload
FROM
     "message.publish"
WHERE
     topic = 't/1'
```

## Using schema registry with rule engine

```
                      +---------------------------+
                     |                           |
 Events/Msgs         |                           |   Events/Msgs
 -------------------->           EMQX            |------------------>
                     |                           |
                     |                           |
                     +-------------|-------------+
                                   |
                             HOOK  |
                                   |
                     +-------------v-------------+           +----------+
                     |                           |   Data    |          |
                     |        Rule Engine        ------------- Backends |
                     |                           |           |          |
                     +------|-------------|------+           +----------+
                            |^            |^
                      Decode||            ||Encode
                            ||            ||
                     +------v|------------v|-----+
                     |                           |
                     |      Schema Registry      |
                     |                           |
                     +---------------------------+
```

## Architecture

```
                              |              |
                       Decode |    [APIs]    | Encode
                              |              |
                              |              |           [Registry]
                       +------v--------------v------+
    REGISTER SCHEMA    |                            |
       INSTANCE        |                            |    +--------+
    ------------------->                            |    |        |
[Management APIs]      |       Schema Registry      ------ Schema |
                       |                            |    |        |
                       |                            |    +--------+
                       |                            |
                       +----------------------------+
                            /        |        \
                       +---/---+ +---|----+ +---\---+
                       |       | |        | |       |
      [Decoders]       | Avro  | |ProtoBuf| |Others |
                       |       | |        | |       |
                       +-------+ +--------+ +-------+

```

- Register schema instance: adds a new instance of a schema of a
  certain type.  For example, when the user may have several Avro or
  Protobuf schemas that they wish to use with different data flows.

# Documentation

- Refer to [Introduction to Schema Registry](https://docs.emqx.com/en/enterprise/v5.1/data-integration/schema-registry.html) for how to use the EMQX dashboard for configuring schemas.

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
