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
    ------------------->                            |    +--------+
                       |                            |    |        |
[Management APIs]      |       Schema Registry      ------ Schema |
                       |                            |    |        |
    ------------------->                            |    +--------+
    LOAD PARSERS       |                            |
                       +----------------------------+
                            /        |        \
                       +---/---+ +---|----+ +---\---+
                       |       | |        | |       |
      [Decoders]       | Avro  | |ProtoBuf| |Others |
                       |       | |        | |       |
                       +-------+ +--------+ +-------+

```
