# Offline Messages

This plugin stores messages in an external database and replays them to subscribers.
MySQL and Redis store selected QoS 1/2 messages for offline replay.
PostgreSQL stores all matching publishes and tracks independent progress for each client.

Supported backends:

- MySQL
- Redis
- [PostgreSQL](POSTGRESQL.md), with ordered live delivery and replay, and independent progress for each client

## Configuration

The plugin is configured through the standard EMQX plugin configuration API:

`PUT /api/v5/plugins/<name-vsn>/config`

The defaults are in `priv/config.hocon`. The plugin configuration schema is `priv/config_schema.avsc`.

The MySQL and Redis backends only persist messages when:

- the publish QoS is greater than `0`
- the topic matches one of the configured `message.topic_filter`s

## Build And Test

Build the plugin from the repository root:

```bash
make
```

Run this plugin's Common Test suite:

```bash
make plugins/emqx_offline_messages-ct
```

## Database Schema

### MySQL

```sql
CREATE TABLE IF NOT EXISTS `mqtt_msg` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `msgid` varchar(64) DEFAULT NULL,
  `topic` varchar(180) NOT NULL,
  `sender` varchar(64) DEFAULT NULL,
  `qos` tinyint(1) NOT NULL DEFAULT '0',
  `retain` tinyint(1) DEFAULT NULL,
  `payload` blob,
  `arrived` datetime NOT NULL,
  PRIMARY KEY (`id`),
  INDEX topic_index(`topic`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8MB4;
```

```sql
CREATE TABLE IF NOT EXISTS `mqtt_sub` (
  `clientid` varchar(64) NOT NULL,
  `topic` varchar(180) NOT NULL,
  `qos` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`clientid`, `topic`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8MB4;
```

### Redis

Redis uses hashes and sorted sets under the `mqtt:sub:*` and `mqtt:msg:*` key spaces.

- Subscription state is stored in hashes keyed by client ID.
- Message payloads are stored in hashes keyed by message ID.
- Topic indexes are stored in sorted sets keyed by topic.
