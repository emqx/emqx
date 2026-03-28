# Offline Messages

This plugin persists selected QoS 1/2 messages when no subscribers are online and replays them when a matching subscriber comes online later.

Supported backends:

- MySQL
- Redis

## Configuration

The plugin is configured through the standard EMQX plugin configuration API:

`PUT /api/v5/plugins/<name-vsn>/config`

The schema is defined in `priv/config.hocon` and includes backend-specific settings for Redis and MySQL.

The plugin only persists messages when:

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
