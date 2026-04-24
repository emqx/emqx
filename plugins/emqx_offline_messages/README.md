# EMQX Offline Message Plugin

This plugin can be used to store messages in a 3rd party database. It allows you to publish messages to a topic even when there are no subscribers online, and the messages will be stored until a subscriber comes online.

Currently supports the following database backends:
- MySQL
- Redis

## Usage

<!-- Do not update plugin version manually, use make bump-version-patch/minor/major instead -->
<!-- TODO: when plugin is uploaded to s3 download from  https://packages.emqx.io/emqx-plugins/e5.10.4/emqx_offline_messages-2.0.0.tar

Download the plugin:

```bash
wget https://github.com/emqx/emqx-offline-message-plugin/releases/download/v2.0.0/emqx_offline_message_plugin-2.0.0.tar.gz
```
-->

Install the plugin:

<!-- Do not update plugin version manually, use make bump-version-patch/minor/major instead -->
```bash
curl -u key:secret -X POST http://localhost:18083/api/v5/plugins/install \
-H "Content-Type: multipart/form-data" \
-F "plugin=@emqx_offline_message_plugin-2.0.0.tar.gz"
```

Check the plugin is installed:

```bash
curl -u key:secret http://localhost:18083/api/v5/plugins | jq
```

Configure the plugin in the Dashboard: http://localhost:18083/#/plugins/detail/emqx_offline_message_plugin-2.0.0

Verify:

Using [MQTTX CLI](https://mqttx.app/cli)

```bash
mqttx pub -q 1 -t 't/2' -m 'hello-from-offline1'
mqttx pub -q 1 -t 't/2' -m 'hello-from-offline2'
mqttx pub -q 1 -t 't/2' -m 'hello-from-offline3'

mqttx sub -q 1 -t 't/2' -i $(pwgen 20 -1)
```

No messages should be received:

```bash
mqttx sub -q 1 -t 't/2' -i $(pwgen 20 -1)
```

## Release

An EMQX plugin release is a tar file including including a subdirectory of this plugin's name and it's version, that contains:

1. A JSON format metadata file describing the plugin
2. Versioned directories for all applications needed for this plugin (source and binaries).
3. Confirm the OTP version used by EMQX that the plugin will be installed on (See also [./.tool-versions](./.tool-versions)).

In a shell from this plugin's working directory execute `make rel` to have the package created like:

```
_build/default/emqx_plugrel/emqx_plugin_template-<vsn>.tar.gz
```
## Format

Format all the files in your project by running:
```
make fmt
```

See [EMQX documentation](https://docs.emqx.com/en/enterprise/v5.0/extensions/plugins.html) for details on how to deploy custom plugins.

## Database Schema

This plugin requires a pre-defined database schema.

### MySQL

**Messages table**

```
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

**Subscriptions table**

```
CREATE TABLE IF NOT EXISTS `mqtt_sub` (
  `clientid` varchar(64) NOT NULL,
  `topic` varchar(180) NOT NULL,
  `qos` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`clientid`, `topic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8MB4;
```

### Redis

Redis uses the following data structures (no predefined schema needed, structures are created automatically):

**Subscriptions** - Redis Hashes

- Key pattern: `mqtt:sub:{clientid}`
- Hash fields: `{topic}` -> `{qos}` (integer)
- Operations: `HSET`, `HDEL`, `HGETALL`

**Messages** - Redis Hashes

- Key pattern: `mqtt:msg:{msgid}` (msgid is base62 encoded)
- Hash fields:
  - `id` -> base62 encoded message ID
  - `from` -> sender/clientid
  - `qos` -> QoS level (0, 1, or 2)
  - `topic` -> topic name
  - `payload` -> message payload (binary)
  - `ts` -> timestamp (integer)
  - `retain` -> "true" or "false" (string)
- Operations: `HMSET`, `HGETALL`, `DEL`, `EXPIRE`

**Message Index by Topic** - Redis Sorted Sets

- Key pattern: `mqtt:msg:{topic}`
- Members: base62 encoded message IDs
- Scores: timestamps (used for TTL/expiration cleanup)
- Operations: `ZADD`, `ZRANGE`, `ZREMRANGEBYSCORE`, `ZREM`

**Required Redis Operations for ACL Control**

If using Redis ACL, the user needs permissions for the following operations:

- Hash operations: `HSET`, `HDEL`, `HGETALL`, `HMSET`, `DEL`, `EXPIRE`
- Sorted set operations: `ZADD`, `ZRANGE`, `ZREMRANGEBYSCORE`, `ZREM`
- Key pattern operations: Access to keys matching `mqtt:sub:*` and `mqtt:msg:*`
