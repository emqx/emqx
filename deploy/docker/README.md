# Quick reference

+ **Where to get help**:

  https://emqx.io or https://github.com/emqx/emqx

+ **Where to file issues:**

  https://github.com/emqx/emqx/issues

+ **Supported architectures**

  `amd64`, `arm64v8`,  `arm32v7`, `i386`, `s390x`


+ **Supported Docker versions**:

  [the latest release](https://github.com/docker/docker-ce/releases/latest)

# What is EMQX

[EMQX  MQTT broker](https://emqx.io/products/broker) is a fully open source, highly scalable, highly available distributed MQTT messaging broker for IoT, M2M and Mobile applications that can handle tens of millions of concurrent clients.

Starting from 3.0 release, *EMQX* broker fully supports MQTT V5.0 protocol specifications and backward compatible with MQTT V3.1 and V3.1.1,  as well as other communication protocols such as MQTT-SN, CoAP, LwM2M, WebSocket and STOMP. The 3.0 release of the *EMQX* broker can scaled to 10+ million concurrent MQTT connections on one cluster.

# How to use this image

### Run emqx

Execute some command under this docker image

``docker run -d --name emqx emqx/emqx:$(tag)``

For example

``docker run -d --name emqx -p 18083:18083 -p 1883:1883 emqx/emqx:latest``

The emqx broker runs as linux user `emqx` in the docker container.

### Configuration

Use the environment variable to configure the EMQX docker container.

By default, the environment variables with ``EMQX_`` prefix are mapped to key-value pairs in configuration files.

You can change the prefix by overriding "CUTTLEFISH_ENV_OVERRIDE_PREFIX".

Example:

```bash
EMQX_LISTENER__SSL__EXTERNAL__ACCEPTORS <--> listener.ssl.external.acceptors
EMQX_MQTT__MAX_PACKET_SIZE              <--> mqtt.max_packet_size
```

+ Prefix ``EMQX_`` is removed
+ All upper case letters is replaced with lower case letters
+ ``__`` is replaced with ``.``

If `CUTTLEFISH_ENV_OVERRIDE_PREFIX=DEV_` is set:

```bash
DEV_LISTENER__SSL__EXTERNAL__ACCEPTORS <--> listener.ssl.external.acceptors
DEV_MQTT__MAX_PACKET_SIZE              <--> mqtt.max_packet_size
```

Non mapped environment variables:

```bash
EMQX_NAME
EMQX_HOST
```

These environment variables will ignore for configuration file.

#### EMQX Configuration

> NOTE: All EMQX Configuration in [etc/emqx.conf](https://github.com/emqx/emqx/blob/main-v4.3/etc/emqx.conf) could config by environment. The following list is just an example, not a complete configuration.

| Options                    | Default            | Mapped                    | Description                           |
| ---------------------------| ------------------ | ------------------------- | ------------------------------------- |
| EMQX_NAME                  | container name     | none                      | emqx node short name                  |
| EMQX_HOST                  | container IP       | none                      | emqx node host, IP or FQDN            |

The list is incomplete and may changed with [etc/emqx.conf](https://github.com/emqx/emqx/blob/main-v4.3/etc/emqx.conf) and plugin configuration files. But the mapping rule is similar.

If set ``EMQX_NAME`` and ``EMQX_HOST``, and unset ``EMQX_NODE_NAME``, ``EMQX_NODE_NAME=$EMQX_NAME@$EMQX_HOST``.

For example, set mqtt tcp port to 1883

``docker run -d --name emqx -e EMQX_LISTENER__TCP__EXTERNAL=1883 -p 18083:18083 -p 1883:1883 emqx/emqx:latest``

#### EMQ Loaded Modules Configuration

| Oprtions                 | Default            | Description                           |
| ------------------------ | ------------------ | ------------------------------------- |
| EMQX_LOADED_MODULES       | see content below  | default modules emqx loaded            |

Default environment variable ``EMQX_LOADED_MODULES``, including

+ ``emqx_mod_acl_internal``
+ ``emqx_mod_presence``

```bash
# The default EMQX_LOADED_MODULES env
EMQX_LOADED_MODULES="emqx_mod_acl_internal,emqx_mod_acl_internal"
```

For example, set ``EMQX_LOADED_MODULES=emqx_mod_delayed,emqx_mod_rewrite`` to load these two modules.

You can use comma, space or other separator that you want.

All the modules defined in env ``EMQX_LOADED_MODULES`` will be loaded.

```bash
EMQX_LOADED_MODULES="emqx_mod_delayed,emqx_mod_rewrite"
EMQX_LOADED_MODULES="emqx_mod_delayed emqx_mod_rewrite"
EMQX_LOADED_MODULES="emqx_mod_delayed | emqx_mod_rewrite"
```

#### EMQ Loaded Plugins Configuration

| Oprtions                 | Default            | Description                           |
| ------------------------ | ------------------ | ------------------------------------- |
| EMQX_LOADED_PLUGINS       | see content below  | default plugins emqx loaded            |

Default environment variable ``EMQX_LOADED_PLUGINS``, including

+ ``emqx_recon``
+ ``emqx_retainer``
+ ``emqx_rule_engine``
+ ``emqx_management``
+ ``emqx_dashboard``

```bash
# The default EMQX_LOADED_PLUGINS env
EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard"
```

For example, set ``EMQX_LOADED_PLUGINS= emqx_auth_redis,emqx_auth_mysql`` to load these two plugins.

You can use comma, space or other separator that you want.

All the plugins defined in ``EMQX_LOADED_PLUGINS`` will be loaded.

```bash
EMQX_LOADED_PLUGINS="emqx_auth_redis,emqx_auth_mysql"
EMQX_LOADED_PLUGINS="emqx_auth_redis emqx_auth_mysql"
EMQX_LOADED_PLUGINS="emqx_auth_redis | emqx_auth_mysql"
```

#### EMQX Plugins Configuration

The environment variables which with ``EMQX_`` prefix are mapped to all emqx plugins' configuration file, ``.`` get replaced by ``__``.

Example:

```bash
EMQX_AUTH__REDIS__SERVER   <--> auth.redis.server
EMQX_AUTH__REDIS__PASSWORD <--> auth.redis.password
```

Don't worry about where to find the configuration file of emqx plugins, this docker image will find and config them automatically using some magic.

All plugin of emqx project could config in this way, following the environment variables mapping rule above.

Assume you are using redis auth plugin, for example:

```bash
#EMQX_AUTH__REDIS__SERVER="redis.at.yourserver"
#EMQX_AUTH__REDIS__PASSWORD="password_for_redis"

docker run -d --name emqx -p 18083:18083 -p 1883:1883 -p 4369:4369 \
    -e EMQX_LISTENER__TCP__EXTERNAL=1883 \
    -e EMQX_LOADED_PLUGINS="emqx_auth_redis" \
    -e EMQX_AUTH__REDIS__SERVER="your.redis.server:6379" \
    -e EMQX_AUTH__REDIS__PASSWORD="password_for_redis" \
    -e EMQX_AUTH__REDIS__PASSWORD_HASH=plain \
    emqx/emqx:latest
```

For numbered configuration options where the number is next to a ``.`` such as:

+ backend.redis.pool1.server
+ backend.redis.hook.message.publish.1

You can configure an arbitrary number of them as long as each has a uniq unber for it's own configuration option:

```bash
docker run -d --name emqx -p 18083:18083 -p 1883:1883 -p 4369:4369 \
    -e EMQX_BACKEND_REDIS_POOL1__SERVER=127.0.0.1:6379
    [...]
    -e EMQX_BACKEND__REDIS__POOL5__SERVER=127.0.0.5:6379
    -e EMQX_BACKEND__REDIS__HOOK_MESSAGE__PUBLISH__1='{"topic": "persistant/topic1", "action": {"function": "on_message_publish"}, "pool": "pool1"}'
    -e EMQX_BACKEND__REDIS__HOOK_MESSAGE__PUBLISH__2='{"topic": "persistant/topic2", "action": {"function": "on_message_publish"}, "pool": "pool1"}'
    -e EMQX_BACKEND__REDIS__HOOK_MESSAGE__PUBLISH__3='{"topic": "persistant/topic3", "action": {"function": "on_message_publish"}, "pool": "pool1"}'
    [...]
    -e EMQX_BACKEND__REDIS__HOOK_MESSAGE__PUBLISH__13='{"topic": "persistant/topic13", "action": {"function": "on_message_publish"}, "pool": "pool1"}'
    emqx/emqx:latest
```

### Cluster

EMQX supports a variety of clustering methods, see our [documentation](https://docs.emqx.io/broker/latest/en/advanced/cluster.html#emqx-service-discovery) for details.

Let's create a static node list cluster from docker-compose.

+ Create `docker-compose.yaml`:

  ```yaml
  version: '3'

  services:
    emqx1:
      image: emqx/emqx:latest
      environment:
      - "EMQX_NAME=emqx"
      - "EMQX_HOST=node1.emqx.io"
      - "EMQX_CLUSTER__DISCOVERY=static"
      - "EMQX_CLUSTER__STATIC__SEEDS=emqx@node1.emqx.io, emqx@node2.emqx.io"
      networks:
        emqx-bridge:
          aliases:
          - node1.emqx.io

    emqx2:
      image: emqx/emqx:latest
      environment:
      - "EMQX_NAME=emqx"
      - "EMQX_HOST=node2.emqx.io"
      - "EMQX_CLUSTER__DISCOVERY=static"
      - "EMQX_CLUSTER__STATIC__SEEDS=emqx@node1.emqx.io, emqx@node2.emqx.io"
      networks:
        emqx-bridge:
          aliases:
          - node2.emqx.io

  networks:
    emqx-bridge:
      driver: bridge

  ```

+ Start the docker-compose cluster

  ```bash
  docker compose -p my_emqx up -d
  ```

+ View cluster

  ```bash
  $ docker exec -it my_emqx_emqx1_1 sh -c "emqx_ctl cluster status"
  Cluster status: #{running_nodes => ['emqx@node1.emqx.io','emqx@node2.emqx.io'],
                    stopped_nodes => []}
  ```

### Persistence

If you want to persist the EMQX docker container, you need to keep the following directories:

+ `/opt/emqx/data`
+ `/opt/emqx/etc`
+ `/opt/emqx/log`

Since data in these folders are partially stored under the `/opt/emqx/data/mnesia/${node_name}`, the user also needs to reuse the same node name to see the previous state. In detail, one needs to specify the two environment variables: `EMQX_NAME` and `EMQX_HOST`, `EMQX_HOST` set as `127.0.0.1` or network alias would be useful.

In if you use docker-compose, the configuration would look something like this:

```YAML
volumes:
  vol-emqx-data:
    name: foo-emqx-data
  vol-emqx-etc:
    name: foo-emqx-etc
  vol-emqx-log:
    name: foo-emqx-log

services:
  emqx:
    image: emqx/emqx:v4.0.0
    restart: always
    environment:
      EMQX_NAME: foo_emqx
      EMQX_HOST: 127.0.0.1
    volumes:
      - vol-emqx-data:/opt/emqx/data
      - vol-emqx-etc:/opt/emqx/etc
      - vol-emqx-log:/opt/emqx/log
```

### Kernel Tuning

Under linux host machine, the easiest way is [Tuning guide](https://docs.emqx.io/en/broker/latest/tutorial/tune.html#linux-kernel-tuning).

If you want tune linux kernel by docker, you must ensure your docker is latest version (>=1.12).

```bash

docker run -d --name emqx -p 18083:18083 -p 1883:1883 -p 4369:4369 \
    --sysctl fs.file-max=2097152 \
    --sysctl fs.nr_open=2097152 \
    --sysctl net.core.somaxconn=32768 \
    --sysctl net.ipv4.tcp_max_syn_backlog=16384 \
    --sysctl net.core.netdev_max_backlog=16384 \
    --sysctl net.ipv4.ip_local_port_range=1000 65535 \
    --sysctl net.core.rmem_default=262144 \
    --sysctl net.core.wmem_default=262144 \
    --sysctl net.core.rmem_max=16777216 \
    --sysctl net.core.wmem_max=16777216 \
    --sysctl net.core.optmem_max=16777216 \
    --sysctl net.ipv4.tcp_rmem=1024 4096 16777216 \
    --sysctl net.ipv4.tcp_wmem=1024 4096 16777216 \
    --sysctl net.ipv4.tcp_max_tw_buckets=1048576 \
    --sysctl net.ipv4.tcp_fin_timeout=15 \
    emqx/emqx:latest

```

> REMEMBER: DO NOT RUN EMQX DOCKER PRIVILEGED OR MOUNT SYSTEM PROC IN CONTAINER TO TUNE LINUX KERNEL, IT IS UNSAFE.

### Thanks

+ [@je-al](https://github.com/emqx/emqx-docker/issues/2)
+ [@RaymondMouthaan](https://github.com/emqx/emqx-docker/pull/91)
+ [@zhongjiewu](https://github.com/emqx/emqx/issues/3427)
