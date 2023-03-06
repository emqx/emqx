# EMQX

## Quick reference

+ **Where to get help**:

<https://emqx.io> or <https://github.com/emqx/emqx>

+ **Where to file issues:**

<https://github.com/emqx/emqx/issues>

+ **Supported architectures**

  `amd64`, `arm64v8`

+ **Supported Docker versions**:

  [the latest release](https://github.com/docker/docker-ce/releases/latest)

## What is EMQX

[EMQX](https://emqx.io/) is the world's most scalable open-source MQTT broker with a high performance that connects 100M+ IoT devices in 1 cluster, while maintaining 1M message per second throughput and sub-millisecond latency.

EMQX supports multiple open standard protocols like MQTT, HTTP, QUIC, and WebSocket. It's 100% compliant with MQTT 5.0 and 3.x standard, and secures bi-directional communication with MQTT over TLS/SSL and various authentication mechanisms.

With the built-in powerful SQL-based rules engine, EMQX can extract, filter, enrich and transform IoT data in real-time. In addition, it ensures high availability and horizontal scalability with a masterless distributed architecture, and provides ops-friendly user experience and great observability.

EMQX boasts more than 20K+ enterprise users across 50+ countries and regions, connecting 100M+ IoT devices worldwide, and is trusted by over 400 customers in mission-critical scenarios of IoT, IIoT, connected vehicles, and more, including over 70 Fortune 500 companies like HPE, VMware, Verifone, SAIC Volkswagen, and Ericsson.

## How to use this image

### Run EMQX

Execute some command under this docker image

```console
$ docker run -d --name emqx emqx/emqx:${tag}
```

For example

```console
$ docker run -d --name emqx -p 18083:18083 -p 1883:1883 emqx/emqx:latest
```

The EMQX broker runs as Linux user `emqx` in the docker container.

### Configuration

All EMQX Configuration in [`etc/emqx.conf`](https://github.com/emqx/emqx/blob/master/apps/emqx/etc/emqx.conf) can be configured via environment variables.

The environment variables with `EMQX_` prefix are mapped to key-value pairs in configuration files.

Example:

```bash
EMQX_DASHBOARD__DEFAULT_PASSWORD       <--> dashboard.default_password
EMQX_NODE__COOKIE                      <--> node.cookie
EMQX_LISTENERS__SSL__default__ENABLE   <--> listeners.ssl.default.enable
```
Note: The lowercase use of 'default' is not a typo. It is used to demonstrate that lowercase environment variables are equivalent.

+ Prefix `EMQX_` is removed
+ All upper case letters is replaced with lower case letters
+ `__` is replaced with `.`

For example, set MQTT TCP port to 1883

```console
$ docker run -d --name emqx -e EMQX_DASHBOARD__DEFAULT_PASSWORD=mysecret -p 18083:18083 -p 1883:1883 emqx/emqx:latest
```

Please read more about EMQX configuration in the [official documentation](https://www.emqx.io/docs/en/v5.0/configuration/configuration.html)

#### EMQX node name configuration

A node name consists of two parts, `EMQX_NAME` part and `EMQX_HOST` part connected by a the symbol `@`. For example: `emqx@127.0.0.1`.

Environment variables `EMQX_NODE_NAME` or `EMQX_NODE__NAME` can be used to set a EMQX node name.
If neither of them is set, EMQX will resolve its node name from the running environment or other environment varialbes used for node discovery.

When running in docker, by default, `EMQX_NAME` and `EMQX_HOST` are resolved as below:

| Options      | Default         | Description                  |
| -------------| --------------- | -----------------------------|
| `EMQX_NAME`  | container name  | EMQX node short name         |
| `EMQX_HOST`  | container IP    | EMQX node host, IP or FQDN   |

### Cluster

EMQX supports a variety of clustering methods, see our [documentation](https://www.emqx.io/docs/en/latest/deploy/cluster/intro.html) for details.

Let's create a static node list cluster from docker-compose.

+ Create `docker-compose.yaml`:

  ```yaml
  version: '3'

  services:
    emqx1:
      image: emqx/emqx:latest
      environment:
      - "EMQX_NODE_NAME=emqx@node1.emqx.io"
      - "EMQX_CLUSTER__DISCOVERY_STRATEGY=static"
      - "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io, emqx@node2.emqx.io]"
      networks:
        emqx-bridge:
          aliases:
          - node1.emqx.io

    emqx2:
      image: emqx/emqx:latest
      environment:
      - "EMQX_NODE_NAME=emqx@node2.emqx.io"
      - "EMQX_CLUSTER__DISCOVERY_STRATEGY=static"
      - "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io, emqx@node2.emqx.io]"
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
  docker-compose -p my_emqx up -d
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
    image: emqx/emqx:latest
    restart: always
    environment:
      EMQX_NODE_NAME: foo_emqx@127.0.0.1
    volumes:
      - vol-emqx-data:/opt/emqx/data
      - vol-emqx-etc:/opt/emqx/etc
      - vol-emqx-log:/opt/emqx/log
```

Note that `/opt/emqx/etc` contains some essential configuration files. If you want to mount a host directory in the container to persist configuration overrides, you will need to bootstrap it with [default configuration files](https://github.com/emqx/emqx/tree/master/apps/emqx/etc).

### Kernel Tuning

Under Linux host machine, the easiest way is [Tuning guide](https://www.emqx.io/docs/en/latest/deploy/tune.html).

If you want tune Linux kernel by docker, you must ensure your docker is latest version (>=1.12).

```bash
docker run -d --name emqx -p 18083:18083 -p 1883:1883 \
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
