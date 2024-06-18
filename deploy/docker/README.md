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

Example:

	EMQX_DASHBOARD__DEFAULT_PASSWORD       <--> dashboard.default_password
	EMQX_NODE__COOKIE                      <--> node.cookie
	EMQX_LISTENERS__SSL__default__ENABLE   <--> listeners.ssl.default.enable

Note: The lowercase use of 'default' is not a typo. It is used to demonstrate that lowercase environment variables are equivalent.

-	Prefix `EMQX_` is removed
-	All upper case letters are replaced with lower case letters
-	`__` is replaced with `.`

For example, set MQTT TCP port to 1883

```console
$ docker run -d --name emqx -e EMQX_DASHBOARD__DEFAULT_PASSWORD=mysecret -p 18083:18083 -p 1883:1883 emqx/emqx:latest
```

Please read more about EMQX configuration in the [official documentation](https://docs.emqx.com/en/emqx/latest/configuration/configuration.html)

#### EMQX node name configuration

Environment variable `EMQX_NODE__NAME` allows you to specify an EMQX node name, which defaults to `<container_name>@<container_ip>`.

If not specified, EMQX determines its node name based on the running environment or other environment variables used for node discovery.

### Cluster

EMQX supports a variety of clustering methods, see our [documentation](https://docs.emqx.com/en/emqx/latest/deploy/cluster/create-cluster.html) for details.

Let's create a static node list cluster from docker-compose.

-	Create `docker-compose.yaml`:

```yaml
  version: '3'

  services:
    emqx1:
      image: emqx/emqx:latest
      environment:
      - "EMQX_NODE__NAME=emqx@node1.emqx.io"
      - "EMQX_CLUSTER__DISCOVERY_STRATEGY=static"
      - "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io, emqx@node2.emqx.io]"
      networks:
        emqx-bridge:
          aliases:
          - node1.emqx.io

    emqx2:
      image: emqx/emqx:latest
      environment:
      - "EMQX_NODE__NAME=emqx@node2.emqx.io"
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

-	Start the docker-compose cluster

```bash
  docker-compose -p my_emqx up -d
```

-	View cluster

```bash
  $ docker exec -it my_emqx_emqx1_1 sh -c "emqx ctl cluster status"
  Cluster status: #{running_nodes => ['emqx@node1.emqx.io','emqx@node2.emqx.io'],
                    stopped_nodes => []}
```

### Persistence

If you want to persist the EMQX docker container, you need to keep the following directories:

-	`/opt/emqx/data`
-	`/opt/emqx/log`

Since data in these folders are partially stored under the `/opt/emqx/data/mnesia/${node_name}`, the user also needs to reuse the same node name to see the previous state. To make this work, one needs to set the host part of `EMQX_NODE__NAME` to something static that does not change when you restart or recreate the container. It could be container name, hostname or loopback IP address `127.0.0.1` if you only have one node.

In if you use docker-compose, the configuration would look something like this:

```YAML
volumes:
  vol-emqx-data:
    name: foo-emqx-data
  vol-emqx-log:
    name: foo-emqx-log

services:
  emqx:
    image: emqx/emqx:latest
    restart: always
    environment:
      EMQX_NODE__NAME: foo_emqx@127.0.0.1
    volumes:
      - vol-emqx-data:/opt/emqx/data
      - vol-emqx-log:/opt/emqx/log
```

### Kernel Tuning

Under Linux host machine, the easiest way is [Tuning guide](https://docs.emqx.com/en/emqx/latest/performance/tune.html).

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
