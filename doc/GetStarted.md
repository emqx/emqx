# eMQTT Get Started

## Overview

eMQTT is a clusterable, massively scalable, fault-tolerant and extensible MQTT V3.1/V3.1.1 broker written in Erlang/OTP. 

eMQTT is aimed to provide a solid-stable broker that could be clusterd to support millions of connections and clients.

## Requires

eMQTT is cross-platform, could run on windows, linux, freebsd and mac os x.

eMQTT requires Erlang R17+ to build from source.

## Featues

### Full MQTT V3.1.1 Support

MQTT V3.1.1 and V3.1 protocol support

QoS0, QoS1, QoS2 Publish and Subscribe

Session Management and Offline Messages

Retained Messages

Passed eclipse paho interoperability tests

### Clusterable, Massively Scalable

Massive Connections Clients Support

Cluster brokers on servers or cloud hosts

Bridge brokers locally or remotelly

## Download, Install

### Download

Dowload binary packages from [http://emqtt.io/downloads](http://emqtt.io/downloads].

Please build from source if no packages for your platform, or contact us.

### Install

Extract tgz package to your installed directory. for example:

```
tar xvf emqtt-ubuntu64-0.3.0-beta.tgz && cd emqtt  
```

### Startup

Startup console for debug:

```
cd emqtt && ./bin/emqtt console
```

You could see all RECV/SENT MQTT Packages on console.

Start as daemon:

```
cd emqtt && ./bin/emqtt start
```

eMQTT occupies 1883 port for MQTT, 8083 for HTTP API.

### Status

```
cd emqtt && ./bin/emqtt_ctl status
```

### Stop

```
cd emqtt && ./bin/emqtt stop
```

## Configuration

### etc/app.config

```
{emqtt, [
    %Authetication. Internal, Anonymous Default.
    {auth, {anonymous, []}},
    {access, []},
    {session, [
        {expires, 1}, %hours
        {max_queue, 1000},
        {store_qos0, false}
    ]},
    {retain, [
        {store_limit, 100000}
    ]},
    {listen, [
        {mqtt, 1883, [
            {acceptors, 4},
            {max_conns, 1024}
        ]},
        {http, 8083, [
            {acceptors, 1},
            {max_conns, 512}
        ]}
    ]}
]}
```

### etc/vm.args

```
-name emqtt@127.0.0.1

-setcookie emqtt
```

## Cluster

Suppose we cluster two nodes on 'host1', 'host2', Steps:

### configure and start node on host1  

configure 'etc/vm.args':

```
-name emqtt@host1
```

then start:

```
./bin/emqtt start
```

### configure and start node on host2

configure 'etc/vm.args':

```
-name emqtt@host2
```

```
./bin/emqtt start
```

### cluster two nodes

Cluster from 'host2':

```
./bin/emqtt_ctl cluster emqtt@host1
```

or cluster from 'host1':

```
./bin/emqtt_ctl cluster emqtt@host2
```

then check clustered nodes on any host:

```
./bin/emqtt_ctl cluster
```

## HTTP API

eMQTT support HTTP API to publish message from your APP to MQTT client.

Example:

```
curl -v --basic -u user:passwd -d "qos=1&retain=0&topic=/a/b/c&message=hello from http..." -k http://localhost:8083/mqtt/publish
```

### URL

```
HTTP POST http://host:8083/mqtt/publish
```

### Parameters

Name    |  Description
--------|---------------
qos     |  QoS(0, 1, 2)
retain  |  Retain(0, 1)
topic   |  Topic
message |  Message

## Contact 

feng@emqtt.io

