# eMQTT [![Build Status](https://travis-ci.org/emqtt/emqtt.svg?branch=master)](https://travis-ci.org/emqtt/emqtt)

eMQTT is a clusterable, massively scalable, fault-tolerant and extensible MQTT V3.1/V3.1.1 broker written in Erlang/OTP.

eMQTT support MQTT V3.1/V3.1.1 Protocol Specification.

eMQTT requires Erlang R17+.


## Featues

Full MQTT V3.1.1 Support

Both V3.1.1 and V3.1 protocol support

QoS0, QoS1, QoS2 Publish and Subscribe

Session Management and Offline Messages

Retained Messages

TCP/SSL connection support

[$SYS/borkers/#](https://github.com/emqtt/emqtt/wiki/$SYS-Topics-of-Broker) support

Passed eclipse paho interoperability tests


## Clusterable, Massively Scalable

Massive Connections Clients Support

Cluster brokers on servers or cloud hosts

Bridge brokers locally or remotelly


## Startup in Five Minutes

```
$ git clone git://github.com/emqtt/emqtt.git

$ cd emqtt

$ make && make dist

$ cd rel/emqtt

$ ./bin/emqtt console
```

## Deploy and Start

### start

```
cp -R rel/emqtt $INSTALL_DIR

cd $INSTALL_DIR/emqtt

./bin/emqtt start

```

### stop

```
./bin/emqtt stop

```

## Configuration

### etc/app.config

```
 {emqtt, [
    {auth, {anonymous, []}}, %internal, anonymous
    {listen, [
        {mqtt, 1883, [
            {max_clients, 1024},
            {acceptor_pool, 4}
        ]},
        {mqtts, 8883, [
            {max_clients, 1024},
            {acceptor_pool, 4},
            %{cacertfile, "etc/ssl/cacert.pem"}, 
            {ssl, [{certfile, "etc/ssl.crt"},
                   {keyfile,  "etc/ssl.key"}]}
        ]},
        {http, 8083, [
            {max_clients, 512},
            {acceptor_pool, 1}
        ]}
    ]}
 ]}

```

### etc/vm.args

```

-name emqtt@127.0.0.1

-setcookie emqtt

```

When nodes clustered, vm.args should be configured as below:

```
-name emqtt@host1
```

## Cluster

Suppose we cluster two nodes on 'host1', 'host2', Steps:

on 'host1':

```
./bin/emqtt start
```

on 'host2':

```
./bin/emqtt start

./bin/emqtt_ctl cluster emqtt@host1
```

Run './bin/emqtt_ctl cluster' on 'host1' or 'host2' to check cluster nodes.

## HTTP API

eMQTT support http to publish message.

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

## Design

[Design Wiki](https://github.com/emqtt/emqtt/wiki)

## License

The MIT License (MIT)

## Author

feng at emqtt.io

## Thanks

@hejin1026 (260495915 at qq.com)

@desoulter (assoulter123 at gmail.com)

