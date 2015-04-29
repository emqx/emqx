## Quick Start

## Startup in Five Minutes

```
$ git clone git://github.com/emqtt/emqttd.git

$ cd emqttd

$ make && make dist

$ cd rel/emqttd

$ ./bin/emqttd console
```

## Deploy and Start

### start

```
cp -R rel/emqttd $INSTALL_DIR

cd $INSTALL_DIR/emqttd

./bin/emqttd start

```

### stop

```
./bin/emqttd stop

```

## Configuration

### etc/app.config

```
 {emqttd, [
    {auth, {anonymous, []}}, %internal, anonymous
    {listen, [
        {mqtt, 1883, [
            {acceptors, 4},
            {max_clients, 1024}
        ]},
        {mqtts, 8883, [
            {acceptors, 4},
            {max_clients, 1024},
            %{cacertfile, "etc/ssl/cacert.pem"}, 
            {ssl, [{certfile, "etc/ssl.crt"},
                   {keyfile,  "etc/ssl.key"}]}
        ]},
        {http, 8083, [
            {acceptors, 1},
            {max_clients, 512}
        ]}
    ]}
 ]}

```

### etc/vm.args

```

-name emqttd@127.0.0.1

-setcookie emqtt

```

When nodes clustered, vm.args should be configured as below:

```
-name emqttd@host1
```

## Cluster

Suppose we cluster two nodes on 'host1', 'host2', Steps:

on 'host1':

```
./bin/emqttd start
```

on 'host2':

```
./bin/emqttd start

./bin/emqttd_ctl cluster emqttd@host1
```

Run './bin/emqttd_ctl cluster' on 'host1' or 'host2' to check cluster nodes.

## HTTP API

emqttd support http to publish message.

Example:

```
curl -v --basic -u user:passwd -d "qos=1&retain=0&topic=a/b/c&message=hello from http..." -k http://localhost:8083/mqtt/publish
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


## Contributors

@hejin1026 <260495915 at qq.com>

@desoulter <assoulter123 at gmail.com>

@turtleDeng
