# eMQTT

eMQTT is a scalable, fault-tolerant and extensible mqtt broker written in Erlang/OTP.

eMQTT requires Erlang R17+.

## Startup in Five Minutes

```
	$ git clone git://github.com/slimpp/emqtt.git

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

......

## Admin and Cluster

......

## HTTP API

eMQTT support http to publish message.

Example:

```
	curl -v --basic -u user:passwd -d "topic=/a/b/c&message=hello from http..." -k http://localhost:8883/mqtt/publish
```

### URL

```
	HTTP POST http://host:8883/mqtt/publish
```

### Parameters

Name | Description
-----|-------------
topic | MQTT Topic
message | Text Message

## Design

[Design Wiki](https://github.com/slimpp/emqtt/wiki)

## License

The MIT License (MIT)

## Author

feng at slimchat.io

