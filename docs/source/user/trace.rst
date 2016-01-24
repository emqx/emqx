
emqttd could trace packets received/sent from/to specific client, or trace publish/subscribe to specific topic.

emqttd use lager:trace_file api and write trace log to file.

TODO: NOTICE 

Trace client
-------------


```
./bin/emqttd_ctl trace client "ClientId" "trace_clientid.log"
```

Trace topic
-------------

```
./bin/emqttd_ctl trace topic "Topic" "trace_topic.log"
```

Stop Trace
-------------

```
./bin/emqttd_ctl trace client "ClientId" off
./bin/emqttd_ctl trace topic "Topic" off
```

Lookup Traces
-------------

```
./bin/emqttd_ctl trace list
```

