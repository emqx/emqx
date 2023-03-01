# Test influxdb integration

This script starts two EMQX nodes and a influxdb server in docker container.
The bootstraping rule engine and data bridge config is provided in influx-bridge.conf
which got included in the bootstraping config bundle emqx.conf.

## Start the cluster

./start.sh

## How to run tests

The rule and bridge are configured to pipe data from MQTT topic `t/#` to the 'myvalues' measurement in the 'mqtt' bucket.

### Manual verification steps

* Start the cluster
* Send mqtt messages to topic `/t/a` with a JSON object as MQTT paylaod like `{"value": 1}`
* Observe data in influxdb `curl -k -H 'Authorization: Token abcdefg' -G 'https://localhost:8086/query?pretty=true' --data-urlencode "db=mqtt" --data-urlencode "q=SELECT * from myvalues"`

Example output the curl query against influxdb:

```
{"results":[{"statement_id":0,"series":[{"name":"myvalues","columns":["time","clientid","value"],"values":[["2023-02-28T11:13:29.039Z","a1",123]]}]}]
```
