# emqx-prometheus

EMQ X Prometheus Agent

## push emqx stats/metrics to prometheus PushGateway

```
prometheus.push.gateway.server = http://127.0.0.1:9091

prometheus.interval = 15000
```

## pull emqx stats/metrics

```
Method: GET
Path: api/v4/emqx_prometheus?type=prometheus
params: type: [prometheus| json]

prometheus data

# TYPE erlang_vm_ets_limit gauge
erlang_vm_ets_limit 256000
# TYPE erlang_vm_logical_processors gauge
erlang_vm_logical_processors 4
# TYPE erlang_vm_logical_processors_available gauge
erlang_vm_logical_processors_available NaN
# TYPE erlang_vm_logical_processors_online gauge
erlang_vm_logical_processors_online 4
# TYPE erlang_vm_port_count gauge
erlang_vm_port_count 17
# TYPE erlang_vm_port_limit gauge
erlang_vm_port_limit 1048576


json data

{
  "stats": {key:value},
  "metrics": {key:value},
  "packets": {key:value},
  "messages": {key:value},
  "delivery": {key:value},
  "client": {key:value},
  "session": {key:value}
}

```


License
-------

Apache License Version 2.0

Author
------

EMQ X Team.

