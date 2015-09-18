
## 0.10.4 Benchmark

CentOS6 server with 8 cores, 32g memory from qingcloud.com:

```
200K Connections,
200K Topics,
200K Subscribers,
10K+ Messages(Qos0)/Sec,
20Mbps In/Out Traffic
```

Consumed:

```
3g Memory
600% CPU( ERL_FULLSWEEP_AFTER ???)
```

### ./bin/emqttd_ctl broker

```
sysdescr: Erlang MQTT Broker
version: 0.10.4
uptime: 46 minutes, 47 seconds
datetime: 2015-09-18 14:20:45
```

### ./bin/emqttd_ctl vm

```
Load:
  load1:0.08
  load5:0.08
  load15:0.07
Memory:
  total:2376320064
  processes:1207095336
  processes_used:1206601368
  system:1169224728
  atom:504409
  atom_used:472338
  binary:315397328
  code:12818701
  ets:449323168
Process:
  process_limit:1048576
  process_count:397857
IO:
  max_fds:500000
```

### ./bin/emqttd_ctl stats
```
subscribers/max: 199439
topics/count: 199471
clients/count: 199429
topics/max: 199473
queues/count: 0
sessions/count: 0
sessions/max: 0
queues/max: 0
clients/max: 199439
subscribers/count: 199437
```

### ./bin/emqttd_ctl metrics

```
bytes/received: 771568337
bytes/sent: 757716556
messages/dropped: 4820
messages/received: 15919837
messages/retained: 2
messages/sent: 15916669
packets/connack: 195026
packets/connect: 195089
packets/disconnect: 0
packets/pingreq: 0
packets/pingresp: 10
packets/publish/received: 15919836
packets/publish/sent: 15916669
packets/received: 16309943
packets/sent: 16306716
packets/suback: 195013
packets/subscribe: 195017
packets/unsuback: 0
packets/unsubscribe: 0
```

### ./bin/emqttd_ctl listeners

```
listener http:8083
  acceptors: 4
  max_clients: 64
  current_clients: 0
  shutdown_count: [{keepalive_timeout,1}]
listener mqtt:1883
  acceptors: 64
  max_clients: 1000000
  current_clients: 198793
  shutdown_count: [{conn_closed,7},{timeout,21},{keepalive_timeout,765}]
listener http:18083
  acceptors: 4
  max_clients: 512
  current_clients: 4
  shutdown_count: []
```
