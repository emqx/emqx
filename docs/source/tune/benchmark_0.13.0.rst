0.13.0 Benchmark (2015-11-08)
=============================

Benchmark with the [emqtt_benchmark](https://github.com/emqtt/emqtt_benchmark) tool.

Server: 8 Core, 32G Memory CentOS6.

etc/vm.args
-----------

::

    ## max process numbers
    +P 1000000

    ## Sets the maximum number of simultaneously existing ports for this system
    +Q 409600

    ## max atom number
    ## +t

    ## Set the distribution buffer busy limit (dist_buf_busy_limit) in kilobytes.
    ## Valid range is 1-2097151. Default is 1024.
    ## +zdbbl 8192

    ##-------------------------------------------------------------------------
    ## Env
    ##-------------------------------------------------------------------------

    ## Increase number of concurrent ports/sockets, deprecated in R17
    -env ERL_MAX_PORTS 409600

    -env ERTS_MAX_PORTS 409600


250K Subscribers on five nodes
------------------------------

Create 50K connections on each node and subscribe test/%i topics::

    ./emqtt_bench_sub -h server -p 1883 -c 50000 -i 5 -t test/%i -q 1


Publish 4K msg/sec on server
------------------------------

Publish qos1 messages at the rate of 4K(msg/sec) on the server:: 

    ./emqtt_bench_pub -h localhost -c 40 -i 10 -I 10 -t test/%i -q 1

Each node will receive 4k msg/sec.


Benchmark Result
----------------

250K Connections,
50K Topics,
250K Subscribers,
4K Qos1 Messages/Sec In,
20K Qos1 Messages/Sec Out,
12M+(bps) In, 56M+(bps) Out Traffic

Consumed about 3.1G memory and 400+ CPU.


./bin/emqttd_ctl broker
-----------------------


sysdescr  : Erlang MQTT Broker
version   : 0.13.0
uptime    : 20 minutes, 25 seconds
datetime  : 2015-11-08 10:38:30

./bin/emqttd_ctl broker stats
-----------------------

```
clients/count       : 250040
clients/max         : 250040
queues/count        : 0
queues/max          : 0
retained/count      : 2
retained/max        : 2
sessions/count      : 0
sessions/max        : 0
subscribers/count   : 250000
subscribers/max     : 250000
topics/count        : 50050
topics/max          : 50050
```

./bin/emqttd_ctl listeners
-----------------------

```
listener on http:8083
acceptors       : 4
max_clients     : 64
current_clients : 0
shutdown_count  : []
listener on mqtts:8883
acceptors       : 4
max_clients     : 512
current_clients : 0
shutdown_count  : []
listener on mqtt:1883
acceptors       : 64
max_clients     : 1000000
current_clients : 250040
shutdown_count  : []
listener on http:18083
acceptors       : 4
max_clients     : 512
current_clients : 0
shutdown_count  : []
```

./bin/emqttd_ctl vm
-----------------------

```
cpu/load1               : 13.44
cpu/load5               : 10.43
cpu/load15              : 5.98
memory/total            : 2710277048
memory/processes        : 1420519328
memory/processes_used   : 1419564424
memory/system           : 1289757720
memory/atom             : 512601
memory/atom_used        : 486464
memory/binary           : 380872488
memory/code             : 13077799
memory/ets              : 408483440
process/limit           : 1048576
process/count           : 500353
io/max_fds              : 500000
io/active_fds           : 75
```

./bin/emqttd_ctl recon node_stats
---------------------------------

```
{[{process_count,500353},
{run_queue,41},
{error_logger_queue_len,0},
{memory_total,2698242896},
{memory_procs,1408734784},
{memory_atoms,486706},
{memory_bin,380825008},
{memory_ets,408483456}],
[{bytes_in,256851},
{bytes_out,1202095},
{gc_count,24423},
{gc_words_reclaimed,4092612},
{reductions,1850034},
{scheduler_usage,[{1,0.46640942781698586},
{2,0.5293768498814092},
{3,0.441425019999723},
{4,0.45550895378436373},
{5,0.45318168320081786},
{6,0.4627325387117833},
{7,0.5144161001107628},
{8,0.46406643808409137}]}]}
```
