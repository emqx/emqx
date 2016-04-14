
.. _tune:

============
Tuning Guide
============

Tuning the Linux Kernel, Networking, Erlang VM and emqttd broker for one million concurrent MQTT connections.

-------------------
Linux Kernel Tuning
-------------------

The system-wide limit on max opened file handles::

    # 2 million system-wide
    sysctl -w fs.file-max=2097152
    sysctl -w fs.nr_open=2097152
    echo 2097152 > /proc/sys/fs/nr_open

The limit on opened file handles for current session::

    ulimit -n 1048576

/etc/sysctl.conf
----------------

Add the 'fs.file-max' to /etc/sysctl.conf, make the changes permanent::

    fs.file-max = 1048576

/etc/security/limits.conf
-------------------------

Persist the limits on opened file handles for users in /etc/security/limits.conf::

    *      soft   nofile      1048576
    *      hard   nofile      1048576

--------------
Network Tuning
--------------

Increase number of incoming connections backlog::

    sysctl -w net.core.somaxconn=32768
    sysctl -w net.ipv4.tcp_max_syn_backlog=16384
    sysctl -w net.core.netdev_max_backlog=16384

Local Port Range::

    sysctl -w net.ipv4.ip_local_port_range="1000 65535"

Read/Write Buffer for TCP connections::

    sysctl -w net.core.rmem_default=262144
    sysctl -w net.core.wmem_default=262144
    sysctl -w net.core.rmem_max=16777216
    sysctl -w net.core.wmem_max=16777216
    sysctl -w net.core.optmem_max=16777216

    #sysctl -w net.ipv4.tcp_mem='16777216 16777216 16777216'
    sysctl -w net.ipv4.tcp_rmem='1024 4096 16777216'
    sysctl -w net.ipv4.tcp_wmem='1024 4096 16777216'

Connection Tracking::

    sysctl -w net.nf_conntrack_max=1000000
    sysctl -w net.netfilter.nf_conntrack_max=1000000
    sysctl -w net.netfilter.nf_conntrack_tcp_timeout_time_wait=30

The TIME-WAIT Buckets Pool, Recycling and Reuse::

    net.ipv4.tcp_max_tw_buckets=1048576
    net.ipv4.tcp_tw_recycle = 1
    net.ipv4.tcp_tw_reuse = 1

Timeout for FIN-WAIT-2 sockets::

    net.ipv4.tcp_fin_timeout = 15

----------------
Erlang VM Tuning
----------------

Tuning and optimize the Erlang VM in etc/vm.args file::

    ## max number of erlang processes
    +P 2097152

    ## Sets the maximum number of simultaneously existing ports for this system
    +Q 1048576

    ## Increase number of concurrent ports/sockets, deprecated in R17
    -env ERL_MAX_PORTS 1048576

    -env ERTS_MAX_PORTS 1048576

    ## Mnesia and SSL will create temporary ets tables.
    -env ERL_MAX_ETS_TABLES 1024

    ## Tweak GC to run more often
    -env ERL_FULLSWEEP_AFTER 1000

-------------
emqttd broker
-------------

Tune the acceptor pool, max_clients limit and sockopts for TCP listener in etc/emqttd.config:

.. code-block:: erlang

    {mqtt, 1883, [
        %% Size of acceptor pool
        {acceptors, 64},

        %% Maximum number of concurrent clients
        {max_clients, 1000000},

        %% Socket Access Control
        {access, [{allow, all}]},

        %% Connection Options
        {connopts, [
            %% Rate Limit. Format is 'burst, rate', Unit is KB/Sec
            %% {rate_limit, "100,10"} %% 100K burst, 10K rate
        ]},
        ...

--------------
Client Machine
--------------

Tune the client machine to benchmark emqttd broker::

    sysctl -w net.ipv4.ip_local_port_range="500 65535"
    sysctl -w fs.file-max=1000000
    echo 1000000 > /proc/sys/fs/nr_open
    ulimit -n 100000

---------------
emqtt_benchmark
---------------

Test tool for concurrent connections: http://github.com/emqtt/emqtt_benchmark
