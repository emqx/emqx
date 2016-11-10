
.. _configuration:

=============
Configuration
=============

The main configuration files of the EMQ broker are under 'etc/' folder:

+----------------------+-----------------------------------+
| File                 | Description                       |
+----------------------+-----------------------------------+
| etc/emq.conf         | EMQ 2.0 Configuration File        |
+----------------------+-----------------------------------+
| etc/acl.conf         | The default ACL File              |
+----------------------+-----------------------------------+
| etc/plugins/\*.conf  | Config Files of Plugins           |
+----------------------+-----------------------------------+

---------------------
EMQ 2.0 Config Syntax
---------------------

The *EMQ* 2.0-rc.2 release integrated with `cuttlefish` library, and adopt a more user-friendly `k = v` syntax for configuration file:

.. code-block:: properties

    ## Node name
    node.name = emqttd@127.0.0.1
    ...
    ## Max ClientId Length Allowed.
    mqtt.max_clientid_len = 1024
    ...

The configuration files will be preprocessed and translated to Erlang `app.config` before the EMQ broker started::

    ----------------------                                          2.0/schema/*.schema      -------------------
    | etc/emq.conf       |                   -----------------              \|/              | data/app.config |
    |       +            | --> mergeconf --> | data/app.conf | -->  cuttlefish generate  --> |                 |
    | etc/plugins/*.conf |                   -----------------                               | data/vm.args    |
    ----------------------                                                                   -------------------

------------------------
OS Environment Variables
------------------------

+-------------------+----------------------------------------+
| EMQ_NODE_NAME     | Erlang node name                       |
+-------------------+----------------------------------------+
| EMQ_NODE_COOKIE   | Cookie for distributed erlang node     |
+-------------------+----------------------------------------+
| EMQ_MAX_PORTS     | Maximum number of opened sockets       |
+-------------------+----------------------------------------+
| EMQ_TCP_PORT      | MQTT TCP Listener Port, Default: 1883  |
+-------------------+----------------------------------------+
| EMQ_SSL_PORT      | MQTT SSL Listener Port, Default: 8883  |
+-------------------+----------------------------------------+
| EMQ_HTTP_PORT     | HTTP/WebSocket Port, Default: 8083     |
+-------------------+----------------------------------------+
| EMQ_HTTPS_PORT    | HTTPS/WebSocket Port, Default: 8084    |
+-------------------+----------------------------------------+

-------------------
EMQ Node and Cookie
-------------------

The node name and cookie of *EMQ* should be configured when clustering:

.. code-block:: properties

    ## Node name
    node.name = emqttd@127.0.0.1

    ## Cookie for distributed node
    node.cookie = emq_dist_cookie

-------------------
Erlang VM Arguments
-------------------

Configure and Optimize Erlang VM:

.. code-block:: properties

    ## SMP support: enable, auto, disable
    node.smp = auto

    ## Enable kernel poll
    node.kernel_poll = on

    ## async thread pool
    node.async_threads = 32

    ## Erlang Process Limit
    node.process_limit = 256000

    ## Sets the maximum number of simultaneously existing ports for this system
    node.max_ports = 65536

    ## Set the distribution buffer busy limit (dist_buf_busy_limit)
    node.dist_buffer_size = 32MB

    ## Max ETS Tables.
    ## Note that mnesia and SSL will create temporary ets tables.
    node.max_ets_tables = 256000

    ## Tweak GC to run more often
    node.fullsweep_after = 1000

    ## Crash dump
    node.crash_dump = log/crash.dump

    ## Distributed node ticktime
    node.dist_net_ticktime = 60

    ## Distributed node port range
    ## node.dist_listen_min = 6000
    ## node.dist_listen_max = 6999

The two most important parameters for Erlang VM:

+--------------------------+---------------------------------------------------------------------------+
| node.process_limit       | Max number of Erlang proccesses. A MQTT client consumes two proccesses.   |
|                          | The value should be larger than max_clients * 2                           |
+--------------------------+---------------------------------------------------------------------------+
| node.max_ports           | Max number of Erlang Ports. A MQTT client consumes one port.              |
|                          | The value should be larger than max_clients.                              |
+--------------------------+---------------------------------------------------------------------------+

------------------
Log Level and File
------------------

Console Log
-----------

.. code-block:: properties

    ## Console log. Enum: off, file, console, both
    log.console = console

    ## Console log level. Enum: debug, info, notice, warning, error, critical, alert, emergency
    log.console.level = error

    ## Console log file
    ## log.console.file = log/console.log

Error Log
---------

.. code-block:: properties

    ## Error log file
    log.error.file = log/error.log

Crash Log
---------

.. code-block:: properties

    ## Enable the crash log. Enum: on, off
    log.crash = on

    log.crash.file = log/crash.log

------------------------
MQTT Protocol Parameters
------------------------

Maximum ClientId Length
-----------------------

.. code-block:: properties

    ## Max ClientId Length Allowed.
    mqtt.max_clientid_len = 1024

Maximum Packet Size
-------------------

.. code-block:: properties

    ## Max Packet Size Allowed, 64K by default.
    mqtt.max_packet_size = 64KB

MQTT Client Idle Timeout
------------------------

.. code-block:: properties

    ## Client Idle Timeout (Second)
    mqtt.client_idle_timeout = 30

----------------------------
Allow Anonymous and ACL File
----------------------------

Allow Anonymous 
---------------

.. code-block:: properties

    ## Allow Anonymous authentication
    mqtt.allow_anonymous = true

Default ACL File
----------------

Enable the default ACL module:

.. code-block:: properties

    ## Default ACL File
    mqtt.acl_file = etc/acl.conf

Define ACL rules in etc/acl.conf. The rules by default:

.. code-block:: erlang

    %% Allow 'dashboard' to subscribe '$SYS/#'
    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    %% Allow clients from localhost to subscribe any topics
    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    %% Deny clients to subscribe '$SYS#' and '#'
    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    %% Allow all by default
    {allow, all}.

An ACL rule is an Erlang tuple. The Access control module of *EMQ* broker matches the rule one by one from top to bottom::

              ---------              ---------              ---------
    Client -> | Rule1 | --nomatch--> | Rule2 | --nomatch--> | Rule3 | --> Default
              ---------              ---------              ---------
                  |                      |                      |
                match                  match                  match
                 \|/                    \|/                    \|/
            allow | deny           allow | deny           allow | deny

-----------------------
MQTT Session Parameters
-----------------------

.. code-block:: properties

    ## Max number of QoS 1 and 2 messages that can be “inflight” at one time.
    ## 0 means no limit
    mqtt.session.max_inflight = 100

    ## Retry interval for redelivering QoS1/2 messages.
    mqtt.session.retry_interval = 60

    ## Awaiting PUBREL Timeout
    mqtt.session.await_rel_timeout = 20

    ## Max Packets that Awaiting PUBREL, 0 means no limit
    mqtt.session.max_awaiting_rel = 0

    ## Statistics Collection Interval(seconds)
    mqtt.session.collect_interval = 0

    ## Expired after 1 day:
    ## w - week
    ## d - day
    ## h - hour
    ## m - minute
    ## s - second
    mqtt.session.expired_after = 1d

+------------------------------+----------------------------------------------------------+
| session.max_inflight         | Max number of QoS1/2 messages that can be delivered in   |
|                              | the same time                                            |
+------------------------------+----------------------------------------------------------+
| session.retry_interval       | Retry interval for unacked QoS1/2 messages.              |
+------------------------------+----------------------------------------------------------+
| session.await_rel_timeout    | Awaiting PUBREL Timeout                                  |
+------------------------------+----------------------------------------------------------+
| session.max_awaiting_rel     | Max number of Packets that Awaiting PUBREL               |
+------------------------------+----------------------------------------------------------+
| session.collect_interval     | Interval of Statistics Collection                        |
+------------------------------+----------------------------------------------------------+
| session.expired_after        | Expired after (unit: minute)                             |
+------------------------------+----------------------------------------------------------+

------------------
MQTT Message Queue
------------------

The message queue of session stores:

1. Offline messages for persistent session.

2. Pending messages for inflight window is full

Queue parameters:

.. code-block:: properties

    ## Type: simple | priority
    mqtt.queue.type = simple

    ## Topic Priority: 0~255, Default is 0
    ## mqtt.queue.priority = topic/1=10,topic/2=8

    ## Max queue length. Enqueued messages when persistent client disconnected,
    ## or inflight window is full.
    mqtt.queue.max_length = infinity

    ## Low-water mark of queued messages
    mqtt.queue.low_watermark = 20%

    ## High-water mark of queued messages
    mqtt.queue.high_watermark = 60%

    ## Queue Qos0 messages?
    mqtt.queue.qos0 = true

+----------------------+---------------------------------------------------+
| queue.type           | Queue type: simple or priority                    |
+----------------------+---------------------------------------------------+
| queue.priority       | Topic priority                                    |
+----------------------+---------------------------------------------------+
| queue.max_length     | Max Queue size, infinity means no limit           |
+----------------------+---------------------------------------------------+
| queue.low_watermark  | Low watermark                                     |
+----------------------+---------------------------------------------------+
| queue.high_watermark | High watermark                                    |
+----------------------+---------------------------------------------------+
| queue.qos0           | If Qos0 message queued?                           |
+----------------------+---------------------------------------------------+

----------------------
Sys Interval of Broker
----------------------

.. code-block:: properties

    ## System Interval of publishing broker $SYS Messages
    mqtt.broker.sys_interval = 60

-----------------
PubSub Parameters
-----------------

.. code-block:: properties

    ## PubSub Pool Size. Default should be scheduler numbers.
    mqtt.pubsub.pool_size = 8

    mqtt.pubsub.by_clientid = true

    ##TODO: Subscribe Asynchronously
    mqtt.pubsub.async = true

----------------------
MQTT Bridge Parameters
----------------------

.. code-block:: properties

    ## Bridge Queue Size
    mqtt.bridge.max_queue_len = 10000

    ## Ping Interval of bridge node. Unit: Second
    mqtt.bridge.ping_down_interval = 1

-------------------
Plugins' Etc Folder
-------------------

.. code-block:: properties

    ## Dir of plugins' config
    mqtt.plugins.etc_dir = etc/plugins/

    ## File to store loaded plugin names.
    mqtt.plugins.loaded_file = data/loaded_plugins

--------------
MQTT Listeners
--------------

Configure the TCP listeners for MQTT, MQTT(SSL), HTTP and HTTPS Protocols.

The most important parameter for MQTT listener is `max_clients`: max concurrent clients allowed.

The TCP Ports occupied by the *EMQ* broker by default:

+-----------+-----------------------------------+
| 1883      | MQTT Port                         |
+-----------+-----------------------------------+
| 8883      | MQTT(SSL) Port                    |
+-----------+-----------------------------------+
| 8083      | MQTT(WebSocket), HTTP API Port    |
+-----------+-----------------------------------+

Listener Parameters:

+-----------------------------+-------------------------------------------------------+
| mqtt.listener.*.acceptors   | TCP Acceptor Pool                                     |
+-----------------------------+-------------------------------------------------------+
| mqtt.listener.*.max_clients | Maximum number of concurrent TCP connections allowed  |
+-----------------------------+-------------------------------------------------------+
| mqtt.listener.*.rate_limit  | Maximum number of concurrent TCP connections allowed  |
+-----------------------------+-------------------------------------------------------+

TCP Listener - 1883
-------------------

.. code-block:: properties

    ## TCP Listener: 1883, 127.0.0.1:1883, ::1:1883
    mqtt.listener.tcp = 1883

    ## Size of acceptor pool
    mqtt.listener.tcp.acceptors = 8

    ## Maximum number of concurrent clients
    mqtt.listener.tcp.max_clients = 1024

    ## Rate Limit. Format is 'burst,rate', Unit is KB/Sec
    ## mqtt.listener.tcp.rate_limit = 100,10

    ## TCP Socket Options
    mqtt.listener.tcp.backlog = 1024
    ## mqtt.listener.tcp.recbuf = 4096
    ## mqtt.listener.tcp.sndbuf = 4096
    ## mqtt.listener.tcp.buffer = 4096
    ## mqtt.listener.tcp.nodelay = true

SSL Listener - 8883
-------------------

.. code-block:: properties

    ## SSL Listener: 8883, 127.0.0.1:8883, ::1:8883
    mqtt.listener.ssl = 8883

    ## Size of acceptor pool
    mqtt.listener.ssl.acceptors = 4

    ## Maximum number of concurrent clients
    mqtt.listener.ssl.max_clients = 512

    ## Rate Limit. Format is 'burst,rate', Unit is KB/Sec
    ## mqtt.listener.ssl.rate_limit = 100,10

    ## Configuring SSL Options
    mqtt.listener.ssl.handshake_timeout = 15
    mqtt.listener.ssl.keyfile = etc/certs/key.pem
    mqtt.listener.ssl.certfile = etc/certs/cert.pem
    ## mqtt.listener.ssl.cacertfile = etc/certs/cacert.pem
    ## mqtt.listener.ssl.verify = verify_peer
    ## mqtt.listener.ssl.fail_if_no_peer_cert = true

HTTP/WS Listener - 8083
-----------------------

.. code-block:: properties

    ## HTTP and WebSocket Listener
    mqtt.listener.http = 8083
    mqtt.listener.http.acceptors = 4
    mqtt.listener.http.max_clients = 64

HTTPS/WSS Listener - 8084
-------------------------

.. code-block:: properties

    ## HTTP(SSL) Listener
    mqtt.listener.https = 8084
    mqtt.listener.https.acceptors = 4
    mqtt.listener.https.max_clients = 64
    mqtt.listener.https.handshake_timeout = 15
    mqtt.listener.https.certfile = etc/certs/cert.pem
    mqtt.listener.https.keyfile = etc/certs/key.pem
    ## mqtt.listener.https.cacertfile = etc/certs/cacert.pem
    ## mqtt.listener.https.verify = verify_peer
    ## mqtt.listener.https.fail_if_no_peer_cert = true

--------------
System Monitor
--------------

.. code-block:: properties

    ## Long GC, don't monitor in production mode for:
    sysmon.long_gc = false

    ## Long Schedule(ms)
    sysmon.long_schedule = 240

    ## 8M words. 32MB on 32-bit VM, 64MB on 64-bit VM.
    sysmon.large_heap = 8MB

    ## Busy Port
    sysmon.busy_port = false

    ## Busy Dist Port
    sysmon.busy_dist_port = true

--------------------------
Plugin Configuration Files
--------------------------

+----------------------------------------+-----------------------------------+
| File                                   | Description                       |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_username.conf     | Username/Password Auth Plugin     |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_clientid.conf     | ClientId Auth Plugin              |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_http.conf         | HTTP Auth/ACL Plugin Config       |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_mongo.conf        | MongoDB Auth/ACL Plugin Config    |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_mysql.conf        | MySQL Auth/ACL Plugin Config      |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_pgsql.conf        | Postgre Auth/ACL Plugin Config    |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_auth_redis.conf        | Redis Auth/ACL Plugin Config      |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_coap.conf              | CoAP Protocol Plugin Config       |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_mod_presence.conf      | Presence Module Config            |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_mod_retainer.conf      | Retainer Module Config            |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_mod_rewrite.config     | Rewrite Module Config             |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_mod_subscription.conf  | Subscription Module Config        |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_dashboard.conf         | Dashboard Plugin Config           |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_plugin_template.conf   | Template Plugin Config            |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_recon.conf             | Recon Plugin Config               |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_reloader.conf          | Reloader Plugin Config            |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_sn.conf                | MQTT-SN Protocal Plugin Config    |
+----------------------------------------+-----------------------------------+
| etc/plugins/emq_stomp.conf             | Stomp Protocl Plugin Config       |
+----------------------------------------+-----------------------------------+

