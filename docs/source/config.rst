
.. _configuration:

=============
Configuration
=============

The two main configuration files of the broker are under 'etc/' folder:

+----------------------+-----------------------------------+
| File                 | Description                       |
+----------------------+-----------------------------------+
| releases/2.0/vm.args | Erlang VM Arguments               |
+----------------------+-----------------------------------+
| etc/emqttd.conf      | emqttd broker Config              |
+----------------------+-----------------------------------+

----------------------------
Plugins' Configuration Files
----------------------------

+----------------------------------------+-----------------------------------+
| File                                   | Description                       |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_auth_http.conf      | HTTP Auth/ACL Plugin Config       |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_auth_mongo.conf     | MongoDB Auth/ACL Plugin Config    |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_auth_mysql.conf     | MySQL Auth/ACL Plugin Config      |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_auth_pgsql.conf     | Postgre Auth/ACL Plugin Config    |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_auth_redis.conf     | Redis Auth/ACL Plugin Config      |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_coap.conf           | CoAP Protocol Plugin Config       |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_dashboard.conf      | Dashboard Plugin Config           |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_plugin_template.conf| Template Plugin Config            |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_recon.conf          | Recon Plugin Config               |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_reloader.conf       | Reloader Plugin Config            |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_sn.conf             | MQTT-SN Protocal Plugin Config    |
+----------------------------------------+-----------------------------------+
| etc/plugins/emqttd_stomp.conf          | Stomp Protocl Plugin Config       |
+----------------------------------------+-----------------------------------+

----------------------------
Modules' Configuration Files
----------------------------

The modules' configuration files are in etc/modules/ folder, and referrenced by etc/emqttd.conf:

+----------------------------+-----------------------------------+
| File                       | Description                       |
+----------------------------+-----------------------------------+
| etc/modules/acl.config     | Internal ACL Rules                |
+----------------------------+-----------------------------------+
| etc/modules/client.config  | Config for ClientId Auth Module   |
+----------------------------+-----------------------------------+
| etc/modules/rewrite.config | Config for Rewrite Module         |
+----------------------------+-----------------------------------+
| etc/ssl/*                  | SSL Certfile and Keyfile          |
+-----------------------------+----------------------------------+

--------------------
releases/2.0/vm.args
--------------------

Configure and Optimize Erlang VM::

    ##-------------------------------------------------------------------------
    ## Name of the node: Name@Host
    ##-------------------------------------------------------------------------
    -name emqttd@127.0.0.1

    # or
    #-name emqttd@localhost.

    ## Cookie for distributed erlang
    -setcookie emqttdsecretcookie

    ##-------------------------------------------------------------------------
    ## Flags
    ##-------------------------------------------------------------------------

    ## Heartbeat management; auto-restarts VM if it dies or becomes unresponsive
    ## (Disabled by default..use with caution!)
    ##-heart
    -smp true

    ## Enable kernel poll and a few async threads
    +K true

    ## 12 threads/core.
    +A 48

    ## max process numbers
    +P 8192

    ## Sets the maximum number of simultaneously existing ports for this system
    +Q 8192

    ## max atom number
    ## +t

    ## Set the distribution buffer busy limit (dist_buf_busy_limit) in kilobytes.
    ## Valid range is 1-2097151. Default is 1024.
    ## +zdbbl 8192

    ## CPU Schedulers
    ## +sbt db

    ##-------------------------------------------------------------------------
    ## Env
    ##-------------------------------------------------------------------------

    ## Increase number of concurrent ports/sockets, deprecated in R17
    -env ERL_MAX_PORTS 8192

    -env ERTS_MAX_PORTS 8192

    -env ERL_MAX_ETS_TABLES 1024

    ## Tweak GC to run more often
    -env ERL_FULLSWEEP_AFTER 1000

The two most important parameters in releases/2.0/vm.args:

+-------+---------------------------------------------------------------------------+
| +P    | Max number of Erlang proccesses. A MQTT client consumes two proccesses.   |
|       | The value should be larger than max_clients * 2                           |
+-------+---------------------------------------------------------------------------+
| +Q    | Max number of Erlang Ports. A MQTT client consumes one port.              |
|       | The value should be larger than max_clients.                              |
+-------+---------------------------------------------------------------------------+

The name and cookie of Erlang Node should be configured when clustering::

    -name emqttd@host_or_ip

    ## Cookie for distributed erlang
    -setcookie emqttdsecretcookie

------------------
Log Level and File
------------------

Logger of emqttd broker is implemented by 'lager' application, which is configured in releases/2.0/sys.config:

.. code-block:: erlang

  {lager, [
    ...
  ]},

Configure log handlers:

.. code-block:: erlang

    {handlers, [
        {lager_console_backend, info},

        {lager_file_backend, [
            {formatter_config, [time, " ", pid, " [",severity,"] ", message, "\n"]},
            {file, "log/emqttd_info.log"},
            {level, info},
            {size, 104857600},
            {date, "$D0"},
            {count, 30}
        ]},

        {lager_file_backend, [
            {formatter_config, [time, " ", pid, " [",severity,"] ", message, "\n"]},
            {file, "log/emqttd_error.log"},
            {level, error},
            {size, 104857600},
            {date, "$D0"},
            {count, 30}
        ]}
    ]}

---------------
etc/emqttd.conf
---------------

This is the main configuration file for emqttd broker.

File Syntax
-----------

The file uses the Erlang term syntax which is like rebar.config or relx.config:

1. [ ]: List, seperated by comma
2. { }: Tuple, Usually {Env, Value}
3. %  : comment

MQTT Protocol Parameters
------------------------

Maximum ClientId Length
.......................

.. code-block:: erlang

    %% Max ClientId Length Allowed.
    {mqtt_max_clientid_len, 512}.

Maximum Packet Size
...................

.. code-block:: erlang

    %% Max Packet Size Allowed, 64K by default.
    {mqtt_max_packet_size, 65536}.

MQTT Client Idle Timeout
........................

.. code-block:: erlang

    %% Client Idle Timeout.
    {mqtt_client_idle_timeout, 30}. % Second

Pluggable Authentication
------------------------

The emqttd broker supports pluggable authentication mechanism with a list of modules and plugins.

The broker provides Username, ClientId, LDAP and anonymous authentication modules by default:

.. code-block:: erlang

    %%--------------------------------------------------------------------
    %% Authentication
    %%--------------------------------------------------------------------

    %% Anonymous: Allow all
    {auth, anonymous, []}.

    %% Authentication with username, password
    {auth, username, [{passwd, "etc/modules/passwd.conf"}]}.

    %% Authentication with clientId
    {auth, clientid, [{config, "etc/modules/client.conf"}, {password, no}]}.

The modules enabled at the same time compose an authentication chain::

               ----------------           ----------------           --------------
    Client --> |   Anonymous  | -ignore-> |  Username    | -ignore-> |  ClientID  |
               ----------------           ----------------           --------------
                      |                         |                         |
                     \|/                       \|/                       \|/
                allow | deny              allow | deny              allow | deny

.. NOTE:: There are also MySQL, Postgre, Redis, MongoDB and HTTP Authentication Plugins.

Username Authentication
.......................

.. code-block:: erlang

    %% Authentication with username, password
    {auth, username, [{passwd, "etc/modules/passwd.conf"}]}.

Two ways to configure users:

1. Configure username and plain password in etc/modules/passwd.conf::

    {"user1", "passwd1"}.
    {"user2", "passwd2"}.

2. Add user by './bin/emqttd_ctl users' command::

   $ ./bin/emqttd_ctl users add <Username> <Password>

ClientID Authentication
.......................

.. code-block:: erlang

    %% Authentication with clientId
    {auth, clientid, [{config, "etc/modules/client.conf"}, {password, no}]}.

Configure ClientIDs in etc/clients.config::

    "testclientid0".
    {"testclientid1", "127.0.0.1"}.
    {"testclientid2", "192.168.0.1/24"}.

Anonymous Authentication
........................

Allow any client to connect to the broker::

    %% Anonymous: Allow all
    {auth, anonymous, []}.

ACL(Authorization)
------------------

Enable the default ACL module:

.. code-block:: erlang

    %% Internal ACL config
    {acl, internal, [{config, "etc/modules/acl.conf"}, {nomatch, allow}]}.

Define ACL rules in etc/modules/acl.conf. The rules by default:

.. code-block:: erlang

    %% Allow 'dashboard' to subscribe '$SYS/#'
    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    %% Allow clients from localhost to subscribe any topics
    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    %% Deny clients to subscribe '$SYS#' and '#'
    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    %% Allow all by default
    {allow, all}.

An ACL rule is an Erlang tuple. The Access control module of emqttd broker matches the rule one by one from top to bottom::

              ---------              ---------              ---------
    Client -> | Rule1 | --nomatch--> | Rule2 | --nomatch--> | Rule3 | --> Default
              ---------              ---------              ---------
                  |                      |                      |
                match                  match                  match
                 \|/                    \|/                    \|/
            allow | deny           allow | deny           allow | deny

Sys Interval of Broker
----------------------

.. code-block:: erlang

    %% System interval of publishing $SYS messages
    {broker_sys_interval, 60}.

Retained Message Configuration
------------------------------

Expiration of Retained Message
...............................

.. code:: erlang

    %% Expired after seconds, never expired if 0
    {retained_expired_after, 0}.

Maximum Number of Retained Message
...................................

.. code:: erlang

    %% Max number of retained messages
    {retained_max_message_num, 100000}.

Maximum Size of Retained Message
................................

.. code:: erlang

    %% Max Payload Size of retained message
    {retained_max_playload_size, 65536}.

MQTT Session
------------

.. code-block:: erlang

    %% Max number of QoS 1 and 2 messages that can be “inflight” at one time.
    %% 0 means no limit
    {session_max_inflight, 100}.

    %% Retry interval for redelivering QoS1/2 messages.
    {session_unack_retry_interval, 60}.

    %% Awaiting PUBREL Timeout
    {session_await_rel_timeout, 20}.

    %% Max Packets that Awaiting PUBREL, 0 means no limit
    {session_max_awaiting_rel, 0}.

    %% Statistics Collection Interval(seconds)
    {session_collect_interval, 0}.

    %% Expired after 2 day (unit: minute)
    {session_expired_after, 2880}.

Session parameters:

+------------------------------+----------------------------------------------------------+
| session_max_inflight         | Max number of QoS1/2 messages that can be delivered in   |
|                              | the same time                                            |
+------------------------------+----------------------------------------------------------+
| session_unack_retry_interval | Retry interval for unacked QoS1/2 messages.              |
+------------------------------+----------------------------------------------------------+
| session_await_rel_timeout    | Awaiting PUBREL Timeout                                  |
+------------------------------+----------------------------------------------------------+
| session_max_awaiting_rel     | Max number of Packets that Awaiting PUBREL               |
+------------------------------+----------------------------------------------------------+
| session_collect_interval     | Interval of Statistics Collection                        |
+------------------------------+----------------------------------------------------------+
| session_expired_after        | Expired after (unit: minute)                             |
+------------------------------+----------------------------------------------------------+

MQTT Message Queue
------------------

The message queue of session stores:

1. Offline messages for persistent session.

2. Pending messages for inflight window is full

Queue parameters:

.. code-block:: erlang

    %% Type: simple | priority
    {queue_type, simple}.

    %% Topic Priority: 0~255, Default is 0
    %% {queue_priority, [{"topic/1", 10}, {"topic/2", 8}]}.

    %% Max queue length. Enqueued messages when persistent client disconnected,
    %% or inflight window is full.
    {queue_max_length, infinity}.

    %% Low-water mark of queued messages
    {queue_low_watermark, 0.2}.

    %% High-water mark of queued messages
    {queue_high_watermark, 0.6}.

    %% Queue Qos0 messages?
    {queue_qos0, true}.

+----------------------+---------------------------------------------------+
| queue_type           | Queue type: simple or priority                    |
+----------------------+---------------------------------------------------+
| queue_priority       | Topic priority                                    |
+----------------------+---------------------------------------------------+
| queue_max_length     | Max Queue size, infinity means no limit           |
+----------------------+---------------------------------------------------+
| queue_low_watermark  | Low watermark                                     |
+----------------------+---------------------------------------------------+
| queue_high_watermark | High watermark                                    |
+----------------------+---------------------------------------------------+
| queue_qos0           | If Qos0 message queued?                           |
+----------------------+---------------------------------------------------+

PubSub and Router
-----------------

PubSub Pool Size
................

.. code-block:: erlang

    %% PubSub Pool Size. Default should be scheduler numbers.
    {pubsub_pool_size, 8}.

MQTT Bridge Parameters
----------------------

Max MQueue Size of Bridge
.........................

.. code:: erlang

    %% TODO: Bridge Queue Size
    {bridge_max_queue_len, 10000}.

Ping Interval of Bridge
.......................

.. code:: erlang

    %% Ping Interval of bridge node
    {bridge_ping_down_interval, 1}. % second

Extended Modules
----------------

Presence Module
...............

'presence' module will publish presence message to $SYS topic when a client connected or disconnected:

.. code:: erlang

    %% Client presence management module. Publish presence messages when 
    %% client connected or disconnected.
    {module, presence, [{qos, 0}]}.

Subscription Module
...................

'subscription' module forces the client to subscribe some topics when connected to the broker:

.. code:: erlang

    %% Subscribe topics automatically when client connected
    {module, subscription, [{"$client/$c", 1}]}.

Rewrite Module
..............

'rewrite' module supports to rewrite the topic path:

.. code:: erlang

    %% [Rewrite](https://github.com/emqtt/emqttd/wiki/Rewrite)
    {module, rewrite, [{config, "etc/modules/rewrite.conf"}]}.

Configure rewrite rules in etc/modules/rewrite.conf::

    {topic, "x/#", [
        {rewrite, "^x/y/(.+)$", "z/y/$1"},
        {rewrite, "^x/(.+)$", "y/$1"}
    ]}.

    {topic, "y/+/z/#", [
        {rewrite, "^y/(.+)/z/(.+)$", "y/z/$2"}
    ]}.

Plugins Folder
--------------

.. code:: erlang

    %% Dir of plugins' config
    {plugins_etc_dir, "etc/plugins/"}.

    %% File to store loaded plugin names.
    {plugins_loaded_file, "data/loaded_plugins"}.


TCP Listeners
-------------

Configure the TCP listeners for MQTT, MQTT(SSL) and HTTP Protocols.

The most important parameter is 'max_clients' - max concurrent clients allowed.

The TCP Ports occupied by emqttd broker by default:

+-----------+-----------------------------------+
| 1883      | MQTT Port                         |
+-----------+-----------------------------------+
| 8883      | MQTT(SSL) Port                    |
+-----------+-----------------------------------+
| 8083      | MQTT(WebSocket), HTTP API Port    |
+-----------+-----------------------------------+

.. code-block:: erlang

Listener Parameters:

+-------------+----------------------------------------------------------------+
| acceptors   | TCP Acceptor Pool                                              |
+-------------+----------------------------------------------------------------+
| max_clients | Maximum number of concurrent TCP connections allowed           |
+-------------+----------------------------------------------------------------+
| access      | Access Control by IP, for example: [{allow, "192.168.1.0/24"}] |
+-------------+----------------------------------------------------------------+
| connopts    | Rate Limit Control, for example: {rate_limit, "100,10"}        |
+-------------+----------------------------------------------------------------+
| sockopts    | TCP Socket parameters                                          |
+-------------+----------------------------------------------------------------+

1883 - Plain MQTT
.................

.. code-block:: erlang

    %% Plain MQTT
    {listener, mqtt, 1883, [
        %% Size of acceptor pool
        {acceptors, 16},

        %% Maximum number of concurrent clients
        {max_clients, 512},

        %% Mount point prefix
        %% {mount_point, "prefix/"},

        %% Socket Access Control
        {access, [{allow, all}]},

        %% Connection Options
        {connopts, [
            %% Rate Limit. Format is 'burst, rate', Unit is KB/Sec
            %% {rate_limit, "100,10"} %% 100K burst, 10K rate
        ]},

        %% Socket Options
        {sockopts, [
            %Set buffer if hight thoughtput
            %{recbuf, 4096},
            %{sndbuf, 4096},
            %{buffer, 4096},
            %{nodelay, true},
            {backlog, 1024}
        ]}
    ]}.

8883 - MQTT(SSL) 
................

.. code-block:: erlang

    %% MQTT/SSL
    {listener, mqtts, 8883, [
        %% Size of acceptor pool
        {acceptors, 4},

        %% Maximum number of concurrent clients
        {max_clients, 512},

        %% Mount point prefix
        %% {mount_point, "secure/"},

        %% Socket Access Control
        {access, [{allow, all}]},

        %% SSL certificate and key files
        {ssl, [{certfile, "etc/ssl/ssl.crt"},
               {keyfile,  "etc/ssl/ssl.key"}]},

        %% Socket Options
        {sockopts, [
            {backlog, 1024}
            %{buffer, 4096},
        ]}
    ]}.

8083 - MQTT(WebSocket)
......................

.. code-block:: erlang

    %% HTTP and WebSocket Listener
    {listener, http, 8083, [
        %% Size of acceptor pool
        {acceptors, 4},

        %% Maximum number of concurrent clients
        {max_clients, 64},

        %% Socket Access Control
        {access, [{allow, all}]},

        %% Socket Options
        {sockopts, [
            {backlog, 1024}
            %{buffer, 4096},
        ]}
    ]}.

Erlang VM Monitor
-----------------

.. code:: erlang

    %% Long GC, don't monitor in production mode for:
    %% https://github.com/erlang/otp/blob/feb45017da36be78d4c5784d758ede619fa7bfd3/erts/emulator/beam/erl_gc.c#L421

    {sysmon_long_gc, false}.

    %% Long Schedule(ms)
    {sysmon_long_schedule, 240}.

    %% 8M words. 32MB on 32-bit VM, 64MB on 64-bit VM.
    %% 8 * 1024 * 1024
    {sysmon_large_heap, 8388608}.

    %% Busy Port
    {sysmon_busy_port, false}.

    %% Busy Dist Port
    {sysmon_busy_dist_port, true}.

