
.. _configuration:

=============
Configuration
=============

Configuration files of the broker are under 'etc/' folder, including:

+-------------------+-----------------------------------+
| File              | Description                       |
+-------------------+-----------------------------------+
| etc/vm.args       | Erlang VM Arguments               |
+-------------------+-----------------------------------+
| etc/emqttd.config | emqttd broker Config              |
+-------------------+-----------------------------------+
| etc/acl.config    | ACL Config                        |
+-------------------+-----------------------------------+
| etc/clients.config| ClientId Authentication           |
+-------------------+-----------------------------------+
| etc/rewrite.config| Rewrite Rules                     |
+-------------------+-----------------------------------+
| etc/ssl/*         | SSL certificate and key files     |
+-------------------+-----------------------------------+

-----------
etc/vm.args
-----------

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

The two most important parameters in etc/vm.args:

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

-----------------
etc/emqttd.config
-----------------

This is the main emqttd broker configuration file.

File Syntax
-----------

The file users the standard Erlang config syntax, consists of a list of erlang applications and their environments.

.. code-block:: erlang

    [{kernel, [
        {start_timer, true},
        {start_pg2, true}
     ]},
     {sasl, [
        {sasl_error_logger, {file, "log/emqttd_sasl.log"}}
     ]},

     ...

     {emqttd, [
        ...
     ]}
    ].

The file adopts Erlang Term Syntax:

1. [ ]: List, seperated by comma
2. { }: Tuple, Usually {Env, Value}
3. %  : comment

Log Level and File
------------------

Logger of emqttd broker is implemented by 'lager' application:

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

emqttd Application
------------------

The MQTT broker is implemented by erlang 'emqttd' application:

.. code-block:: erlang

 {emqttd, [
    %% Authentication and Authorization
    {access, [
        ...
    ]},
    %% MQTT Protocol Options
    {mqtt, [
        ...
    ]},
    %% Broker Options
    {broker, [
        ...
    ]},
    %% Modules
    {modules, [
        ...
    ]},
    %% Plugins
    {plugins, [
        ...
    ]},

    %% Listeners
    {listeners, [
        ...
    ]},

    %% Erlang System Monitor
    {sysmon, [
    ]}
 ]}

Pluggable Authentication
------------------------

The emqttd broker supports pluggable authentication mechanism with a list of modules and plugins.

The broker provides Username, ClientId, LDAP and anonymous authentication modules by default:

.. code-block:: erlang

    %% Authetication. Anonymous Default
    {auth, [
        %% Authentication with username, password
        %% Add users: ./bin/emqttd_ctl users add Username Password
        %% {username, [{"test", "public"}]},

        %% Authentication with clientid
        % {clientid, [{password, no}, {file, "etc/clients.config"}]},

        %% Authentication with LDAP
        % {ldap, [
        %    {servers, ["localhost"]},
        %    {port, 389},
        %    {timeout, 30},
        %    {user_dn, "uid=$u,ou=People,dc=example,dc=com"},
        %    {ssl, fasle},
        %    {sslopts, [
        %        {"certfile", "ssl.crt"},
        %        {"keyfile", "ssl.key"}]}
        % ]},

        %% Allow all
        {anonymous, []}
    ]},

The modules enabled at the same time compose an authentication chain::

               ----------------           ----------------           -------------
    Client --> |   Username   | -ignore-> |   ClientID   | -ignore-> | Anonymous |
               ----------------           ----------------           -------------
                      |                         |                         |
                     \|/                       \|/                       \|/
                allow | deny              allow | deny              allow | deny

.. NOTE:: There are also MySQL、PostgreSQL、Redis、MongoDB Authentication Plugins.

Username Authentication
.......................

.. code-block:: erlang

    {username, [{client1, "passwd1"}, {client2, "passwd2"}]},

Two ways to configure users:

1. Configure username and plain password directly::

    {username, [{client1, "passwd1"}, {client2, "passwd2"}]},

2. Add user by './bin/emqttd_ctl users' command::

   $ ./bin/emqttd_ctl users add <Username> <Password>

ClientID Authentication
.......................

.. code-block:: erlang

    {clientid, [{password, no}, {file, "etc/clients.config"}]},

Configure ClientIDs in etc/clients.config::

    testclientid0
    testclientid1 127.0.0.1
    testclientid2 192.168.0.1/24

LDAP Authentication
...................

.. code-block:: erlang

    {ldap, [
       {servers, ["localhost"]},
       {port, 389},
       {timeout, 30},
       {user_dn, "uid=$u,ou=People,dc=example,dc=com"},
       {ssl, fasle},
       {sslopts, [
           {"certfile", "ssl.crt"},
           {"keyfile", "ssl.key"}]}
    ]},


Anonymous Authentication
........................

Allow any client to connect to the broker::

    {anonymous, []}


ACL
---

Enable the default ACL module:

.. code-block:: erlang

    {acl, [
        %% Internal ACL module
        {internal,  [{file, "etc/acl.config"}, {nomatch, allow}]}
    ]}

MQTT Packet and ClientID
------------------------

.. code-block:: erlang

    {packet, [

        %% Max ClientId Length Allowed
        {max_clientid_len, 1024},

        %% Max Packet Size Allowed, 64K default
        {max_packet_size,  65536}
    ]},

MQTT Client Idle Timeout
------------------------

.. code-block:: erlang

    {client, [
        %% Socket is connected, but no 'CONNECT' packet received
        {idle_timeout, 10}
    ]},

MQTT Session
------------

.. code-block:: erlang

    {session, [
        %% Max number of QoS 1 and 2 messages that can be “in flight” at one time.
        %% 0 means no limit
        {max_inflight, 100},

        %% Retry interval for unacked QoS1/2 messages.
        {unack_retry_interval, 20},

        %% Awaiting PUBREL Timeout
        {await_rel_timeout, 20},

        %% Max Packets that Awaiting PUBREL, 0 means no limit
        {max_awaiting_rel, 0},

        %% Interval of Statistics Collection(seconds)
        {collect_interval, 20},

        %% Expired after 2 days
        {expired_after, 48}

    ]},

Session parameters:

+----------------------+----------------------------------------------------------+
| max_inflight         | Max number of QoS1/2 messages that can be delivered in   |
|                      | the same time                                            |
+----------------------+----------------------------------------------------------+
| unack_retry_interval | Retry interval for unacked QoS1/2 messages.              |
+----------------------+----------------------------------------------------------+
| await_rel_timeout    | Awaiting PUBREL Timeout                                  |
+----------------------+----------------------------------------------------------+
| max_awaiting_rel     | Max number of Packets that Awaiting PUBREL               |
+----------------------+----------------------------------------------------------+
| collect_interval     | Interval of Statistics Collection                        |
+----------------------+----------------------------------------------------------+
| expired_after        | Expired after                                            |
+----------------------+----------------------------------------------------------+

MQTT Message Queue
------------------

The message queue of session stores:

1. Offline messages for persistent session.

2. Pending messages for inflight window is full

Queue parameters:

.. code-block:: erlang

    {queue, [
        %% simple | priority
        {type, simple},

        %% Topic Priority: 0~255, Default is 0
        %% {priority, [{"topic/1", 10}, {"topic/2", 8}]},

        %% Max queue length. Enqueued messages when persistent client disconnected,
        %% or inflight window is full.
        {max_length, infinity},

        %% Low-water mark of queued messages
        {low_watermark, 0.2},

        %% High-water mark of queued messages
        {high_watermark, 0.6},

        %% Queue Qos0 messages?
        {queue_qos0, true}
    ]}

+----------------------+---------------------------------------------------+
| type                 | Queue type: simple or priority                    |
+----------------------+---------------------------------------------------+
| priority             | Topic priority                                    |
+----------------------+---------------------------------------------------+
| max_length           | Max Queue size, infinity means no limit           |
+----------------------+---------------------------------------------------+
| low_watermark        | Low watermark                                     |
+----------------------+---------------------------------------------------+
| high_watermark       | High watermark                                    |
+----------------------+---------------------------------------------------+
| queue_qos0           | If Qos0 message queued?                           |
+----------------------+---------------------------------------------------+

Sys Interval of Broker
-----------------------

.. code-block:: erlang

    %% System interval of publishing $SYS messages
    {sys_interval, 60},

Retained messages
-----------------

.. code-block:: erlang

    {retained, [
        %% Expired after seconds, never expired if 0
        {expired_after, 0},

        %% Maximum number of retained messages
        {max_message_num, 100000},

        %% Max Payload Size of retained message
        {max_playload_size, 65536}
    ]},

PubSub and Router
-----------------

.. code-block:: erlang

    {pubsub, [
        %% PubSub Pool
        {pool_size, 8},

        %% Subscription: true | false
        {subscription, true},

        %% Route aging time(seconds)
        {route_aging, 5}
    ]},

Bridge Parameters
-----------------

.. code-block:: erlang

    {bridge, [
        %% Bridge Queue Size
        {max_queue_len, 10000},

        %% Ping Interval of bridge node
        {ping_down_interval, 1}
    ]}


Enable Modules
--------------

'presence' module will publish presence message to $SYS topic when a client connected or disconnected::

    {presence, [{qos, 0}]},

'subscription' module forces the client to subscribe some topics when connected to the broker:

.. code-block:: erlang

        %% Subscribe topics automatically when client connected
        {subscription, [
            %% Subscription from stored table
            stored,

            %% $u will be replaced with username
            {"$Q/username/$u", 1},

            %% $c will be replaced with clientid
            {"$Q/client/$c", 1}
        ]}

'rewrite' module supports to rewrite the topic path:

.. code-block:: erlang

        %% Rewrite rules
        {rewrite, [{file, "etc/rewrite.config"}]}

Plugins Folder
--------------

.. code-block:: erlang

    {plugins, [
        %% Plugin App Library Dir
        {plugins_dir, "./plugins"},

        %% File to store loaded plugin names.
        {loaded_file, "./data/loaded_plugins"}
    ]},


TCP Listeners
-------------

Congfigure the TCP listeners for MQTT, MQTT(SSL) and HTTP Protocols.

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

    {listeners, [

        {mqtt, 1883, [
            %% Size of acceptor pool
            {acceptors, 16},

            %% Maximum number of concurrent clients
            {max_clients, 8192},

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
        ]},

        {mqtts, 8883, [
            %% Size of acceptor pool
            {acceptors, 4},

            %% Maximum number of concurrent clients
            {max_clients, 512},

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
        ]},
        %% WebSocket over HTTPS Listener
        %% {https, 8083, [
        %%  %% Size of acceptor pool
        %%  {acceptors, 4},
        %%  %% Maximum number of concurrent clients
        %%  {max_clients, 512},
        %%  %% Socket Access Control
        %%  {access, [{allow, all}]},
        %%  %% SSL certificate and key files
        %%  {ssl, [{certfile, "etc/ssl/ssl.crt"},
        %%         {keyfile,  "etc/ssl/ssl.key"}]},
        %%  %% Socket Options
        %%  {sockopts, [
        %%      %{buffer, 4096},
        %%      {backlog, 1024}
        %%  ]}
        %%]},

        %% HTTP and WebSocket Listener
        {http, 8083, [
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
        ]}
    ]},

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

.. _config_acl:

--------------
etc/acl.config
--------------

The 'etc/acl.config' is the default ACL config for emqttd broker. The rules by default:

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

.. _config_rewrite:

------------------
etc/clients.config
------------------

Enable ClientId Authentication in 'etc/emqttd.config':

.. code-block:: erlang

    {auth, [
        %% Authentication with clientid
        {clientid, [{password, no}, {file, "etc/clients.config"}]}
    ]},

Configure all allowed ClientIDs, IP Addresses in etc/clients.config::

    testclientid0
    testclientid1 127.0.0.1
    testclientid2 192.168.0.1/24

------------------
etc/rewrite.config
------------------

The Rewrite Rules for emqttd_mod_rewrite:

.. code-block:: erlang

    {topic, "x/#", [
        {rewrite, "^x/y/(.+)$", "z/y/$1"},
        {rewrite, "^x/(.+)$", "y/$1"}
    ]}.

    {topic, "y/+/z/#", [
        {rewrite, "^y/(.+)/z/(.+)$", "y/z/$2"}
    ]}.
