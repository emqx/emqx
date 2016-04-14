
.. _guide:

==========
User Guide
==========

--------------
Authentication
--------------

The emqttd broker supports to authenticate MQTT clients with ClientID, Username/Password, IpAddress and even HTTP Cookies.

The authentication is provided by a list of extended modules, or MySQL, PostgreSQL and Redis Plugins.

Enable an authentication module in etc/emqttd.config:

.. code-block:: erlang

    %% Authentication and Authorization
    {access, [
        %% Authetication. Anonymous Default
        {auth, [
            %% Authentication with username, password
            %{username, []},

            %% Authentication with clientid
            %{clientid, [{password, no}, {file, "etc/clients.config"}]},

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

.. NOTE:: "%" comments the line.

If we enable several modules at the same time, the authentication process::

               ----------------           ----------------           -------------
    Client --> |   Username   | -ignore-> |   ClientID   | -ignore-> | Anonymous |
               ----------------           ----------------           -------------
                      |                         |                         |
                     \|/                       \|/                       \|/
                allow | deny              allow | deny              allow | deny

The authentication plugins developed by emqttd:

+---------------------------+---------------------------+
| Plugin                    | Description               |
+===========================+===========================+
| `emqttd_plugin_mysql`_    | MySQL Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emqttd_plugin_pgsql`_    | PostgreSQL Auth/ACL Plugin|
+---------------------------+---------------------------+
| `emqttd_plugin_redis`_    | Redis Auth/ACL Plugin     |
+---------------------------+---------------------------+

.. NOTE:: If we load an authentication plugin, the authentication modules will be disabled.

Username
--------

Authenticate MQTT client with Username/Password::

    {username, [{client1, "passwd1"}, {client1, "passwd2"}]},

Two ways to add users:

1. Configure username and plain password directly::

    {username, [{client1, "passwd1"}, {client1, "passwd2"}]},

2. Add user by './bin/emqttd_ctl users' command::

   $ ./bin/emqttd_ctl users add <Username> <Password>

ClientId
--------

.. code-block:: erlang

    {clientid, [{password, no}, {file, "etc/clients.config"}]},

Configure ClientIDs in etc/clients.config::

    testclientid0
    testclientid1 127.0.0.1
    testclientid2 192.168.0.1/24

LDAP
----

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

Anonymous
---------

Allow any client to connect to the broker::

    {anonymous, []}

MySQL
-----

Authenticate against MySQL database. Support we create a mqtt_user table:

.. code-block:: sql

    CREATE TABLE `mqtt_user` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `username` varchar(100) DEFAULT NULL,
      `password` varchar(100) DEFAULT NULL,
      `salt` varchar(20) DEFAULT NULL,
      `created` datetime DEFAULT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `mqtt_username` (`username`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;

Configure the 'authquery' and 'password_hash' in emqttd_plugin_mysql/etc/plugin.config:

.. code-block:: erlang

    [

    {emqttd_plugin_mysql, [

        ...

        %% select password only
        {authquery, "select password from mqtt_user where username = '%u' limit 1"},

        %% hash algorithm: md5, sha, sha256, pbkdf2?
        {password_hash, sha256},

        ...

    ]}
    ].

Load the plugin::

    ./bin/emqttd_ctl plugins load emqttd_plugin_mysql


PostgreSQL
----------

Authenticate against PostgreSQL database. Create a mqtt_user table:

.. code-block:: sql

    CREATE TABLE mqtt_user (
      id SERIAL primary key,
      username character varying(100),
      password character varying(100),
      salt character varying(40)
    );

Configure the 'authquery' and 'password_hash' in emqttd_plugin_pgsql/etc/plugin.config:

.. code-block:: erlang

    [

      {emqttd_plugin_pgsql, [

        ...

        %% select password only
        {authquery, "select password from mqtt_user where username = '%u' limit 1"},

        %% hash algorithm: md5, sha, sha256, pbkdf2?
        {password_hash, sha256},

        ...

      ]}
    ].

Load the plugin::

    ./bin/emqttd_ctl plugins load emqttd_plugin_pgsql

Redis
-----

Authenticate against Redis. MQTT users could be stored in redis HASH, the key is "mqtt_user:<Username>".

Configure 'authcmd' and 'password_hash' in emqttd_plugin_redis/etc/plugin.config:

.. code-block:: erlang

    [
      {emqttd_plugin_redis, [

        ...

        %% HMGET mqtt_user:%u password
        {authcmd, ["HGET", "mqtt_user:%u", "password"]},

        %% Password hash algorithm: plain, md5, sha, sha256, pbkdf2?
        {password_hash, sha256},

        ...

      ]}
    ].

Load the plugin::

    ./bin/emqttd_ctl plugins load emqttd_plugin_redis

---
ACL
---

The ACL of emqttd broker is responsbile for authorizing MQTT clients to publish/subscribe topics.

The ACL rules define::

    Allow|Deny Who Publish|Subscribe Topics

Access Control Module of emqttd broker will match the rules one by one::

              ---------              ---------              ---------
    Client -> | Rule1 | --nomatch--> | Rule2 | --nomatch--> | Rule3 | --> Default
              ---------              ---------              ---------
                  |                      |                      |
                match                  match                  match
                 \|/                    \|/                    \|/
            allow | deny           allow | deny           allow | deny

Internal
--------

The default ACL of emqttd broker is implemented by an 'internal' module.

Enable the 'internal' ACL module in etc/emqttd.config:

.. code-block:: erlang

    {acl, [
        %% Internal ACL module
        {internal,  [{file, "etc/acl.config"}, {nomatch, allow}]}
    ]}

The ACL rules of 'internal' module are defined in 'etc/acl.config' file:

.. code-block:: erlang

    %% Allow 'dashboard' to subscribe '$SYS/#'
    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    %% Allow clients from localhost to subscribe any topics
    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    %% Deny clients to subscribe '$SYS#' and '#'
    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    %% Allow all by default
    {allow, all}.

MySQL
-----

ACL against MySQL database. The mqtt_acl table and default data:

.. code-block:: sql

    CREATE TABLE `mqtt_acl` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `allow` int(1) DEFAULT NULL COMMENT '0: deny, 1: allow',
      `ipaddr` varchar(60) DEFAULT NULL COMMENT 'IpAddress',
      `username` varchar(100) DEFAULT NULL COMMENT 'Username',
      `clientid` varchar(100) DEFAULT NULL COMMENT 'ClientId',
      `access` int(2) NOT NULL COMMENT '1: subscribe, 2: publish, 3: pubsub',
      `topic` varchar(100) NOT NULL DEFAULT '' COMMENT 'Topic Filter',
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    INSERT INTO mqtt_acl (id, allow, ipaddr, username, clientid, access, topic)
    VALUES
        (1,1,NULL,'$all',NULL,2,'#'),
        (2,0,NULL,'$all',NULL,1,'$SYS/#'),
        (3,0,NULL,'$all',NULL,1,'eq #'),
        (5,1,'127.0.0.1',NULL,NULL,2,'$SYS/#'),
        (6,1,'127.0.0.1',NULL,NULL,2,'#'),
        (7,1,NULL,'dashboard',NULL,1,'$SYS/#');

Configure 'aclquery' and 'acl_nomatch' in emqttd_plugin_mysql/etc/plugin.config:

.. code-block:: erlang

    [

      {emqttd_plugin_mysql, [

        ...

        %% comment this query, the acl will be disabled
        {aclquery, "select * from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"},

        %% If no rules matched, return...
        {acl_nomatch, allow}

      ]}
    ].

PostgreSQL
----------

ACL against PostgreSQL database. The mqtt_acl table and default data:

.. code-block:: sql

    CREATE TABLE mqtt_acl (
      id SERIAL primary key,
      allow integer,
      ipaddr character varying(60),
      username character varying(100),
      clientid character varying(100),
      access  integer,
      topic character varying(100)
    );

    INSERT INTO mqtt_acl (id, allow, ipaddr, username, clientid, access, topic)
    VALUES
        (1,1,NULL,'$all',NULL,2,'#'),
        (2,0,NULL,'$all',NULL,1,'$SYS/#'),
        (3,0,NULL,'$all',NULL,1,'eq #'),
        (5,1,'127.0.0.1',NULL,NULL,2,'$SYS/#'),
        (6,1,'127.0.0.1',NULL,NULL,2,'#'),
        (7,1,NULL,'dashboard',NULL,1,'$SYS/#');

Configure 'aclquery' and 'acl_nomatch' in emqttd_plugin_pgsql/etc/plugin.config:

.. code-block:: erlang

    [

      {emqttd_plugin_pgsql, [

        ...

        %% Comment this query, the acl will be disabled. Notice: don't edit this query!
        {aclquery, "select allow, ipaddr, username, clientid, access, topic from mqtt_acl
                     where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"},

        %% If no rules matched, return...
        {acl_nomatch, allow}

        ...

      ]}
    ].

Redis
-----

ACL against Redis. We store ACL rules for each MQTT client in a Redis List by defualt. The key is "mqtt_acl:<Username>", the value is a list of "publish <Topic>", "subscribe <Topic>" or "pubsub <Topic>".

Configure 'aclcmd' and 'acl_nomatch' in emqttd_plugin_redis/etc/plugin.config:

.. code-block:: erlang

    [
      {emqttd_plugin_redis, [

        ...

        %% SMEMBERS mqtt_acl:%u
        {aclcmd, ["SMEMBERS", "mqtt_acl:%u"]},

        %% If no rules matched, return...
        {acl_nomatch, deny},

        ...

      ]}
    ].

----------------------
MQTT Publish/Subscribe
----------------------

MQTT is a an extremely lightweight publish/subscribe messaging protocol desgined for IoT, M2M and Mobile applications.

.. image:: _static/images/pubsub_concept.png

Install and start the emqttd broker, and then any MQTT client could connect to the broker, subscribe topics and publish messages.

MQTT Client Libraries: https://github.com/mqtt/mqtt.github.io/wiki/libraries

For example, we use mosquitto_sub/pub commands::

    mosquitto_sub -t topic -q 2
    mosquitto_pub -t topic -q 1 -m "Hello, MQTT!"

MQTT V3.1.1 Protocol Specification: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html

MQTT Listener of emqttd broker is configured in etc/emqttd.config:

.. code-block:: erlang

        {mqtt, 1883, [
            %% Size of acceptor pool
            {acceptors, 16},

            %% Maximum number of concurrent clients
            {max_clients, 512},

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
                {backlog, 512}
            ]}
        ]},

MQTT(SSL) Listener, Default Port is 8883:

.. code-block:: erlang

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

----------------
HTTP Publish API
----------------

The emqttd broker provides a HTTP API to help application servers publish messages to MQTT clients.

HTTP API: POST http://host:8083/mqtt/publish

Web servers such as PHP, Java, Python, NodeJS and Ruby on Rails could use HTTP POST to publish MQTT messages to the broker::

    curl -v --basic -u user:passwd -d "qos=1&retain=0&topic=/a/b/c&message=hello from http..." -k http://localhost:8083/mqtt/publish

Parameters of the HTTP API:

+---------+----------------+
| Name    | Description    |
+=========+================+
| client  | clientid       |
+---------+----------------+
| qos     | QoS(0, 1, 2)   |
+---------+----------------+
| retain  | Retain(0, 1)   |
+---------+----------------+
| topic   | Topic          |
+---------+----------------+
| message | Payload        |
+---------+----------------+

.. NOTE:: The API uses HTTP Basic Authentication.

-------------------
MQTT Over WebSocket
-------------------

Web browsers could connect to the emqttd broker directly by MQTT Over WebSocket.

+-------------------------+----------------------------+
| WebSocket URI:          | ws(s)://host:8083/mqtt     |
+-------------------------+----------------------------+
| Sec-WebSocket-Protocol: | 'mqttv3.1' or 'mqttv3.1.1' |
+-------------------------+----------------------------+

The Dashboard plugin provides a test page for WebSocket::

    http://127.0.0.1:18083/websocket.html

Listener of WebSocket and HTTP Publish API is configured in etc/emqttd.config:

.. code-block:: erlang

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

-----------
$SYS Topics
-----------

The emqttd broker periodically publishes internal status, MQTT statistics, metrics and client online/offline status to $SYS/# topics.

For emqttd broker is clustered, the $SYS topic path is started with::

    $SYS/brokers/${node}/

'${node}' is the erlang node name of emqttd broker. For example::

    $SYS/brokers/emqttd@127.0.0.1/version

    $SYS/brokers/emqttd@host2/uptime

.. NOTE:: The broker only allows clients from localhost to subscribe $SYS topics by default.

Sys Interval of publishing $SYS messages, could be configured in etc/emqttd.config::

    {broker, [
        %% System interval of publishing broker $SYS messages
        {sys_interval, 60},


Broker Version, Uptime and Description
---------------------------------------

+--------------------------------+-----------------------+
| Topic                          | Description           |
+================================+=======================+
| $SYS/brokers                   | Broker nodes          |
+--------------------------------+-----------------------+
| $SYS/brokers/${node}/version   | Broker Version        |
+--------------------------------+-----------------------+
| $SYS/brokers/${node}/uptime    | Broker Uptime         |
+--------------------------------+-----------------------+
| $SYS/brokers/${node}/datetime  | Broker DateTime       |
+--------------------------------+-----------------------+
| $SYS/brokers/${node}/sysdescr  | Broker Description    |
+--------------------------------+-----------------------+

Online/Offline Status of MQTT Client
------------------------------------

The topic path started with: $SYS/brokers/${node}/clients/

+--------------------------+--------------------------------------------+------------------------------------+
| Topic                    | Payload(JSON)                              | Description                        |
+==========================+============================================+====================================+
| ${clientid}/connected    | {ipaddress: "127.0.0.1", username: "test", | Publish when a client connected    |
|                          |  session: false, version: 3, connack: 0,   |                                    |
|                          |  ts: 1432648482}                           |                                    |
+--------------------------+--------------------------------------------+------------------------------------+
| ${clientid}/disconnected | {reason: "keepalive_timeout",              | Publish when a client disconnected |
|                          |  ts: 1432749431}                           |                                    |
+--------------------------+--------------------------------------------+------------------------------------+

Properties of 'connected' Payload::

    ipaddress: "127.0.0.1",
    username:  "test",
    session:   false,
    protocol:  3,
    connack:   0,
    ts:        1432648482

Properties of 'disconnected' Payload::

    reason: normal,
    ts:     1432648486

Broker Statistics
-----------------

Topic path started with: $SYS/brokers/${node}/stats/

Clients
.......

+---------------------+---------------------------------------------+
| Topic               | Description                                 |
+---------------------+---------------------------------------------+
| clients/count       | Count of current connected clients          |
+---------------------+---------------------------------------------+
| clients/max         | Max number of cocurrent connected clients   |
+---------------------+---------------------------------------------+

Sessions
........

+---------------------+---------------------------------------------+
| Topic               | Description                                 |
+---------------------+---------------------------------------------+
| sessions/count      | Count of current sessions                   |
+---------------------+---------------------------------------------+
| sessions/max        | Max number of sessions                      |
+---------------------+---------------------------------------------+

Subscriptions
.............

+---------------------+---------------------------------------------+
| Topic               | Description                                 |
+---------------------+---------------------------------------------+
| subscriptions/count | Count of current subscriptions              |
+---------------------+---------------------------------------------+
| subscriptions/max   | Max number of subscriptions                 |
+---------------------+---------------------------------------------+

Topics
......

+---------------------+---------------------------------------------+
| Topic               | Description                                 |
+---------------------+---------------------------------------------+
| topics/count        | Count of current topics                     |
+---------------------+---------------------------------------------+
| topics/max          | Max number of topics                        |
+---------------------+---------------------------------------------+

Broker Metrics
--------------

Topic path started with: $SYS/brokers/${node}/metrics/

Bytes Sent/Received
...................

+---------------------+---------------------------------------------+
| Topic               | Description                                 |
+---------------------+---------------------------------------------+
| bytes/received      | MQTT Bytes Received since broker started    |
+---------------------+---------------------------------------------+
| bytes/sent          | MQTT Bytes Sent since the broker started    |
+---------------------+---------------------------------------------+

Packets Sent/Received
.....................

+--------------------------+---------------------------------------------+
| Topic                    | Description                                 |
+--------------------------+---------------------------------------------+
| packets/received         | MQTT Packets received                       |
+--------------------------+---------------------------------------------+
| packets/sent             | MQTT Packets sent                           |
+--------------------------+---------------------------------------------+
| packets/connect          | MQTT CONNECT Packet received                |
+--------------------------+---------------------------------------------+
| packets/connack          | MQTT CONNACK Packet sent                    |
+--------------------------+---------------------------------------------+
| packets/publish/received | MQTT PUBLISH packets received               |
+--------------------------+---------------------------------------------+
| packets/publish/sent     | MQTT PUBLISH packets sent                   |
+--------------------------+---------------------------------------------+
| packets/subscribe        | MQTT SUBSCRIBE Packets received             |
+--------------------------+---------------------------------------------+
| packets/suback           | MQTT SUBACK packets sent                    |
+--------------------------+---------------------------------------------+
| packets/unsubscribe      | MQTT UNSUBSCRIBE Packets received           |
+--------------------------+---------------------------------------------+
| packets/unsuback         | MQTT UNSUBACK Packets sent                  |
+--------------------------+---------------------------------------------+
| packets/pingreq          | MQTT PINGREQ packets received               |
+--------------------------+---------------------------------------------+
| packets/pingresp         | MQTT PINGRESP Packets sent                  |
+--------------------------+---------------------------------------------+
| packets/disconnect       | MQTT DISCONNECT Packets received            |
+--------------------------+---------------------------------------------+

Messages Sent/Received
......................

+--------------------------+---------------------------------------------+
| Topic                    | Description                                 |
+--------------------------+---------------------------------------------+
| messages/received        | Messages Received                           |
+--------------------------+---------------------------------------------+
| messages/sent            | Messages Sent                               |
+--------------------------+---------------------------------------------+
| messages/retained        | Messages Retained                           |
+--------------------------+---------------------------------------------+
| messages/stored          | TODO: Messages Stored                       |
+--------------------------+---------------------------------------------+
| messages/dropped         | Messages Dropped                            |
+--------------------------+---------------------------------------------+

Broker Alarms
-------------

Topic path started with: $SYS/brokers/${node}/alarms/

+------------------+------------------+
| Topic            | Description      |
+------------------+------------------+
| ${alarmId}/alert | New Alarm        |
+------------------+------------------+
| ${alarmId}/clear | Clear Alarm      |
+------------------+------------------+

Broker Sysmon
-------------

Topic path started with: '$SYS/brokers/${node}/sysmon/'

+------------------+--------------------+
| Topic            | Description        |
+------------------+--------------------+
| long_gc          | Long GC Warning    |
+------------------+--------------------+
| long_schedule    | Long Schedule      |
+------------------+--------------------+
| large_heap       | Large Heap Warning |
+------------------+--------------------+
| busy_port        | Busy Port Warning  |
+------------------+--------------------+
| busy_dist_port   | Busy Dist Port     |
+------------------+--------------------+


-----
Trace
-----

The emqttd broker supports to trace MQTT packets received/sent from/to a client, or trace MQTT messages published to a topic.

Trace a client::

    ./bin/emqttd_ctl trace client "clientid" "trace_clientid.log"

Trace a topic::

    ./bin/emqttd_ctl trace topic "topic" "trace_topic.log"

Lookup Traces::

    ./bin/emqttd_ctl trace list

Stop a Trace::

    ./bin/emqttd_ctl trace client "clientid" off

    ./bin/emqttd_ctl trace topic "topic" off

.. _emqttd_plugin_mysql:    https://github.com/emqtt/emqttd_plugin_mysql
.. _emqttd_plugin_pgsql:    https://github.com/emqtt/emqttd_plugin_pgsql
.. _emqttd_plugin_redis:    https://github.com/emqtt/emqttd_plugin_redis
