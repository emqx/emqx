
.. _guide:

==========
User Guide
==========

--------------
Authentication
--------------

The *EMQ* broker supports to authenticate MQTT clients with ClientID, Username/Password, IpAddress and even HTTP Cookies.

The authentication is provided by a list of plugins such as MySQL, PostgreSQL and Redis...

If we enable several authentication plugins at the same time, the authentication process::

               ----------------           ----------------           -------------
    Client --> |   Username   | -ignore-> |   ClientID   | -ignore-> | Anonymous |
               ----------------           ----------------           -------------
                      |                         |                         |
                     \|/                       \|/                       \|/
                allow | deny              allow | deny              allow | deny

The authentication plugins implemented by default:

+---------------------------+---------------------------+
| Plugin                    | Description               |
+===========================+===========================+
| `emq_auth_clientid`_      | ClientId Auth Plugin      |
+---------------------------+---------------------------+
| `emq_auth_username`_      | Username Auth Plugin      |
+---------------------------+---------------------------+
| `emq_auth_ldap`_          | LDAP Auth Plugin          |
+---------------------------+---------------------------+
| `emq_auth_http`_          | HTTP Auth/ACL Plugin      |
+---------------------------+---------------------------+
| `emq_auth_mysql`_         | MySQL Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emq_auth_pgsql`_         | Postgre Auth/ACL Plugin   |
+---------------------------+---------------------------+
| `emq_auth_redis`_         | Redis Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emq_auth_mongo`_         | MongoDB Auth/ACL Plugin   |
+---------------------------+---------------------------+

---------------
Allow Anonymous
---------------

Configure etc/emq.conf to allow anonymous authentication:

.. code-block:: properties

    ## Allow Anonymous authentication
    mqtt.allow_anonymous = true

Username/Password
-----------------

Authenticate MQTT client with Username/Password::

Configure default users in etc/plugins/emq_auth_username.conf:

.. code-block:: properties

    auth.username.$name=$password

Enable `emq_auth_username`_ plugin:

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emq_auth_username

Add user by './bin/emqttd_ctl users' command::

   $ ./bin/emqttd_ctl users add <Username> <Password>

ClientId
--------

Authentication with MQTT ClientId.

Configure Client Ids in etc/plugins/emq_auth_clientid.conf:

.. code-block:: properties

    auth.clientid.$id=$password

Enable `emq_auth_clientid`_ plugin:

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emq_auth_clientid

LDAP
----

etc/plugins/emq_auth_ldap.conf:

.. code-block:: properties

    auth.ldap.servers = 127.0.0.1

    auth.ldap.port = 389

    auth.ldap.timeout = 30

    auth.ldap.user_dn = uid=%u,ou=People,dc=example,dc=com

    auth.ldap.ssl = false

Enable LDAP plugin::

    ./bin/emqttd_ctl plugins load emq_auth_ldap

HTTP
----

etc/plugins/emq_auth_http.conf:

.. code-block:: properties

    ## Variables: %u = username, %c = clientid, %a = ipaddress, %P = password, %t = topic

    auth.http.auth_req = http://127.0.0.1:8080/mqtt/auth
    auth.http.auth_req.method = post
    auth.http.auth_req.params = clientid=%c,username=%u,password=%P

    auth.http.super_req = http://127.0.0.1:8080/mqtt/superuser
    auth.http.super_req.method = post
    auth.http.super_req.params = clientid=%c,username=%u

Enable HTTP Plugin::

    ./bin/emqttd_ctl plugins load emq_auth_http

MySQL
-----

Authenticate with MySQL database. Suppose that we create a mqtt_user table:

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

Configure the 'auth_query' and 'password_hash' in etc/plugins/emq_auth_mysql.conf:

.. code-block:: properties

    ## Mysql Server
    auth.mysql.server = 127.0.0.1:3306

    ## Mysql Pool Size
    auth.mysql.pool = 8

    ## Mysql Username
    ## auth.mysql.username = 

    ## Mysql Password
    ## auth.mysql.password = 

    ## Mysql Database
    auth.mysql.database = mqtt

    ## Variables: %u = username, %c = clientid

    ## Authentication Query: select password only
    auth.mysql.auth_query = select password from mqtt_user where username = '%u' limit 1

    ## Password hash: plain, md5, sha, sha256, pbkdf2
    auth.mysql.password_hash = sha256

    ## %% Superuser Query
    auth.mysql.super_query = select is_superuser from mqtt_user where username = '%u' limit 1

Enable MySQL plugin:

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emq_plugin_mysql

PostgreSQL
----------

Authenticate with PostgreSQL database. Create a mqtt_user table:

.. code-block:: sql

    CREATE TABLE mqtt_user (
      id SERIAL primary key,
      username character varying(100),
      password character varying(100),
      salt character varying(40)
    );

Configure the 'auth_query' and 'password_hash' in etc/plugins/emq_auth_pgsql.conf:

.. code-block:: properties

    ## Postgre Server
    auth.pgsql.server = 127.0.0.1:5432

    auth.pgsql.pool = 8

    auth.pgsql.username = root

    #auth.pgsql.password = 

    auth.pgsql.database = mqtt

    auth.pgsql.encoding = utf8

    auth.pgsql.ssl = false

    ## Variables: %u = username, %c = clientid, %a = ipaddress

    ## Authentication Query: select password only
    auth.pgsql.auth_query = select password from mqtt_user where username = '%u' limit 1

    ## Password hash: plain, md5, sha, sha256, pbkdf2
    auth.pgsql.password_hash = sha256

    ## sha256 with salt prefix
    ## auth.pgsql.password_hash = salt sha256

    ## sha256 with salt suffix
    ## auth.pgsql.password_hash = sha256 salt

    ## Superuser Query
    auth.pgsql.super_query = select is_superuser from mqtt_user where username = '%u' limit 1

Enable the plugin:

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emq_plugin_pgsql

Redis
-----

Authenticate with Redis. MQTT users could be stored in redis HASH, the key is "mqtt_user:<Username>".

Configure 'auth_cmd' and 'password_hash' in etc/plugins/emq_auth_redis.conf:

.. code-block:: properties

    ## Redis Server
    auth.redis.server = 127.0.0.1:6379

    ## Redis Pool Size
    auth.redis.pool = 8

    ## Redis Database
    auth.redis.database = 0

    ## Redis Password
    ## auth.redis.password =

    ## Variables: %u = username, %c = clientid

    ## Authentication Query Command
    auth.redis.auth_cmd = HGET mqtt_user:%u password

    ## Password hash: plain, md5, sha, sha256, pbkdf2
    auth.redis.password_hash = sha256

    ## Superuser Query Command
    auth.redis.super_cmd = HGET mqtt_user:%u is_superuser

Enable the plugin:

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emq_auth_redis

MongoDB
-------

Create a `mqtt_user` collection::

    {
        username: "user",
        password: "password hash",
        is_superuser: boolean (true, false),
        created: "datetime"
    }

Configure `super_query`, `auth_query` in etc/plugins/emq_auth_mongo.conf:

.. code-block:: properties

    ## Mongo Server
    auth.mongo.server = 127.0.0.1:27017

    ## Mongo Pool Size
    auth.mongo.pool = 8

    ## Mongo User
    ## auth.mongo.user = 

    ## Mongo Password
    ## auth.mongo.password = 

    ## Mongo Database
    auth.mongo.database = mqtt

    ## auth_query
    auth.mongo.auth_query.collection = mqtt_user

    auth.mongo.auth_query.password_field = password

    auth.mongo.auth_query.password_hash = sha256

    auth.mongo.auth_query.selector = username=%u

    ## super_query
    auth.mongo.super_query.collection = mqtt_user

    auth.mongo.super_query.super_field = is_superuser

    auth.mongo.super_query.selector = username=%u

Enable the plugin:

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emq_auth_mongo

.. _acl:

---
ACL
---

The ACL of *EMQ* broker is responsbile for authorizing MQTT clients to publish/subscribe topics.

The ACL rules define::

    Allow|Deny Who Publish|Subscribe Topics

Access Control Module of *EMQ* broker will match the rules one by one::

              ---------              ---------              ---------
    Client -> | Rule1 | --nomatch--> | Rule2 | --nomatch--> | Rule3 | --> Default
              ---------              ---------              ---------
                  |                      |                      |
                match                  match                  match
                 \|/                    \|/                    \|/
            allow | deny           allow | deny           allow | deny

Internal
--------

The default ACL of *EMQ* broker is implemented by an 'internal' module.

Enable the 'internal' ACL module in etc/emq.conf:

.. code-block:: properties

    ## Default ACL File
    mqtt.acl_file = etc/acl.conf

The ACL rules of 'internal' module are defined in 'etc/acl.conf' file:

.. code-block:: erlang

    %% Allow 'dashboard' to subscribe '$SYS/#'
    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    %% Allow clients from localhost to subscribe any topics
    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    %% Deny clients to subscribe '$SYS#' and '#'
    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    %% Allow all by default
    {allow, all}.

HTTP API
--------

ACL by HTTP API: https://github.com/emqtt/emq_auth_http

Configure etc/plugins/emq_auth_http.conf and enable the plugin:

.. code-block:: properties

    ## 'access' parameter: sub = 1, pub = 2
    auth.http.acl_req = http://127.0.0.1:8080/mqtt/acl
    auth.http.acl_req.method = get
    auth.http.acl_req.params = access=%A,username=%u,clientid=%c,ipaddr=%a,topic=%t

    auth.http.acl_nomatch = deny

MySQL
-----

ACL with MySQL database. The `mqtt_acl` table and default data:

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

Configure 'acl-query' and 'acl_nomatch' in etc/plugins/emq_auth_mysql.conf:

.. code-block:: properties

    ## ACL Query Command
    auth.mysql.acl_query = select allow, ipaddr, username, clientid, access, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'

    ## ACL nomatch
    auth.mysql.acl_nomatch = deny

PostgreSQL
----------

ACL with PostgreSQL database. The mqtt_acl table and default data:

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

Configure 'acl_query' and 'acl_nomatch' in etc/plugins/emq_auth_pgsql.conf:

.. code-block:: properties

    ## ACL Query. Comment this query, the acl will be disabled.
    auth.pgsql.acl_query = select allow, ipaddr, username, clientid, access, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'

    ## If no rules matched, return...
    auth.pgsql.acl_nomatch = deny

Redis
-----

ACL with Redis. The ACL rules are stored in a Redis HashSet::

    HSET mqtt_acl:<username> topic1 1
    HSET mqtt_acl:<username> topic2 2
    HSET mqtt_acl:<username> topic3 3

Configure `acl_cmd` and `acl_nomatch` in etc/plugins/emq_auth_redis.conf:

.. code-block:: properties

    ## ACL Query Command
    auth.redis.acl_cmd = HGETALL mqtt_acl:%u

    ## ACL nomatch
    auth.redis.acl_nomatch = deny

MongoDB
-------

Store ACL Rules in a `mqtt_acl` collection:

.. code-block:: json

    {
        username: "username",
        clientid: "clientid",
        publish: ["topic1", "topic2", ...],
        subscribe: ["subtop1", "subtop2", ...],
        pubsub: ["topic/#", "topic1", ...]
    }

For example, insert rules into `mqtt_acl` collection::

    db.mqtt_acl.insert({username: "test", publish: ["t/1", "t/2"], subscribe: ["user/%u", "client/%c"]})
    db.mqtt_acl.insert({username: "admin", pubsub: ["#"]})

Configure `acl_query` and `acl_nomatch` in etc/plugins/emq_auth_mongo.conf:

.. code-block:: properties

    ## acl_query
    auth.mongo.acl_query.collection = mqtt_user

    auth.mongo.acl_query.selector = username=%u

    ## acl_nomatch
    auth.mongo.acl_nomatch = deny

----------------------
MQTT Publish/Subscribe
----------------------

MQTT is a an extremely lightweight publish/subscribe messaging protocol desgined for IoT, M2M and Mobile applications.

.. image:: _static/images/pubsub_concept.png

Install and start the *EMQ* broker, and then any MQTT client could connect to the broker, subscribe topics and publish messages.

MQTT Client Libraries: https://github.com/mqtt/mqtt.github.io/wiki/libraries

For example, we use mosquitto_sub/pub commands::

    mosquitto_sub -t topic -q 2
    mosquitto_pub -t topic -q 1 -m "Hello, MQTT!"

MQTT V3.1.1 Protocol Specification: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html

MQTT Listener of emqttd broker is configured in etc/emq.conf:

.. code-block:: erlang

.. code-block:: properties

    ## TCP Listener: 1883, 127.0.0.1:1883, ::1:1883
    mqtt.listener.tcp = 1883

    ## Size of acceptor pool
    mqtt.listener.tcp.acceptors = 8

    ## Maximum number of concurrent clients
    mqtt.listener.tcp.max_clients = 1024

MQTT(SSL) Listener, Default Port is 8883:

.. code-block:: properties

    ## SSL Listener: 8883, 127.0.0.1:8883, ::1:8883
    mqtt.listener.ssl = 8883

    ## Size of acceptor pool
    mqtt.listener.ssl.acceptors = 4

    ## Maximum number of concurrent clients
    mqtt.listener.ssl.max_clients = 512

----------------
HTTP Publish API
----------------

The *EMQ* broker provides a HTTP API to help application servers publish messages to MQTT clients.

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

.. code-block:: properties

    ## HTTP and WebSocket Listener
    mqtt.listener.http = 8083
    mqtt.listener.http.acceptors = 4
    mqtt.listener.http.max_clients = 64

-----------
$SYS Topics
-----------

The *EMQ* broker periodically publishes internal status, MQTT statistics, metrics and client online/offline status to $SYS/# topics.

For the *EMQ* broker could be clustered, the $SYS topic path is started with::

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

.. _emq_auth_clientid: https://github.com/emqtt/emq_auth_clientid
.. _emq_auth_username: https://github.com/emqtt/emq_auth_username
.. _emq_auth_ldap:     https://github.com/emqtt/emq_auth_ldap
.. _emq_auth_http:     https://github.com/emqtt/emq_auth_http
.. _emq_auth_mysql:    https://github.com/emqtt/emq_plugin_mysql
.. _emq_auth_pgsql:    https://github.com/emqtt/emq_plugin_pgsql
.. _emq_auth_redis:    https://github.com/emqtt/emq_plugin_redis
.. _emq_auth_mongo:    https://github.com/emqtt/emq_plugin_mongo

