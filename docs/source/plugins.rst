
.. _plugins:

=======
Plugins
=======

The emqttd broker could be extended by plugins. Users could develop plugins to customize authentication, ACL and functions of the broker, or integrate the broker with other systems.

The plugins that emqttd 2.0 released:

+---------------------------+---------------------------+
| Plugin                    | Description               |
+===========================+===========================+
| `emqttd_dashboard`_       | Web Dashboard             |
+---------------------------+---------------------------+
| `emqttd_plugin_template`_ | Template Plugin           |
+---------------------------+---------------------------+
| `emqttd_auth_ldap`_       | LDAP Auth                 |
+---------------------------+---------------------------+
| `emqttd_auth_http`_       | HTTP Auth/ACL Plugin      |
+---------------------------+---------------------------+
| `emqttd_auth_mysql`_      | MySQL Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emqttd_auth_pgsql`_      | PostgreSQL Auth/ACL Plugin|
+---------------------------+---------------------------+
| `emqttd_auth_redis`_      | Redis Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emqttd_auth_mongo`_      | MongoDB Auth/ACL Plugin   |
+---------------------------+---------------------------+
| `emqttd_sn`_              | MQTT-SN Protocol Plugin   |
+---------------------------+---------------------------+
| `emqttd_stomp`_           | STOMP Protocol Plugin     |
+---------------------------+---------------------------+
| `emqttd_sockjs`_          | STOMP over SockJS Plugin  |
+---------------------------+---------------------------+
| `emqttd_recon`_           | Recon Plugin              |
+---------------------------+---------------------------+
| `emqttd_reloader`_        | Reloader Plugin           |
+---------------------------+---------------------------+

----------------------------------------
emqttd_plugin_template - Template Plugin
----------------------------------------

A plugin is just a normal Erlang application which has its own configuration file: 'etc/<PluginName>.config'.

emqttd_plugin_template is a demo plugin. 

Load, unload Plugin
-------------------

Use 'bin/emqttd_ctl plugins' CLI to load, unload a plugin::

    ./bin/emqttd_ctl plugins load <PluginName>

    ./bin/emqttd_ctl plugins unload <PluginName>

    ./bin/emqttd_ctl plugins list

-----------------------------------
emqttd_dashboard - Dashboard Plugin
-----------------------------------

The Web Dashboard for emqttd broker. The plugin will be loaded automatically when the broker started successfully.

+------------------+---------------------------+
| Address          | http://localhost:18083    |
+------------------+---------------------------+
| Default User     | admin                     |
+------------------+---------------------------+
| Default Password | public                    |
+------------------+---------------------------+

.. image:: _static/images/dashboard.png

Configure Dashboard Plugin
--------------------------

etc/plugins/emqttd_dashboard.conf:

.. code-block:: erlang

    {listener,
      {dashboard, 18083, [
        {acceptors, 4},
        {max_clients, 512}
      ]}
    }.

----------------------------------
emqttd_auth_ldap: LDAP Auth Plugin
----------------------------------

LDAP Auth Plugin: https://github.com/emqtt/emqttd_auth_ldap

.. NOTE:: Supported in 2.0-beta1 release

Configure LDAP Plugin
---------------------

etc/plugins/emqttd_auth_ldap.conf:

.. code-block:: erlang

    {ldap, [
        {servers, ["localhost"]},
        {port, 389},
        {timeout, 30},
        {user_dn, "uid=$u,ou=People,dc=example,dc=com"},
        {ssl, fasle},
        {sslopts, [
            {certfile, "ssl.crt"},
            {keyfile, "ssl.key"}
        ]}
    ]}.

Load LDAP Plugin
----------------

./bin/emqttd_ctl plugins load emqttd_auth_ldap

---------------------------------------
emqttd_auth_http - HTTP Auth/ACL Plugin
---------------------------------------

MQTT Authentication/ACL with HTTP API: https://github.com/emqtt/emqttd_auth_http

.. NOTE:: Supported in 1.1 release

Configure HTTP Auth/ACL Plugin
------------------------------

etc/plugins/emqttd_auth_http.conf:

.. code-block:: erlang

    %% Variables: %u = username, %c = clientid, %a = ipaddress, %t = topic

    {super_req, [
      {method, post},
      {url, "http://localhost:8080/mqtt/superuser"},
      {params, [
        {username, "%u"},
        {clientid, "%c"}
      ]}
    ]}.

    {auth_req, [
      {method, post},
      {url, "http://localhost:8080/mqtt/auth"},
      {params, [
        {clientid, "%c"},
        {username, "%u"},
        {password, "%P"}
      ]}
    ]}.

    %% 'access' parameter: sub = 1, pub = 2

    {acl_req, [
      {method, post},
      {url, "http://localhost:8080/mqtt/acl"},
      {params, [
        {access,   "%A"},
        {username, "%u"},
        {clientid, "%c"},
        {ipaddr,   "%a"},
        {topic,    "%t"}
      ]}
    ]}.


HTTP Auth/ACL API
-----------------

Return 200 if ok

Return 4xx if unauthorized

Load HTTP Auth/ACL Plugin
----------------------------

.. code:: bash

    ./bin/emqttd_ctl plugins load emqttd_auth_http

-------------------------------------------
emqttd_plugin_mysql - MySQL Auth/ACL Plugin
-------------------------------------------

MQTT Authentication, ACL with MySQL database.

MQTT User Table
---------------

.. code-block:: sql

    CREATE TABLE `mqtt_user` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `username` varchar(100) DEFAULT NULL,
      `password` varchar(100) DEFAULT NULL,
      `salt` varchar(20) DEFAULT NULL,
      `is_superuser` tinyint(1) DEFAULT 0,
      `created` datetime DEFAULT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `mqtt_username` (`username`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;

MQTT ACL Table
--------------

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

    INSERT INTO `mqtt_acl` (`id`, `allow`, `ipaddr`, `username`, `clientid`, `access`, `topic`)
    VALUES
        (1,1,NULL,'$all',NULL,2,'#'),
        (2,0,NULL,'$all',NULL,1,'$SYS/#'),
        (3,0,NULL,'$all',NULL,1,'eq #'),
        (5,1,'127.0.0.1',NULL,NULL,2,'$SYS/#'),
        (6,1,'127.0.0.1',NULL,NULL,2,'#'),
        (7,1,NULL,'dashboard',NULL,1,'$SYS/#');

Configure MySQL Auth/ACL Plugin
-------------------------------

etc/plugins/emqttd_plugin_mysql.conf:

.. code-block:: erlang

    {mysql_pool, [
      %% pool options
      {pool_size, 8},
      {auto_reconnect, 1},

      %% mysql options
      {host,     "localhost"},
      {port,     3306},
      {user,     ""},
      {password, ""},
      {database, "mqtt"},
      {encoding, utf8},
      {keep_alive, true}
    ]}.

    %% Variables: %u = username, %c = clientid, %a = ipaddress

    %% Superuser Query
    {superquery, "select is_superuser from mqtt_user where username = '%u' limit 1"}.

    %% Authentication Query: select password only
    {authquery, "select password from mqtt_user where username = '%u' limit 1"}.

    %% hash algorithm: plain, md5, sha, sha256, pbkdf2?
    {password_hash, sha256}.

    %% select password with salt
    %% {authquery, "select password, salt from mqtt_user where username = '%u'"}.

    %% sha256 with salt prefix
    %% {password_hash, {salt, sha256}}.

    %% sha256 with salt suffix
    %% {password_hash, {sha256, salt}}.

    %% '%a' = ipaddress, '%u' = username, '%c' = clientid
    %% Comment this query, the acl will be disabled
    {aclquery, "select allow, ipaddr, username, clientid, access, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"}.

    %% If no ACL rules matched, return...
    {acl_nomatch, allow}.

Load MySQL Auth/ACL plugin
--------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_plugin_mysql

------------------------------------------------
emqttd_plugin_pgsql - PostgreSQL Auth/ACL Plugin
------------------------------------------------

MQTT Authentication, ACL with PostgreSQL Database.

Postgre MQTT User Table
-----------------------

.. code-block:: sql

    CREATE TABLE mqtt_user (
      id SERIAL primary key,
      is_superuser boolean,
      username character varying(100),
      password character varying(100),
      salt character varying(40)
    );

Postgre MQTT ACL Table
----------------------

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

Configure Postgre Auth/ACL Plugin
-----------------------------------------------

Plugin Config: etc/plugins/emqttd_plugin_pgsql.conf.

Configure host, username, password and database of PostgreSQL:

.. code-block:: erlang

    {pgsql_pool, [
      %% pool options
      {pool_size, 8},
      {auto_reconnect, 3},

      %% pgsql options
      {host, "localhost"},
      {port, 5432},
      {ssl, false},
      {username, "feng"},
      {password, ""},
      {database, "mqtt"},
      {encoding,  utf8}
    ]}.

    %% Variables: %u = username, %c = clientid, %a = ipaddress

    %% Superuser Query
    {superquery, "select is_superuser from mqtt_user where username = '%u' limit 1"}.

    %% Authentication Query: select password only
    {authquery, "select password from mqtt_user where username = '%u' limit 1"}.

    %% hash algorithm: plain, md5, sha, sha256, pbkdf2?
    {password_hash, sha256}.

    %% select password with salt
    %% {authquery, "select password, salt from mqtt_user where username = '%u'"}.

    %% sha256 with salt prefix
    %% {password_hash, {salt, sha256}}.

    %% sha256 with salt suffix
    %% {password_hash, {sha256, salt}}.

    %% Comment this query, the acl will be disabled. Notice: don't edit this query!
    {aclquery, "select allow, ipaddr, username, clientid, access, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"}.

    %% If no rules matched, return...
    {acl_nomatch, allow}.

Load Postgre Auth/ACL Plugin
-----------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_auth_pgsql

-------------------------------------------
emqttd_plugin_redis - Redis Auth/ACL Plugin
-------------------------------------------

MQTT Authentication, ACL with Redis: https://github.com/emqtt/emqttd_plugin_redis

Configure Redis Auth/ACL Plugin
-------------------------------

etc/plugins/emqttd_auth_redis.conf:

.. code-block:: erlang

    {redis_pool, [
      %% pool options
      {pool_size, 8},
      {auto_reconnect, 2},

      %% redis options
      {host, "127.0.0.1"},
      {port, 6379},
      {database, 0},
      {password, ""}
    ]}.

    %% Variables: %u = username, %c = clientid

    %% HMGET mqtt_user:%u password
    {authcmd, "HGET mqtt_user:%u password"}.

    %% Password hash algorithm: plain, md5, sha, sha256, pbkdf2?
    {password_hash, sha256}.

    %% HMGET mqtt_user:%u is_superuser
    {supercmd, "HGET mqtt_user:%u is_superuser"}.

    %% HGETALL mqtt_acl:%u
    {aclcmd, "HGETALL mqtt_acl:%u"}.

    %% If no rules matched, return...
    {acl_nomatch, deny}.

    %% Load Subscriptions form Redis when client connected.
    {subcmd, "HGETALL mqtt_sub:%u"}.

Redis User Hash
---------------

Set a 'user' hash with 'password' field, for example::

    HSET mqtt_user:<username> is_superuser 1
    HSET mqtt_user:<username> password "passwd"

Redis ACL Rule Hash
-------------------

The plugin uses a redis Hash to store ACL rules::

    HSET mqtt_acl:<username> topic1 1
    HSET mqtt_acl:<username> topic2 2
    HSET mqtt_acl:<username> topic3 3

.. NOTE:: 1: subscribe, 2: publish, 3: pubsub

Redis Subscription Hash
-----------------------

The plugin can store static subscriptions in a redis Hash::

    HSET mqtt_subs:<username> topic1 0
    HSET mqtt_subs:<username> topic2 1
    HSET mqtt_subs:<username> topic3 2

Load Redis Auth/ACL Plugin
--------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_auth_redis

---------------------------------------------
emqttd_plugin_mongo - MongoDB Auth/ACL Plugin
---------------------------------------------

MQTT Authentication, ACL with MongoDB: https://github.com/emqtt/emqttd_plugin_mongo

Configure MongoDB Auth/ACL Plugin
---------------------------------

etc/plugins/emqttd_plugin_mongo.conf:

.. code-block:: erlang

    {mongo_pool, [
      {pool_size, 8},
      {auto_reconnect, 3},

      %% Mongodb Opts
      {host, "localhost"},
      {port, 27017},
      %% {login, ""},
      %% {password, ""},
      {database, "mqtt"}
    ]}.

    %% Variables: %u = username, %c = clientid

    %% Superuser Query
    {superquery, [
      {collection, "mqtt_user"},
      {super_field, "is_superuser"},
      {selector, {"username", "%u"}}
    ]}.

    %% Authentication Query
    {authquery, [
      {collection, "mqtt_user"},
      {password_field, "password"},
      %% Hash Algorithm: plain, md5, sha, sha256, pbkdf2?
      {password_hash, sha256},
      {selector, {"username", "%u"}}
    ]}.

    %% ACL Query: "%u" = username, "%c" = clientid
    {aclquery, [
      {collection, "mqtt_acl"},
      {selector, {"username", "%u"}}
    ]}.

    %% If no ACL rules matched, return...
    {acl_nomatch, deny}.

MongoDB Database
----------------

.. code-block::

    use mqtt
    db.createCollection("mqtt_user")
    db.createCollection("mqtt_acl")
    db.mqtt_user.ensureIndex({"username":1})

MongoDB User Collection
-----------------------

.. code-block:: json

    {
        username: "user",
        password: "password hash",
        is_superuser: boolean (true, false),
        created: "datetime"
    }

For example::

    db.mqtt_user.insert({username: "test", password: "password hash", is_superuser: false})
    db.mqtt_user:insert({username: "root", is_superuser: true})

MongoDB ACL Collection
----------------------

.. code-block:: json

    {
        username: "username",
        clientid: "clientid",
        publish: ["topic1", "topic2", ...],
        subscribe: ["subtop1", "subtop2", ...],
        pubsub: ["topic/#", "topic1", ...]
    }

For example::

    db.mqtt_acl.insert({username: "test", publish: ["t/1", "t/2"], subscribe: ["user/%u", "client/%c"]})
    db.mqtt_acl.insert({username: "admin", pubsub: ["#"]})

Load MongoDB Auth/ACL Plugin
----------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_auth_mongo

---------------------------
emqttd_sn: MQTT-SN Protocol
--------------------------

MQTT-SN Protocol/Gateway Plugin.

Configure MQTT-SN Plugin
-------------------------

.. NOTE:: UDP Port for MQTT-SN: 1884

etc/plugins/emqttd_sn.conf::

    {listener, {1884, []}}.

Load MQTT-SN Plugin
-------------------

.. code::

    ./bin/emqttd_ctl plugins load emqttd_sn

-----------------------------
emqttd_stomp - STOMP Protocol
-----------------------------

Support STOMP 1.0/1.1/1.2 clients to connect to emqttd broker and communicate with MQTT Clients.

Configure Stomp Plugin
----------------------

etc/plugins/emqttd_stomp.conf:

.. NOTE:: Default Port for STOMP Protocol: 61613

.. code-block:: erlang

    {default_user, [
        {login,    "guest"},
        {passcode, "guest"}
    ]}.

    {allow_anonymous, true}.

    {frame, [
      {max_headers,       10},
      {max_header_length, 1024},
      {max_body_length,   8192}
    ]}.

    {listener, emqttd_stomp, 61613, [
        {acceptors,   4},
        {max_clients, 512}
    ]}.


Load Stomp Plugin
-----------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_stomp

-----------------------------------
emqttd_sockjs - STOMP/SockJS Plugin
-----------------------------------

emqttd_sockjs plugin enables web browser to connect to emqttd broker and communicate with MQTT clients.

.. NOTE:: Default TCP Port: 61616

Configure emqttd_sockjs
-----------------------

.. code-block:: erlang

    {sockjs, []}.

    {cowboy_listener, {stomp_sockjs, 61616, 4}}.

    %% TODO: unused...
    {stomp, [
      {frame, [
        {max_headers,       10},
        {max_header_length, 1024},
        {max_body_length,   8192}
      ]}
    ]}.

Load SockJS Plugin
------------------

.. NOTE:: emqttd_stomp Plugin required.

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_stomp

    ./bin/emqttd_ctl plugins load emqttd_sockjs

SockJS Demo Page
----------------

http://localhost:61616/index.html

---------------------------
emqttd_recon - Recon Plugin
---------------------------

The plugin loads `recon`_ library on a running emqttd broker. Recon libray helps debug and optimize an Erlang application.

Load Recon Plugin
-----------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_recon

Recon CLI
---------

.. code-block:: bash

    ./bin/emqttd_ctl recon

    recon memory                 #recon_alloc:memory/2
    recon allocated              #recon_alloc:memory(allocated_types, current|max)
    recon bin_leak               #recon:bin_leak(100)
    recon node_stats             #recon:node_stats(10, 1000)
    recon remote_load Mod        #recon:remote_load(Mod)

---------------------------------
emqttd_reloader - Reloader Plugin
---------------------------------

Erlang Module Reloader for Development

.. NOTE:: Don't load the plugin in production!

Load 'Reloader' Plugin
----------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_reloader

reload CLI
----------

.. code-block:: bash

    ./bin/emqttd_ctl reload

    reload <Module>             # Reload a Module

------------------------
Plugin Development Guide
------------------------

Create a Plugin Project
-----------------------

Clone emqttd_plugin_template source from github.com::

    git clone https://github.com/emqtt/emqttd_plugin_template.git

Create a plugin project with erlang.mk and depends on 'emqttd' application, the 'Makefile'::

    PROJECT = emqttd_plugin_abc
    PROJECT_DESCRIPTION = emqttd abc plugin
    PROJECT_VERSION = 1.0

    DEPS = emqttd 
    dep_emqttd = git https://github.com/emqtt/emqttd emq20

    COVER = true

    include erlang.mk

Template Plugin: https://github.com/emqtt/emqttd_plugin_template

Register Auth/ACL Modules
-------------------------

emqttd_auth_demo.erl - demo authentication module:

.. code-block:: erlang

    -module(emqttd_auth_demo).

    -behaviour(emqttd_auth_mod).

    -include_lib("emqttd/include/emqttd.hrl").

    -export([init/1, check/3, description/0]).

    init(Opts) -> {ok, Opts}.

    check(#mqtt_client{client_id = ClientId, username = Username}, Password, _Opts) ->
        io:format("Auth Demo: clientId=~p, username=~p, password=~p~n",
                  [ClientId, Username, Password]),
        ok.

    description() -> "Demo Auth Module".

emqttd_acl_demo.erl - demo ACL module:

.. code-block:: erlang

    -module(emqttd_acl_demo).

    -include_lib("emqttd/include/emqttd.hrl").

    %% ACL callbacks
    -export([init/1, check_acl/2, reload_acl/1, description/0]).

    init(Opts) ->
        {ok, Opts}.

    check_acl({Client, PubSub, Topic}, Opts) ->
        io:format("ACL Demo: ~p ~p ~p~n", [Client, PubSub, Topic]),
        allow.

    reload_acl(_Opts) ->
        ok.

    description() -> "ACL Module Demo".

emqttd_plugin_template_app.erl - Register the auth/ACL modules:

.. code-block:: erlang

    ok = emqttd_access_control:register_mod(auth, emqttd_auth_demo, []),
    ok = emqttd_access_control:register_mod(acl, emqttd_acl_demo, []),


Register Callbacks for Hooks
-----------------------------

The plugin could register callbacks for hooks. The hooks will be run by the broker when a client connected/disconnected, a topic subscribed/unsubscribed or a message published/delivered:

+------------------------+-----------------------------------------+
| Name                   | Description                             |
+------------------------+-----------------------------------------+
| client.connected       | Run when a client connected to the      |
|                        | broker successfully                     |
+------------------------+-----------------------------------------+
| client.subscribe       | Run before a client subscribes topics   |
+------------------------+-----------------------------------------+
| client.unsubscribe     | Run when a client unsubscribes topics   |
+------------------------+-----------------------------------------+
| session.subscribed     | Run after a client subscribed a topic   |
+------------------------+-----------------------------------------+
| session.unsubscribed   | Run after a client unsubscribed a topic |
+------------------------+-----------------------------------------+
| message.publish        | Run when a message is published         |
+------------------------+-----------------------------------------+
| message.delivered      | Run when a message is delivered         |
+------------------------+-----------------------------------------+
| message.acked          | Run when a message(qos1/2) is acked     |
+------------------------+-----------------------------------------+
| client.disconnected    | Run when a client is disconnnected      |
+------------------------+-----------------------------------------+

emqttd_plugin_template.erl for example:

.. code-block:: erlang

    %% Called when the plugin application start
    load(Env) ->
        emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
        emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
        emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
        emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
        emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
        emqttd:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
        emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
        emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
        emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

Register CLI Modules
--------------------

emqttd_cli_demo.erl:

.. code-block:: erlang

    -module(emqttd_cli_demo).

    -include_lib("emqttd/include/emqttd_cli.hrl").

    -export([cmd/1]).

    cmd(["arg1", "arg2"]) ->
        ?PRINT_MSG("ok");

    cmd(_) ->
        ?USAGE([{"cmd arg1 arg2", "cmd demo"}]).

emqttd_plugin_template_app.erl - register the CLI module to emqttd broker:

.. code-block:: erlang

    emqttd_ctl:register_cmd(cmd, {emqttd_cli_demo, cmd}, []).

There will be a new CLI after the plugin loaded::

    ./bin/emqttd_ctl cmd arg1 arg2

.. _emqttd_dashboard:       https://github.com/emqtt/emqttd_dashboard
.. _emqttd_auth_ldap:       https://github.com/emqtt/emqttd_auth_ldap
.. _emqttd_auth_http:       https://github.com/emqtt/emqttd_auth_http
.. _emqttd_auth_mysql:      https://github.com/emqtt/emqttd_auth_mysql
.. _emqttd_auth_pgsql:      https://github.com/emqtt/emqttd_auth_pgsql
.. _emqttd_auth_redis:      https://github.com/emqtt/emqttd_auth_redis
.. _emqttd_auth_mongo:      https://github.com/emqtt/emqttd_auth_mongo
.. _emqttd_sn:              https://github.com/emqtt/emqttd_sn
.. _emqttd_stomp:           https://github.com/emqtt/emqttd_stomp
.. _emqttd_sockjs:          https://github.com/emqtt/emqttd_sockjs
.. _emqttd_recon:           https://github.com/emqtt/emqttd_recon
.. _emqttd_reloader:        https://github.com/emqtt/emqttd_reloader
.. _emqttd_plugin_template: https://github.com/emqtt/emqttd_plugin_template
.. _recon:                  http://ferd.github.io/recon/

