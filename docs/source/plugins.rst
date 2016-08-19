
.. _plugins:

=======
Plugins
=======

The emqttd broker could be extended by plugins. Users could develop plugins to customize authentication, ACL and functions of the broker, or integrate the broker with other systems.

The plugins that emqtt project released:

+---------------------------+---------------------------+
| Plugin                    | Description               |
+===========================+===========================+
| `emqttd_plugin_template`_ | Template Plugin           |
+---------------------------+---------------------------+
| `emqttd_dashboard`_       | Web Dashboard             |
+---------------------------+---------------------------+
| `emqttd_auth_http`_       | HTTP Auth/ACL Plugin      |
+---------------------------+---------------------------+
| `emqttd_plugin_mysql`_    | MySQL Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emqttd_plugin_pgsql`_    | PostgreSQL Auth/ACL Plugin|
+---------------------------+---------------------------+
| `emqttd_plugin_redis`_    | Redis Auth/ACL Plugin     |
+---------------------------+---------------------------+
| `emqttd_plugin_mongo`_    | MongoDB Auth/ACL Plugin   |
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

A plugin is just a normal Erlang application under the 'emqttd/plugins' folder. Each plugin has e configuration file: 'etc/plugin.config'.

plugins/emqttd_plugin_template is a demo plugin. The folder structure:

+------------------------+---------------------------+
| File                   | Description               |
+========================+===========================+
| etc/plugin.config      | Plugin config file        |
+------------------------+---------------------------+
| ebin/                  | Erlang program files      |
+------------------------+---------------------------+

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

Configure Dashboard
-------------------

emqttd_dashboard/etc/plugin.config:

.. code-block:: erlang

    [
      {emqttd_dashboard, [
        {listener,
          {emqttd_dashboard, 18083, [
            {acceptors, 4},
            {max_clients, 512}]}
        }
      ]}
    ].

---------------------------------------
emqttd_auth_http - HTTP Auth/ACL Plugin
---------------------------------------

MQTT Authentication/ACL with HTTP API: https://github.com/emqtt/emqttd_auth_http

.. NOTE:: Supported in 1.1 release

Configure emqttd_auth_http/etc/plugin.config
--------------------------------------------

.. code:: erlang

    [

      {emqttd_auth_http, [

        %% Variables: %u = username, %c = clientid, %a = ipaddress, %t = topic

        {super_req, [
          {method, post},
          {url, "http://localhost:8080/mqtt/superuser"},
          {params, [
            {username, "%u"},
            {clientid, "%c"}
          ]}
        ]},

        {auth_req, [
          {method, post},
          {url, "http://localhost:8080/mqtt/auth"},
          {params, [
            {clientid, "%c"},
            {username, "%u"},
            {password, "%P"}
          ]}
        ]},

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
        ]}
      ]}

    ].

HTTP API
--------

Return 200 if ok

Return 4xx if unauthorized

Load emqttd_auth_http plugin
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

Configure emqttd_plugin_mysql/etc/plugin.config
-----------------------------------------------

Configure MySQL host, username, password and database:

.. code-block:: erlang

    [

      {emqttd_plugin_mysql, [

        {mysql_pool, [
            %% ecpool options
            {pool_size, 8},
            {auto_reconnect, 3},

            %% mysql options
            {host,     "localhost"},
            {port,     3306},
            {user,     ""},
            {password, ""},
            {database, "mqtt"},
            {encoding, utf8}
        ]},

        %% Variables: %u = username, %c = clientid, %a = ipaddress

        %% Superuser Query
        {superquery, "select is_superuser from mqtt_user where username = '%u' limit 1"},

        %% Authentication Query: select password only
        {authquery, "select password from mqtt_user where username = '%u' limit 1"},

        %% hash algorithm: plain, md5, sha, sha256, pbkdf2?
        {password_hash, sha256},

        %% select password with salt
        %% {authquery, "select password, salt from mqtt_user where username = '%u'"},

        %% sha256 with salt prefix
        %% {password_hash, {salt, sha256}},

        %% sha256 with salt suffix
        %% {password_hash, {sha256, salt}},

        %% '%a' = ipaddress, '%u' = username, '%c' = clientid
        %% Comment this query, the acl will be disabled
        {aclquery, "select allow, ipaddr, username, clientid, access, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"},

        %% If no ACL rules matched, return...
        {acl_nomatch, allow}

      ]}

    ].

Load emqttd_plugin_mysql plugin
-------------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_plugin_mysql

------------------------------------------------
emqttd_plugin_pgsql - PostgreSQL Auth/ACL Plugin
------------------------------------------------

MQTT Authentication, ACL with PostgreSQL Database.

MQTT User Table
---------------

.. code-block:: sql

    CREATE TABLE mqtt_user (
      id SERIAL primary key,
      is_superuser boolean,
      username character varying(100),
      password character varying(100),
      salt character varying(40)
    );

MQTT ACL Table
--------------

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

Configure emqttd_plugin_pgsql/etc/plugin.config
-----------------------------------------------

Configure host, username, password and database of PostgreSQL:

.. code-block:: erlang

    [

      {emqttd_plugin_pgsql, [

        {pgsql_pool, [
            %% ecpool options
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
        ]},

        %% Variables: %u = username, %c = clientid, %a = ipaddress

        %% Superuser Query
        {superquery, "select is_superuser from mqtt_user where username = '%u' limit 1"},

        %% Authentication Query: select password only
        {authquery, "select password from mqtt_user where username = '%u' limit 1"},

        %% hash algorithm: plain, md5, sha, sha256, pbkdf2?
        {password_hash, sha256},

        %% select password with salt
        %% {authquery, "select password, salt from mqtt_user where username = '%u'"},

        %% sha256 with salt prefix
        %% {password_hash, {salt, sha256}},

        %% sha256 with salt suffix
        %% {password_hash, {sha256, salt}},

        %% Comment this query, the acl will be disabled. Notice: don't edit this query!
        {aclquery, "select allow, ipaddr, username, clientid, access, topic from mqtt_acl
                     where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"},

        %% If no rules matched, return...
        {acl_nomatch, allow}
      ]}
    ].

Load emqttd_plugin_pgsql Plugin
-------------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_plugin_pgsql

-------------------------------------------
emqttd_plugin_redis - Redis Auth/ACL Plugin
-------------------------------------------

MQTT Authentication, ACL with Redis: https://github.com/emqtt/emqttd_plugin_redis

Configure emqttd_plugin_redis/etc/plugin.config
-----------------------------------------------

.. code-block:: erlang

    [
      {emqttd_plugin_redis, [

        {eredis_pool, [
          %% ecpool options
          {pool_size, 8},
          {auto_reconnect, 2},

          %% eredis options
          {host, "127.0.0.1"},
          {port, 6379},
          {database, 0},
          {password, ""}
        ]},

        %% Variables: %u = username, %c = clientid

        %% HMGET mqtt_user:%u is_superuser
        {supercmd, ["HGET", "mqtt_user:%u", "is_superuser"]},

        %% HMGET mqtt_user:%u password
        {authcmd, ["HGET", "mqtt_user:%u", "password"]},

        %% Password hash algorithm: plain, md5, sha, sha256, pbkdf2?
        {password_hash, sha256},

        %% SMEMBERS mqtt_acl:%u
        {aclcmd, ["SMEMBERS", "mqtt_acl:%u"]},

        %% If no rules matched, return...
        {acl_nomatch, deny},

        %% Load Subscriptions form Redis when client connected.
        {subcmd, ["HGETALL", "mqtt_subs:%u"]}
      ]}
    ].

User HASH
---------

Set a 'user' hash with 'password' field, for example::

    HSET mqtt_user:<username> is_superuser 1
    HSET mqtt_user:<username> password "passwd"

ACL Rule SET
------------

The plugin uses a redis SET to store ACL rules::

    SADD mqtt_acl:<username> "publish topic1"
    SADD mqtt_acl:<username> "subscribe topic2"
    SADD mqtt_acl:<username> "pubsub topic3"

Subscription HASH
-----------------

The plugin can store static subscriptions in a redis Hash::

    HSET mqtt_subs:<username> topic1 0
    HSET mqtt_subs:<username> topic2 1
    HSET mqtt_subs:<username> topic3 2

Load emqttd_plugin_redis Plugin
-------------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_plugin_redis

---------------------------------------------
emqttd_plugin_mongo - MongoDB Auth/ACL Plugin
---------------------------------------------

MQTT Authentication, ACL with MongoDB: https://github.com/emqtt/emqttd_plugin_mongo

Configure emqttd_plugin_mongo/etc/plugin.config
-----------------------------------------------

.. code-block:: erlang

    [
      {emqttd_plugin_mongo, [

        {mongo_pool, [
          {pool_size, 8},
          {auto_reconnect, 3},

          %% Mongodb Driver Opts
          {host, "localhost"},
          {port, 27017},
          %% {login, ""},
          %% {password, ""},
          {database, "mqtt"}
        ]},

        %% Variables: %u = username, %c = clientid

        %% Superuser Query
        {superquery, [
          {collection, "mqtt_user"},
          {super_field, "is_superuser"},
          {selector, {"username", "%u"}}
        ]},

        %% Authentication Query
        {authquery, [
          {collection, "mqtt_user"},
          {password_field, "password"},
          %% Hash Algorithm: plain, md5, sha, sha256, pbkdf2?
          {password_hash, sha256},
          {selector, {"username", "%u"}}
        ]},

        %% ACL Query: "%u" = username, "%c" = clientid
        {aclquery, [
          {collection, "mqtt_acl"},
          {selector, {"username", "%u"}}
        ]},

        %% If no ACL rules matched, return...
        {acl_nomatch, deny}

      ]}
    ].

MongoDB Database
----------------

.. code-block::

    use mqtt
    db.createCollection("mqtt_user")
    db.createCollection("mqtt_acl")
    db.mqtt_user.ensureIndex({"username":1})

User Collection
---------------

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

ACL Collection
--------------

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

Load emqttd_plugin_mongo Plugin
-------------------------------

.. code-block:: bash

    ./bin/emqttd_ctl plugins load emqttd_plugin_mongo

-----------------------------
emqttd_stomp - STOMP Protocol
-----------------------------

Support STOMP 1.0/1.1/1.2 clients to connect to emqttd broker and communicate with MQTT Clients.

Configure emqttd_stomp/etc/plugin.config
----------------------------------------

.. NOTE:: Default Port for STOMP Protocol: 61613

.. code-block:: erlang

    [
      {emqttd_stomp, [

        {default_user, [
            {login,    "guest"},
            {passcode, "guest"}
        ]},

        {allow_anonymous, true},

        %%TODO: unused...
        {frame, [
          {max_headers,       10},
          {max_header_length, 1024},
          {max_body_length,   8192}
        ]},

        {listeners, [
          {emqttd_stomp, 61613, [
            {acceptors,   4},
            {max_clients, 512}
          ]}
        ]}

      ]}
    ].

Load emqttd_stomp Plugin
------------------------

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

    [
      {emqttd_sockjs, [

        {sockjs, []},

        {cowboy_listener, {stomp_sockjs, 61616, 4}},

      ]}
    ].

Load emqttd_sockjs Plugin
-------------------------

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

Load emqttd_recon Plugin
------------------------

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

Load emqttd_reloader Plugin
---------------------------

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

Clone emqttd source from github.com::

    git clone https://github.com/emqtt/emqttd.git

Create a plugin project under 'plugins' folder::

    cd plugins && mkdir emqttd_my_plugin

    cd emqttd_my_plugin && rebar create-app appid=emqttd_my_plugin

Template Plugin: https://github.com/emqtt/emqttd_plugin_template

Register Auth/ACL Modules
-------------------------

emqttd_auth_demo.erl - demo authentication module:

.. code-block:: erlang

    -module(emqttd_auth_demo).

    -behaviour(emqttd_auth_mod).

    -include("../../../include/emqttd.hrl").

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

    -include("../../../include/emqttd.hrl").

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

+------------------------+---------------------------------------+
| Name                   | Description                           |
+------------------------+---------------------------------------+
| client.connected       | Run when a client connected to the    |
|                        | broker successfully                   |
+------------------------+---------------------------------------+
| client.subscribe       | Run before a client subscribes topics |
+------------------------+---------------------------------------+
| client.subscribe.after | Run after a client subscribed topics  |
+------------------------+---------------------------------------+
| client.unsubscribe     | Run when a client unsubscribes topics |
+------------------------+---------------------------------------+
| message.publish        | Run when a message is published       |
+------------------------+---------------------------------------+
| message.delivered      | Run when a message is delivered       |
+------------------------+---------------------------------------+
| message.acked          | Run when a message(qos1/2) is acked   |
+------------------------+---------------------------------------+
| client.disconnected    | Run when a client is disconnnected    |
+------------------------+---------------------------------------+

emqttd_plugin_template.erl for example:

.. code-block:: erlang

    %% Called when the plugin application start
    load(Env) ->
        emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
        emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
        emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
        emqttd:hook('client.subscribe.after', fun ?MODULE:on_client_subscribe_after/3, [Env]),
        emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
        emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
        emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
        emqttd:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]).


Register CLI Modules
--------------------

emqttd_cli_demo.erl:

.. code-block:: erlang

    -module(emqttd_cli_demo).

    -include("../../../include/emqttd_cli.hrl").

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
.. _emqttd_auth_http:       https://github.com/emqtt/emqttd_auth_http
.. _emqttd_plugin_mysql:    https://github.com/emqtt/emqttd_plugin_mysql
.. _emqttd_plugin_pgsql:    https://github.com/emqtt/emqttd_plugin_pgsql
.. _emqttd_plugin_redis:    https://github.com/emqtt/emqttd_plugin_redis
.. _emqttd_plugin_mongo:    https://github.com/emqtt/emqttd_plugin_mongo
.. _emqttd_stomp:           https://github.com/emqtt/emqttd_stomp
.. _emqttd_sockjs:          https://github.com/emqtt/emqttd_sockjs
.. _emqttd_recon:           https://github.com/emqtt/emqttd_recon
.. _emqttd_reloader:        https://github.com/emqtt/emqttd_reloader
.. _emqttd_plugin_template: https://github.com/emqtt/emqttd_plugin_template
.. _recon:                  http://ferd.github.io/recon/

