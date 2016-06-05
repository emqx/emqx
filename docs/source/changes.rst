
.. _changes:

=======
Changes
=======

.. _release_1.1.1:

-------------
Version 1.1.1
-------------

*Release Date: 2016-06-04*

Compatible with the Qos0 PUBREL packet (#575)

phpMqtt Client Compatibility (#572)

java.io.EOFException using paho java client (#551)

.. _release_1.1:

-----------
Version 1.1
-----------

*Release Date: 2016-06-01*

Highlights
----------

Upgrade eSockd library to 4.0 and Support IPv6

Support to listen on specific IP Address::

    {mqtt, {"192.168.1.20", 1883}, [
        ...
    ]},

Add MongoDB, HTTP Authentication/ACL Plugins

Upgrade MySQL, PostgreSQL, Redis Plugins to support superuser authentication and avoid SQL Injection

Enhancements
------------

Allow human-friendly IP addresses (PR#395)

File operation error: emfile (#445)

emqttd_plugin_mongo not found in emqttd (#489)

emqttd_plugin_mongo Error While Loading in emqttd (#505)

Feature request: HTTP Authentication (#541)

Compatible with the Qos0 PUBREL packet (#575)

Bugfix
------

Bugfix: function_clause exception occurs when registering a duplicated authentication module (#542)

Bugfix: ./emqttd_top msg_q result: {"init terminating in do_boot",{undef,[{etop,start,[],[]},{init,start_it,1,[]},{init,start_em,1,[]}]}} (#557)

Tests
-----

111 common test cases.

Dashboard Plugin
----------------

WebSocket Page: Support 'Clean Session', Qos, Retained parameters (emqttd_dashboard#52)

Upgrade eSockd library to 4.0, Show OTP Release on Overview Page (emqttd_dashboard#61)

Changing dashboard credentials for username authentication (emqttd_dashboard#56)

Add './bin/emqttd_ctl admins' CLI，support to add/delete admins

HTTP Auth Plugin
----------------

Authentication/ACL by HTTP API: https://github.com/emqtt/emqttd_auth_http

MongoDB Plugin
--------------

Upgrade Erlang MongoDB driver to v1.0.0

Support superuser authentication

Support ACL (emqttd_plugin_mongo#3)

MySQL Plugin
------------

Support superuser authentication

Use parameterized query to avoid SQL Injection

Postgre Plugin
--------------

Support superuser authentication

Use parameterized query to avoid SQL Injection

Redis Plugin
------------

Support superuser authentication

Support ClientId authentication by '%c' variable

Reloader Plugin
---------------

Reload modified modules during development automatically.

.. _release_1.0.3:

-------------
Version 1.0.3
-------------

*Release Date: 2016-05-23*

eSockd 3.2

MochiWeb 4.0.1

.. _release_1.0.2:

-------------
Version 1.0.2
-------------

*Release Date: 2016-05-04*

Issue#534 - './bin/emqttd_ctl vm' - add 'port/count', 'port/limit' statistics

Issue#535 - emqttd_client should be terminated properly even if exception happened when sending data

PR#519 - The erlang '-name' requires the fully qualified host name

emqttd_reloader plugin - help reload modified modules during development.

.. _release_l.0.1:

-------------
Version 1.0.1
-------------

*Release Date: 2016-04-16*

PR#515 - Fix '$queue' pubsub, add 'pubsub_queue' test and update docs

.. _release_1.0:

------------------------------------
Version 1.0 (The Seven Mile Journey)
------------------------------------

*Release Date: 2016-04-13*

*Release Name: The Seven Mile Journey*

We finally released Version 1.0 (The Seven Mile Journey) with full documentation after two years' development and more than fifty iterations.

The emqttd 1.0 implements a fully-featured, scalable, distributed and extensible open-source MQTT broker for IoT, M2M and Mobile applications:

1. Full MQTT V3.1/3.1.1 Protocol Specifications Support

2. Massively scalable - Scaling to 1 million connections on a single server

3. Distributed - Route MQTT Messages among clustered or bridged broker nodes

4. Extensible - LDAP, MySQL, PostgreSQL, Redis Authentication/ACL Plugins

Bugfix and Enhancements
-----------------------

Possible race condition using emqttd_cm (#486)

Improve the design of retained message expiration (#503)

Do not expire the retained messages from $SYS/# topics (#500)

Documentation
--------------

http://emqtt.io/docs

http://docs.emqtt.com/

Thanks
------

Thank Ericsson for the Great Erlang/OTP Platform (http://erlang.org/)!

Contributors on GitHub: @callbay @lsxredrain @hejin1026 @desoulter @turtleDeng @Hades32 @huangdan @phanimahesh @dvliman @Prots @joaohf

Partners: EACG (http://eacg.de/)

Favorite Band: The Seven Mile Journey (http://www.thesevenmilejourney.dk/)

.. _release_0.17.1:

-------------------
Version 0.17.1-beta
-------------------

*Release Date: 2016-03-22*

Enhancements
------------

Time unit of session 'expired_after' changed to minute. (#479)

Dashboard
---------

Code Review and improve the design of Dashboard.

.. _release_0.17.0:

-------------------
Version 0.17.0-beta
-------------------

*Release Date: 2016-03-15*

Highlights
----------

Installation and Configuration Guide released on http://docs.emqtt.com

Improve and Consolidate the design of Hook, Server, PubSub and Router

Upgrade the [Web Dashboard](https://github.com/emqtt/emqttd_dashboard) to support pagination

Bridge emqttd broker to another emqttd broker & emqttd to mosquitto bridge (#438)

Enhancements
------------

emqttd_ctl: better error message (#450)

./bin/emqttd_ctl: add 'routes' command::

    routes list             # List all routes
    routes show <Topic>     # Show a route

Add 'backend_subscription' table and support static subscriptions (emqttd_backend)

Add 'retained_message' table and refactor emqttd_retainer module (emqttd_backend)

A New Hook and Callback Design (emqttd_hook)

Add PubSub, Hooks APIs to emqttd module (emqttd)

Move start_listeners/0, stop_listeners/0 APIs to emqttd_app module (emqttd_app)

Tests
-----

Add 100+ common test cases.

Plugins
-------

Upgrade Dashboard, Redis, Stomp and Template Plugins

.. _release_0.16.0:

-------------------
Version 0.16.0-beta
-------------------

*Release Date: 2016-02-16*

Highlights
----------

Licensed under the Apache License, Version 2.0 Now.

Improve the design of cluster, support to join or leave the cluster (#449)::

    $ ./bin/emqttd_ctl cluster
    cluster join <Node>                     #Join the cluster
    cluster leave                           #Leave the cluster
    cluster remove <Node>                   #Remove the node from cluster
    cluster status                          #Cluster status

Improve the design of Trie and Route, only the wildcard topics stored in Trie.

Common Test to replace EUnit.

Enhancements
------------

mqtt_message record: add 'sender' field (#440)

refactor the emqttd, emqttd_time, emqttd_opts, emqttd_node modules.

Bugfix
------

noproc error when call to gen_server2:call(false, {add_route,Topic,<0.685.0>}, infinity) (#446)

Plugins
-------

Changed the license of all plugins.

.. _release_0.15.0:

-------------------
Version 0.15.0-beta
-------------------

*Release Date: 2016-01-31*

Highlights
----------

Optimize for Push Application, 500K+ Subscribers to a Topic.

Optimization for Route ETS insertion (#427)

Priority Message Queue for Persistent Session (#432)

Add Redis, MongoDB Plugins (#417)

Enhancements
------------

Username/Password Authentication: Support to configure default users (#428)

Improve CLI Commands: pubsub, bridges, trace (#429)

emqttd_mod_subscription: fix client_connected/3

emqttd_auth_mod: add passwd_hash/2 function

priority_queue: add plen/2, out/2 functions

Bugfix
------

Fix dequeue/1 of emqttd_bridge...

Add emqttd:seed_now/0 function

Plugins
-------

emqttd_plubin_mysql: Changed mysql driver to mysql-otp

emqttd_plugin_pgsql: Integrate with ecpool

emqttd_plugin_redis: First release

emqttd_plugin_mongo: First release

.. _release_0.14.1:

-------------------
Version 0.14.1-beta
-------------------

*Release Date: 2015-12-28*

Bugfix: emqttd_ws_client.erl: Unexpected Info: {'EXIT',<0.27792.18>,{shutdown,destroy}} (#413)

Improve: fix spec errors found by dialyzer

.. _release_0.14.0:

-------------------
Version 0.14.0-beta
-------------------

*Release Date: 2015-12-18*

Highlights
----------

Scaling to 1.3 Million Concurrent MQTT Connections on a 12 Core, 32G CentOS server.

New PubSub, Router Design (#402). Prepare for scaling to 10 millions on one cluster.

Enhancements
------------

Improve the gproc_pool usage with a general emqttd_pool_sup

Improve the design of emqttd_pubsub, add a new emqttd_router module

Improve the design of the whole supervisor tree

Route aging mechanism to remove the topics that have no subscriptions

Improve the dashboard, mysql, pgsql, stomp, sockjs plugins

Add 'topics', 'subscriptions' admin commands

Avoid using mnesia table index and mnesia:index_read API to lower CPU usage

Subscribe timeout exception (#366)

Long Delay on Multiple Topic Subscription (#365)

Subscriptions persistence (#344)

emqttd_ctl: 'subscriptions' command to force clients to subscribe some topics (#361)

Bugfix
------

emqttd_sm: spec of lookup_session/1 is not right BUG (#411)

Observer application should be removed from reltool.config for 'wx' app is not available (#410)

Benchmark
---------

1.3 million concurrent MQTT connections on a 12 Core, 32G CentOS Server, consume about 15G Memory and 200% CPU.

.. _release_0.13.1:

-------------------
Version 0.13.1-beta
-------------------

*Release Date: 2015-11-28*

Bugfix: Plugin pathes error under windows (#387)

Improve: Too many error logs "[error] Session ..... Unexpected EXIT: client_pid=<0.14137.35>, exit_pid=<0.30829.22>, reason=nop..." (#383)

Improve: Define QOS0/1/2, Pooler Error (PR#382)

Improve: High CPU load when 400K unstable mobile connections (#377)

BugFix: emqttd_plugin_pgsql - error using same query with latest update plugin (pgsql#5)

.. _release_0.13.0:

-------------------
Version 0.13.0-beta
-------------------

*Release Date: 2015-11-08*

Highlights
----------

Rate Limiting based on [Token Bucket](https://en.wikipedia.org/wiki/Token_bucket) and [Leaky Bucket](https://en.wikipedia.org/wiki/Leaky_bucket#The_Leaky_Bucket_Algorithm_as_a_Meter) Algorithm

Upgrade eSockd and MochiWeb libraries to support Parameterized Connection Module

Improve emqttd_client to support fully asynchronous socket networking

Enhancements
------------

Protocol Compliant - Session Present Flag (#163)

Compilation fails if repo is cloned with a different name (#348)

emqttd_client: replace gen_tcp:send with port_command (#358)

TCP sndbuf, recbuf, buffer tuning (#359)

emqttd_client.erl to handle 'inet_async', 'inet_reply' properly (#360)

Refator the [client/session management design](https://github.com/emqtt/emqttd/blob/master/doc/design/ClientSession.md)

Bugfix
------

Cannot kick transient client out when clientId collision (#357)

Fix the order of emqttd_app:start_server/1 (#367)

emqttd_session:subscribe/2 will crash (#374)

Benchmark
---------

[benchmark for 0.13.0 release](https://github.com/emqtt/emqttd/wiki/benchmark-for-0.13.0-release)

3.1G memory and 50+ CPU/core:

.. code-block:: bash

    Connections: 250K
    Subscribers: 250K
    Topics:      50K
    Qos1 Messages/Sec In:  4K
    Qos1 Messages/Sec Out: 20K
    Traffic In(bps):  12M+
    Traffic Out(bps): 56M+

.. _release_0.12.3:

-------------------
Version 0.12.3-beta
-------------------

*Release Date: 2015-10-22*

Bugfix: emqttd_sysmon crasher for 'undefined' process_info (#350)

Bugfix: emqttd_client: catch parser exception (#353)

.. _release_0.12.2:

-------------------
Version 0.12.2-beta
-------------------

*Release Date: 2015-10-16*

Bugfix: Retained messages should not be expired if 'broker.retained.expired_after = 0' (#346)

.. _release_0.12.1:

-------------------
Version 0.12.1-beta
-------------------

*Release Date: 2015-10-15*

Highlight: Release for Bugfix and Code Refactor.

Feature: Retained message expiration (#182)

Improve: '$SYS/#' publish will not match '#' or '+/#' (#68)

Improve: Add more metrics and ignore '$SYS/#' publish (#266)

Improve: emqttd_sm should be optimized for clustered nodes may be crashed (#282)

Improve: Refactor emqttd_sysmon and suppress 'monitor' messages (#328)

Task: benchmark for 0.12.0 release (#225)

Benchmark: About 900K concurrent connections established on a 20Core, 32G CentOS server.

.. _release_0.12.0:

-------------------
Version 0.12.0-beta
-------------------

*Release Date: 2015-10-08*

Highlights
----------

Enhance the **emqttd_ctl** module to allow plugins to register new commands (#256)

Add [emqttd_recon plugin](https://github.com/emqtt/emqttd_recon) to debug/optimize the broker (#235)

Add **'./bin/emqttd_ctl broker pubsub'** command to check the status of core pubsub processes

Add **'./bin/emqttd_top'** command(like etop) to show the top 'msg_q', 'reductions', 'memory' or 'runtime' processes

'rel/files/emqttd.config.production' for production deployment(default)

'rel/files/emqttd.config.development' for development deployment

Enhancements
------------

Qos1/2 messages will not be dropped under unstable mobile network (#264)

**emqttd_session:subscribe/2, emqttd_session:unsubscribe/2** APIs should be asynchronous (#292)

**etc/emqttd.config**: 'idle_timeout' option to close the idle client(socket connected but no 'CONNECT' frame received)

**etc/emqttd.config**: 'unack_retry_interval' option for redelivering Qos1/2 messages

How to monitor large 'message_queue_len' (#283)

Bugfix
------

Behaviour emqttd_auth_mod is missing init callback (#318)

Benchmark
---------

Write a new [benchmark tool](https://github.com/emqtt/emqtt_benchmark) to benchmark this release

Hw requirements - 5K users, 25-50 msgs/sec, QoS=1 (#209)

Supported Number of Connections Greatly Reduced When Clients are Subscribing (#324)

.. _release_0.11.0:

-------------------
Version 0.11.0-beta
-------------------

*Release Date: 2015-09-25*

Highlight: Rebar to manage plugin dependencies.

Highlight: [Stomp](https://github.com/emqtt/emqttd_stomp) and [SockJS](https://github.com/emqtt/emqttd_sockjs) Plugins!

Improve: add rel/files/emqttd.config.development|production.

Improve: rel/reltool.config.script to release deps of plugin.

Improve: persist mnesia schema on slave nodes.

Improve: use timer:seconds/1 api.

Improve: The binary release will be compiled with R18.1 now.

Bugfix: issue#306 - emqttd_cm should unregister the duplicated client

Bugfix: issue#310 - usage of emqttd_ctl error: 'session list' should be 'sessions list'

Bugfix: issue#311 - './bin/emqttd_ctl sessions list' error

Bugfix: issue#312 - unsubcribe will lead to crash if emqttd_plugin_template plugin loaded

.. _release_0.10.4:

-------------------
Version 0.10.4-beta
-------------------

*Release Date: 2015-09-18*

Optimize session management and upgrade eSockd library to 2.7.1

[Benchmark for 0.10.4 release](https://github.com/emqtt/emqttd/wiki/benchmark-for-0.10.4-release)

Improve: issue#294 - [error] failed to start connection on 0.0.0.0:1883 - enotconn

Improve: issue#297 - How do I allow user with some pattern to access topic with some pattern?

Bugfix:  issue#291 - "./bin/emqttd attach ..." cannot work

Bugfix:  issue#284 - Should not use erlang:list_to_atom/1 in emqttd_vm.erl

.. _release_0.10.3:

-------------------
Version 0.10.3-beta
-------------------

*Release Date: 2015-08-30*

Bugfix:  issue#271 - add emqttd_ws_client:subscribe/2 function

Bugfix:  issue#269 - bin/emqttd Syntax error on ubuntu

Improve: issue#265 - client under unstable mobile network generate a lot of logs

.. _release_0.10.2:

-------------------
Version 0.10.2-beta
-------------------

*Release Date: 2015-08-26*

Improve: issue#257 - After the node name changed, the broker cannot restart for mnesia schema error.

.. _release_0.10.1:

-------------------
Version 0.10.1-beta
-------------------

*Release Date: 2015-08-25*

Bugfix: issue#259 - when clustered the emqttd_dashboard port is close, and the 'emqttd' application cannot stop normally.

Feature: issue#262 - Add 'http://host:8083/mqtt/status' Page for health check

.. _release_0.10.0:

-------------------
Version 0.10.0-beta
-------------------

*Release Date: 2015-08-20*

[Web Dashboard](https://github.com/emqtt/emqttd_dashboard) and [MySQL](https://github.com/emqtt/emqttd_plugin_mysql), [PostgreSQL](https://github.com/emqtt/emqttd_plugin_pgsql) Authentication/ACL Plugins!

Highlight: Web Dashboard to monitor Statistics, Metrics, Clients, Sessions and Topics of the broker.

Highlight: JSON/HTTP API to query all clients connected to broker.

Highlight: A new [Plugin Design](https://github.com/emqtt/emqttd/wiki/Plugin%20Design) and a [Template project](https://github.com/emqtt/emqttd_plugin_template) for plugin development.

Highlight: Authentication/ACL with MySQL, PostreSQl databases (#194, #172)

Feature: Session Statistics including inflight_queue, message_queue, message_dropped, awaiting_rel, awaiting_ack, awaiting_comp (#213)

Feature: Cookie based authentication for MQTT over websocket connections (#231)

Feature: Get all clients connected to the broker (#228, #230, #148, #129)

Feature: "./bin/emqttd_ctl clients show ClientId" to query client status (#226)

Feature: "./bin/emqttd_ctl clients kick ClientId" to kick out a client

Feature: "./bin/emqttd_ctl sessions list" to show all sessions

Feature: "./bin/emqttd_ctl sessions show ClientId" to show a session

Feature: Erlang VM metrics monitor with Web Dashboard (#59)

Improve: Too many "inflight queue is full!" log when session is overloaded (#247)

Improve: There are two many "MQueue(~s) drop ~s" logs if the message queue of session is small (#244)

Improve: gen_server2(from RabbitMQ) to improve emqttd_session, emqttd_pubsub

Improve: Makefile to build plugins 

Bugfix: emqttd_broker:unhook/2 cannot work (#238)

Bugfix: emqttd plugin cannot include_lib("emqttd/include/emqttd.hrl") (#233)

Bugfix: Too many 'Session ~s cannot find PUBACK' logs (#212)

Bugfix: emqttd_pooler cannot work

.. _release_0.9.3:

-------------------
Version 0.9.3-alpha
-------------------

*Release Date: 2015-07-25*

Wiki: [Bridge](https://github.com/emqtt/emqttd/wiki/Bridge)

Improve: emqttd_protocol.hrl to define 'QOS_I'

Improve: emqttd_pubsub to add subscribe/2 API

Improve: ./bin/emqttd_ctl to support new bridges command

Bugfix: issue #206 - Cannot bridge two nodes

.. _release_0.9.2:

-------------------
Version 0.9.2-alpha
-------------------

*Release Date: 2015-07-18*

Improve: issue #196 - Add New Hook 'client.subscribe.after' 

.. _release_0.9.1:

-------------------
Version 0.9.1-alpha
-------------------

*Release Date: 2015-07-10*

Bugfix: issue #189 - MQTT over WebSocket(SSL) cannot work?

Bugfix: issue #193 - 'client.ack' hook should be renamed to 'message.acked', and called by emqttd_broker:foreach_hooks 

.. _release_0.9.0:

-------------------
Version 0.9.0-alpha
-------------------

*Release Date: 2015-07-09*

[Session, Queue, Inflight Window, Hooks, Global MessageId and More Protocol Compliant](https://github.com/emqtt/emqttd/releases/tag/0.9.0-alpha) Now!

Feature: Session/Queue/Inflight Window Design (#145).

Feature: Support to resume a persistent session on other clustered node.

Feature: Support alarm management.

Feature: emqttd_guid to generate global unique message id.

Feature: Hooks for message pub/ack.

Feature: Protocol compliant - message ordering, timeout and retry.

Improve: Every client will start_link a session process, whether or not the client is persistent.

Improve: etc/emqttd.config to support more session, queue configuration.

Improve: issue #179 - Max offline message queue {max_queue, 100} meaning.

Improve: issue #180 - Should change project structure for other projects maybe depend on 'emqttd'. Merge emqtt, emqttd apps.

Improve: issue #185 - PacketId and MessageId: the broker should generate global unique message id.

Improve: issue #187 - etc/emqttd.config to support https listener

Improve: issue #186 - emqttd_cm to store client details

Improve: issue #174 - add 'from' field to mqtt_message record.

Improve: issue #170 - $SYS Topics should support alarms.

Improve: issue #169 - Add More [Hooks](https://github.com/emqtt/emqttd/wiki/Hooks-Design)

Improve: issue #167 - Inflight window to assure message ordering.

Improve: issue #166 - Message delivery timeout and retry.

Improve: issue #143 - Qos1, Qos2 PubSub message timeout.

Improve: issue #122 - Labeling message with unique id. emqttd_guid module to generate global unique msgid.

Improve: emqttd_bridge to support pending message queue, and fix the wrong Qos design.

Improve: mqtt_message record to add 'msgid', 'from' and 'sys' fields.

Change: Add emqttd_mqueue, emqttd_guid, emqttd_alarm modules.

Bugfix: issue #184 - emqttd_stats:setstats is not right.

Bugfix: Closed issues #181, #119.

Tests: fix the parser, acl test cases.

.. _release_0.8.6:

------------------
Version 0.8.6-beta
------------------

*Release Date: 2015-06-17*

Bugfix: issue #175 - publish Will message when websocket is closed without 'DISCONNECT' packet

.. _release_0.8.5:

------------------
Version 0.8.5-beta
------------------

*Release Date: 2015-06-10*

Bugfix: issue #53 - client will receive duplicate messages when overlapping subscription

.. _release_0.8.4:

------------------
Version 0.8.4-beta
------------------

*Release Date: 2015-06-08*

Bugfix: issue #165 - duplicated message when publish 'retained' message to persistent client

.. _release_0.8.3:

------------------
Version 0.8.3-beta
------------------

*Release Date: 2015-06-05*

Bugfix: issue #158 - should queue:in new message after old one dropped

Bugfix: issue #155 - emqtt_parser.erl: parse_topics/3 should reverse topics

Bugfix: issue #149 - Forget to merge plugins/emqttd_auth_mysql from 'dev' branch to 'master' in 0.8.x release

.. _release_0.8.2:

-------------------
Version 0.8.2-alpha
-------------------

*Release Date: 2015-06-01*

Bugfix: issue #147 - WebSocket client cannot subscribe queue '$Q/queue/${clientId}'

Bugfix: issue #146 - emqttd_auth_ldap: fill(Username, UserDn) is not right

.. _release_0.8.1:

-------------------
Version 0.8.1-alpha
-------------------

*Release Date: 2015-05-28*

Client [Presence](https://github.com/emqtt/emqttd/wiki/Presence) Support and [$SYS Topics](https://github.com/emqtt/emqttd/wiki/$SYS-Topics) Redesigned!

Bugfix: issue #138 - when client disconnected normally, broker will not publish disconnected $SYS message

Bugfix: fix websocket url in emqttd/priv/www/websocket.html

Improve: etc/emqttd.config to allow websocket connections from any hosts

Improve: rel/reltool.config to exclude unnecessary apps.

.. _release_0.8.0:

-------------------
Version 0.8.0-alpha
-------------------

*Release Date: 2015-05-25*

[Hooks](https://github.com/emqtt/emqttd/wiki/Hooks%20Design), Modules and [Plugins](https://github.com/emqtt/emqttd/wiki/Plugin%20Design) to extend the broker Now!

Plugin: emqttd_auth_mysql - MySQL authentication plugin (issues #116, #120)

Plugin: emqttd_auth_ldap - LDAP authentication plugin

Feature: emqttd_broker to support Hooks API

Feature: issue #111 - Support 'Forced Subscriptions' by emqttd_mod_autosub module

Feature: issue #126 - Support 'Rewrite rules' by emqttd_mod_rewrite module

Improve: Support hooks, modules to extend the broker

Improve: issue #76 - dialyzer check

Improve: 'Get Started', 'User Guide', 'Developer Guide' Wiki

Improve: emqtt_topic to add join/1, feed_var/3, is_queue/1

Improve: emqttd_pooler to execute common tasks

Improve: add emqttd_sm_sup module, and use 'hash' gproc_pool to manage sessions

Tests: add more test cases for 'emqttd' app

.. _release_0.7.1:

-------------------
Version 0.7.1-alpha
-------------------

*Release Date: 2015-05-04*

Add doc/design/* and merge doc/* to github Wiki

Bugfix: issue #121 - emqttd cluster issuse

Bugfix: issue #123 - emqttd:unload_all_plugins/0 cannot unload any plugin

Bugfix: fix errors found by dialyzer

.. _release_0.7.0:

-------------------
Version 0.7.0-alpha
-------------------

*Release Date: 2015-05-02*

[MQTT over WebSocket(SSL)](https://github.com/emqtt/emqttd/wiki/MQTT-Over-WebSocket) Now!

[Plugin Achitecture](https://github.com/emqtt/emqttd/wiki/Plugin%20Design) based on OTP application 

[Trace MQTT Packets or Messages](https://github.com/emqtt/emqttd/wiki/Trace%20Design) to log files

Feature: issue #40, #115 - WebSocket/SSL Support

Feature: issue #49, #105 - Plugin Architecture Support

Feature: issue #93 - Trace API Design

Improve: issue #109 - emqttd_broker should add subscribe, notify API 

Improve: update README.md to add 'Goals', 'Contributors' chapters

Change: rename etc/app.config to etc/emqttd.config

Change: etc/emqttd.config changed

Bugfix: critical issue #54 - error when resume session!
 
Bugfix: issue #118 - error report when UNSUBSCRIBE with no topics

Bugfix: issue #117 - sys_interval = 0 config cannot work

Bugfix: issue #112 - Makefile to support build plugins

Bugfix: issue #96 - "make clean" cannot work

.. _release_0.6.2:

-------------------
Version 0.6.2-alpha
-------------------

*Release Date: 2015-04-24*

Bugfix: critical issue #54, #104, #106 - error when resume session 

Improve: add emqttd_cm_sup module, and use 'hash' gproc_pool to register/unregister client ids

Improve: kick old client out when session is duplicated.

Improve: move mnesia dir config from etc/app.config to etc/vm.args

.. _release_0.6.1:

-------------------
Version 0.6.1-alpha
-------------------

*Release Date: 2015-04-20*

Integrate with [gproc library](https://github.com/uwiger/gproc) to support pool

Feature: issues#91 - should use worker_pool to handle some async work?

Feature: issues#95 - Topic filters in ACL rule should support 'eq' tag

Improve: issues#84 - emqttd_pubsub is redesigned again to protect mnesia transaction

Improve: issues#74 - ACL Support and update [ACL Design Wiki](https://github.com/emqtt/emqttd/wiki/ACL-Design)

.. _release_0.6.0:

-------------------
Version 0.6.0-alpha
-------------------

*Release Date: 2015-04-17*

ACL Support Now: [ACL-Design Wiki](https://github.com/emqtt/emqttd/wiki/ACL-Design)

Authentication with username, clientid Now: [Authentication Wiki](https://github.com/emqtt/emqttd/wiki/Authentication)

Seperate common MQTT library to 'emqtt' application

Redesign message pubsub, route and retain modules

Redesign mnesia database cluster

Feature: issues#47 - authentication, authorization support

Feature: issues#92 - merge emqttd_acl and emqttd_auth to emqttd_access_control

Feature: emqttd_acl_mod, emqttd_auth_mod behaviour to extend ACL, authentication 

Feature: issues#85 - lager:info to log subscribe, unsubscribe actions

Feature: issues#77 - authentication with clientid, ipaddress 

Improve: issues#90 - fix lager_file_backend log format, and rotate 10 log files

Improve: issues#88 - use '-mneisa_create', '-mnesia_replicate' attributes to init mneisa

Improve: issues#87 - record mqtt_user and mqtt_client is duplicated

Improve: issues#81 - redesign nodes cluster to support disc_copies mnesia tables

Improve: issues#80 - redesign emqttd_cm to handle more concurrent connections

Improve: issues#70 - how to handle connection flood? Now could support 2K+ CONNECT/sec

Change: redesign mnesia tables: message, topic, subscriber, trie, trie_node

Bugfix: issues#83 - emqttd_broker stats cannot work

Bugfix: issues#75 - careless about function name when emqttd_pubsub handle getstats message

.. _release_0.5.5:

------------------
Version 0.5.5-beta
------------------

*Release Date: 2015-04-09*

Bugfix: issue #75 - careless about function name when emqttd_pubsub handle getstats message.

Bugfix: issue #79 - cannot find topic_subscriber table after cluster with other nodes.

.. _release_0.5.4:

-------------------
Version 0.5.4-alpha
-------------------

*Release Date: 2015-03-22*

Benchmark this release on a ubuntu/14.04 server with 8 cores, 32G memory from QingCloud.com::

    200K Connections,
    30K Messages/Sec,
    20Mbps In/Out Traffic,
    200K Topics,
    200K Subscribers,
     
    Consumed 7G memory, 40% CPU/core

Benchmark code: https://github.com/emqtt/emqttd_benchmark

Change: rewrite emqttd_pubsub to handle more concurrent subscribe requests.

Change: ./bin/emqttd_ctl add 'stats', 'metrics' commands.

Bugfix: issue #71, #72

.. _release_0.5.3:

-------------------
Version 0.5.3-alpha
-------------------

*Release Date: 2015-03-19*

Bugfix: issues#72 - emqttd_cm, emqtt_sm ets:match_delete/2 with wrong pattern

.. _release_0.5.2:

-------------------
Version 0.5.2-alpha
-------------------

*Release Date: 2015-03-18*

Change: upgrade esockd to 2.1.0-alpha, do not tune socket buffer for mqtt connection.

.. _release_0.5.1:

-------------------
Version 0.5.1-alpha
-------------------

*Release Date: 2015-03-13*

Change: upgrade esockd to v1.2.0-beta, rename 'acceptor_pool' to 'acceptors'

.. _release_0.5.0:

-------------------
Version 0.5.0-alpha
-------------------

*Release Date: 2015-03-12*

RENAME 'emqtt' to 'emqttd'!

Support [Broker Bridge](https://github.com/emqtt/emqttd/wiki/Bridge-Design) Now!

Change: rename project from 'emqtt' to 'emqttd'

Change: lager:debug to dump RECV/SENT packets

Feature: emqttd_bridge, emqttd_bridge_sup to support broker bridge

Feature: emqtt_event to publish client connected/disconnected message to $SYS topics

Feature: ./bin/emqttd_ctl add more commands: listeners, broker, bridges, start_bridge, stop_bridge...

Feature: issue#57 - support to configure max packet size

Feature: issue#68 - if sys_interval = 0, emqttd_broker will not publish messages to $SYS/brokers/#

Bugfix: issue#67 - subscribe '#' to receive all messages

Bugfix: issue#64 - emqtt_app start/2: should wait_for_databases

Test: emqttd_topic_tests add more '_match_test'

.. _release_0.4.0:

-------------------
Version 0.4.0-alpha
-------------------

*Release Date: 2015-03-10*

Support [$SYS Topics of Broker](https://github.com/emqtt/emqttd/wiki/$SYS-Topics-of-Broker) Now!

Feature: emqtt_broker to publish version, uptime, datetime to $SYS/brokers/# topics

Feature: emqtt_broker to publish count of clients, sessions, suscribers to $SYS/brokers/# topics

Feature: emqtt_metrics to publish bytes, packets, messages metrics to $SYS/brokers/# topics

Feature: add include/emqtt_systop.hrl

Change: emqtt_cm to count current clients

Change: emqtt_sm to count current sessions

Change: emqtt_pubsub to count current topics and suscribers

Change: emqtt_pubsub to add create/1 API

Change: emqtt_pubsub dispatch/2 to return number of subscribers

Change: emqtt_pubsub to count 'dropped' messages

Change: emqtt_opts to add merge/2 function

Test: add emqtt_serialiser_tests.erl

.. _release_0.3.4:

------------------
Version 0.3.4-beta
------------------

*Release Date: 2015-03-08*

Bugfix: emqtt_serialiser.erl cannot serialise UNSUBACK packets

.. _release_0.3.3:

------------------
Version 0.3.3-beta
------------------

*Release Date: 2015-03-07*

Bugfix: emqtt_serialiser.erl cannot serialise PINGRESP issue#60

.. _release_0.3.2:

------------------
Version 0.3.2-beta
------------------

*Release Date: 2015-03-05*

Improve: merge emqttc serialiser, parser, packet

Add: emqtt_opts to merge socket options

.. _release_0.3.1:

------------------
Version 0.3.1-beta
------------------

*Release Date: 2015-03-02*

Feature: SSL Socket Support

Feature: issue#44 HTTP API should add Qos parameter

Bugfix: issue#52 emqtt_session crash

Bugfix: issue#53 sslsocket keepalive error

Upgrade: esockd to v0.2.0

Upgrade: mochiweb to v3.0.0

.. _release_0.3.0:

------------------
Version 0.3.0-beta
------------------

*Release Date: 2015-01-19*

Feature: HTTP POST API to support 'qos', 'retain' parameters

Feature: $SYS system topics support

Change: Rewrite emqtt_topic.erl, use '', '#', '+' to replace <<"">>, <<"#">>, <<"+">>

Change: fix emqtt_pubsub.erl to match '#', '+'

Tests: emqtt_topic_tests.erl add more test cases

-------------------
Version 0.3.0-alpha
-------------------

*Release Date: 2015-01-08*

NOTICE: Full MQTT 3.1.1 support now!

Feature: Passed org.eclipse.paho.mqtt.testing/interoperability tests

Feature: Qos0, Qos1 and Qos2 publish and suscribe

Feature: session(clean_sess=false) management and offline messages

Feature: redeliver awaiting puback/pubrec messages(doc: Chapter 4.4)

Feature: retain messages, add emqtt_server module

Feature: MQTT 3.1.1 null client_id support

Bugfix: keepalive timeout to send will message 

Improve: overlapping subscription support

Improve: add emqtt_packet:dump to dump packets

Test: passed org.eclipse.paho.mqtt.testing/interoperability

Test: simple cluster test

Closed Issues: #22, #24, #27, #28, #29, #30, #31, #32, #33, #34, #36, #37, #38, #39, #41, #42, #43

.. _release_0.2.1:

------------------
Version 0.2.1-beta
------------------

*Release Date: 2015-01-08*

pull request 26: Use binaries for topic paths and fix wildcard topics

emqtt_pubsub.erl: fix wildcard topic match bug caused by binary topic in 0.2.0 

Makefile: deps -> get-deps

rebar.config: fix mochiweb git url

tag emqtt release accoding to [Semantic Versioning](http://semver.org/)

max clientId length is 1024 now.

.. _release_0.2.0:

-------------
Version 0.2.0
-------------

*Release Date: 2014-12-07*

rewrite the project, integrate with esockd, mochiweb

support MQTT 3.1.1

support HTTP to publish message

.. _release_0.1.5:

-------------
Version 0.1.5
-------------

*Release Date: 2013-01-05*

Bugfix: remove QOS_1 match when handle PUBREL request 
 
Bugfix: reverse word in emqtt_topic:words/1 function

.. _release_0.1.4:

-------------
Version 0.1.4
-------------

*Release Date: 2013-01-04*

Bugfix: fix "mosquitto_sub -q 2 ......" bug

Bugfix: fix keep alive bug

.. _release_0.1.3:

-------------
Version 0.1.3
-------------

*Release Date: 2013-01-04*

Feature: Support QOS2 PUBREC, PUBREL, PUBCOMP messages

Bugfix: fix emqtt_frame to encode/decoe PUBREC/PUBREL messages

.. _release_0.1.2:

-------------
Version 0.1.2
-------------

*Release Date: 2012-12-27*

Feature: release support like riak

Bugfix: use ?INFO/?ERROR to print log in tcp_listener.erl

.. _release_0.1.1:

-------------
Version 0.1.1
-------------

*Release Date: 2012-09-24*

Feature: use rebar to generate release

Feature: support retained messages

Bugfix: send will msg when network error

.. _release_0.1.0:

-------------
Version 0.1.0
-------------

*Release Date: 2012-09-21*

The first public release.

