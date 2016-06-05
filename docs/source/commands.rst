
.. _commands::

========
Commands
========

The './bin/emqttd_ctl' command line could be used to query and administrate emqttd broker.

.. WARNING:: Cannot work on Windows

.. _command_status::

------
status
------

Show running status of the broker::

    $ ./bin/emqttd_ctl status

    Node 'emqttd@127.0.0.1' is started
    emqttd 1.1 is running

.. _command_broker::

------
broker
------

Query basic information,  statistics and metrics of the broker.

+----------------+-------------------------------------------------+
| broker         | Show version, description, uptime of the broker |
+----------------+-------------------------------------------------+
| broker pubsub  | Show status of the core pubsub process          |
+----------------+-------------------------------------------------+
| broker stats   | Show statistics of client, session, topic,      |
|                | subscription and route of the broker            |
+----------------+-------------------------------------------------+
| broker metrics | Show metrics of MQTT bytes, packets, messages   |
|                | sent/received.                                  |
+----------------+-------------------------------------------------+

Query version, description and uptime of the broker::

    $ ./bin/emqttd_ctl broker

    sysdescr  : Erlang MQTT Broker
    version   : 0.15.0
    uptime    : 1 hours, 25 minutes, 24 seconds
    datetime  : 2016-01-16 13:17:32

broker stats
------------

Query statistics of MQTT Client, Session, Topic, Subscription and Route::

    $ ./bin/emqttd_ctl broker stats

    clients/count       : 1
    clients/max         : 1
    queues/count        : 0
    queues/max          : 0
    retained/count      : 2
    retained/max        : 2
    routes/count        : 2
    routes/reverse      : 2
    sessions/count      : 0
    sessions/max        : 0
    subscriptions/count : 1
    subscriptions/max   : 1
    topics/count        : 54
    topics/max          : 54

broker metrics
--------------

Query metrics of Bytes, MQTT Packets and Messages(sent/received)::

    $ ./bin/emqttd_ctl broker metrics

    bytes/received          : 297
    bytes/sent              : 40
    messages/dropped        : 348
    messages/qos0/received  : 0
    messages/qos0/sent      : 0
    messages/qos1/received  : 0
    messages/qos1/sent      : 0
    messages/qos2/received  : 0
    messages/qos2/sent      : 0
    messages/received       : 0
    messages/retained       : 2
    messages/sent           : 0
    packets/connack         : 5
    packets/connect         : 5
    packets/disconnect      : 0
    packets/pingreq         : 0
    packets/pingresp        : 0
    packets/puback/received : 0
    packets/puback/sent     : 0
    packets/pubcomp/received: 0
    packets/pubcomp/sent    : 0
    packets/publish/received: 0
    packets/publish/sent    : 0
    packets/pubrec/received : 0
    packets/pubrec/sent     : 0
    packets/pubrel/received : 0
    packets/pubrel/sent     : 0
    packets/received        : 9
    packets/sent            : 9
    packets/suback          : 4
    packets/subscribe       : 4
    packets/unsuback        : 0
    packets/unsubscribe     : 0

.. _command_cluster::

-------
cluster
-------

Cluster two or more emqttd brokers.

+-----------------------+--------------------------------+
| cluster join <Node>   | Join the cluster               |
+-----------------------+--------------------------------+
| cluster leave         | Leave the cluster              |
+-----------------------+--------------------------------+
| cluster remove <Node> | Remove a node from the cluster |
+-----------------------+--------------------------------+
| cluster status        | Query cluster status and nodes |
+-----------------------+--------------------------------+

Suppose we create two emqttd nodes on localhost and cluster them:

+-----------+---------------------+-------------+
| Folder    | Node                | MQTT Port   |
+-----------+---------------------+-------------+
| emqttd1   | emqttd1@127.0.0.1   | 1883        |
+-----------+---------------------+-------------+
| emqttd2   | emqttd2@127.0.0.1   | 2883        |
+-----------+---------------------+-------------+

Start emqttd1 node::

    cd emqttd1 && ./bin/emqttd start

Start emqttd2 node::

    cd emqttd2 && ./bin/emqttd start

Under emqttd2 folder:: 

    $ ./bin/emqttd_ctl cluster join emqttd1@127.0.0.1

    Join the cluster successfully.
    Cluster status: [{running_nodes,['emqttd1@127.0.0.1','emqttd2@127.0.0.1']}]

Query cluster status::

    $ ./bin/emqttd_ctl cluster status

    Cluster status: [{running_nodes,['emqttd2@127.0.0.1','emqttd1@127.0.0.1']}]

Message Route between nodes::

    # Subscribe topic 'x' on emqttd1 node
    mosquitto_sub -t x -q 1 -p 1883

    # Publish to topic 'x' on emqttd2 node
    mosquitto_pub -t x -q 1 -p 2883 -m hello

emqttd2 leaves the cluster::

    cd emqttd2 && ./bin/emqttd_ctl cluster leave

Or remove emqttd2 from the cluster on emqttd1 node::

    cd emqttd1 && ./bin/emqttd_ctl cluster remove emqttd2@127.0.0.1

.. _command_clients::

-------
clients
-------

Query MQTT clients connected to the broker:

+-------------------------+----------------------------------+
| clients list            | List all MQTT clients            |
+-------------------------+----------------------------------+
| clients show <ClientId> | Show a MQTT Client               |
+-------------------------+----------------------------------+
| clients kick <ClientId> | Kick out a MQTT client           |
+-------------------------+----------------------------------+

clients lists
-------------

Query All MQTT clients connected to the broker::

    $ ./bin/emqttd_ctl clients list

    Client(mosqsub/43832-airlee.lo, clean_sess=true, username=test, peername=127.0.0.1:64896, connected_at=1452929113)
    Client(mosqsub/44011-airlee.lo, clean_sess=true, username=test, peername=127.0.0.1:64961, connected_at=1452929275)
    ...

Properties of the Client:

+--------------+---------------------------------------------------+
| clean_sess   | Clean Session Flag                                |
+--------------+---------------------------------------------------+
| username     | Username of the client                            |
+--------------+---------------------------------------------------+
| peername     | Peername of the TCP connection                    |
+--------------+---------------------------------------------------+
| connected_at | The timestamp when client connected to the broker |
+--------------+---------------------------------------------------+

clients show <ClientId>
-----------------------

Show a specific MQTT Client::

    ./bin/emqttd_ctl clients show "mosqsub/43832-airlee.lo"

    Client(mosqsub/43832-airlee.lo, clean_sess=true, username=test, peername=127.0.0.1:64896, connected_at=1452929113)

clients kick <ClientId>
-----------------------
        
Kick out a MQTT Client::

    ./bin/emqttd_ctl clients kick "clientid"

.. _command_sessions::

--------
sessions
--------

Query all MQTT sessions. The broker will create a session for each MQTT client. Persistent Session if clean_session flag is true, transient session otherwise.

+--------------------------+-------------------------------+
| sessions list            | List all Sessions             |
+--------------------------+-------------------------------+
| sessions list persistent | Query all persistent Sessions |
+--------------------------+-------------------------------+
| sessions list transient  | Query all transient Sessions  |
+--------------------------+-------------------------------+
| sessions show <ClientId> | Show a session                |
+--------------------------+-------------------------------+

sessions list
-------------

Query all sessions::

    $ ./bin/emqttd_ctl sessions list

    Session(clientid, clean_sess=false, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935508)
    Session(mosqsub/44101-airlee.lo, clean_sess=true, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935401)

Properties of Session:

TODO:??

+-------------------+----------------------------------------------------------------+
| clean_sess        | clean sess flag. false: persistent, true: transient            |
+-------------------+----------------------------------------------------------------+
| max_inflight      | Inflight window (Max number of messages delivering)            |
+-------------------+----------------------------------------------------------------+
| inflight_queue    | Inflight Queue Size                                            |
+-------------------+----------------------------------------------------------------+
| message_queue     | Message Queue Size                                             |
+-------------------+----------------------------------------------------------------+
| message_dropped   | Number of Messages Dropped for queue is full                   |
+-------------------+----------------------------------------------------------------+
| awaiting_rel      | The number of QoS2 messages received and waiting for PUBREL    |
+-------------------+----------------------------------------------------------------+
| awaiting_ack      | The number of QoS1/2 messages delivered and waiting for PUBACK |
+-------------------+----------------------------------------------------------------+
| awaiting_comp     | The number of QoS2 messages delivered and waiting for PUBCOMP  |
+-------------------+----------------------------------------------------------------+
| created_at        | Timestamp when the session is created                          |
+-------------------+----------------------------------------------------------------+

sessions list persistent
------------------------

Query all persistent sessions::

    $ ./bin/emqttd_ctl sessions list persistent

    Session(clientid, clean_sess=false, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935508)

sessions list transient
-----------------------

Query all transient sessions::

    $ ./bin/emqttd_ctl sessions list transient

    Session(mosqsub/44101-airlee.lo, clean_sess=true, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935401)

sessions show <ClientId>
------------------------

Show a session::

    $ ./bin/emqttd_ctl sessions show clientid

    Session(clientid, clean_sess=false, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935508)

.. _command_routes::

------
routes
------

Show routing table of the broker.

routes list
-----------

List all routes::

    $ ./bin/emqttd_ctl routes list

    t2/# -> emqttd2@127.0.0.1
    t/+/x -> emqttd2@127.0.0.1,emqttd@127.0.0.1

routes show <Topic>
-------------------

Show a route::

    $ ./bin/emqttd_ctl routes show t/+/x

    t/+/x -> emqttd2@127.0.0.1,emqttd@127.0.0.1

.. _command_topics::

------
topics
------

Query topic table of the broker.

topics list
-----------

Query all the topics::

    $ ./bin/emqttd_ctl topics list

    $SYS/brokers/emqttd@127.0.0.1/metrics/packets/subscribe: static
    $SYS/brokers/emqttd@127.0.0.1/stats/subscriptions/max: static
    $SYS/brokers/emqttd2@127.0.0.1/stats/subscriptions/count: static
    ...

topics show <Topic>
-------------------

Show a topic::

    $ ./bin/emqttd_ctl topics show '$SYS/brokers'

    $SYS/brokers: static

.. _command_subscriptions::

-------------
subscriptions
-------------

Query the subscription table of the broker:

+--------------------------------------------+--------------------------------------+
| subscriptions list                         | List all subscriptions               |
+--------------------------------------------+--------------------------------------+
| subscriptions show <ClientId>              | Show a subscription                  |
+--------------------------------------------+--------------------------------------+
| subscriptions add <ClientId> <Topic> <Qos> | Add a static subscription manually   |
+--------------------------------------------+--------------------------------------+
| subscriptions del <ClientId> <Topic>       | Remove a static subscription manually|
+--------------------------------------------+--------------------------------------+

subscriptions list
------------------

Query all subscriptions::

    $ ./bin/emqttd_ctl subscriptions list

    mosqsub/91042-airlee.lo -> t/y:1
    mosqsub/90475-airlee.lo -> t/+/x:2

subscriptions list static
-------------------------

List all static subscriptions::

    $ ./bin/emqttd_ctl subscriptions list static

    clientid -> new_topic:1

subscriptions show <ClientId>
-----------------------------

Show the subscriptions of a MQTT client::

    $ ./bin/emqttd_ctl subscriptions show clientid

    clientid: [{<<"x">>,1},{<<"topic2">>,1},{<<"topic3">>,1}]

subscriptions add <ClientId> <Topic> <QoS>
------------------------------------------

Add a static subscription manually::

    $ ./bin/emqttd_ctl subscriptions add clientid new_topic 1
    ok

subscriptions del <ClientId> <Topic>
------------------------------------

Remove a static subscription manually::

    $ ./bin/emqttd_ctl subscriptions del clientid new_topic
    ok

.. _command_plugins::

-------
plugins
-------

List, load or unload plugins of emqttd broker.

+---------------------------+-------------------------+
| plugins list              | List all plugins        |
+---------------------------+-------------------------+
| plugins load <Plugin>     | Load Plugin             |
+---------------------------+-------------------------+
| plugins unload <Plugin>   | Unload (Plugin)         |
+---------------------------+-------------------------+

plugins list
------------

List all plugins::

    $ ./bin/emqttd_ctl plugins list

    Plugin(emqttd_dashboard, version=0.16.0, description=emqttd web dashboard, active=true)
    Plugin(emqttd_plugin_mysql, version=0.16.0, description=emqttd Authentication/ACL with MySQL, active=false)
    Plugin(emqttd_plugin_pgsql, version=0.16.0, description=emqttd PostgreSQL Plugin, active=false)
    Plugin(emqttd_plugin_redis, version=0.16.0, description=emqttd Redis Plugin, active=false)
    Plugin(emqttd_plugin_template, version=0.16.0, description=emqttd plugin template, active=false)
    Plugin(emqttd_recon, version=0.16.0, description=emqttd recon plugin, active=false)
    Plugin(emqttd_stomp, version=0.16.0, description=Stomp Protocol Plugin for emqttd broker, active=false)

Properties of a plugin:

+-------------+--------------------------+
| version     | Plugin Version           |
+-------------+--------------------------+
| description | Plugin Description       |
+-------------+--------------------------+
| active      | If the plugin is Loaded  | 
+-------------+--------------------------+

load <Plugin>
-------------

Load a Plugin::

    $ ./bin/emqttd_ctl plugins load emqttd_recon

    Start apps: [recon,emqttd_recon]
    Plugin emqttd_recon loaded successfully.

unload <Plugin>
---------------

Unload a Plugin::

    $ ./bin/emqttd_ctl plugins unload emqttd_recon

    Plugin emqttd_recon unloaded successfully.

.. _command_bridges::

-------
bridges
-------

Bridge two or more emqttd brokers::

                  ---------                     ---------
    Publisher --> | node1 | --Bridge Forward--> | node2 | --> Subscriber
                  ---------                     ---------

commands for bridge:

+----------------------------------------+------------------------------+
| bridges list                           | List all bridges             |
+----------------------------------------+------------------------------+
| bridges options                        | Show bridge options          |
+----------------------------------------+------------------------------+
| bridges start <Node> <Topic>           | Create a bridge              |
+----------------------------------------+------------------------------+
| bridges start <Node> <Topic> <Options> | Create a bridge with options |
+----------------------------------------+------------------------------+
| bridges stop <Node> <Topic>            | Delete a bridge              |
+----------------------------------------+------------------------------+

Suppose we create a bridge between emqttd1 and emqttd2 on localhost:

+---------+---------------------+-----------+
| Name    | Node                | MQTT Port |
+---------+---------------------+-----------+
| emqttd1 | emqttd1@127.0.0.1   | 1883      |
+---------+---------------------+-----------+
| emqttd2 | emqttd2@127.0.0.1   | 2883      |
+---------+---------------------+-----------+

The bridge will forward all the the 'sensor/#' messages from emqttd1 to emqttd2:: 

    $ ./bin/emqttd_ctl bridges start emqttd2@127.0.0.1 sensor/#

    bridge is started.
    
    $ ./bin/emqttd_ctl bridges list

    bridge: emqttd1@127.0.0.1--sensor/#-->emqttd2@127.0.0.1

The the 'emqttd1--sensor/#-->emqttd2' bridge:: 

    #emqttd2 node

    mosquitto_sub -t sensor/# -p 2883 -d

    #emqttd1节点上

    mosquitto_pub -t sensor/1/temperature -m "37.5" -d 

bridges options
---------------

Show bridge options::

    $ ./bin/emqttd_ctl bridges options

    Options:
      qos     = 0 | 1 | 2
      prefix  = string
      suffix  = string
      queue   = integer
    Example:
      qos=2,prefix=abc/,suffix=/yxz,queue=1000

bridges stop <Node> <Topic>
---------------------------

Delete the emqttd1--sensor/#-->emqttd2 bridge::

    $ ./bin/emqttd_ctl bridges stop emqttd2@127.0.0.1 sensor/#

    bridge is stopped.

.. _command_vm::

--
vm
--

Query the load, cpu, memory, processes and IO information of the Erlang VM.

+-------------+-----------------------------------+
| vm all      | Query all                         |
+-------------+-----------------------------------+
| vm load     | Query VM Load                     |
+-------------+-----------------------------------+
| vm memory   | Query Memory Usage                |
+-------------+-----------------------------------+
| vm process  | Query Number of Erlang Processes  |
+-------------+-----------------------------------+
| vm io       | Query Max Fds of VM               |
+-------------+-----------------------------------+

vm load
-------

Query load::

    $ ./bin/emqttd_ctl vm load

    cpu/load1               : 2.21
    cpu/load5               : 2.60
    cpu/load15              : 2.36

vm memory
---------

Query memory::

    $ ./bin/emqttd_ctl vm memory

    memory/total            : 23967736
    memory/processes        : 3594216
    memory/processes_used   : 3593112
    memory/system           : 20373520
    memory/atom             : 512601
    memory/atom_used        : 491955
    memory/binary           : 51432
    memory/code             : 13401565
    memory/ets              : 1082848

vm process
----------

Query number of erlang processes::

    $ ./bin/emqttd_ctl vm process

    process/limit           : 8192
    process/count           : 221

vm io
-----

Query max, active file descriptors of IO::

    $ ./bin/emqttd_ctl vm io

    io/max_fds              : 2560
    io/active_fds           : 1

.. _command_trace::

-----
trace
-----

Trace MQTT packets, messages(sent/received) by ClientId or Topic.

+-----------------------------------+-----------------------------------+
| trace list                        | List all the traces               |
+-----------------------------------+-----------------------------------+
| trace client <ClientId> <LogFile> | Trace a client                    |
+-----------------------------------+-----------------------------------+
| trace client <ClientId> off       | Stop tracing the client           |
+-----------------------------------+-----------------------------------+
| trace topic <Topic> <LogFile>     | Trace a topic                     |
+-----------------------------------+-----------------------------------+
| trace topic <Topic> off           | Stop tracing the topic            |
+-----------------------------------+-----------------------------------+

trace client <ClientId> <LogFile>
---------------------------------

Start to trace a client::

    $ ./bin/emqttd_ctl trace client clientid log/clientid_trace.log

    trace client clientid successfully.

trace client <ClientId> off
---------------------------

Stop tracing the client::

    $ ./bin/emqttd_ctl trace client clientid off
    
    stop tracing client clientid successfully.

trace topic <Topic> <LogFile>
-----------------------------

Start to trace a topic::

    $ ./bin/emqttd_ctl trace topic topic log/topic_trace.log

    trace topic topic successfully.

trace topic <Topic> off
-----------------------

Stop tracing the topic::

    $ ./bin/emqttd_ctl trace topic topic off

    stop tracing topic topic successfully.

trace list
----------

List all traces::

    $ ./bin/emqttd_ctl trace list

    trace client clientid -> log/clientid_trace.log
    trace topic topic -> log/topic_trace.log

.. _command_listeners::

---------
listeners
---------

Show all the TCP listeners::

    $ ./bin/emqttd_ctl listeners

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
      acceptors       : 16
      max_clients     : 8192
      current_clients : 1
      shutdown_count  : [{closed,1}]
    listener on http:18083
      acceptors       : 4
      max_clients     : 512
      current_clients : 0
      shutdown_count  : []

listener parameters:

+-----------------+--------------------------------------+
| acceptors       | TCP Acceptor Pool                    |
+-----------------+--------------------------------------+
| max_clients     | Max number of clients                |
+-----------------+--------------------------------------+
| current_clients | Count of current clients             |
+-----------------+--------------------------------------+
| shutdown_count  | Statistics of client shutdown reason |
+----------------+---------------------------------------+

.. _command_mnesia::

------
mnesia
------

Show system_info of mnesia database.

------
admins
------

The 'admins' CLI is used to add/del admin account, which is registered by the dashboard plugin.

+------------------------------------+-----------------------------+
| admins add <Username> <Password>   | Add admin account           |
+------------------------------------+-----------------------------+
| admins passwd <Username> <Password>| Reset admin password        |
+------------------------------------+-----------------------------+
| admins del <Username>              | Delete admin account        |
+------------------------------------+-----------------------------+

admins add
----------

Add admin account::

    $ ./bin/emqttd_ctl admins add root public
    ok

admins passwd
-------------

Reset password::

    $ ./bin/emqttd_ctl admins passwd root private
    ok

admins del
----------

Delete admin account::

    $ ./bin/emqttd_ctl admins del root
    ok

