
.. _commands::

============
Command Line
============

emqttd消息服务器提供了'./bin/emqttd_ctl'的管理命令行。

.. WARNING:: 限制: Windows平台无法使用。

----------
status
----------

查询emqttd消息服务器运行状态::
    
    $ ./bin/emqttd_ctl status

    Node 'emqttd@127.0.0.1' is started
    emqttd 0.16.0 is running


----------
broker
----------

broker命令查询服务器基本信息，启动时间，统计数据与性能数据。

+----------------+-----------------------------------------------+
| broker         | 查询emqttd消息服务器描述、版本、启动时间      |
+----------------+-----------------------------------------------+
| broker pubsub  | 查询核心的Erlang PubSub进程状态(调试)         |
+----------------+-----------------------------------------------+
| broker stats   | 查询连接(Client)、会话(Session)、主题(Topic)、|
|                | 订阅(Subscription)、路由(Route)统计信息       |
+----------------+-----------------------------------------------+
| broker metrics | 查询MQTT报文(Packet)、消息(Message)收发统计   |
+----------------+-----------------------------------------------+

查询emqttd消息服务器基本信息包括版本、启动时间等::

    $ ./bin/emqttd_ctl broker

    sysdescr  : Erlang MQTT Broker
    version   : 0.15.0
    uptime    : 1 hours, 25 minutes, 24 seconds
    datetime  : 2016-01-16 13:17:32

查询服务器客户端连接(Client)、会话(Session)、主题(Topic)、订阅(Subscription)、路由(Route)统计::

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

查询服务器流量(Bytes)、MQTT报文(Packets)、消息(Messages)收发统计::

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


-------
cluster
-------

cluster命令集群多个emqttd消息服务器节点(进程):

+-----------------------+---------------------+
| cluster join <Node>   | 加入集群            |
+-----------------------+---------------------+
| cluster leave         | 离开集群            |
+-----------------------+---------------------+
| cluster remove <Node> | 从集群删除节点      |
+-----------------------+---------------------+
| cluster status        | 查询集群状态        |
+-----------------------+---------------------+

cluster命令集群本机两个emqttd节点示例:

+-----------+---------------------+-------------+
| 目录      | 节点名              | MQTT端口    |
+-----------+---------------------+-------------+
| emqttd1   | emqttd1@127.0.0.1   | 1883        |
+-----------+---------------------+-------------+
| emqttd2   | emqttd2@127.0.0.1   | 2883        |
+-----------+---------------------+-------------+

启动emqttd1::

    cd emqttd1 && ./bin/emqttd start

启动emqttd2::

    cd emqttd2 && ./bin/emqttd start

emqttd2节点与emqttd1集群，emqttd2目录下:: 

    $ ./bin/emqttd_ctl cluster join emqttd1@127.0.0.1

    Join the cluster successfully.
    Cluster status: [{running_nodes,['emqttd1@127.0.0.1','emqttd2@127.0.0.1']}]

任意节点目录下查询集群状态::

    $ ./bin/emqttd_ctl cluster status

    Cluster status: [{running_nodes,['emqttd2@127.0.0.1','emqttd1@127.0.0.1']}]

集群消息路由测试::

    # emqttd1节点上订阅x
    mosquitto_sub -t x -q 1 -p 1883

    # emqttd2节点上向x发布消息
    mosquitto_pub -t x -q 1 -p 2883 -m hello

emqttd2节点离开集群::

    cd emqttd2 && ./bin/emqttd_ctl cluster leave

emqttd1节点下删除emqttd2::

    cd emqttd1 && ./bin/emqttd_ctl cluster remove emqttd2@127.0.0.1

-------
clients
-------

clients命令查询连接的MQTT客户端。

+-------------------------+-----------------------------+
| clients list            | 查询全部客户端连接          |
+-------------------------+-----------------------------+
| clients show <ClientId> | 根据ClientId查询客户端      |
+-------------------------+-----------------------------+
| clients kick <ClientId> | 根据ClientId踢出客户端      |
+-------------------------+-----------------------------+

查询全部客户端连接::

    $ ./bin/emqttd_ctl clients list

    Client(mosqsub/43832-airlee.lo, clean_sess=true, username=test, peername=127.0.0.1:64896, connected_at=1452929113)
    Client(mosqsub/44011-airlee.lo, clean_sess=true, username=test, peername=127.0.0.1:64961, connected_at=1452929275)
    ...

根据ClientId查询客户端::

    ./bin/emqttd_ctl clients show "mosqsub/43832-airlee.lo"

    Client(mosqsub/43832-airlee.lo, clean_sess=true, username=test, peername=127.0.0.1:64896, connected_at=1452929113)
        
根据ClientId踢出客户端::

    ./bin/emqttd_ctl clients kick "clientid"

返回Client对象的属性:

+--------------+-----------------------------+
| clean_sess   | 清除会话标记                |
+--------------+-----------------------------+
| username     | 用户名                      |
+--------------+-----------------------------+
| peername     | 对端TCP地址                 |
+--------------+-----------------------------+
| connected_at | 客户端连接时间              |
+--------------+-----------------------------+

--------
sessions
--------

sessions命令查询MQTT连接会话。emqttd消息服务器会为每个连接创建会话，clean_session标记true，创建临时(transient)会话；clean_session标记为false，创建持久会话(persistent)。

+--------------------------+-----------------------------+
| sessions list            | 查询全部会话                |
+--------------------------+-----------------------------+
| sessions list persistent | 查询全部持久会话            |
+--------------------------+-----------------------------+
| sessions list transient  | 查询全部临时会话            |
+--------------------------+-----------------------------+
| sessions show <ClientId> | 根据ClientID查询会话        |
+--------------------------+-----------------------------+

查询全部会话::

    $ ./bin/emqttd_ctl sessions list

    Session(clientid, clean_sess=false, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935508)
    Session(mosqsub/44101-airlee.lo, clean_sess=true, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935401)

查询全部持久会话::

    $ ./bin/emqttd_ctl sessions list persistent

    Session(clientid, clean_sess=false, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935508)

查询全部临时会话::

    $ ./bin/emqttd_ctl sessions list transient

    Session(mosqsub/44101-airlee.lo, clean_sess=true, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935401)

根据ClientId查询会话::

    $ ./bin/emqttd_ctl sessions show clientid

    Session(clientid, clean_sess=false, max_inflight=100, inflight_queue=0, message_queue=0, message_dropped=0, awaiting_rel=0, awaiting_ack=0, awaiting_comp=0, created_at=1452935508)

返回Session对象属性:

+-------------------+------------------------------------+
| clean_sess        | false: 持久会话，true: 临时会话    |
+-------------------+------------------------------------+
| max_inflight      | 飞行窗口(最大允许同时下发消息数)   |
+-------------------+------------------------------------+
| inflight_queue    | 当前正在下发的消息数               |
+-------------------+------------------------------------+
| message_queue     | 当前缓存消息数                     |
+-------------------+------------------------------------+
| message_dropped   | 会话丢掉的消息数                   |
+-------------------+------------------------------------+
| awaiting_rel      | 等待客户端发送PUBREL的QoS2消息数   |
+-------------------+------------------------------------+
| awaiting_ack      | 等待客户端响应PUBACK的QoS1/2消息数 |
+-------------------+------------------------------------+
| awaiting_comp     | 等待客户端响应PUBCOMP的QoS2消息数  |
+-------------------+------------------------------------+
| created_at        | 会话创建时间戳                     |
+-------------------+------------------------------------+

------
topics
------

topics命令查询emqttd消息服务器当前的主题(Topic)表。

'topics list'查询全部主题(Topic)::

    $ ./bin/emqttd_ctl topics list

    y: ['emqttd2@127.0.0.1']
    x: ['emqttd1@127.0.0.1','emqttd2@127.0.0.1']

'topics show <Topic>'查询某个主题(Topic)::

    $ ./bin/emqttd_ctl topics show x

    x: ['emqttd1@127.0.0.1','emqttd2@127.0.0.1']

返回结果显示主题(Topic)所在集群节点列表。

-------------
subscriptions
-------------

subscriptions命令查询消息服务器的订阅(Subscription)表。

+--------------------------------------------+-------------------------+
| subscriptions list                         | 查询全部订阅            |
+--------------------------------------------+-------------------------+
| subscriptions show <ClientId>              | 查询某个ClientId的订阅  |
+--------------------------------------------+-------------------------+
| subscriptions add <ClientId> <Topic> <Qos> | 手工添加一条订阅        |
+--------------------------------------------+-------------------------+
| subscriptions del <ClientId> <Topic>       | 手工删除一条订阅        |
+--------------------------------------------+-------------------------+

查询全部订阅::

    $ ./bin/emqttd_ctl subscriptions list

    mosqsub/45744-airlee.lo: [{<<"y">>,0},{<<"x">>,0}]

.. todo:: 打印结果格式需修改。

查询某个ClientId的订阅::

    $ ./bin/emqttd_ctl subscriptions show clientid

    clientid: [{<<"x">>,1},{<<"topic2">>,1},{<<"topic3">>,1}]

手工添加一条订阅::

    $ ./bin/emqttd_ctl subscriptions add clientid new_topic 1
    ok

手工删除一条订阅::

    $ ./bin/emqttd_ctl subscriptions del clientid new_topic
    ok


-------
plugins
-------


plugins命令用于加载、卸载、查询插件应用。emqttd消息服务器通过插件扩展认证、定制功能，插件置于plugins/目录下。

+---------------------------+-------------------------+
| plugins list              | 列出全部插件(Plugin)    |
+---------------------------+-------------------------+
| plugins load <Plugin>     | 加载插件(Plugin)        |
+---------------------------+-------------------------+
| plugins unload <Plugin>   | 卸载插件(Plugin)        |
+---------------------------+-------------------------+

列出插件::

    $ ./bin/emqttd_ctl plugins list

    Plugin(emqttd_dashboard, version=0.16.0, description=emqttd web dashboard, active=true)
    Plugin(emqttd_plugin_mysql, version=0.16.0, description=emqttd Authentication/ACL with MySQL, active=false)
    Plugin(emqttd_plugin_pgsql, version=0.16.0, description=emqttd PostgreSQL Plugin, active=false)
    Plugin(emqttd_plugin_redis, version=0.16.0, description=emqttd Redis Plugin, active=false)
    Plugin(emqttd_plugin_template, version=0.16.0, description=emqttd plugin template, active=false)
    Plugin(emqttd_recon, version=0.16.0, description=emqttd recon plugin, active=false)
    Plugin(emqttd_stomp, version=0.16.0, description=Stomp Protocol Plugin for emqttd broker, active=false)

插件属性:

+-------------+-----------------+
| version     | 插件版本        |
+-------------+-----------------+
| description | 插件描述        |
+-------------+-----------------+
| active      | 是否已加载      | 
+-------------+-----------------+

加载插件::

    $ ./bin/emqttd_ctl plugins load emqttd_recon

    Start apps: [recon,emqttd_recon]
    Plugin emqttd_recon loaded successfully.

卸载插件::

    $ ./bin/emqttd_ctl plugins unload emqttd_recon

    Plugin emqttd_recon unloaded successfully.


-------
bridges
-------

plugins命令用于加载、卸载、查询插件应用。emqttd消息服务器通过插件扩展认证、定制功能，插件置于plugins/目录下。

+---------------------------+-------------------------+
| plugins list              | 列出全部插件(Plugin)    |
+---------------------------+-------------------------+
| plugins load <Plugin>     | 加载插件(Plugin)        |
+---------------------------+-------------------------+
| plugins unload <Plugin>   | 卸载插件(Plugin)        |
+---------------------------+-------------------------+

列出插件::

    $ ./bin/emqttd_ctl plugins list

    Plugin(emqttd_dashboard, version=0.16.0, description=emqttd web dashboard, active=true)
    Plugin(emqttd_plugin_mysql, version=0.16.0, description=emqttd Authentication/ACL with MySQL, active=false)
    Plugin(emqttd_plugin_pgsql, version=0.16.0, description=emqttd PostgreSQL Plugin, active=false)
    Plugin(emqttd_plugin_redis, version=0.16.0, description=emqttd Redis Plugin, active=false)
    Plugin(emqttd_plugin_template, version=0.16.0, description=emqttd plugin template, active=false)
    Plugin(emqttd_recon, version=0.16.0, description=emqttd recon plugin, active=false)
    Plugin(emqttd_stomp, version=0.16.0, description=Stomp Protocol Plugin for emqttd broker, active=false)

插件属性:

+-------------+-----------------+
| version     | 插件版本        |
+-------------+-----------------+
| description | 插件描述        |
+-------------+-----------------+
| active      | 是否已加载      | 
+-------------+-----------------+

加载插件::

    $ ./bin/emqttd_ctl plugins load emqttd_recon

    Start apps: [recon,emqttd_recon]
    Plugin emqttd_recon loaded successfully.

卸载插件::

    $ ./bin/emqttd_ctl plugins unload emqttd_recon

    Plugin emqttd_recon unloaded successfully.


-------
bridges
-------

bridges命令用于在多台emqttd服务器节点间创建桥接。

+----------------------------------------+---------------------------+
| bridges list                           | 查询全部桥接              |
+----------------------------------------+---------------------------+
| bridges options                        | 查询创建桥接选项          |
+----------------------------------------+---------------------------+
| bridges start <Node> <Topic>           | 创建桥接                  |
+----------------------------------------+---------------------------+
| bridges start <Node> <Topic> <Options> | 创建桥接并带选项设置      |
+----------------------------------------+---------------------------+
| bridges stop <Node> <Topic>            | 删除桥接                  |
+----------------------------------------+---------------------------+

创建一条emqttd1 -> emqttd2节点的桥接，转发传感器主题(Topic)消息到emqttd2::

    $ ./bin/emqttd_ctl bridges start emqttd2@127.0.0.1 sensor/#

    bridge is started.
    
    $ ./bin/emqttd_ctl bridges list

    bridge: emqttd1@127.0.0.1--sensor/#-->emqttd2@127.0.0.1

测试emqttd1--sensor/#-->emqttd2的桥接::

    #emqttd2节点上

    mosquitto_sub -t sensor/# -p 2883 -d

    #emqttd1节点上

    mosquitto_pub -t sensor/1/temperature -m "37.5" -d 

查询bridge创建选项设置::

    $ ./bin/emqttd_ctl bridges options

    Options:
      qos     = 0 | 1 | 2
      prefix  = string
      suffix  = string
      queue   = integer
    Example:
      qos=2,prefix=abc/,suffix=/yxz,queue=1000

删除emqttd1--sensor/#-->emqttd2的桥接::

    $ ./bin/emqttd_ctl bridges stop emqttd2@127.0.0.1 sensor/#

    bridge is stopped.
    
--
vm
--

vm命令用于查询Erlang虚拟机负载、内存、进程、IO信息。

+-------------+------------------------+
| vm all      | 查询VM全部信息         |
+-------------+------------------------+
| vm load     | 查询VM负载             |
+-------------+------------------------+
| vm memory   | 查询VM内存             |
+-------------+------------------------+
| vm process  | 查询VM Erlang进程数量  |
+-------------+------------------------+
| vm io       | 查询VM io最大文件句柄  |
+-------------+------------------------+

查询VM负载::

    $ ./bin/emqttd_ctl vm load

    cpu/load1               : 2.21
    cpu/load5               : 2.60
    cpu/load15              : 2.36

查询VM内存::

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

查询Erlang进程数量::

    $ ./bin/emqttd_ctl vm process

    process/limit           : 8192
    process/count           : 221

查询IO最大句柄数::

    $ ./bin/emqttd_ctl vm io

    io/max_fds              : 2560
    io/active_fds           : 1

-----
trace
-----

trace命令用于追踪某个客户端或Topic，打印日志信息到文件。

+-----------------------------------+-----------------------------------+
| trace list                        | 查询全部开启的追踪                |
+-----------------------------------+-----------------------------------+
| trace client <ClientId> <LogFile> | 开启Client追踪，日志到文件        |
+-----------------------------------+-----------------------------------+
| trace client <ClientId> off       | 关闭Client追踪                    |
+-----------------------------------+-----------------------------------+
| trace topic <Topic> <LogFile>     | 开启Topic追踪，日志到文件         |
+-----------------------------------+-----------------------------------+
| trace topic <Topic> off           | 关闭Topic追踪                     |
+-----------------------------------+-----------------------------------+

开启Client追踪::

    $ ./bin/emqttd_ctl trace client clientid log/clientid_trace.log

    trace client clientid successfully.

关闭Client追踪::

    $ ./bin/emqttd_ctl trace client clientid off
    
    stop to trace client clientid successfully.

开启Topic追踪::

    $ ./bin/emqttd_ctl trace topic topic log/topic_trace.log

    trace topic topic successfully.

关闭Topic追踪::

    $ ./bin/emqttd_ctl trace topic topic off

    stop to trace topic topic successfully.

查询全部开启的追踪::

    $ ./bin/emqttd_ctl trace list

    trace client clientid -> log/clientid_trace.log
    trace topic topic -> log/topic_trace.log


---------
listeners
---------

listeners命令用于查询开启的TCP服务监听器::

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

listener参数说明:

+-----------------+-----------------------------------+
| acceptors       | TCP Acceptor池                    |
+-----------------+-----------------------------------+
| max_clients     | 最大允许连接数                    |
+-----------------+-----------------------------------+
| current_clients | 当前连接数                        |
+-----------------+-----------------------------------+
| shutdown_count  | Socket关闭原因统计                |
+-----------------+-----------------------------------+

----------
mnesia
----------

查询mnesia数据库当前状态，用于调试。

