
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
    ## Name of the node
    ##-------------------------------------------------------------------------
    -name emqttd@127.0.0.1

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

The main configuration file for emqttd broker.

File Syntax
-----------

The config consists of a list of Erlang Applications and their environments.

.. code:: erlang

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

Logger of emqttd broker is implemented by 'lager' application::

  {lager, [
    ...
  ]},

Configure log handlers::

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

The MQTT broker is implemented by erlang 'emqttd' application::

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

Authentication
--------------

emqttd消息服务器认证由一系列认证模块(module)或插件(plugin)提供，系统默认支持用户名、ClientID、LDAP、匿名(anonymouse)认证模块::

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

系统默认采用匿名认证(anonymous)，通过删除注释可开启其他认证方式。同时开启的多个认证模块组成认证链::

               ----------------           ----------------           ------------
    Client --> | Username认证 | -ignore-> | ClientID认证 | -ignore-> | 匿名认证 |
               ----------------           ----------------           ------------
                      |                         |                         |
                     \|/                       \|/                       \|/
                allow | deny              allow | deny              allow | deny
 
.. NOTE:: emqttd消息服务器还提供了MySQL、PostgreSQL、Redis、MongoDB认证插件，
          认证插件加载后认证模块失效。


用户名密码认证
..............

.. code:: erlang

    {username, [{test1, "passwd1"}, {test2, "passwd2"}]},

两种方式添加用户:

1. 直接在[]中明文配置默认用户::

    [{test1, "passwd1"}, {test2, "passwd2"}]

2. 通过'./bin/emqttd_ctl'管理命令行添加用户::

   $ ./bin/emqttd_ctl users add <Username> <Password>

ClientID认证
............

.. code:: erlang

    {clientid, [{password, no}, {file, "etc/clients.config"}]},

etc/clients.config文件中添加ClientID::

    testclientid0
    testclientid1 127.0.0.1
    testclientid2 192.168.0.1/24


LDAP认证
........

.. code:: erlang

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


匿名认证
........

默认开启。允许任意客户端登录::

    {anonymous, []}


access用户访问控制(ACL)
-----------------------

emqttd消息服务器支持基于etc/acl.config文件或MySQL、PostgreSQL插件的访问控制规则。

默认开启基于etc/acl.config文件的访问控制::

    %% ACL config
    {acl, [
        %% Internal ACL module
        {internal,  [{file, "etc/acl.config"}, {nomatch, allow}]}
    ]}

etc/acl.config访问控制规则定义::

    允许|拒绝  用户|IP地址|ClientID  发布|订阅  主题列表

etc/acl.config默认访问规则设置::

    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    {allow, all}.

.. NOTE:: 默认规则只允许本机用户订阅'$SYS/#'与'#'

emqttd消息服务器接收到MQTT客户端发布(PUBLISH)或订阅(SUBSCRIBE)请求时，会逐条匹配ACL访问控制规则，

直到匹配成功返回allow或deny。


MQTT报文(Packet)尺寸与ClientID长度限制
--------------------------------------

'packet'段落设置最大报文尺寸、最大客户端ID长度::

    {packet, [

        %% ClientID长度, 默认1024
        {max_clientid_len, 1024},

        %% 最大报文长度，默认64K
        {max_packet_size,  65536}
    ]},


MQTT客户端(Client)连接闲置时间
------------------------------

'client'段落设置客户端最大允许闲置时间(Socket连接建立，但未发送CONNECT报文)::

    {client, [
        %% 单位: 秒
        {idle_timeout, 10}
    ]},


MQTT会话(Session)参数设置
-------------------------

'session'段落设置MQTT会话参数::

    {session, [
        %% Max number of QoS 1 and 2 messages that can be “in flight” at one time.
        %% 0 means no limit
        {max_inflight, 100},

        %% Retry interval for redelivering QoS1/2 messages.
        {unack_retry_interval, 20},

        %% Awaiting PUBREL Timeout
        {await_rel_timeout, 20},

        %% Max Packets that Awaiting PUBREL, 0 means no limit
        {max_awaiting_rel, 0},

        %% Statistics Collection Interval(seconds)
        {collect_interval, 20},

        %% Expired after 2 days
        {expired_after, 48}

    ]},

会话参数详细说明:

+----------------------+----------------------------------------------------------+
| max_inflight         | 飞行窗口。最大允许同时下发的Qos1/2报文数，0表示没有限制。|
|                      | 窗口值越大，吞吐越高；窗口值越小，消息顺序越严格         |
+----------------------+----------------------------------------------------------+
| unack_retry_interval | 下发QoS1/2消息未收到PUBACK响应的重试间隔                 |
+----------------------+----------------------------------------------------------+
| await_rel_timeout    | 收到QoS2消息，等待PUBREL报文超时时间                     |
+----------------------+----------------------------------------------------------+
| max_awaiting_rel     | 最大等待PUBREL的QoS2报文数                               |
+----------------------+----------------------------------------------------------+
| collect_interval     | 采集会话统计数据间隔，默认0表示关闭统计                  |
+----------------------+----------------------------------------------------------+
| expired_after        | 持久会话到期时间，从客户端断开算起，单位：小时           |
+----------------------+----------------------------------------------------------+

MQTT会话消息队列(MQueue)设置
----------------------------

emqttd消息服务器会话通过队列缓存Qos1/Qos2消息:

1. 持久会话(Session)的离线消息

2. 飞行窗口满而延迟下发的消息

队列参数设置::

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

队列参数说明:

+----------------------+---------------------------------------------------+
| type                 | 队列类型。simple: 简单队列，priority: 优先级队列  |
+----------------------+---------------------------------------------------+
| priority             | 主题(Topic)队列优先级设置                         |
+----------------------+---------------------------------------------------+
| max_length           | 队列长度, infinity表示不限制                      |
+----------------------+---------------------------------------------------+
| low_watermark        | 解除告警水位线                                    |
+----------------------+---------------------------------------------------+
| high_watermark       | 队列满告警水位线                                  |
+----------------------+---------------------------------------------------+
| queue_qos0           | 是否缓存QoS0消息                                  |
+----------------------+---------------------------------------------------+

broker消息服务器参数
--------------------

'broker'段落设置消息服务器内部模块参数。

sys_interval设置系统发布$SYS消息周期::

    {sys_interval, 60},

broker retained消息设置
-----------------------

retained设置MQTT retain消息处理参数::

    {retained, [
        %% retain消息过期时间，单位: 秒
        {expired_after, 0},

        %% 最大retain消息数量
        {max_message_num, 100000},

        %% retain消息payload最大尺寸
        {max_playload_size, 65536}
    ]},

+-----------------+-------------------------------------+
| expired_after   | Retained消息过期时间，0表示永不过期 |
+-----------------+-------------------------------------+
| max_message_num | 最大存储的Retained消息数量          |
+-----------------+-------------------------------------+
| max_packet_size | Retained消息payload最大允许尺寸     |
+-----------------+-------------------------------------+

broker pubsub路由设置
-----------------------

发布/订阅(Pub/Sub)路由模块参数::

    {pubsub, [
        %% PubSub Erlang进程池
        {pool_size, 8},
        
        %% 订阅存储类型，ram: 内存, disc: 磁盘, false: 不保存
        {subscription, ram},

        %% 路由老化时间
        {route_aging, 5}
    ]},

Bridge Parameters
-----------------

    {bridge, [
        %% 最大缓存桥接消息数
        {max_queue_len, 10000},

        %% 桥接节点宕机检测周期，单位: 秒
        {ping_down_interval, 1}
    ]}


Enable Modules
--------------

'presence' module will publish presence message to $SYS topic when a client connected or disconnected::

        {presence, [{qos, 0}]},

'subscription' module forces the client to subscribe some topics when connected to the broker::

        %% Subscribe topics automatically when client connected
        {subscription, [
            %% Subscription from stored table
            stored,

            %% $u will be replaced with username
            {"$Q/username/$u", 1},

            %% $c will be replaced with clientid
            {"$Q/client/$c", 1}
        ]}

'rewrite' module supports to rewrite the topic path::

        %% Rewrite rules
        {rewrite, [{file, "etc/rewrite.config"}]}


Plugins Folder
--------------

.. code:: erlang

    {plugins, [
        %% Plugin App Library Dir
        {plugins_dir, "./plugins"},

        %% File to store loaded plugin names.
        {loaded_file, "./data/loaded_plugins"}
    ]},


TCP Listeners
-------------

Congfigure the TCP listener for MQTT, MQTT(SSL) and HTTP Protocols.

The most important parameter is 'max_clients' - max concurrent clients allowed.

The TCP Ports occupied by emqttd broker by default:

+-----------+-----------------------------------+
| 1883      | MQTT Port                         |
+-----------+-----------------------------------+
| 8883      | MQTT(SSL) Port                    |
+-----------+-----------------------------------+
| 8083      | MQTT(WebSocket), HTTP API Port    |
+-----------+-----------------------------------+

.. code:: erlang

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

The 'etc/acl.config' is the default ACL config for emqttd broker. The rules by default::

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
etc/rewrite.config
------------------

The Rewrite Rules for emqttd_mod_rewrite::

    {topic, "x/#", [
        {rewrite, "^x/y/(.+)$", "z/y/$1"},
        {rewrite, "^x/(.+)$", "y/$1"}
    ]}.

    {topic, "y/+/z/#", [
        {rewrite, "^y/(.+)/z/(.+)$", "y/z/$2"}
    ]}.

