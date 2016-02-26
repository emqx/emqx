
.. _getstarted:

===========
Get Started
===========

--------
Overview
--------

emqttd(Erlang MQTT Broker) is an open source MQTT broker written in Erlang/OTP. Erlang/OTP is a concurrent, fault-tolerant, soft-realtime and distributed programming platform. MQTT is anextremely lightweight publish/subscribe messaging protocol powering IoT, M2M applications.

The emqttd project is aimed to implement a scalable, distributed, extensible open-source MQTT broker for IoT, M2M and Mobile applications that hope to handle ten millions of concurrent MQTT clients.

The emqttd broker is:

* Full MQTT V3.1/3.1.1 Protocol Specifications Support
* Easy to Install - Quick Install on Linux, FreeBSD, Mac and Windows
* Massively scalable - Scaling to 1 million connections on a single server
* Easy to extend - Hooks and plugins to customize or extend the broker
* Pluggable Authentication - LDAP, MySQL, PostgreSQL, Redis Authentication Plugins


--------
Features
--------

* Full MQTT V3.1/V3.1.1 protocol specification support
* QoS0, QoS1, QoS2 Publish and Subscribe
* Session Management and Offline Messages
* Retained Message
* Last Will Message
* TCP/SSL Connection
* MQTT Over Websocket(SSL)
* HTTP Publish API
* STOMP protocol
* STOMP over SockJS
* $SYS/# Topics
* Client Authentication with clientId, ipaddress
* Client Authentication with username, password
* Client ACL control with ipaddress, clientid, username
* LDAP, Redis, MySQL, PostgreSQL authentication
* Cluster brokers on several servers.
* Bridge brokers locally or remotely
* mosquitto, RSMB bridge
* Extensible architecture with Hooks, Modules and Plugins
* Passed eclipse paho interoperability tests


-----------
Quick Start
-----------

Download and Install
--------------------

Download binary package for Linux, Mac, FreeBSD and Windows platform from http://emqtt.io/downloads.

.. code:: console

    unzip emqttd-macosx-0.16.0-beta-20160216.zip && cd emqttd

    # Start emqttd
    ./bin/emqttd start

    # Check Status
    ./bin/emqttd_ctl status

    # Stop emqttd
    ./bin/emqttd stop

Installing from Source
-----------------------

.. NOTE:: emqttd requires Erlang R17+ to build.

.. code:: console

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd && make && make dist

-------------
Web Dashboard
-------------

.. image:: ./_static/images/dashboard.png


-------------------
Modules and Plugins
-------------------

The emqttd broker could be extended by modules and plugins.

Modules
-------

+-------------------------+-----------------------------------+
| emqttd_auth_clientid    | ClientId认证                      |
+-------------------------+-----------------------------------+
| emqttd_auth_username    | 用户名密码认证                    |
+-------------------------+-----------------------------------+
| emqttd_auth_ldap        | LDAP认证                          |
+-------------------------+-----------------------------------+
| emqttd_mod_presence     | 客户端上下线状态消息发布          |
+-------------------------+-----------------------------------+
| emqttd_mod_subscription | 客户端上线自动主题订阅            |
+-------------------------+-----------------------------------+
| emqttd_mod_rewrite      | 重写客户端订阅主题(Topic)         |
+-------------------------+-----------------------------------+

扩展模块通过'etc/emqttd.config'配置文件的auth, modules段落启用。

例如启用用户名密码认证::

    {access, [
        %% Authetication. Anonymous Default
        {auth, [
            %% Authentication with username, password
            {username, []},

            ...

启用客户端状态发布模块::

    {modules, [
        %% Client presence management module.
        %% Publish messages when client connected or disconnected
        {presence, [{qos, 0}]}

Plugins
--------

+-------------------------+-----------------------------------+
| emqttd_plugin_template  | 插件模版与演示代码                |
+-------------------------+-----------------------------------+
| emqttd_dashboard        | Web管理控制台，默认加载           |
+-------------------------+-----------------------------------+
| emqttd_plugin_mysql     | MySQL认证插件                     |
+-------------------------+-----------------------------------+
| emqttd_plugin_pgsql     | PostgreSQL认证插件                |
+-------------------------+-----------------------------------+
| emqttd_plugin_redis     | Redis认证插件                     |
+-------------------------+-----------------------------------+
| emqttd_plugin_mongo     | MongoDB认证插件                   |
+-------------------------+-----------------------------------+
| emqttd_stomp            | Stomp协议插件                     |
+-------------------------+-----------------------------------+
| emqttd_sockjs           | SockJS插件                        |
+-------------------------+-----------------------------------+
| emqttd_recon            | Recon优化调测插件                 |
+-------------------------+-----------------------------------+

扩展插件通过'bin/emqttd_ctl'管理命令行，加载启动运行。

例如启用PostgreSQL认证插件::

    ./bin/emqttd_ctl plugins load emqttd_plugin_pgsql

----------------------------------
One million Connections
----------------------------------

.. NOTE::

    emqttd消息服务器默认设置，允许最大客户端连接是512，因为大部分操作系统'ulimit -n'限制为1024。

emqttd消息服务器当前版本，连接压力测试到130万线，8核心/32G内存的CentOS云服务器。

操作系统内核参数、TCP协议栈参数、Erlang虚拟机参数、emqttd最大允许连接数设置简述如下：

Linux Kernel Parameters
-----------------------

# 2M - 系统所有进程可打开的文件数量::

.. code::

    sysctl -w fs.file-max=2097152
    sysctl -w fs.nr_open=2097152

# 1M - 系统允许当前进程打开的文件数量::

    ulimit -n 1048576

TCP Stack Parameters
-----------------------

# backlog - Socket监听队列长度::

.. code::

    sysctl -w net.core.somaxconn=65536

Erlang VM
-----------------

emqttd/etc/vm.args::

    ## max process numbers
    +P 2097152

    ## Sets the maximum number of simultaneously existing ports for this system
    +Q 1048576

    ## Increase number of concurrent ports/sockets
    -env ERL_MAX_PORTS 1048576

    -env ERTS_MAX_PORTS 1048576

emqttd.config
-----------------

emqttd/etc/emqttd.config::

        {mqtt, 1883, [
            %% Size of acceptor pool
            {acceptors, 64},

            %% Maximum number of concurrent clients
            {max_clients, 1000000},

            %% Socket Access Control
            {access, [{allow, all}]},

            %% Connection Options
            {connopts, [
                %% Rate Limit. Format is 'burst, rate', Unit is KB/Sec
                %% {rate_limit, "100,10"} %% 100K burst, 10K rate
            ]},
            ...

Test Client
-----------

.. code::

    sysctl -w net.ipv4.ip_local_port_range="500 65535"
    echo 1000000 > /proc/sys/fs/nr_open

---------------------
MQTT Client Libraries
---------------------

GitHub: https://github.com/emqtt

+--------------------+----------------------+
| `emqttc`_          | Erlang MQTT Client   |
+--------------------+----------------------+
| `emqtt_benchmark`_ | MQTT benchmark Tool  |
+--------------------+----------------------+
| `CocoaMQTT`_       | Swift MQTT Client    |
+--------------------+----------------------+
| `QMQTT`_           | QT MQTT Client       |
+--------------------+----------------------+

.. _emqttc:          https://github.com/emqtt/emqttc
.. _emqtt_benchmark: https://github.com/emqtt/emqtt_benchmark
.. _CocoaMQTT:       https://github.com/emqtt/CocoaMQTT
.. _QMQTT:           https://github.com/emqtt/qmqtt

