
.. _getstarted:

===========
Get Started
===========

--------
Overview
--------

emqttd(Erlang MQTT Broker) is an open source MQTT broker written in Erlang/OTP. Erlang/OTP is a concurrent, fault-tolerant, soft-realtime and distributed programming platform. MQTT is an extremely lightweight publish/subscribe messaging protocol powering IoT, M2M and Mobile applications.

The emqttd project is aimed to implement a scalable, distributed, extensible open-source MQTT broker for IoT, M2M and Mobile applications that hope to handle millions of concurrent MQTT clients.

Highlights of the emqttd broker:

* Full MQTT V3.1/3.1.1 Protocol Specifications Support
* Easy to Install - Quick Install on Linux, FreeBSD, Mac and Windows
* Massively scalable - Scaling to 1 million connections on a single server
* Cluster and Bridge Support
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
* MQTT Over WebSocket(SSL)
* HTTP Publish API
* STOMP protocol
* STOMP over SockJS
* $SYS/# Topics
* ClientID Authentication
* IpAddress Authentication
* Username and Password Authentication
* Access control based on IpAddress, ClientID, Username
* Authentication with LDAP, Redis, MySQL, PostgreSQL and HTTP API
* Cluster brokers on several servers
* Bridge brokers locally or remotely
* mosquitto, RSMB bridge
* Extensible architecture with Hooks, Modules and Plugins
* Passed eclipse paho interoperability tests

-----------
Quick Start
-----------

Download and Install
--------------------

The emqttd broker is cross-platform, which could be deployed on Linux, FreeBSD, Mac, Windows and even Raspberry Pi.

Download binary package from: http://emqtt.io/downloads.

Installing on Mac, for example:

.. code-block:: bash

    unzip emqttd-macosx-1.1-beta-20160601.zip && cd emqttd

    # Start emqttd
    ./bin/emqttd start

    # Check Status
    ./bin/emqttd_ctl status

    # Stop emqttd
    ./bin/emqttd stop

Installing from Source
----------------------

.. NOTE:: emqttd broker requires Erlang R18+ to build since 1.1 release.

.. code-block:: bash

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd && make && make dist

    cd rel/emqttd && ./bin/emqttd console

-------------
Web Dashboard
-------------

A Web Dashboard will be loaded when the emqttd broker is started successfully.

The Dashboard helps check running status of the broker, monitor statistics and metrics of MQTT packets, query clients, sessions, topics and subscriptions.

+------------------+---------------------------+
| Default Address  | http://localhost:18083    |
+------------------+---------------------------+
| Default User     | admin                     |
+------------------+---------------------------+
| Default Password | public                    |
+------------------+---------------------------+

.. image:: ./_static/images/dashboard.png

-------------------
Modules and Plugins
-------------------

The Authentication and Authorization(ACL) are usually implemented by a Module or Plugin.

Modules
-------

+-------------------------+--------------------------------------------+
| emqttd_auth_clientid    | Authentication with ClientId               |
+-------------------------+--------------------------------------------+
| emqttd_auth_username    | Authentication with Username and Password  |
+-------------------------+--------------------------------------------+
| emqttd_auth_ldap        | Authentication with LDAP                   |
+-------------------------+--------------------------------------------+
| emqttd_mod_presence     | Publish presence message to $SYS topics    |
|                         | when client connected or disconnected      |
+-------------------------+--------------------------------------------+
| emqttd_mod_subscription | Subscribe topics automatically when client |
|                         | connected                                  |
+-------------------------+--------------------------------------------+
| emqttd_mod_rewrite      | Topics rewrite like HTTP rewrite module    |
+-------------------------+--------------------------------------------+

Configure the 'auth', 'module' paragraph in 'etc/emqttd.config' to enable a module.

Enable 'emqttd_auth_username' module:

.. code-block:: erlang

    {access, [
        %% Authetication. Anonymous Default
        {auth, [
            %% Authentication with username, password
            {username, []},

            ...

Enable 'emqttd_mod_presence' module:

.. code-block:: erlang

    {modules, [
        %% Client presence management module.
        %% Publish messages when client connected or disconnected
        {presence, [{qos, 0}]}

Plugins
-------

A plugin is an Erlang application to extend the emqttd broker.

+----------------------------+-----------------------------------+
| `emqttd_plugin_template`_  | Plugin template and demo          |
+----------------------------+-----------------------------------+
| `emqttd_dashboard`_        | Web Dashboard                     |
+----------------------------+-----------------------------------+
| `emqttd_auth_http`_        | Authentication/ACL with HTTP API  |
+----------------------------+-----------------------------------+
| `emqttd_plugin_mysql`_     | Authentication with MySQL         |
+----------------------------+-----------------------------------+
| `emqttd_plugin_pgsql`_     | Authentication with PostgreSQL    |
+----------------------------+-----------------------------------+
| `emqttd_plugin_redis`_     | Authentication with Redis         |
+----------------------------+-----------------------------------+
| `emqttd_plugin_mongo`_     | Authentication with MongoDB       |
+----------------------------+-----------------------------------+
| `emqttd_stomp`_            |  STOMP Protocol Plugin            |
+----------------------------+-----------------------------------+
| `emqttd_sockjs`_           | SockJS(Stomp) Plugin              |
+----------------------------+-----------------------------------+
| `emqttd_recon`_            | Recon Plugin                      |
+----------------------------+-----------------------------------+

A plugin could be enabled by 'bin/emqttd_ctl plugins load' command.

For example, enable 'emqttd_plugin_pgsql' plugin::

    ./bin/emqttd_ctl plugins load emqttd_plugin_pgsql

-----------------------
One Million Connections
-----------------------

Latest release of emqttd broker is scaling to 1.3 million MQTT connections on a 12 Core, 32G CentOS server.

.. NOTE::

    The emqttd broker only allows 512 concurrent connections by default, for 'ulimit -n' limit is 1024 on most platform.

We need tune the OS Kernel, TCP Stack, Erlang VM and emqttd broker for one million connections benchmark.

Linux Kernel Parameters
-----------------------

.. code-block:: bash

    # 2M:
    sysctl -w fs.file-max=2097152
    sysctl -w fs.nr_open=2097152
    echo 2097152 > /proc/sys/fs/nr_open

    # 1M:
    ulimit -n 1048576

TCP Stack Parameters
--------------------

.. code-block:: bash

    # backlog
    sysctl -w net.core.somaxconn=65536

Erlang VM
---------

emqttd/etc/vm.args::

    ## max process numbers
    +P 2097152

    ## Sets the maximum number of simultaneously existing ports for this system
    +Q 1048576

    ## Increase number of concurrent ports/sockets
    -env ERL_MAX_PORTS 1048576

    -env ERTS_MAX_PORTS 1048576

emqttd broker
-------------

emqttd/etc/emqttd.config:

.. code-block:: erlang

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

.. code-block:: bash

    sysctl -w net.ipv4.ip_local_port_range="500 65535"
    echo 1000000 > /proc/sys/fs/nr_open
    ulimit -n 100000

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

.. _emqttd_plugin_template: https://github.com/emqtt/emqttd_plugin_template
.. _emqttd_dashboard:       https://github.com/emqtt/emqttd_dashboard
.. _emqttd_auth_http:       https://github.com/emqtt/emqttd_auth_http
.. _emqttd_plugin_mysql:    https://github.com/emqtt/emqttd_plugin_mysql
.. _emqttd_plugin_pgsql:    https://github.com/emqtt/emqttd_plugin_pgsql
.. _emqttd_plugin_redis:    https://github.com/emqtt/emqttd_plugin_redis
.. _emqttd_plugin_mongo:    https://github.com/emqtt/emqttd_plugin_mongo
.. _emqttd_stomp:           https://github.com/emqtt/emqttd_stomp
.. _emqttd_sockjs:          https://github.com/emqtt/emqttd_sockjs
.. _emqttd_recon:           https://github.com/emqtt/emqttd_recon
