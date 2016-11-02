
.. _getstarted:

===========
Get Started
===========

--------
Overview
--------

*EMQ* (Erlang MQTT Broker) is an open source MQTT broker written in Erlang/OTP. Erlang/OTP is a concurrent, fault-tolerant, soft-realtime and distributed programming platform. MQTT is an extremely lightweight publish/subscribe messaging protocol powering IoT, M2M and Mobile applications.

The *EMQ* project is aimed to implement a scalable, distributed, extensible open-source MQTT broker for IoT, M2M and Mobile applications that hope to handle millions of concurrent MQTT clients.

Highlights of the *EMQ* broker:

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
* MQTT-SN Protocol
* CoAP Protocol
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
* Local subscription
* Shared subscription

-----------
Quick Start
-----------

Download and Install
--------------------

The *EMQ* broker is cross-platform, which could be deployed on Linux, FreeBSD, Mac, Windows and even Raspberry Pi.

Download binary package from: http://emqtt.io/downloads.

Installing on Mac, for example:

.. code-block:: bash

    unzip emqttd-macosx-v2.0-rc.2-20161019.zip && cd emqttd

    # Start emqttd
    ./bin/emqttd start

    # Check Status
    ./bin/emqttd_ctl status

    # Stop emqttd
    ./bin/emqttd stop

Installing from Source
----------------------

.. NOTE:: The *EMQ* broker requires Erlang R18+ to build since 1.1 release.

.. code-block:: bash

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd && make && make dist

    cd rel/emqttd && ./bin/emqttd console

-------------
Web Dashboard
-------------

A Web Dashboard will be loaded when the *EMQ* broker is started successfully.

The Dashboard helps check running status of the broker, monitor statistics and metrics of MQTT packets, query clients, sessions, topics and subscriptions.

+------------------+---------------------------+
| Default Address  | http://localhost:18083    |
+------------------+---------------------------+
| Default User     | admin                     |
+------------------+---------------------------+
| Default Password | public                    |
+------------------+---------------------------+

.. image:: ./_static/images/dashboard.png

-------
Plugins
-------

The *EMQ* broker could be extended by Plugins.  A plugin is an Erlang application to extend the *EMQ* broker:

+-------------------------+--------------------------------------------+
| `emq_auth_clientid`_    | Authentication with ClientId               |
+-------------------------+--------------------------------------------+
| `emq_auth_username`_    | Authentication with Username and Password  |
+-------------------------+--------------------------------------------+
| `emq_plugin_template`_  | Plugin template and demo                   |
+-------------------------+--------------------------------------------+
| `emq_dashboard`_        | Web Dashboard                              |
+-------------------------+--------------------------------------------+
| `emq_auth_ldap`_        | LDAP Auth Plugin                           |
+-------------------------+--------------------------------------------+
| `emq_auth_http`_        | Authentication/ACL with HTTP API           |
+-------------------------+--------------------------------------------+
| `emq_auth_mysql`_       | Authentication with MySQL                  |
+-------------------------+--------------------------------------------+
| `emq_auth_pgsql`_       | Authentication with PostgreSQL             |
+-------------------------+--------------------------------------------+
| `emq_auth_redis`_       | Authentication with Redis                  |
+-------------------------+--------------------------------------------+
| `emq_mod_rewrite`_      | Topics rewrite like HTTP rewrite module    |
+-------------------------+--------------------------------------------+
| `emq_mod_retainer`_     | Retainer Module                            |
+-------------------------+--------------------------------------------+
| `emq_mod_presence`_     | Presence Module                            |
+-------------------------+--------------------------------------------+
| `emq_mod_subscription`_ | Subscription Module                        |
+-------------------------+--------------------------------------------+
| `emq_mod_mongo`_        | Authentication with MongoDB                |
+-------------------------+--------------------------------------------+
| `emq_sn`_               | MQTT-SN Protocol Plugin                    |
+-------------------------+--------------------------------------------+
| `emq_coap`_             | CoAP Protocol Plugin                       |
+-------------------------+--------------------------------------------+
| `emq_stomp`_            | STOMP Protocol Plugin                      |
+-------------------------+--------------------------------------------+
| `emq_sockjs`_           | SockJS(Stomp) Plugin                       |
+-------------------------+--------------------------------------------+
| `emq_recon`_            | Recon Plugin                               |
+-------------------------+--------------------------------------------+
| `emq_reloader`_         | Reloader Plugin                            |
+-------------------------+--------------------------------------------+

A plugin could be enabled by 'bin/emqttd_ctl plugins load' command.

For example, enable 'emq_auth_pgsql' plugin::

    ./bin/emqttd_ctl plugins load emq_auth_pgsql

-----------------------
One Million Connections
-----------------------

Latest release of the *EMQ* broker is scaling to 1.3 million MQTT connections on a 12 Core, 32G CentOS server.

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

emqttd/etc/emq.conf:

.. code-block:: properties

    ## Erlang Process Limit
    node.process_limit = 2097152

    ## Sets the maximum number of simultaneously existing ports for this system
    node.max_ports = 1048576

Max Allowed Connections
-----------------------

emqttd/etc/emq.conf 'listeners':

.. code-block:: properties

    ## Size of acceptor pool
    mqtt.listener.tcp.acceptors = 64

    ## Maximum number of concurrent clients
    mqtt.listener.tcp.max_clients = 1000000

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

Eclipse Paho: https://www.eclipse.org/paho/

MQTT.org: https://github.com/mqtt/mqtt.github.io/wiki/libraries

.. _emqttc: https://github.com/emqtt/emqttc
.. _emqtt_benchmark: https://github.com/emqtt/emqtt_benchmark
.. _CocoaMQTT: https://github.com/emqtt/CocoaMQTT
.. _QMQTT: https://github.com/emqtt/qmqtt

.. _emq_plugin_template: https://github.com/emqtt/emq_plugin_template
.. _emq_dashboard:       https://github.com/emqtt/emq_dashboard
.. _emq_mod_rewrite:     https://github.com/emqtt/emq_mod_rewrite
.. _emq_auth_clientid:   https://github.com/emqtt/emq_auth_clientid
.. _emq_auth_username:   https://github.com/emqtt/emq_auth_username
.. _emq_auth_ldap:       https://github.com/emqtt/emq_auth_ldap
.. _emq_auth_http:       https://github.com/emqtt/emq_auth_http
.. _emq_auth_mysql:      https://github.com/emqtt/emq_plugin_mysql
.. _emq_auth_pgsql:      https://github.com/emqtt/emq_plugin_pgsql
.. _emq_auth_redis:      https://github.com/emqtt/emq_plugin_redis
.. _emq_auth_mongo:      https://github.com/emqtt/emq_plugin_mongo
.. _emq_reloader:        https://github.com/emqtt/emq_reloader
.. _emq_stomp:           https://github.com/emqtt/emq_stomp
.. _emq_sockjs:          https://github.com/emqtt/emq_sockjs
.. _emq_recon:           https://github.com/emqtt/emq_recon
.. _emq_sn:              https://github.com/emqtt/emq_sn
.. _emq_coap:            https://github.com/emqtt/emq_coap
.. _emq_mod_retainer:     https://github.com/emqtt/emq_mod_retainer
.. _emq_mod_presence:     https://github.com/emqtt/emq_mod_presence
.. _emq_mod_subscription: https://github.com/emqtt/emq_mod_subscription

