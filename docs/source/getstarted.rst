
.. _getstarted:

===========
Get Started
===========

--------
Overview
--------

emqttd is a massively scalable and clusterable MQTT V3.1/V3.1.1 broker written in Erlang/OTP.

emqttd is aimed to provide a solid, enterprise grade, extensible open-source MQTT broker for IoT, M2M and Mobile applications that need to support ten millions of concurrent MQTT clients.

* Easy to install
* Massively scalable
* Easy to extend
* Solid stable

-----------
Quick Start
-----------

Download binary packeges for Linux, Mac, FreeBSD and Windows from http://emqtt.io/downloads.

.. code:: console

    unzip emqttd-macosx-0.16.0-beta-20160216.zip && cd emqttd

    # Start emqttd
    ./bin/emqttd start

    # Check Status
    ./bin/emqttd_ctl status

    # Stop emqttd
    ./bin/emqttd stop

-----------------
Build from Source
-----------------

.. code:: console

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd && make && make dist

--------------------
Web Dashboard
--------------------

.. image:: ./_static/images/dashboard.png


--------------------
Features List
--------------------

* Full MQTT V3.1/V3.1.1 protocol specification support
* QoS0, QoS1, QoS2 Publish and Subscribe
* Session Management and Offline Messages
* Retained Messages Support
* Last Will Message Support
* TCP/SSL Connection Support
* MQTT Over Websocket(SSL) Support
* HTTP Publish API Support
* [$SYS/brokers/#](https://github.com/emqtt/emqtt/wiki/$SYS-Topics-of-Broker) Support
* Client Authentication with clientId, ipaddress
* Client Authentication with username, password.
* Client ACL control with ipaddress, clientid, username.
* Cluster brokers on several servers.
* [Bridge](https://github.com/emqtt/emqttd/wiki/Bridge) brokers locally or remotely
* 500K+ concurrent clients connections per server
* Extensible architecture with Hooks, Modules and Plugins
* Passed eclipse paho interoperability tests

--------------------
Modules and Plugins
--------------------

Modules
--------

* [emqttd_auth_clientid](https://github.com/emqtt/emqttd/wiki/Authentication) - Authentication with ClientIds
* [emqttd_auth_username](https://github.com/emqtt/emqttd/wiki/Authentication) - Authentication with Username and Password
* [emqttd_auth_ldap](https://github.com/emqtt/emqttd/wiki/Authentication) - Authentication with LDAP
* [emqttd_mod_presence](https://github.com/emqtt/emqttd/wiki/Presence) - Publish presence message to $SYS topics when client connected or disconnected
* emqttd_mod_autosub - Subscribe topics when client connected
* [emqttd_mod_rewrite](https://github.com/emqtt/emqttd/wiki/Rewrite) - Topics rewrite like HTTP rewrite module

Plugins
--------

* [emqttd_plugin_template](https://github.com/emqtt/emqttd_plugin_template) - Plugin template and demo
* [emqttd_dashboard](https://github.com/emqtt/emqttd_dashboard) - Web Dashboard
* [emqttd_plugin_mysql](https://github.com/emqtt/emqttd_plugin_mysql) - Authentication with MySQL
* [emqttd_plugin_pgsql](https://github.com/emqtt/emqttd_plugin_pgsql) - Authentication with PostgreSQL
* [emqttd_plugin_kafka](https://github.com/emqtt/emqtt_kafka) - Publish MQTT Messages to Kafka
* [emqttd_plugin_redis](https://github.com/emqtt/emqttd_plugin_redis) - Redis Plugin
* [emqttd_plugin_mongo](https://github.com/emqtt/emqttd_plugin_mongo) - MongoDB Plugin
* [emqttd_stomp](https://github.com/emqtt/emqttd_stomp) - Stomp Protocol Plugin
* [emqttd_sockjs](https://github.com/emqtt/emqttd_sockjs) - SockJS(Stomp) Plugin
* [emqttd_recon](https://github.com/emqtt/emqttd_recon) - Recon Plugin

----------------------------------
One million Connections
----------------------------------

Linux Kernel Parameters
-----------------------

.. code::

    sysctl -w fs.file-max=2097152
    sysctl -w fs.nr_open=2097152

TCP Stack Parameters
-----------------------

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

----------------------
emqtt Client Libraries
----------------------

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

