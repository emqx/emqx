
=======================
Installation
=======================


.. NOTE::

    Linux, FreeBSD Recommended.

----------------
Download 
----------------

Download binary package from: http://emqtt.io/downloads

+-----------+-----------------------------------+
| Ubuntu    | http://emqtt.io/downloads/ubuntu  |
+-----------+-----------------------------------+
| CentOS    | http://emqtt.io/downloads/centos  |
+-----------+-----------------------------------+
| FreeBSD   | http://emqtt.io/downloads/freebsd |
+-----------+-----------------------------------+
| Mac OS X  | http://emqtt.io/downloads/macosx  |
+-----------+-----------------------------------+
| Windows   | http://emqtt.io/downloads/windows |
+-----------+-----------------------------------+


--------------------
Installing on Linux
--------------------

CentOS: http://emqtt.io/downloads/centos

.. code:: console

    unzip emqttd-centos64-0.16.0-beta-20160216.zip



.. code:: console

    cd emqttd && ./bin/emqttd console


.. code:: console

    starting emqttd on node 'emqttd@127.0.0.1'
    emqttd ctl is starting...[done]
    emqttd trace is starting...[done]
    emqttd pubsub is starting...[done]
    emqttd stats is starting...[done]
    emqttd metrics is starting...[done]
    emqttd retainer is starting...[done]
    emqttd pooler is starting...[done]
    emqttd client manager is starting...[done]
    emqttd session manager is starting...[done]
    emqttd session supervisor is starting...[done]
    emqttd broker is starting...[done]
    emqttd alarm is starting...[done]
    emqttd mod supervisor is starting...[done]
    emqttd bridge supervisor is starting...[done]
    emqttd access control is starting...[done]
    emqttd system monitor is starting...[done]
    http listen on 0.0.0.0:18083 with 4 acceptors.
    mqtt listen on 0.0.0.0:1883 with 16 acceptors.
    mqtts listen on 0.0.0.0:8883 with 4 acceptors.
    http listen on 0.0.0.0:8083 with 4 acceptors.
    Erlang MQTT Broker 0.16.0 is running now
    Eshell V6.4  (abort with ^G)
    (emqttd@127.0.0.1)1>


.. code:: console

    ./bin/emqttd start


.. code:: console

    ./bin/emqttd_ctl status


.. code:: console

    $ ./bin/emqttd_ctl status
    Node 'emqttd@127.0.0.1' is started
    emqttd 0.16.0 is running


    http://localhost:8083/status


    ./bin/emqttd stop

---------------------
Installing on FreeBSD
---------------------

FreeBSD: http://emqtt.io/downloads/freebsd

-----------------------
Installing on Mac
-----------------------

.. code:: erlang

-----------------------
Installing on Windows
-----------------------

-----------------------
Installing From Source
-----------------------

.. code:: console

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd

    make && make dist

Binary Package::

    rel/emqttd

-------------------
/etc/init.d/emqttd
-------------------




