
=======================
Installation
=======================

emqttd broker is cross-platform, could deploy on Linux, FreeBSD, Mac OS X and Windows.

.. NOTE::

    Linux, FreeBSD Recommended.

-----------------
Download  Package
-----------------

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

The package name consists of platform, version and release time.

For example: emqttd-centos64-0.16.0-beta-20160216.zip

--------------------
Installing on Linux
--------------------

Download CentOS Package from: http://emqtt.io/downloads/centos, and then unzip:

.. code:: console

    unzip emqttd-centos64-0.16.0-beta-20160216.zip

Start the broker in console mode::

.. code:: console

    cd emqttd && ./bin/emqttd console

If the broker started successfully, console will print:

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

CTRL+C to close the console and stop the broker.

Start the broker in daemon mode:

.. code:: console

    ./bin/emqttd start

The boot logs in log/emqttd_sasl.log file.

Check the running status of the broker:

.. code:: console

    $ ./bin/emqttd_ctl status
    Node 'emqttd@127.0.0.1' is started
    emqttd 0.16.0 is running

Or check the status by URL::

    http://localhost:8083/status

Stop the broker::

    ./bin/emqttd stop


---------------------
Installing on FreeBSD
---------------------

Download FreeBSD Package from: http://emqtt.io/downloads/freebsd

The installing process is same to Linux.


----------------------
Installing on Mac OS X
----------------------

We could install the broker on Mac OS X to develop and debug MQTT applications.

Download Mac Package from: http://emqtt.io/downloads/macosx

Configure 'lager' log level in 'etc/emqttd.config', all MQTT messages recevied/sent will be printed on console:

.. code:: erlang

    {lager, [
        ...
        {handlers, [
            {lager_console_backend, info},
            ...
        ]}
    ]},

The install and boot process on Mac are same to Linux.

---------------------
Installing on Windows
---------------------

Download Package from: http://emqtt.io/downloads/windows.

Unzip the package to install folder. Open the command line window and 'cd' to the folder.

Start the broker in console mode::

    .\bin\emqttd console

If the broker started successfully, a Erlang console window will popup.

Close the console window and stop the emqttd broker. Prepare to register emqttd as window service.

Install emqttd serivce::
    
    .\bin\emqttd install

Start emqttd serivce::

    .\bin\emqttd start

Stop emqttd serivce::

    .\bin\emqttd stop

Uninstall emqttd service::

    .\bin\emqttd uninstall

.. WARNING:: './bin/emqttd_ctl' command line cannot work on Windows.

----------------------
Installing From Source
----------------------

The emqttd broker requires Erlang/OTP R17+ and git client to build:

Install Erlang: http://www.erlang.org/

Install Git Client: http://www.git-scm.com/

Could use apt-get on Ubuntu, yum on CentOS/RedHat and brew on Mac to install Erlang and Git.

When all dependencies are ready, clone the emqttd project from github.com and build:

.. code:: console

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd

    make && make dist

The binary package output in folder::

    rel/emqttd

------------------
TCP Ports Occupied
------------------

+-----------+-----------------------------------+
| 1883      | MQTT Port                         |
+-----------+-----------------------------------+
| 8883      | MQTT Over SSL Port                |
+-----------+-----------------------------------+
| 8083      | MQTT(WebSocket), HTTP API Port    |
+-----------+-----------------------------------+
| 18083     | Dashboard Port                    |
+-----------+-----------------------------------+

The TCP ports could be configured in etc/emqttd.config:

.. code:: erlang

    {listeners, [
        {mqtt, 1883, [
            ...
        ]},

        {mqtts, 8883, [
            ...
        ]},
        %% HTTP and WebSocket Listener
        {http, 8083, [
            ...
        ]}
    ]},

The 18083 port is used by Web Dashboard of the broker. Default login: admin, Password: public

-----------
Quick Setup
-----------

emqttd消息服务器主要配置文件:

+-------------------+-----------------------------------+
| etc/vm.args       | Erlang VM的启动参数设置           |
+-------------------+-----------------------------------+
| etc/emqttd.config | emqttd消息服务器参数设置          |
+-------------------+-----------------------------------+

etc/vm.args中两个重要的启动参数:

+-------+------------------------------------------------------------------+
| +P    | Erlang虚拟机允许的最大进程数，emqttd一个连接会消耗2个Erlang进程  |
+-------+------------------------------------------------------------------+
| +Q    | Erlang虚拟机允许的最大Port数量，emqttd一个连接消耗1个Port        |
+-------+------------------------------------------------------------------+

+P 参数值 > 最大允许连接数 * 2

+Q 参数值 > 最大允许连接数

.. WARNING:: 实际连接数量超过Erlang虚拟机参数设置，会引起emqttd消息服务器宕机!

etc/emqttd.config文件listeners段落设置最大允许连接数:

.. code:: erlang

    {listeners, [
        {mqtt, 1883, [
            %% TCP Acceptor池设置
            {acceptors, 16},

            %% 最大允许连接数设置
            {max_clients, 8192},

            ...

        ]},

emqttd消息服务器详细设置，请参见文档: :ref:`config`


-------------------
/etc/init.d/emqttd
-------------------

.. code:: shell

    #!/bin/sh
    #
    # emqttd       Startup script for emqttd.
    #
    # chkconfig: 2345 90 10
    # description: emqttd is mqtt broker.

    # source function library
    . /etc/rc.d/init.d/functions

    # export HOME=/root

    start() {
        echo "starting emqttd..."
        cd /opt/emqttd && ./bin/emqttd start
    }

    stop() {
        echo "stopping emqttd..."
        cd /opt/emqttd && ./bin/emqttd stop
    }

    restart() {
        stop
        start
    }

    case "$1" in
        start)
            start
            ;;
        stop)
            stop
            ;;
        restart)
            restart
            ;;
        *)
            echo $"Usage: $0 {start|stop}"
            RETVAL=2
    esac


chkconfig::

    chmod +x /etc/init.d/emqttd
    chkconfig --add emqttd
    chkconfig --list

boot test::

    service emqttd start

.. NOTE::

    ## erlexec: HOME must be set
    uncomment '# export HOME=/root' if "HOME must be set" error.




