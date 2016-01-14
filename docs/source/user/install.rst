Installation
============

TODO: ...

emqttd is cross-platform, could run on windows, linux, freebsd and mac os x.

emqttd is easy to install and takes about 5 minutes to download and startup.

Install on Linux, Freebsd and Mac OS X
--------------------------------------

Download binary packages from http://emqtt.io/downloads, and then:: 

    unzip emqttd-platform-0.x.y-beta.zip && cd emqttd

Startup with console for debug::

    ./bin/emqttd console

All the MQTT Packets(RECV/SENT) will be printed on console.

Startup as daemon::

    ./bin/emqttd start

TCP Ports used by emqttd:

+-----------+-----------------------------+
|  Port     | Description                 |
+-----------+-----------------------------+
|  1883     |  MQTT                       |
+-----------+-----------------------------+
|  8883     |  MQTT(SSL)                  |
+-----------+-----------------------------+
|  8083     |  MQTT Over WebSocket        |
+-----------+-----------------------------+


Check the broker status::

    ./bin/emqttd_ctl status

Stop the broker::

    ./bin/emqttd stop


Install on Windows
------------------

Download windows package from http://emqtt.io/downloads.

Start console to check if emqttd works::

    .\bin\emqttd console

.. NOTE:: execute '.\bin\emqttd', should not cd to 'bin' folder.

A console window will show if the broker started successfully.

Register emqttd as window service::

    .\bin\emqttd install

Starte emqttd service::

    .\bin\emqttd start

.. NOTE:: The console window should be closed if opened.

Stop emqttd service::

    .\bin\emqttd stop

Uninstall emqttd service::

    .\bin\emqttd uninstall


Build from Source
-----------------

emqttd requires Erlang R17+ to build from source.

Erlang R17+ and git client should be installed before building emqttd.

::

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd

    git submodule update --init --recursive

    make && make dist

    cd rel/emqttd && ./bin/emqttd console

