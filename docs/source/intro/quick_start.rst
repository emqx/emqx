
Download and Install
=====================

Download binary packeges for linux, mac and freebsd from http://emqtt.io/downloads

::

    tar xvf emqttd-ubuntu64-0.7.0-alpha.tgz && cd emqttd

    # start console
    ./bin/emqttd console

    # start as daemon
    ./bin/emqttd start

    # check status
    ./bin/emqttd_ctl status

    # stop
    ./bin/emqttd stop

Build from Source
==================

::

    git clone https://github.com/emqtt/emqttd.git

    cd emqttd && make && make dist
