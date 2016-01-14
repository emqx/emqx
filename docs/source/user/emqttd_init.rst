
/etc/init.d/emqttd
==================

::
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

chkconfig
---------

::

    chmod +x /etc/init.d/emqttd
    chkconfig --add emqttd
    chkconfig --list

boot test on ubuntu
-------------------

::
    service emqttd start

erlexec: HOME must be set
-------------------

uncomment '# export HOME=/root' if "HOME must be set" error.


Referrence
---------

.. `How to make unix service see environment variables?`_: http://unix.stackexchange.com/questions/44370/how-to-make-unix-service-see-environment-variables/44378#44378


