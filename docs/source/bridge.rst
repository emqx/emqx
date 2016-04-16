
.. _bridge::

======
Bridge
======

-------------
emqttd Bridge
-------------

Two or more emqttd brokers could be bridged together. Bridges forward MQTT messages from one broker node to another::

                  ---------                     ---------                     ---------
    Publisher --> | node1 | --Bridge Forward--> | node2 | --Bridge Forward--> | node3 | --> Subscriber
                  ---------                     ---------                     ---------

Configure Bridge
----------------

Suppose that we create two emqttd brokers on localhost:

+---------+---------------------+-----------+
| Name    | Node                | MQTT Port |
+---------+---------------------+-----------+
| emqttd1 | emqttd1@127.0.0.1   | 1883      |
+---------+---------------------+-----------+
| emqttd2 | emqttd2@127.0.0.1   | 2883      |
+---------+---------------------+-----------+

Create a bridge that forwards all the 'sensor/#' messages from emqttd1 to emqttd2.

1. Start Brokers
................

.. code-block:: bash

    cd emqttd1/ && ./bin/emqttd start
    cd emqttd2/ && ./bin/emqttd start

2. Create bridge: emqttd1--sensor/#-->emqttd2
.............................................

.. code-block:: bash

    $ cd emqttd1 && ./bin/emqttd_ctl bridges start emqttd2@127.0.0.1 sensor/#

    bridge is started.
    
    $ ./bin/emqttd_ctl bridges list

    bridge: emqttd1@127.0.0.1--sensor/#-->emqttd2@127.0.0.1

3. Test the bridge
...................

.. code-block:: bash

    #emqttd2
    mosquitto_sub -t sensor/# -p 2883 -d

    #emqttd1
    mosquitto_pub -t sensor/1/temperature -m "37.5" -d 

4. Delete the bridge
.....................

.. code-block:: bash

    ./bin/emqttd_ctl bridges stop emqttd2@127.0.0.1 sensor/#

-----------------
emqttd Bridge CLI
-----------------

.. code-block:: bash

    #query bridges
    ./bin/emqttd_ctl bridges list

    #start bridge                       
    ./bin/emqttd_ctl bridges start <Node> <Topic>

    #start bridge with options
    ./bin/emqttd_ctl bridges start <Node> <Topic> <Options>

    #stop bridge  
    ./bin/emqttd_ctl bridges stop <Node> <Topic>

-----------------
mosquitto Bridge
-----------------

Bridge mosquitto to emqttd broker::

                 -------------             -----------------
    Sensor ----> | mosquitto | --Bridge--> |               |
                 -------------             |    emqttd     |
                 -------------             |    Cluster    |
    Sensor ----> | mosquitto | --Bridge--> |               |
                 -------------             -----------------

mosquitto.conf
--------------

Suppose that we start an emqttd broker on localost:2883, and mosquitto on localhost:1883.

A bridge configured in mosquitto.conf::

    connection emqttd
    address 127.0.0.1:2883
    topic sensor/# out 2

    # Set the version of the MQTT protocol to use with for this bridge. Can be one
    # of mqttv31 or mqttv311. Defaults to mqttv31.
    bridge_protocol_version mqttv311

-------------
rsmb Bridge
-------------

Bridge RSMB to emqttd broker, same settings as mosquitto.

broker.cfg::

    connection emqttd
    addresses 127.0.0.1:2883
    topic sensor/#

