
.. _bridge::


====================
Bridge Guide
====================


-------------------
emqttd Node Bridge
-------------------

::
                  ---------                     ---------                     ---------
    Publisher --> | node1 | --Bridge Forward--> | node2 | --Bridge Forward--> | node3 | --> Subscriber
                  ---------                     ---------                     ---------


-----------------
mosquitto Bridge
-----------------

::
                 -------------             -----------------
    Sensor ----> | mosquitto | --Bridge--> |               |
                 -------------             |    emqttd     |
                 -------------             |    Cluster    |
    Sensor ----> | mosquitto | --Bridge--> |               |
                 -------------             -----------------


mosquitto.conf
--------------


-------------
rsmb Bridge
-------------

broker.cfg
----------

::
    connection emqttd
    addresses 127.0.0.1:2883
    topic sensor/#

