
.. _advanced:

========
Advanced
========

*EMQ* 2.0-rc.2 release supports `Local Subscription` and `Shared Subscription`.

------------------
Local Subscription
------------------

The *EMQ* broker will not create global routes for `Local Subscription`, and only dispatch MQTT messages on local node.

.. code-block:: shell

    mosquitto_sub -t '$local/topic'

    mosquitto_pub -t 'topic'

Usage: subscribe a topic with `$local/` prefix.

-------------------
Shared Subscription
-------------------

Shared Subscription supports Load balancing to distribute MQTT messages between multiple subscribers in the same group::

                                ---------
                                |       | --Msg1--> Subscriber1
    Publisher--Msg1,Msg2,Msg3-->|  EMQ  | --Msg2--> Subscriber2
                                |       | --Msg3--> Subscriber3
                                ---------

Two ways to create a shared subscription:

+-----------------+-------------------------------------------+
|  Prefix         | Examples                                  |
+-----------------+-------------------------------------------+
| $queue/         | mosquitto_sub -t '$queue/topic            |
+-----------------+-------------------------------------------+
| $share/<group>/ | mosquitto_sub -t '$share/group/topic      |
+-----------------+-------------------------------------------+

