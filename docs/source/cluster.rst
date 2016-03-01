
.. _cluster:

==========
Clustering
==========

----------------------
Distributed Erlang/OTP
----------------------

Erlang/OTP is a concurrent, fault-tolerant, distributed programming platform. A distributed Erlang/OTP system consists of a number of Erlang runtime systems called 'node'. Nodes connect to each other with TCP/IP sockets and communite by Message Passing.

.. code::

    ---------         ---------
    | Node1 | --------| Node2 |
    ---------         ---------
        |     \     /    |
        |       \ /      |
        |       / \      |
        |     /     \    |
    ---------         ---------
    | Node3 | --------| Node4 |
    ---------         ---------

Node
----

An erlang runtime system called 'node' is identified by a unique name like email addreass. Erlang nodes communicate with each other by the name.

Suppose we start four Erlang nodes on localhost:

.. code:: console

    erl -name node1@127.0.0.1
    erl -name node2@127.0.0.1
    erl -name node3@127.0.0.1
    erl -name node4@127.0.0.1

connect all the nodes::

    (node1@127.0.0.1)1> net_kernel:connect_node('node2@127.0.0.1').
    true
    (node1@127.0.0.1)2> net_kernel:connect_node('node3@127.0.0.1').
    true
    (node1@127.0.0.1)3> net_kernel:connect_node('node4@127.0.0.1').
    true
    (node1@127.0.0.1)4> nodes().
    ['node2@127.0.0.1','node3@127.0.0.1','node4@127.0.0.1']

epmd
----

epmd(Erlang Port Mapper Daemon) is a daemon service that is responsible for mapping node names to machine addresses(TCP sockets). The daemon is started automatically on every host where an Erlang node started.

.. code:: console

    (node1@127.0.0.1)6> net_adm:names().
    {ok,[{"node1",62740},
         {"node2",62746},
         {"node3",62877},
         {"node4",62895}]}

Cookie
------

Erlang nodes authenticate each other by a magic cookie when communicating. The cookie could be configured by::

    1. $HOME/.erlang.cookie

    2. erl -setcookie <Cookie>

.. NOTE:: Content of this chapter is from: http://erlang.org/doc/reference_manual/distributed.html

---------------------
emqttd Cluster Design
---------------------

The cluster architecture of emqttd broker is based on distrubuted Erlang/OTP and Mnesia database.

The cluster design could be summarized by the following two rules::

1. When a MQTT client SUBSCRIBE a Topic on a node, the node will tell all the other nodes in the cluster: I subscribed a Topic.

2. When a MQTT Client PUBLISH a message to a node, the node will lookup the Topic table and forard the message to nodes that subscribed the Topic.

Finally there will be a global route table(Topic -> Node) that replicated to all nodes in the cluster::

    topic1 -> node1, node2
    topic2 -> node3
    topic3 -> node2, node4

Topic Trie and Route Table
--------------------------

Every node in the cluster will store a topic trie and route table in mnesia database. 

Suppose that we create subscriptions:

+----------------+-------------+----------------------------+
| Client         | Node        |  Topics                    |
+================+=============+============================+
| client1        | node1       | t/+/x, t/+/y               |
+----------------+-------------+----------------------------+
| client2        | node2       | t/#                        |
+----------------+-------------+----------------------------+
| client3        | node3       | t/+/x, t/a                 |
+----------------+-------------+----------------------------+

Finally the topic trie and route table in the cluster::

    --------------------------
    |          t             |
    |         / \            |
    |        +   #           |
    |      /  \              |
    |    x      y            |
    --------------------------
    | t/+/x -> node1, node3  |
    | t/+/y -> node1         |
    | t/#   -> node2         |
    | t/a   -> node3         |
    --------------------------

Message Route and Deliver
--------------------------

The brokers in the cluster route messages by topic trie and route table, deliver messages to MQTT clients by subscriptions. Subscriptions are mapping from topic to subscribers, are stored only in the local node, will not be replicated to other nodes.

Suppose client1 PUBLISH a message to the topic 't/a', the message Route and Deliver process::

    title: Message Route and Deliver

    client1->node1: Publish[t/a]
    node1-->node2: Route[t/#]
    node1-->node3: Route[t/a]
    node2-->client2: Deliver[t/#]
    node3-->client3: Deliver[t/a]

.. image:: _static/images/route.png

-------------
Cluster Setup
-------------

Suppose we deploy two nodes cluster on host1, host2:

+----------------+-----------+---------------------+
| Node           | Host      |  IP and Port        |
+----------------+-----------+---------------------+
| emqttd@host1   | host1     | 192.168.1.10:1883   |
+----------------+-----------+---------------------+
| emqttd@host2   | host2     | 192.168.1.20:1883   |
+----------------+-----------+---------------------+

emqttd@host1 setting
--------------------

emqttd/etc/vm.args::

    -name emqttd@host1

    or

    -name emqttd@192.168.0.10

.. WARNING:: The name cannot be changed after node joined the cluster.

emqttd@host2 setting
--------------------

emqttd/etc/vm.args::

    -name emqttd@host2

    or

    -name emqttd@192.168.0.20

Join the cluster
----------------

Start the two broker nodes, and 'cluster join ' on emqttd@host2::

    $ ./bin/emqttd_ctl cluster join emqttd@host1

    Join the cluster successfully.
    Cluster status: [{running_nodes,['emqttd@host1','emqttd@host2']}]

Or 'cluster join' on emqttd@host1::

    $ ./bin/emqttd_ctl cluster join emqttd@host2

    Join the cluster successfully.
    Cluster status: [{running_nodes,['emqttd@host1','emqttd@host2']}]

Query the cluster status::

    $ ./bin/emqttd_ctl cluster status

    Cluster status: [{running_nodes,['emqttd@host1','emqttd@host2']}]

Leave the cluster
-----------------

Two ways to leave the cluster:

1. leave: this node leaves the cluster

2. remove: remove other nodes from the cluster

emqttd@host2 node tries to leave the cluster::

    $ ./bin/emqttd_ctl cluster leave

Or remove emqttd@host2 node from the cluster on emqttd@host1::

    $ ./bin/emqttd_ctl cluster remove emqttd@host2


--------------------
Session across Nodes
--------------------

The persistent MQTT sessions (clean session = false) are across nodes in the cluster.

If a persistent MQTT client connected to node1 first, then disconnected and connects to node2, the MQTT connection and session will be located on different nodes::

                                      node1
                                   -----------
                               |-->| session |
                               |   -----------
                 node2         |
              --------------   |
     client-->| connection |<--|
              --------------

----------------
Notice: NetSplit
----------------

The emqttd cluster does not support deployment across IDC, and the cluster will not handle NetSplit automatically. If NetSplit occures, nodes have to be rebooted manually.


-----------------------
Consistent Hash and DHT
-----------------------

Consistent Hash and DHT are popular in the design of NoSQL databases. Cluster of emqttd broker could support 10 million size of global routing table now. We could use the Consistent Hash or DHT to partition the routing table, and evolve the cluster to larger size.


