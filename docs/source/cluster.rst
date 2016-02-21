
.. _cluster:

====================
Cluster Guide
====================

-----------------------------------
Erlang/OPT Distributed
-----------------------------------

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
----------

.. code:: shell

    erl -name node1@127.0.0.1
    erl -name node2@127.0.0.1
    erl -name node3@127.0.0.1
    erl -name node4@127.0.0.1


    (node1@127.0.0.1)1> net_kernel:connect_node('node2@127.0.0.1').
    true
    (node1@127.0.0.1)2> net_kernel:connect_node('node3@127.0.0.1').
    true
    (node1@127.0.0.1)3> net_kernel:connect_node('node4@127.0.0.1').
    true
    (node1@127.0.0.1)4> nodes().
    ['node2@127.0.0.1','node3@127.0.0.1','node4@127.0.0.1']

epmd
----------

    (node1@127.0.0.1)6> net_adm:names().
    {ok,[{"node1",62740},
         {"node2",62746},
         {"node3",62877},
         {"node4",62895}]}

Cookie
--------

    1. $HOME/.erlang.cookie

    2. erl -setcookie <Cookie>

From: http://erlang.org/doc/reference_manual/distributed.html

----------------------
Cluster Design
----------------------


    topic1 -> node1, node2
    topic2 -> node3
    topic3 -> node2, node4


Topic Trie and Route Table
---------------------------

::
    --------------------------
    |             t          |
    |            / \         |
    |           +   #        |
    |         /  \           |
    |       x      y         |
    --------------------------
    | t/+/x -> node1, node3  |
    | t/+/y -> node1         |
    | t/#   -> node2         |
    | t/a   -> node3         |
    --------------------------


Subscription and Message Dispatch
---------------------------------

::
    title: Message Route and Deliver

    client1->node1: Publish[t/a]
    node1-->node2: Route[t/#]
    node1-->node3: Route[t/a]
    node2-->client2: Deliver[t/#]
    node3-->client3: Deliver[t/a]

.. image:: _static/images/route.png


-----------------------
Cluster Setup
-----------------------



-----------------------
Sessions across Nodes
-----------------------


                                      node1
                                   -----------
                               |-->| session |
                               |   -----------
                 node2         |
              --------------   |
     client-->| connection |<--|
              --------------


------------------
Notice: NetSplit
------------------


------------------------
Consistent Hash and DHT
------------------------

