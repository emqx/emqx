
Clustering
==========

Suppose we cluster two nodes on hosts:

Node | Host   | IpAddress  
-----|--------|-------------
node1(disc_copy)| host1 | 192.168.0.10
node2(ram_copy) | host2  | 192.168.0.20


Configure and start 'node1'
---------------------------

configure 'etc/vm.args'::

    -name emqttd@192.168.0.10

If host1, host2 added to /etc/hosts of OS::

    -name emqttd@host1

Start node1::

    ./bin/emqttd start

.. NOTE:: Notice that data/mnesia/* should be removed before you start the broker with different node name.


Configure and start 'node2'
---------------------------

Configure 'etc/vm.args'::

    -name emqttd@192.168.0.20

or::

    -name emqttd@host2


Then start node2::

    ./bin/emqttd start

Cluster two nodes
---------------------------

Run './bin/emqttd_ctl cluster' on host2::

    ./bin/emqttd_ctl cluster emqttd@192.168.0.10


Check cluster status
---------------------------

And then check clustered status on any host::

    ./bin/emqttd_ctl cluster

