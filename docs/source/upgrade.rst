
.. _upgrade:

=======
Upgrade
=======

.. _upgrade_1.1.2:

----------------
Upgrade to 1.1.2
----------------

.. NOTE:: 1.0+ releases can be upgraded to 1.1.2 smoothly

Steps:

1. Download and install emqttd-1.1.2 to the new directory, for example::

    Old installation: /opt/emqttd_1_0_0/

    New installation: /opt/emqttd_1_1_2/

2. Copy the 'etc/' and 'data/' from the old installation::

    cp -R /opt/emqttd_1_0_0/etc/* /opt/emqttd_1_1_2/etc/

    cp -R /opt/emqttd_1_0_0/data/* /opt/emqttd_1_1_2/data/

3. Copy the plugins/{plugin}/etc/* from the old installation if you loaded plugins.

4. Stop the old emqttd, and start the new one.

