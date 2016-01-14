
HTTP API
========

emqttd support HTTP API to publish message from your application server to MQTT clients. For example:: 

    curl -v --basic -u user:passwd -d "qos=1&retain=0&topic=/a/b/c&message=hello from http..." -k http://localhost:8083/mqtt/publish

HTTP API URL
-----------

::
    HTTP POST http://host:8083/mqtt/publish


HTTP Parameters
---------------

+---------+-----------------+
| Name    |  Description    |
+-------------------+-------+
| client  |  ClientId       |
+-------------------+-------+
| qos     |  QoS(0, 1, 2)   |
+-------------------+-------+
| retain  |  Retain(0, 1)   |
+-------------------+-------+
| topic   |  Topic          |
+-------------------+-------+
| message |  Message        |
+-------------------+-------+


