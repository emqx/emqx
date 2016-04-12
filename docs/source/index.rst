.. Erlang MQTT Broker documentation master file, created by
   sphinx-quickstart on Mon Feb 22 00:46:47 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

===========================
emqttd - Erlang MQTT Broker
===========================

emqttd(Erlang MQTT Broker) is a massively scalable and clusterable MQTT V3.1/V3.1.1 broker written in Erlang/OTP.

emqttd is fully open source and licensed under the Apache Version 2.0. emqttd implements both MQTT V3.1 and V3.1.1 protocol specifications, and supports WebSocket, STOMP, SockJS, CoAP and MQTT-SN at the same time.

Latest release of the emqttd broker is scaling to 1.3 million MQTT connections on a 12 Core, 32G CentOS server.

.. image:: ./_static/images/emqtt.png

The emqttd project provides a scalable, enterprise grade, extensible open-source MQTT broker for IoT, M2M, Smart Hardware, Mobile Messaging and HTML5 Web Messaging Applications.

Sensors, Mobiles, Web Browsers and Application Servers could be connected by emqttd brokers with asynchronous PUB/SUB MQTT messages.

+---------------+-----------------------------------------+
| Homepage:     | http://emqtt.io                         |
+---------------+-----------------------------------------+
| Downloads:    | http://emqtt.io/downloads               |
+---------------+-----------------------------------------+
| GitHub:       | https://github.com/emqtt                |
+---------------+-----------------------------------------+
| Twitter:      | @emqtt                                  |
+---------------+-----------------------------------------+
| Forum:        | https://groups.google.com/d/forum/emqtt |
+---------------+-----------------------------------------+
| Mailing List: | emqtt@googlegroups.com                  |
+---------------+-----------------------------------------+
| Author:       | Feng Lee <feng@emqtt.io>                |
+---------------+-----------------------------------------+

.. NOTE:: MQTT-SN，CoAP Protocols are planned to 1.x release.

Contents:

.. toctree::
   :maxdepth: 2

   getstarted
   install
   config
   cluster
   bridge
   guide
   design
   commands
   plugins
   tune
   changes

-------
License
-------

Apache License Version 2.0

