eMQTT ChangeLog
==================

v0.3.0-alpha (2015-01-18)
------------------------

NOTICE: Full MQTT 3.1.1 support now!

Feature: Passed org.eclipse.paho.mqtt.testing/interoperability tests

Feature: Qos0, Qos1 and Qos2 publish and suscribe

Feature: session(clean_sess=false) management and offline messages

Feature: redeliver awaiting puback/pubrec messages(doc: Chapter 4.4)

Feature: retain messages, add emqtt_server module

Feature: MQTT 3.1.1 null client_id support

Bugfix: keepalive timeout to send will message 

Improve: overlapping subscription support

Improve: add emqtt_packet:dump to dump packets

Test: passed org.eclipse.paho.mqtt.testing/interoperability

Test: simple cluster test

Closed Issues: #22, #24, #27, #28, #29, #30, #31, #32, #33, #34, #36, #37, #38, #39, #41, #42, #43


v0.2.1-beta (2015-01-08)
------------------------

pull request 26: Use binaries for topic paths and fix wildcard topics

emqtt_pubsub.erl: fix wildcard topic match bug caused by binary topic in 0.2.0 

Makefile: deps -> get-deps

rebar.config: fix mochiweb git url

tag emqtt release accoding to [Semantic Versioning](http://semver.org/)

max clientId length is 1024 now.

0.2.0 (2014-12-07)
-------------------

rewrite the project, integrate with esockd, mochiweb

support MQTT 3.1.1

support HTTP to publish message

0.1.5 (2013-01-05)
-------------------

Bugfix: remove QOS_1 match when handle PUBREL request 
 
Bugfix: reverse word in emqtt_topic:words/1 function

0.1.4 (2013-01-04)
-------------------

Bugfix: fix "mosquitto_sub -q 2 ......" bug

Bugfix: fix keep alive bug

0.1.3 (2012-01-04)
-------------------

Feature: support QOS2 PUBREC, PUBREL,PUBCOMP messages

Bugfix: fix emqtt_frame to encode/decoe PUBREC/PUBREL messages


0.1.2 (2012-12-27)
-------------------

Feature: release support like riak

Bugfix: use ?INFO/?ERROR to print log in tcp_listener.erl


0.1.1 (2012-09-24)
-------------------

Feature: use rebar to generate release

Feature: support retained messages

Bugfix: send will msg when network error

0.1.0 (2012-09-21)
-------------------

The first public release.

