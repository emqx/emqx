===
FAQ
===


##### Q1. Is port 4369 and another random port secure?

```
HI, when start emqttd , I found the port 4369 and another random port(63703) is open, are this security ?

Example: 
tcp 0 0 0.0.0.0:4369 0.0.0.0:* LISTEN 13736/epmd
tcp 0 0 0.0.0.0:8083 0.0.0.0:* LISTEN 16745/beam.smp
tcp 0 0 0.0.0.0:8883 0.0.0.0:* LISTEN 16745/beam.smp
tcp 0 0 0.0.0.0:63703 0.0.0.0:* LISTEN 16745/beam.smp
tcp 0 0 0.0.0.0:1883 0.0.0.0:* LISTEN 16745/beam.smp

1883: mqtt no ssl
8883: mqtt with ssl
8083: websocket
```

4369 and some random ports are opened by erlang node for internal communication. Configure your firewall to allow 1883, 8883, 8083 ports to be accessed from outside for security.

Access control of emqttd broker has two layers:

eSockd TCP Acceptor, Ipaddress based Access Control, Example:

```
{access, [{deny, "192.168.1.1"},
{allow, "192.168.1.0/24"},
{deny, all}]}
```

MQTT Subscribe/Publish Access Control by etc/acl.config, Example:

```
{allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

{deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

{allow, all}.
```

##### Q2. cannot compile emqttd under Chinese folder?

It seems that rebar cannot support Chinese folder name.

##### Q3. emqttd is ready for production ? 

The core features are solid and scalable. A small full-time team with many contributors are developing this project. You could submit issues if any feature requests or bug reports.

##### Q4. Benchmark and performance issue

Wiki: https://github.com/emqtt/emqttd/wiki/One-Million-Connections

##### Q5. 'session' identified by clientID？when session expired what will happen？All queued messages will be deleted and subscribed topics will be deleted too？when reconnected， need redo subscription？(#150)

When a client connected to broker with 'clean session' flag 0, a session identified by clientId will be created. The session will expire after 48 hours(configured in etc/emqttd.config) if no client connections bind with it, and all queued messages and subscriptions will be dropped.

##### Q6. "{max_queued_messages, 100}" in 0.8 release or "{queue, {max_length, 1000},..." means queue for one session or one topic？If it stands for session，while one topic has lots of offline messages(100), the user's other topic offline messages will be flushed? (#150)

For session. Topic just dispatch messages to clients or sessions that matched the subscriptions. Will Flood.

##### Q7. About the retained message, how to config one topic only keep the latest retained message, the older retained messages will be auto deleted?(#150)

By default, the broker only keep the latest retained message of one topic.

##### Q8. When the persistent client with 'clean session' flag 0 is offline but not expired, will the broker put session's subscribed topic new messages to session's queue?(#150)

Yes

##### Q9. If max_length of queue is 100, when the session subscribed topic1 and topic2, what will happen when topic1 fill 70 messages, then topic2 fill 80 messages? After the reconnection, will the session lose first 50 message?(#150)

Lose the oldest 50 messages.


