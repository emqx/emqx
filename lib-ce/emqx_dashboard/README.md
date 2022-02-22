
emqx-dashboard
==============

Dashboard for the EMQX Broker.

REST API
--------

The prefix of REST API is '/api/v4/'.

Method | Path                                  |    Description
-------|---------------------------------------|------------------------------------
GET    |  /nodes/                              |  A list of nodes in the cluster
GET    |  /nodes/:node                         |  Lookup a node in the cluster
GET    |  /brokers/                            |  A list of brokers in the cluster
GET    |  /brokers/:node                       |  Get broker info of a node
GET    |  /metrics/                            |  A list of metrics of all nodes in the cluster
GET    |  /nodes/:node/metrics/                |  A list of metrics of a node
GET    |  /stats/                              |  A list of stats of all nodes in the cluster
GET    |  /nodes/:node/stats/                  |  A list of stats of a node
GET    |  /nodes/:node/clients/                |  A list of clients on a node
GET    |  /listeners/                          |  A list of listeners in the cluster
GET    |  /nodes/:node/listeners               |  A list of listeners on the node
GET    |  /nodes/:node/sessions/               |  A list of sessions on a node
GET    |  /subscriptions/:clientid             |  A list of subscriptions of a client
GET    |  /nodes/:node/subscriptions/:clientid |  A list of subscriptions of a client on the node
GET    |  /nodes/:node/subscriptions/          |  A list of subscriptions on a node
PUT    |  /clients/:clientid/clean_acl_cache   |  Clean ACL cache of a client
GET    |  /configs/                            |  Get all configs
GET    |  /nodes/:node/configs/                |  Get all configs of a node
GET    |  /nodes/:node/plugin_configs/:plugin  |  Get configurations of a plugin on the node
DELETE |  /clients/:clientid                   |  Kick out a client
GET    |  /alarms/:node                        |  List alarms of a node
GET    |  /alarms/                             |  List all alarms
GET    |  /plugins/                            |  List all plugins in the cluster
GET    |  /nodes/:node/plugins/                |  List all plugins on a node
GET    |  /routes/                             |  List routes
POST   |  /nodes/:node/plugins/:plugin/load    |  Load a plugin
GET    |  /clients/:clientid                   |  Lookup a client in the cluster
GET    |  nodes/:node/clients/:clientid        |  Lookup a client on node
GET    |  nodes/:node/sessions/:clientid       |  Lookup a session in the cluster
GET    |  nodes/:node/sessions/:clientid       |  Lookup a session on the node
POST   |  /mqtt/publish                        |  Publish a MQTT message
POST   |  /mqtt/subscribe                      |  Subscribe a topic
POST   |  /nodes/:node/plugins/:plugin/unload  |  Unload a plugin
POST   |  /mqtt/unsubscribe                    |  Unsubscribe a topic
PUT    |  /configs/:app                        |  Update config of an application in the cluster
PUT    |  /nodes/:node/configs/:app            |  Update config of an application on a node
PUT    |  /nodes/:node/plugin_configs/:plugin  |  Update configurations of a plugin on the node

Build
-----

make && make ct

Configurtion
------------

```
dashboard.listener = 18083

dashboard.listener.acceptors = 2

dashboard.listener.max_clients = 512
```

Load Plugin
-----------

```
./bin/emqx_ctl plugins load emqx_dashboard
```

Login
-----

URL: http://host:18083

Username: admin

Password: public

License
-------

Apache License Version 2.0

