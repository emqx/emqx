---
theme: gaia
color: #000
colorSecondary: #333
backgroundColor: #fff
backgroundImage: url('https://marp.app/assets/hero-background.jpg')
paginate: true
marp: true
---

<!-- _class: lead -->

# EMQ X Resource

---

## What is it for

The [emqx_resource](https://github.com/emqx/emqx/tree/master/apps/emqx_resource) is a behavior that manages configuration specs and runtime states for resources like mysql or redis backends.

It is intended to be used by the emqx_data_bridges and all other resources that need CRUD operations to their configs, and need to initialize the states when creating.

---

<!-- _class: lead -->

# The Demo

The data_bridge for mysql

---
## The callback module 'emqx_mysql_connector'

1. include the emqx_resource_behaviour.hrl:
```
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").
```
---
2. provide the hocon schema for validating the configs:
```
schema() ->
  emqx_connector_schema_lib:relational_db_fields() ++
  emqx_connector_schema_lib:ssl_fields().
...
```

---
3. write the callback functions for starting or stopping the resource instance:

```
on_start/2,
on_stop/2,
on_query/4,
on_health_check/2

```
---
## Start the emqx_data_bridge

```
application:ensure_all_started(emqx_data_bridge).
```

---

## To use the mysql resource from code:

```
emqx_resource:query(ResourceID, {sql, SQL}).
```

```
(emqx@127.0.0.1)2> emqx_resource:list_instances_verbose().
[#{config =>
       #{<<"auto_reconnect">> => true,<<"cacertfile">> => [],
         <<"certfile">> => [],<<"database">> => "mqtt",
         <<"keyfile">> => [],<<"password">> => "public",
         <<"pool_size">> => 1,
         <<"server">> => {{127,0,0,1},3306},
         <<"ssl">> => false,<<"user">> => "root",
         <<"verify">> => false},
   id => <<"bridge:mysql-def">>,mod => emqx_connector_mysql,
   state => #{poolname => 'bridge:mysql-def'},
   status => started}]

(emqx@127.0.0.1)3> emqx_resource:query(<<"bridge:mysql-def">>, {sql, <<"SELECT count(1)">>}).
{ok,[<<"count(1)">>],[[1]]}
```

---

## To get all available data bridges:

```
curl -q --basic -u admin:public -X GET "http://localhost:8081/api/v4/data_bridges/" | jq .
```

---

## Create

To create a mysql data bridge:

```
BridgeMySQL='{
    "type": "mysql",
    "status": "started",
    "config": {
      "verify": false,
      "user": "root",
      "ssl": false,
      "server": "127.0.0.1:3306",
      "pool_size": 1,
      "password": "public",
      "keyfile": "",
      "database": "mqtt",
      "certfile": "",
      "cacertfile": "",
      "auto_reconnect": true
    }
  }'

curl -q --basic -u admin:public -X POST "http://localhost:8081/api/v4/data_bridges/mysql-aaaa" -d $BridgeMySQL | jq .
```

---

## Update

To update an existing data bridge:

```
BridgeMySQL='{
    "type": "mysql",
    "status": "started",
    "config": {
      "verify": false,
      "user": "root",
      "ssl": false,
      "server": "127.0.0.1:3306",
      "pool_size": 2,
      "password": "public",
      "keyfile": "",
      "database": "mqtt",
      "certfile": "",
      "cacertfile": "",
      "auto_reconnect": true
    }
  }'

curl -q --basic -u admin:public -X PUT "http://localhost:8081/api/v4/data_bridges/mysql-aaaa" -d $BridgeMySQL | jq .
```
