emqx_auth_mongo
===============

EMQX Authentication/ACL with MongoDB

Build the Plugin
----------------

```
make & make tests
```

Configuration
-------------

File: etc/emqx_auth_mongo.conf

```
## MongoDB Topology Type.
##
## Value: single | unknown | sharded | rs
auth.mongo.type = single

## Sets the set name if type is rs.
##
## Value: String
## auth.mongo.rs_set_name =

## MongoDB server list.
##
## Value: String
##
## Examples: 127.0.0.1:27017,127.0.0.2:27017...
auth.mongo.server = 127.0.0.1:27017

## MongoDB pool size
##
## Value: Number
auth.mongo.pool = 8

## MongoDB login user.
##
## Value: String
## auth.mongo.login =

## MongoDB password.
##
## Value: String
## auth.mongo.password =

## MongoDB AuthSource
##
## Value: String
## Default: mqtt
## auth.mongo.auth_source = admin

## MongoDB database
##
## Value: String
auth.mongo.database = mqtt

## MongoDB write mode.
##
## Value: unsafe | safe
## auth.mongo.w_mode =

## Mongo read mode.
##
## Value: master | slave_ok
## auth.mongo.r_mode =

## MongoDB topology options.
auth.mongo.topology.pool_size = 1
auth.mongo.topology.max_overflow = 0
## auth.mongo.topology.overflow_ttl = 1000
## auth.mongo.topology.overflow_check_period = 1000
## auth.mongo.topology.local_threshold_ms = 1000
## auth.mongo.topology.connect_timeout_ms = 20000
## auth.mongo.topology.socket_timeout_ms = 100
## auth.mongo.topology.server_selection_timeout_ms = 30000
## auth.mongo.topology.wait_queue_timeout_ms = 1000
## auth.mongo.topology.heartbeat_frequency_ms = 10000
## auth.mongo.topology.min_heartbeat_frequency_ms = 1000

## Authentication query.
auth.mongo.auth_query.collection = mqtt_user

auth.mongo.auth_query.password_field = password

## Password hash.
##
## Value: plain | md5 | sha | sha256 | bcrypt
auth.mongo.auth_query.password_hash = sha256

## sha256 with salt suffix
## auth.mongo.auth_query.password_hash = sha256,salt

## sha256 with salt prefix
## auth.mongo.auth_query.password_hash = salt,sha256

## bcrypt with salt prefix
## auth.mongo.auth_query.password_hash = salt,bcrypt

## pbkdf2 with macfun iterations dklen
## macfun: md4, md5, ripemd160, sha, sha224, sha256, sha384, sha512
## auth.mongo.auth_query.password_hash = pbkdf2,sha256,1000,20

auth.mongo.auth_query.selector = username=%u

## Enable superuser query.
auth.mongo.super_query = on

auth.mongo.super_query.collection = mqtt_user

auth.mongo.super_query.super_field = is_superuser

auth.mongo.super_query.selector = username=%u

## Enable ACL query.
auth.mongo.acl_query = on

auth.mongo.acl_query.collection = mqtt_acl

auth.mongo.acl_query.selector = username=%u
```

Load the Plugin
---------------

```
./bin/emqx_ctl plugins load emqx_auth_mongo
```

MongoDB Database
----------------

```
use mqtt
db.createCollection("mqtt_user")
db.createCollection("mqtt_acl")
db.mqtt_user.ensureIndex({"username":1})
```

mqtt_user Collection
--------------------

```
{
    username: "user",
    password: "password hash",
    salt: "password salt",
    is_superuser: boolean (true, false),
    created: "datetime"
}
```

For example:
```
db.mqtt_user.insert({username: "test", password: "password hash", salt: "password salt", is_superuser: false})
db.mqtt_user.insert({username: "root", is_superuser: true})
```

mqtt_acl Collection
-------------------

```
{
    username: "username",
    clientid: "clientid",
    publish: ["topic1", "topic2", ...],
    subscribe: ["subtop1", "subtop2", ...],
    pubsub: ["topic/#", "topic1", ...]
}
```

For example:

```
db.mqtt_acl.insert({username: "test", publish: ["t/1", "t/2"], subscribe: ["user/%u", "client/%c"]})
db.mqtt_acl.insert({username: "admin", pubsub: ["#"]})
```

License
-------

Apache License Version 2.0

Author
------

EMQX Team.

