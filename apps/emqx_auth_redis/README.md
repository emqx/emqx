emqx_auth_redis
===============

EMQX Redis Authentication/ACL Plugin

Features
---------

- Full *Authentication*, *Superuser*, *ACL* support
- IPv4, IPv6 support
- Connection pool by [ecpool](https://github.com/emqx/ecpool)
- Support `single`, `sentinel`, `cluster` deployment structures of Redis
- Completely cover Redis 5, Redis 6 in our tests


Build Plugin
------------

```
make && make tests
```

Configure Plugin
----------------

File: etc/emqx_auth_redis.conf

```
## Redis server address.
##
## Value: Port | IP:Port
##
## Redis Server: 6379, 127.0.0.1:6379, localhost:6379, Redis Sentinel: 127.0.0.1:26379
auth.redis.server = 127.0.0.1:6379

## redis sentinel cluster name
## auth.redis.sentinel = mymaster

## Redis pool size.
##
## Value: Number
auth.redis.pool = 8

## Redis database no.
##
## Value: Number
auth.redis.database = 0

## Redis password.
##
## Value: String
## auth.redis.password =

## Authentication query command.
##
## Value: Redis cmd
##
## Variables:
##  - %u: username
##  - %c: clientid
##  - %C: common name of client TLS cert
##  - %d: subject of client TLS cert
##
## Examples:
##  - HGET mqtt_user:%u password
##  - HMGET mqtt_user:%u password
##  - HMGET mqtt_user:%u password salt
auth.redis.auth_cmd = HMGET mqtt_user:%u password

## Password hash.
##
## Value: plain | md5 | sha | sha256 | bcrypt
auth.redis.password_hash = plain

## sha256 with salt prefix
## auth.redis.password_hash = salt,sha256

## sha256 with salt suffix
## auth.redis.password_hash = sha256,salt

## bcrypt with salt prefix
## auth.redis.password_hash = salt,bcrypt

## pbkdf2 with macfun iterations dklen
## macfun: md4, md5, ripemd160, sha, sha224, sha256, sha384, sha512
## auth.redis.password_hash = pbkdf2,sha256,1000,20

## Superuser query command.
##
## Value: Redis cmd
##
## Variables:
##  - %u: username
##  - %c: clientid
##  - %C: common name of client TLS cert
##  - %d: subject of client TLS cert
auth.redis.super_cmd = HGET mqtt_user:%u is_superuser

## ACL query command.
##
## Value: Redis cmd
##
## Variables:
##  - %u: username
##  - %c: clientid
auth.redis.acl_cmd = HGETALL mqtt_acl:%u
```

SuperUser
---------

```
HSET mqtt_user:<username> is_superuser 1
```

User Hash with Password Salt
----------------------------

Set a 'user' hash with 'password' 'salt' field, for example:

```
HMSET mqtt_user:<username> password "password" salt "salt"
```

User Set with Password
-----------------------

Set a 'user' Set with 'password' field for example:

```
HSET mqtt_user:<username> password "password"
```

ACL Rule Hash
-------------

The plugin uses a redis hash to store ACL rules:

```
HSET mqtt_acl:<username> topic1 1
HSET mqtt_acl:<username> topic2 2
HSET mqtt_acl:<username> topic3 3
```

NOTE: 1: subscribe, 2: publish, 3: pubsub

Subscription Hash
-----------------

NOTICE: Move to emqx_backend_redis...

The plugin could store the static subscriptions into a redis Hash:

```
HSET mqtt_sub:<username> topic1 0
HSET mqtt_sub:<username> topic2 1
HSET mqtt_sub:<username> topic3 2
```

Load Plugin
-----------

```
./bin/emqx_ctl plugins load emqx_auth_redis
```

Author
------

EMQX Team.

