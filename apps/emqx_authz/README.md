# emqx_authz

## Configure

File: etc/plugins/authz.conf

```json
authz:{
    rules: [
       {
           type: mysql
           config: {
              server: "127.0.0.1:3306"
              database: mqtt
              pool_size: 1
              username: root
              password: public
              auto_reconnect: true
              ssl: {
                enable: true
                cacertfile:  "etc/certs/cacert.pem"
                certfile: "etc/certs/client-cert.pem"
                keyfile: "etc/certs/client-key.pem"
              }
           }
           sql: "select ipaddress, username, clientid, action, permission, topic from mqtt_authz where ipaddr = ${peerhost} or username = ${username} or clientid = ${clientid}"
       },
       {
           type: postgresql
           config: {
              server: "127.0.0.1:5432"
              database: mqtt
              pool_size: 1
              username: root
              password: public
              auto_reconnect: true
              ssl: {enable: false}
           }
           sql: "select ipaddress, username, clientid, action, permission, topic from mqtt_authz where ipaddr = ${peerhost} or username = ${username} or username = '$all' or clientid = ${clientid}"
       },
       {
           type: redis
           config: {
              servers: "127.0.0.1:6379"
              database: 0
              pool_size: 1
              password: public
              auto_reconnect: true
              ssl: {enable: false}
           }
           cmd: "HGETALL mqtt_authz:${username}"
       },
       {
           principal: {username: "^admin?"}
           permission: allow
           action: subscribe
           topics: ["$SYS/#"]
       },
       {
           permission: deny
           action: subscribe
           topics: ["$SYS/#"]
       },
       {
           permission: allow
           action: all
           topics: ["#"]
       }
    ]
}
```

## Database Management

#### MySQL

Create Example Table

```sql
CREATE TABLE `mqtt_authz` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ipaddress` VARCHAR(60) NOT NULL DEFAULT '',
  `username` VARCHAR(100) NOT NULL DEFAULT '',
  `clientid` VARCHAR(100) NOT NULL DEFAULT '',
  `action` ENUM('publish', 'subscribe', 'all') NOT NULL,
  `permission` ENUM('allow', 'deny') NOT NULL,
  `topic` VARCHAR(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Sample data in the default configuration:

```sql
-- Only 127.0.0.1 users can subscribe to system topics
INSERT INTO mqtt_authz (ipaddress, username, clientid, action, permission, topic) VALUES ('127.0.0.1', '', '', 'subscribe', 'allow', '$SYS/#');
```

#### PostgreSQL

Create Example Table

```sql
CREATE TYPE ACTION AS ENUM('publish','subscribe','all');
CREATE TYPE PERMISSION AS ENUM('allow','deny');

CREATE TABLE mqtt_authz (
  id SERIAL PRIMARY KEY,
  ipaddress CHARACTER VARYING(60) NOT NULL DEFAULT '',
  username CHARACTER VARYING(100) NOT NULL DEFAULT '',
  clientid CHARACTER VARYING(100) NOT NULL DEFAULT '',
  action ACTION,
  permission PERMISSION,
  topic CHARACTER VARYING(100) NOT NULL
);

```

Sample data in the default configuration:

```sql
-- Only 127.0.0.1 users can subscribe to system topics
INSERT INTO mqtt_authz (ipaddress, username, clientid, action, permission, topic) VALUES ('127.0.0.1', '', '', 'subscribe', 'allow', '$SYS/#');
```

#### Redis

Sample data in the default configuration:

```
HSET mqtt_authz:emqx '$SYS/#' subscribe
```

A rule of Redis AuthZ defines `publish`, `subscribe`, or `all `information. All lists in the rule are **allow** lists.

#### MongoDB

Create Example BSON documents
```sql
db.inventory.insertOne(
    {username: "emqx",
     clientid: "emqx",
     ipaddress: "127.0.0.1",
     permission: "allow",
     action: "all",
     topics: ["#"]
    })
```
