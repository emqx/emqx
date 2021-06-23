# emqx_authz

## Configure

File: etc/pulgins/authz.conf

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
              ssl: false
           }
           sql: "select ipaddress, username, clientid, action, permission, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or clientid = '%c'"
       },
       {
           type: pgsql
           config: {
              server: "127.0.0.1:5432"
              database: mqtt
              pool_size: 1
              username: root
              password: public
              auto_reconnect: true
              ssl: false
           }
           sql: "select ipaddress, username, clientid, action, permission, topic from mqtt_acl where ipaddr = '%a' or username = '%u' or username = '$all' or clientid = '%c'"
       },
       {
           type: redis
           config: {
              servers: "127.0.0.1:6379"
              database: 0
              pool_size: 1
              password: public
              auto_reconnect: true
              ssl: false
           }
           cmd: "HGETALL mqtt_acl:%u"
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

#### Mysql

Create Example Table

```sql
CREATE TABLE `mqtt_acl` (
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
INSERT INTO mqtt_acl (ipaddress, username, clientid, action, permission, topic) VALUES ('127.0.0.1', '', '', 'subscribe', 'allow', '$SYS/#');
```

#### Pgsql

Create Example Table

```sql
CREATE TYPE ACTION AS ENUM('publish','subscribe','all');
CREATE TYPE PERMISSION AS ENUM('allow','deny');

CREATE TABLE mqtt_acl (
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
INSERT INTO mqtt_acl (ipaddress, username, clientid, action, permission, topic) VALUES ('127.0.0.1', '', '', 'subscribe', 'allow', '$SYS/#');
```

#### Redis

Sample data in the default configuration:

```
HSET mqtt_acl:emqx '$SYS/#' subscribe
```

A rule of Redis ACL defines `publish`, `subscribe`, or `all `information. All lists in the rule are **allow** lists.

