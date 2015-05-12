
## Overview

Authentication with user table of MySQL database.

## etc/plugin.config

```
 {emysql, [
    {pool,      4},
    {host,      "localhost"},
    {port,      3306},
    {username,  ""}, 
    {password,  ""},
    {database,  "mqtt"},
    {encoding,  utf8}
 ]},
 {emqttd_auth_mysql, [
    {user_table, mqtt_users},
    %% plain password only
    {password_hash, plain},
    {field_mapper, [
        {username, username},
        {password, password}
    ]}
 ]}
```

## Users Table(Demo)

Notice: This is a demo table. You could authenticate with any user tables.

```
CREATE TABLE `mqtt_users` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(60) DEFAULT NULL,
  `password` varchar(60) DEFAULT NULL,
  `salt` varchar(20) DEFAULT NULL,
  `created` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `mqtt_users_username` (`username`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
```

## Load Plugin

Merge the'etc/plugin.config' to emqttd/etc/plugins.config, and the plugin will be loaded by the  broker.

