##  Overview

Authentication with user table of MySQL database.

## User Table

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

## Plugins config

Please configure 'etc/plugins.config' to loade emysql and emqttd_auth_mysql plugins.


