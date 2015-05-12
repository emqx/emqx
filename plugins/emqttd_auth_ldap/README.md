
## Overview

Authentication with LDAP.

## Plugin Config

```
 {emqttd_auth_ldap, [
    {servers, ["localhost"]},
    {port, 389},
    {timeout, 30},
    {user_dn, "uid=$u,ou=People,dc=example,dc=com"},
    {ssl, fasle},
    {sslopts, [
        {"certfile", "ssl.crt"},
        {"keyfile", "ssl.key"}]}
 ]}

```

## Load Plugin

Merge the'etc/plugin.config' to emqttd/etc/plugins.config, and the plugin will be loaded automatically.

