
emqx-stomp
==========

The plugin adds STOMP 1.0/1.1/1.2 protocol supports to the EMQX broker.

The STOMP clients could PubSub to the MQTT clients.

Configuration
-------------

etc/emqx_stomp.conf

```
## The Port that stomp listener will bind.
##
## Value: Port
stomp.listener = 61613

## The acceptor pool for stomp listener.
##
## Value: Number
stomp.listener.acceptors = 4

## Maximum number of concurrent stomp connections.
##
## Value: Number
stomp.listener.max_connections = 512

## Default login user
##
## Value: String
stomp.default_user.login = guest

## Default login password
##
## Value: String
stomp.default_user.passcode = guest

## Allow anonymous authentication.
##
## Value: true | false
stomp.allow_anonymous = true

## Maximum numbers of frame headers.
##
## Value: Number
stomp.frame.max_headers = 10

## Maximum length of frame header.
##
## Value: Number
stomp.frame.max_header_length = 1024

## Maximum body length of frame.
##
## Value: Number
stomp.frame.max_body_length = 8192
```

Load the Plugin
---------------

```
./bin/emqx_ctl plugins load emqx_stomp
```

License
-------

Apache License Version 2.0

Author
------

EMQX Team.

