
EMQX Retainer
==============

The retainer plugin is responsible for storing retained MQTT messages.

Configuration
-------------

etc/emqx_retainer.conf:

```
## Where to store the retained messages.
## Notice that all nodes in a cluster are to have the same storage_type.
##
## Value: ram | disc
##  - ram: memory only
##  - disc: both memory and disc
##
## Default: ram
retainer.storage_type = ram

## Maximum number of retained messages allowed.
##
## Value: Number >= 0
retainer.max_retained_messages = 1000000

## Maximum payload size of a retained message.
##
## Value: Bytes
retainer.max_payload_size = 64KB

## Expiration interval of the retained messages. Never expire if the value is 0.
##
## Value: Duration
##  - h: hour
##  - m: minute
##  - s: second
##
## Examples:
##  - 2h:  2 hours
##  - 30m: 30 minutes
##  - 20s: 20 seconds
##
## Default: 0
retainer.expiry_interval = 0
```

License
-------

Apache License Version 2.0

Author
------

EMQX Team
