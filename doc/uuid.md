## Mongodb ObjectId

* 4-byte value representing the seconds since the Unix epoch,
* 3-byte machine identifier,
* 2-byte process id, and
* 3-byte counter, starting with a random value.

## Flake Id

* 64bits Timestamp
* 48bits WorkerId
* 16bits Sequence

## emqttd Id

* 64bits Timestamp: erlang:now(), erlang:system_time
* 48bits (node+pid): Node + Pid -> Integer 
* 16bits Sequence: PktId

