GUID
====

Each QoS1/2 MQTT message is identified by a 128 bits, k-ordered GUID(global unique ID).

Structure
---------

::

  --------------------------------------------------------
  |        Timestamp       |  NodeID + PID  |  Sequence  | 
  |<------- 64bits ------->|<--- 48bits --->|<- 16bits ->|
  --------------------------------------------------------


1. Timestamp: erlang:system_time if Erlang >= R18, otherwise os:timestamp
2. NodeId:    encode node() to 2 bytes integer
3. Pid:       encode pid to 4 bytes integer
4. Sequence:  2 bytes sequence in one process

