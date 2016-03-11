==============
Design Guide
==============

---------------
Pubsub Sequence
---------------

## PubSub Sequence

### Clean Session = 1

```

title PubSub Sequence(Clean Session = 1)

ClientA-->PubSub: Publish Message
PubSub-->ClientB: Dispatch Message
```

![PubSub_CleanSess_1](http://emqtt.io/static/img/design/PubSub_CleanSess_1.png)

### Clean Session = 0

```
title PubSub Sequence(Clean Session = 0)

ClientA-->SessionA: Publish Message
SessionA-->PubSub: Publish Message
PubSub-->SessionB: Dispatch Message
SessionB-->ClientB: Dispatch Message

```
![PubSub_CleanSess_0](http://emqtt.io/static/img/design/PubSub_CleanSess_0.png)


## Qos

PubQos | SubQos | In Message | Out Message
-------|--------|------------|-------------
0   |   0    |   0        | 0
0   |   1    |   0        | 0
0   |   2    |   0        | 0
1   |   0    |   1        | 0
1   |   1    |   1        | 1
1   |   2    |   1        | 1
2   |   0    |   2        | 0
2   |   1    |   2        | 1
2   |   2    |   2        | 2


## Topic Functions Benchmark

Mac Air(11):

Function     | Time(microseconds)
-------------|--------------------
match        | 6.25086
triples      | 13.86881
words        | 3.41177
binary:split | 3.03776

iMac:

Function     | Time(microseconds)
-------------|--------------------
match        | 3.2348
triples      | 6.93524
words        | 1.89616
binary:split | 1.65243


--------------
Cluster Design
--------------

## Cluster Design

1. One 'disc_copies' node and many 'ram_copies' nodes.

   2. Topic trie tree will be copied to every clusterd node.

   3. Subscribers to topic will be stored in each node and will not be copied.

   ## Cluster Strategy

   TODO:...

   1. A message only gets forwarded to other cluster nodes if a cluster node is interested in it. this reduces the network traffic tremendously, because it prevents nodes from forwarding unnecessary messages.

   2. As soon as a client on a node subscribes to a topic it becomes known within the cluster. If one of the clients somewhere in the cluster is publishing to this topic, the message will be delivered to its subscriber no matter to which cluster node it is connected.

   ....

## Cluster Architecture

![Cluster Design](http://emqtt.io/static/img/Cluster.png)
## Cluster Command

```sh
./bin/emqttd_ctl cluster DiscNode
```

## Mnesia Example

```
(emqttd3@127.0.0.1)3> mnesia:info().
---> Processes holding locks <---
---> Processes waiting for locks <---
---> Participant transactions <---
---> Coordinator transactions <---
---> Uncertain transactions <---
---> Active tables <---
mqtt_retained : with 6 records occupying 221 words of mem
topic_subscriber: with 0 records occupying 305 words of mem
topic_trie_node: with 129 records occupying 3195 words of mem
topic_trie : with 128 records occupying 3986 words of mem
topic : with 93 records occupying 1797 words of mem
schema : with 6 records occupying 1081 words of mem
===> System info in version "4.12.4", debug level = none <===
opt_disc. Directory "/Users/erylee/Projects/emqttd/rel/emqttd3/data/mnesia" is NOT used.
use fallback at restart = false
running db nodes = ['emqttd2@127.0.0.1','emqttd@127.0.0.1','emqttd3@127.0.0.1']
stopped db nodes = []
master node tables = []
remote = []
ram_copies = [mqtt_retained,schema,topic,topic_subscriber,topic_trie,
topic_trie_node]
disc_copies = []
disc_only_copies = []
[{'emqttd2@127.0.0.1',ram_copies},
{'emqttd3@127.0.0.1',ram_copies},
{'emqttd@127.0.0.1',disc_copies}] = [schema]
[{'emqttd2@127.0.0.1',ram_copies},
{'emqttd3@127.0.0.1',ram_copies},
{'emqttd@127.0.0.1',ram_copies}] = [topic,topic_trie,topic_trie_node,
mqtt_retained]
[{'emqttd3@127.0.0.1',ram_copies}] = [topic_subscriber]
44 transactions committed, 5 aborted, 0 restarted, 0 logged to disc
   0 held locks, 0 in queue; 0 local transactions, 0 remote
   0 transactions waits for other nodes: []
   ```

   ## Cluster vs Bridge

   Cluster will copy topic trie tree between nodes, Bridge will not.



-------------
Hooks Design
-------------

## Overview 

emqttd supported a simple hooks mechanism in 0.8.0 release to extend the broker. The designed is improved in 0.9.0 release.

## API

emqttd_broker Hook API:

```
-export([hook/3, unhook/2, foreach_hooks/2, foldl_hooks/3]).
```

### Hook 

``` 
-spec hook(Hook :: atom(), Name :: any(), MFA :: mfa()) -> ok | {error, any()}.
hook(Hook, Name, MFA) ->
    ...
    ```

 ### Unhook

 ```
 -spec unhook(Hook :: atom(), Name :: any()) -> ok | {error, any()}.
 unhook(Hook, Name) ->
     ...
     ```

  ### Foreach Hooks

  ```
  -spec foreach_hooks(Hook :: atom(), Args :: list()) -> any().
  foreach_hooks(Hook, Args) ->
      ...
      ```

   ### Foldl Hooks

   ```
   -spec foldl_hooks(Hook :: atom(), Args :: list(), Acc0 :: any()) -> any().
   foldl_hooks(Hook, Args, Acc0) ->
       ...
       ```

    ## Hooks 

    Name             | Type      | Description
    ---------------  | ----------| --------------
    client.connected | foreach   | Run when client connected successfully
    client.subscribe | foldl     | Run before client subscribe topics
    client.subscribe.after | foreach | Run After client subscribe topics
    client.unsubscribe | foldl   | Run when client unsubscribe topics
    message.publish   | foldl     | Run when message is published
    message.acked   | foreach     | Run when message is acked
    client.disconnected | foreach | Run when client is disconnnected

    ## End-to-End Message Pub/Ack

    Could use 'message.publish', 'message.acked' hooks to implement end-to-end message pub/ack:

    ```
     PktId <-- --> MsgId <-- --> MsgId <-- --> PktId
          |<--- Qos --->|<---PubSub--->|<-- Qos -->|
          ```
## Limit

The design is experimental.


--------------
Plugin Design
--------------

## Overview

**Notice that 0.11.0 release use rebar to manage plugin's deps.**

A plugin is just an erlang application that extends emqttd broker.

The plugin application should be put in "emqttd/plugins/" folder to build. 


## Plugin Project

You could create a standalone plugin project outside emqttd, and then add it to "emqttd/plugins/" folder by "git submodule". 

Git submodule to compile emqttd_dashboard plugin with the broker, For example:

```
git submodule add https://github.com/emqtt/emqttd_dashboard.git plugins/emqttd_dashboard
make && make dist
```

## plugin.config

**Each plugin should have a 'etc/plugin.config' file**

For example, project structure of emqttd_dashboard plugin:

```
LICENSE
README.md
ebin
etc
priv
rebar.config
src
```

etc/plugin.config for emqttd_dashboard plugin:

```
[
{emqttd_dashboard, [
{listener,
{emqttd_dashboard, 18083, [
{acceptors, 4},
{max_clients, 512}]}}
]}
].
```

## rebar.config

**Plugin should use 'rebar.config' to manage depencies**

emqttd_plugin_pgsql plugin's rebar.config, for example:

```
%% -*- erlang -*-

{deps, [
{epgsql, ".*",{git, "https://github.com/epgsql/epgsql.git", {branch, "master"}}}
]}.
```

## Build emqttd with plugins

Put all the plugins you required in 'plugins/' folder of emqttd project, and then:

```
make && make dist
```

## Load Plugin

'./bin/emqttd_ctl' to load/unload plugin, when emqttd broker started.

```
./bin/emqttd_ctl plugins load emqttd_plugin_demo

./bin/emqttd_ctl plugins unload emqttd_plugin_demo
```

## List Plugins

```
./bin/emqttd_ctl plugins list
```

## API

```
%% Load all active plugins after broker started
emqttd_plugins:load() 

%% Load new plugin
emqttd_plugins:load(Name)

%% Unload all active plugins before broker stopped
emqttd_plugins:unload()

%% Unload a plugin
emqttd_plugins:unload(Name)
```


