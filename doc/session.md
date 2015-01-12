# Session Design

## session manager

```erlang

%% lookup sesssion
emqtt_sm:lookup_session(ClientId)

%% Start new or resume existing session
emqtt_sm:start_session(ClientId)

%% destroy session, discard all data
emqtt_sm:destory_session(ClientId)

%% close session, save all data
emqtt_sm:close_session(ClientId)
```

## session supervisor

usage?

## session

```
%%system process
process_flag(trap_exit, true),

session:start()
session:subscribe(
session:publish(
session:resume(
session:suspend(
%%destory all data
session:destory(
%%save all data
session:close()

```

## sm and session

sm manage and monitor session

## client and session

    client(normal process)<--link to -->session(system process)


