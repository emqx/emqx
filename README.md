# Erlang Connection/Client Pool

`ecpool` is different with worker-pool libraries in that it is designed to pool connection/clients to server or database.

`ecpool` tries to avoid the erlang application crash when the server or database going down.

## Overview

A pool worker to manage/monitor the connection to server or database:

```
PoolWorker -> Conn -> DB
```

Use client:

```
ecpool:with_client(Pool, fun(Client) -> call(Client) end).
```

## Usage

### Start the pool

```
ecpool:start_pool(Pool, Mod, Opts).
```

For example:

```
PgOpts = [%% Pool Size
          {pool_size, 10},
          %% Pool Type: round_robin | random | hash
          {pool_type, round_robin},
          %% Auto reconnect
          {auto_reconnect, 3},

          %% epgsql opts
          {host, "localhost"},
          {port, 5432},
          {username, "feng"},
          {password, ""},
          {database, "mqtt"},
          {encoding,  utf8}],

ecpool:start_pool(epgsql_pool, epgsql_pool_client, PgOpts)
```

### The Callback Module

Pool Worker Callback:

```
-callback connect(ConnOpts :: list()) -> {ok, pid()} | {error, any()}.
```

For example, epgsql_pool_client module:

```
-module(epgsql_pool_client).

-behavihour(ecpool_worker).

connect(Opts) ->
    Host = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts),
    epgsql:connect(Host, Username, Password, conn_opts(Opts)).

squery(Pool, Sql) ->
    ecpool:with_client(Pool, fun(Client) -> epgsql:squery(Client, Sql) end).
```

## Design

The `ecpool` supervisor tree:

```
pool_sup[one_for_all supervisor]
    pool[gen-server]
    worker_sup[one_for_one supervisor]
        worker1 -> connection1
        worker2 -> connection1
        ....
```

## Author

Feng Lee <feng@emqx.io>

## License

The Apache License Version 2.0


