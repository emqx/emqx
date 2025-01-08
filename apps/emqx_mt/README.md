# MQTT Client Multi-tenancy

For a MQTT broker, multi-tenancy is a feature that allows different groups of users
to have isolated environments within the same broker instance.

The 'users' or 'tenants' here can be a group of MQTT clients with a shared namespace,
or a group of admins with different access scopes.
This app (so far) implements MQTT client multi-tenancy but not yet admin multi-tenancy.

## Tenant Namespace Extraction (available in Open-Source Edition)

Starting from **EMQX 5.9**, MQTT clients with a client attribute named `tns`
will be considered a client belonging to the corresponding tenant.

This client attribute assignment is from config `mqtt.client_attrs_init` setting.
For example:

```
mqtt.client_attrs_init = [{expression = "username", set_as_attr = "tns"}]
```

In this configuration, the username is assigned as the tenant namespace (`tns`).

More examples are:

- `nth(1,tokens(clientid,'-'))`: client ID prefix before `-`.
- `cn`: TLS certificate common name.
- `user_property.foobar`: Use `foobar` field from User-Property of the CONNECT packet.
- `peersni`: Use the server name indication sent by TLS client.

## Tenant Client ID Isolation (available in Open-Source Edition)

Ideally, if well planed, MQTT clients should be pre-assigned to avoid ID clashes between
different tenants.
And authentication mechanisms can be employed to enforces that invalid client IDs are not
allowed to login.

Nonetheless, starting from EMQX 5.8.3, the config `mqtt.clientid_override` can be used
to assign a prefix for all clients so to avoid client ID clashing between the tenants.

For example this config `mqtt.clientid_override = "concat([username, '-', clientid])"`
should add `username-` as prefix to the client ID internally in EMQX.

## Tenant Topic Isolation (available in Open-Source Edition)

This can be done by making use of the MQTT listener's `mountpoint` config.
For example: `listeners.tcp.default.mountpoint = "${client_attrs.tns}/"`.

The topic in below packets will be automatically mounted (receive) or unmounted (send)
with the prefix:

- `PUBLISH`
- `SUBSCRIBE`
- `UNSUBSCRIBE`
- Will message in `CONNECT`

## Tenant Management Milestone 1

With all the above building blocks available,
this application (`emqx_mt`) can focus on providing the following functionalities:

- **Tenant Listing**: Paginated listing of all tenant namespaces.
- **Live Session Count**: Given a tenant namespace (`tns`),
  quickly retrieve the count of live sessions registered. Due to the nature of asynchronous, the count may not be accurate. For example, when a client is disconnected, the count may not be updated immediately, if the client immediately reconnects, the count may be off by one. When a node restarts, the other nodes in the cluster may take a while to clean up the records, so the count may be off by by many.
- **Paginated Client Iteration**: Given a tenant namespace (`tns`),
  provide a paginated iterator for traversing the client IDs.
- **Tenant Session Count Limit**: Limit the total number of sessions allowed for each tenant. This is so far a global limit for all tenants, but in milestone 2, there will be per-tenant limits.

### Data Structure

This application uses **mnesia** tables, ETS tables:

- **`emqx_mt_ns`**: An `ordered_set` `disc_copies` table for tracking tenant namespaces.
  The records follow the structure: `{_Key = Ns, _Value = []}`.
  Records are not garbage collected, so it's not suitable for randomized namespaces.

- **`emqx_mt_records`**: An `ordered_set` table for tracking records:
  - `{_Key = {Ns, ClientId, Pid}, _Value = Node}` for tenant namespaces (`Ns`).

- **`emqx_mt_count`**: A `ordered_set` table for maintaining counters for each `Ns` in `emqx_mt_records`.
  The records follow the structure: `{{Ns, Node}, Count}`.

- **`emqx_mt_monitor`**: A `set` ETS table to index from `Pid` to `{Ns, ClientId}`
  in order to perform clean-ups when process exits.

- **`emqx_mt_ccache`**: A `set` ETS table to cache the counter for each `Ns` in `emqx_mt_count`.
  The records follow the structure: `{Ns, Timestamp, Count}`. The `Timestamp` is used to expire the cache every 5 seconds.

### Algorithm

- To query client IDs for a tenant: traverse from `ets:next(emqx_mt_records, {Ns, 0, 0})`.
- To query the number of clients for a tenant sum up all the counts: `lists:sum(ets:match(emqx_mt_count, [{{{Ns,'_'},'$1'},[],['$1']}]))`.

### Event Handling

### Register

The application spawns a pool of workers to handle client PID registration
when the `session.created` callback is triggered.

```
hook_callback(ClientInfo) ->
  Msg = {add, get_tns(ClientInfo), get_clientid(ClientInfo), self()},
  _ = erlang:send(pick_pool_worker(emqx_mt), Msg),
  ok.

handle_info({add, Ns, ClientId, Pid}, State) ->
    ok = monitor_and_insert_ets(Pid),
    ok = insert_record(Ns, ClientId, Pid),
    %% when a client reconnects, there can be multiple Pids registered
    %% due to race condition or session-takeover transition period
    ok = inc_counter(Ns);
    {noreply, State1}.
```

### Deregister

When a `DOWN` message is received, the process deregisters the corresponding client.

```
handle_info({'DOWN', _, _, Pid, _}, State) ->
  {Ns, ClientId} = delete_monitor(Pid),
  ok = delete_record(Ns, ClientId),
  ok = dec_counter(Ns),
  {noreply, State}.
```

### Counter Keeping

It's challenging to keep cluster-wide counters.
The proposed idea is for each woker to keep their own counters,
and periodically emits it to a counter collector process for aggregation (per tns-node).

To get a sum of all nodes: `lists:sum(ets:select(emqx_mt_count, MatchSpec))`

Similar to routes, when a node is down, one of the core nodes should be responsible
to clean up the records and the counter for the peer node.

## Tenant Session Count Limit [milestone 1]

Limit the total number of sessions allowed for each tenant.
Will maybe need to extend the `client.authenticate` hook callback return value
to support 'quota-exceeded' error reason, because `client_attrs` is not available
before `client.authenticate` hook callback.

Configuration:

```
multi_tenancy {
    # default limit for all tenants,
    # in milestone 2, there will be per-tenant limits
    # however per-tenant configs are to be stored in a mria table instead of in files
    default_max_sessions = 1000
}
```

## Tenant Management Milestone 2

Allow sys-admin to create tenants before hand. Meaning there should be a
`client.authenticate` hook callback to check if `client_attrs.tns` is
already created. This hook should have a higher priority than the default
priority `HP_AUTHN` so it can be checked before the authentication chain.

## Per-tenant Message Rate Limit [milestone 2]

This depens on the named rate limiter feature to be ready with an overridable design.
e.g. each client puts the rate limiter name as `put(rate_limiter, 'global')`.

According to how it's configured, `emqx_mt` should be able to create named rate limiter(s)
and override the defalut limiters for the client processes.

e.g. in the `session.created` callback function, lookup the rate limiter for
the current client, and `put(rate_limiter, Name)`.

## Limitations

There is so far no limit for the total number of tenants (unique `tns`).
Although creating large number of tenants is only a matter of RAM usage,
it's still a good idea to have a limit to avoid abuse.

When there is a large number of tenants, the tenant listing API may cause excessive
CPU and RAM usage because it's so far not possible to be paginated.

To avoid DoS attacks, it is the authentication subsystem's responsibility to
ensure only legit tenants are allowed to connect.
