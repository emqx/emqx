%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_limiter).

-moduledoc """
This module handles the limiters used by multi-tenancy clients.

We have 2 limiters per tenant: one shared (between all clients in the tenant), called
"tenant limiter", and one exclusive, called "client limiter".

Assuming both are configured, we alter the client channel's limiters as follows:

- _Compose_ the tenant limiter with the zone limiter.
- _Replace_ the listener exclusive limiter with the client limiter.

If one of the limiters lack configuration, we simply don't do each action above.
""".

%% API
-export([
    create_channel_client_container/3,

    create_tenant_limiter_group/2,
    update_tenant_limiter_group/2,
    delete_tenant_limiter_group/1,

    create_client_limiter_group/2,
    update_client_limiter_group/2,
    delete_client_limiter_group/1
]).

-export_type([
    client_config/0,
    tenant_config/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(BYTES_LIM_NAME, bytes).
-define(MESSAGES_LIM_NAME, messages).

-type tns() :: emqx_mt:tns().

-type tenant_config() :: #{
    ?BYTES_LIM_NAME := limiter_options(),
    ?MESSAGES_LIM_NAME := limiter_options()
}.

-type client_config() :: tenant_config().

-type limiter_options() :: emqx_limiter:options().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec create_channel_client_container(emqx_types:zone(), emqx_listeners:listener_id(), tns()) ->
    emqx_limiter_client_container:t().
create_channel_client_container(ZoneName, ListenerId, Ns) ->
    create_client_container(ZoneName, ListenerId, Ns, limiter_names()).

create_tenant_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:create_group(emqx_limiter_shared, tenant_group(Ns), LimiterConfigs).

update_tenant_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:update_group(tenant_group(Ns), LimiterConfigs).

delete_tenant_limiter_group(Ns) ->
    emqx_limiter:delete_group(tenant_group(Ns)).

%%

create_client_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:create_group(emqx_limiter_exclusive, client_group(Ns), LimiterConfigs).

update_client_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:update_group(client_group(Ns), LimiterConfigs).

delete_client_limiter_group(Ns) ->
    emqx_limiter:delete_group(client_group(Ns)).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

zone_group(Zone) ->
    {zone, Zone}.

listener_group(ListenerId) ->
    {listener, ListenerId}.

tenant_group(Ns) ->
    {mt_tenant, Ns}.

client_group(Ns) ->
    {mt_client, Ns}.

limiter_names() ->
    [?MESSAGES_LIM_NAME, ?BYTES_LIM_NAME].

to_limiter_options(Config) ->
    lists:map(
        fun(Name) ->
            #{rate := Rate, burst := Burst} = maps:get(Name, Config),
            {Name, emqx_limiter:config_from_rate_and_burst(Rate, Burst)}
        end,
        limiter_names()
    ).

create_client_container(Zone, ListenerId, Ns, Names) ->
    Clients = lists:map(
        fun(Name) ->
            LimiterClient = create_limiter(Zone, ListenerId, Ns, Name),
            {Name, LimiterClient}
        end,
        Names
    ),
    emqx_limiter_client_container:new(Clients).

create_limiter(Zone, ListenerId, Ns, Name) ->
    TenantLimiters = create_tenant_limiters(Zone, Ns, Name),
    ClientLimiters = create_client_limiters(ListenerId, Ns, Name),
    Clients = TenantLimiters ++ ClientLimiters,
    emqx_limiter_composite:new(Clients).

create_tenant_limiters(Zone, Ns, Name) ->
    ZoneLimiterId = {zone_group(Zone), Name},
    ZoneLimiterClient = emqx_limiter:connect(ZoneLimiterId),
    case emqx_mt_config:get_tenant_limiter_config(Ns) of
        {ok, #{}} ->
            TenantLimiterId = {tenant_group(Ns), Name},
            TenantLimiterClient = emqx_limiter:connect(TenantLimiterId),
            [ZoneLimiterClient, TenantLimiterClient];
        _ ->
            [ZoneLimiterClient]
    end.

create_client_limiters(ListenerId, Ns, Name) ->
    case emqx_mt_config:get_client_limiter_config(Ns) of
        {ok, #{}} ->
            ClientLimiterId = {client_group(Ns), Name},
            ClientLimiterClient = emqx_limiter:connect(ClientLimiterId),
            [ClientLimiterClient];
        _ ->
            ListenerLimiterId = {listener_group(ListenerId), Name},
            ListenerLimiterClient = emqx_limiter:connect(ListenerLimiterId),
            [ListenerLimiterClient]
    end.
