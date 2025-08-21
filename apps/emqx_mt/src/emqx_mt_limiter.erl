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
    ensure_tenant_limiter_group_absent/1,

    create_client_limiter_group/2,
    update_client_limiter_group/2,
    delete_client_limiter_group/1,
    ensure_client_limiter_group_absent/1,

    cleanup_configs/2
]).

%% 'channel.limiter_adjustment' hookpoint
-export([adjust_limiter/2]).

-export_type([
    root_config/0,

    limiter_kind/0,
    client_config/0,
    tenant_config/0
]).

-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(BYTES_LIM_NAME, bytes).
-define(MESSAGES_LIM_NAME, messages).

-define(tenant, tenant).
-define(client, client).

-type tns() :: emqx_mt:tns().

-type root_config() :: #{
    ?tenant => tenant_config(),
    ?client => client_config()
}.

-type limiter_kind() :: ?tenant | ?client.

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
create_channel_client_container(Zone, ListenerId, Ns) ->
    create_client_container(Zone, ListenerId, Ns, limiter_names()).

create_tenant_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:create_group(emqx_limiter_shared, tenant_group(Ns), LimiterConfigs).

update_tenant_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:update_group(tenant_group(Ns), LimiterConfigs).

delete_tenant_limiter_group(Ns) ->
    emqx_limiter:delete_group(tenant_group(Ns)).

ensure_tenant_limiter_group_absent(Ns) ->
    ensure_group_absent(tenant_group(Ns)).

%%

create_client_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:create_group(emqx_limiter_exclusive, client_group(Ns), LimiterConfigs).

update_client_limiter_group(Ns, Config) ->
    LimiterConfigs = to_limiter_options(Config),
    emqx_limiter:update_group(client_group(Ns), LimiterConfigs).

delete_client_limiter_group(Ns) ->
    emqx_limiter:delete_group(client_group(Ns)).

ensure_client_limiter_group_absent(Ns) ->
    ensure_group_absent(client_group(Ns)).

%%

cleanup_configs(Ns, _Configs) ->
    %% Note: we may safely delete the limiter groups here: when clients attempt to consume
    %% from the now dangling limiters, `emqx_limiter_client' will log the error but don't
    %% do any limiting when it fails to fetch the missing limiter group configuration.
    %% The user may choose to later kick all clients from this namespace.
    _ = ensure_group_absent(tenant_group(Ns)),
    _ = ensure_group_absent(client_group(Ns)),
    ok.

%%------------------------------------------------------------------------------
%% 'channel.limiter_adjustment' hookpoint
%%------------------------------------------------------------------------------

adjust_limiter(#{tns := undefined}, _Limiter) ->
    ok;
adjust_limiter(Context, _Limiter) ->
    #{
        zone := Zone,
        listener_id := ListenerId,
        tns := Ns
    } = Context,
    Limiter = create_channel_client_container(Zone, ListenerId, Ns),
    ?tp("channel_limiter_adjusted", #{}),
    {ok, Limiter}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

zone_group(Zone) ->
    {zone, Zone}.

channel_group(ListenerId) ->
    {channel, ListenerId}.

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
            %% TODO: Isolate implementation details in `emqx_limiter` API.
            ChannelLimiterId = {channel_group(ListenerId), Name},
            ChannelLimiterClient = emqx_limiter:connect(ChannelLimiterId),
            [ChannelLimiterClient]
    end.

ensure_group_absent(Group) ->
    try emqx_limiter:delete_group(Group) of
        ok ->
            ok
    catch
        error:{limiter_group_not_found, _} ->
            ok
    end.
