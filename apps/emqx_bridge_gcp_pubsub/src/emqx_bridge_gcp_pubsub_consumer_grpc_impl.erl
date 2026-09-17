%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_grpc_impl).

-behaviour(emqx_resource).

%% `emqx_resource` API
-export([
    resource_type/0,
    callback_mode/0,
    query_mode/1,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3
]).

%% API
-export([]).

%% Internal exports
-export([set_health/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_gcp_pubsub_consumer_grpc.hrl").

-define(connect_timeout, connect_timeout).
-define(health_check_timeout, health_check_timeout).
-define(worker_pool, worker_pool).
-define(pool_size, pool_size).

%% Allocatable resources
-define(grpc_source(SOURCERESID), {grpc_source, SOURCERESID}).

%% Only one worker per channel
-define(OPTVAR_CHAN_HEALTH(CHANRESID), {?MODULE, chan_health, CHANRESID}).

-define(installed_channels, installed_channels).

-type connector_config() :: #{
    authentication := _,
    connect_timeout := _,
    pool_size := _,
    ssl := _,
    url := _,
    resource_opts := #{health_check_timeout := timeout(), any() => term()}
}.
-type connector_state() :: #{
    ?auth_ctx := emqx_bridge_gcp_pubsub_client:auth_ctx(),
    ?client_pool := _,
    ?connect_timeout := timeout(),
    ?health_check_timeout := timeout(),
    ?installed_channels := #{channel_id() => channel_state()}
}.

-type channel_config() :: #{
    parameters := #{}
}.
-type channel_state() :: #{
    ?health_check_timeout := timeout(),
    ?request_ttl := timeout(),
    ?worker_pool := _
}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    gcp_pubsub_consumer_grpc.

-spec callback_mode() -> no_queries.
callback_mode() ->
    no_queries.

-spec query_mode(any()) -> query_mode().
query_mode(_Config) ->
    no_queries.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    start_connector(ConnResId, ConnConfig).

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    stop_connector(ConnResId),
    ?tp("gcp_pubsub_consumer_grpc_connector_stop", #{instance_id => ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(_ConnResId, ConnState) ->
    grpc_connector_health_check(ConnState).

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), channel_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    channel_config()
) ->
    {ok, connector_state()}.
on_add_channel(ConnResId, ConnState0, ChanResId, ChanConfig) ->
    #{?installed_channels := InstalledChannels0} = ConnState0,
    maybe
        {ok, ChanState} ?= create_channel(ConnResId, ChanResId, ChanConfig, ConnState0),
        InstalledChannels = InstalledChannels0#{ChanResId => ChanState},
        ConnState = ConnState0#{?installed_channels := InstalledChannels},
        {ok, ConnState}
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    ConnResId,
    ConnState0 = #{?installed_channels := InstalledChannels0},
    ChanResId
) when
    is_map_key(ChanResId, InstalledChannels0)
->
    {ChanState, InstalledChannels} = maps:take(ChanResId, InstalledChannels0),
    destroy_channel(ConnResId, ChanResId, ChanState),
    ConnState = ConnState0#{?installed_channels := InstalledChannels},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ChanResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ChanResId,
    ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    #{ChanResId := ChanState} = InstalledChannels,
    grpc_channel_health_check(ChanResId, ChanState, ConnState);
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

set_health(ChanResId, HealthStatus) ->
    optvar:set(?OPTVAR_CHAN_HEALTH(ChanResId), HealthStatus),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

allocate(ConnResId, Key, Value) ->
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, Key, Value).

deallocate(ConnResId, Key) ->
    ok = emqx_resource:deallocate_resource(ConnResId, Key).

clear_health(ChanResId) ->
    optvar:unset(?OPTVAR_CHAN_HEALTH(ChanResId)).

gun_opts(ConnConfig) ->
    #{
        connect_timeout := ConnectTimeout,
        ssl := SSL,
        url := URL
    } = ConnConfig,
    case uri_string:parse(URL) of
        #{scheme := "https"} ->
            #{
                transport => ssl,
                connect_timeout => ConnectTimeout,
                tls_opts => emqx_tls_lib:to_client_opts(SSL#{enable => true}),
                retry => 0
            };
        _ ->
            #{
                transport => tcp,
                connect_timeout => ConnectTimeout,
                retry => 0
            }
    end.

start_connector(ConnResId, ConnConfig) ->
    #{
        authentication := Authentication,
        connect_timeout := ConnectTimeout,
        pool_size := PoolSize,
        url := URL,
        resource_opts := #{health_check_timeout := HCTimeout}
    } = ConnConfig,
    ClientOpts = #{
        authentication => Authentication,
        jwt_opts => #{
            %% fixed for pubsub; trailing slash is important.
            aud => <<"https://pubsub.googleapis.com/">>
        },
        supervisor => ?TOP_SUP,
        token_table => ?TOKEN_TAB,
        sa_server_ref => ?SA_SERVER_REF,
        sa_token_table => ?SA_TOKEN_RESP_TAB
    },
    GRPCOpts = #{
        url => URL,
        grpc_opts => #{
            pool_size => PoolSize,
            gun_opts => gun_opts(ConnConfig)
        }
    },
    maybe
        {ok, AuthCtx} ?=
            emqx_bridge_gcp_pubsub_client:maybe_initialize_auth_resources(ConnResId, ClientOpts),
        {ok, #{client_pool := ClientPool}} ?=
            emqx_bridge_gcp_pubsub_consumer_grpc_sup:ensure_connector_started(ConnResId, GRPCOpts),
        ConnectorState = #{
            ?auth_ctx => AuthCtx,
            ?client_pool => ClientPool,
            ?connect_timeout => ConnectTimeout,
            ?health_check_timeout => HCTimeout,
            ?installed_channels => #{}
        },
        {ok, ConnectorState}
    else
        Error ->
            stop_connector(ConnResId),
            Error
    end.

stop_connector(ConnResId) ->
    destroy_channel_allocated_resources(ConnResId, '_'),
    ?SOURCE_SUP:ensure_connector_stopped(ConnResId),
    Ctx = #{
        supervisor => ?TOP_SUP,
        token_table => ?TOKEN_TAB,
        sa_server_ref => ?SA_SERVER_REF,
        sa_token_table => ?SA_TOKEN_RESP_TAB
    },
    emqx_bridge_gcp_pubsub_client:stop_auth_resources(ConnResId, Ctx),
    ok.

grpc_connector_health_check(ConnState) ->
    #{
        ?client_pool := ClientPool,
        ?health_check_timeout := HCTimeout
    } = ConnState,
    Workers = grpc_client_sup:workers(ClientPool),
    Timeout = health_check_connect_timeout(ConnState),
    Fn = fun({_Id, Worker}) ->
        Opts = #{connect_timeout => Timeout},
        grpc_client:health_check(Worker, Opts)
    end,
    try emqx_utils:pmap(Fn, Workers, HCTimeout) of
        [] ->
            {?status_connecting, <<"empty_pool">>};
        Results ->
            Errors = lists:filter(fun(Res) -> Res /= ok end, Results),
            case Errors of
                [] ->
                    ?status_connected;
                [{error, Reason} | _] ->
                    {?status_disconnected, map_grpc_health_check_error(Reason)};
                [Error | _] ->
                    {?status_disconnected, map_grpc_health_check_error(Error)}
            end
    catch
        exit:timeout ->
            {?status_disconnected, timeout};
        Kind:Reason:Stacktrace ->
            {?status_disconnected, {Kind, Reason, Stacktrace}}
    end.

%% grpc_client:health_check forces the `connect_timeout` parameter to become the call
%% timeout...
health_check_connect_timeout(ConnState) ->
    #{
        ?connect_timeout := ConnectTimeout,
        ?health_check_timeout := HCTimeout
    } = ConnState,
    case {HCTimeout, ConnectTimeout} of
        {infinity, _} ->
            ConnectTimeout;
        _ when ConnectTimeout > HCTimeout ->
            HCTimeout;
        _ ->
            ConnectTimeout
    end.

map_grpc_health_check_error({shutdown, Reason}) ->
    map_grpc_health_check_error(Reason);
map_grpc_health_check_error({error, Reason}) ->
    map_grpc_health_check_error(Reason);
map_grpc_health_check_error(Reason) ->
    Reason.

grpc_channel_health_check(ChanResId, ChanState, _ConnState) ->
    #{?health_check_timeout := HCTimeout} = ChanState,
    Key = ?OPTVAR_CHAN_HEALTH(ChanResId),
    case optvar:read(Key, HCTimeout) of
        {ok, Status} ->
            Status;
        timeout ->
            {?status_disconnected, ~"gcp_pubsub_consumer_grpc_health_check_timeout"}
    end.

create_channel(ConnResId, ChanResId, ChanConfig, ConnState) ->
    #{namespace := Namespace} = emqx_resource:parse_channel_id(ChanResId),
    #{
        ?auth_ctx := AuthCtx,
        ?client_pool := ClientPool
    } = ConnState,
    #{
        bridge_name := SourceName,
        hookpoints := Hookpoints,
        parameters := #{
            ack_deadline := AckDeadline,
            max_outstanding_messages := MaxOutstandingMsgs,
            topic := Topic
        },
        resource_opts := #{
            request_ttl := RequestTTL,
            health_check_timeout := HCTimeout
        }
    } = ChanConfig,
    Opts = #{
        ack_deadline => AckDeadline,
        auth_ctx => AuthCtx,
        client_pool => ClientPool,
        conn_res_id => ConnResId,
        hookpoints => Hookpoints,
        max_outstanding_messages => MaxOutstandingMsgs,
        namespace => Namespace,
        request_ttl => RequestTTL,
        source_name => SourceName,
        source_res_id => ChanResId,
        topic => Topic
    },
    allocate(ConnResId, ?grpc_source(ChanResId), #{?source_res_id => ChanResId}),
    clear_health(ChanResId),
    maybe
        {ok, #{worker_pool := WorkerPool}} ?=
            ?SOURCE_SUP:ensure_source_started(
                ConnResId, ChanResId, Opts
            ),
        ChanState = #{
            ?health_check_timeout => HCTimeout,
            ?request_ttl => RequestTTL,
            ?worker_pool => WorkerPool
        },
        {ok, ChanState}
    else
        Error ->
            destroy_channel_allocated_resources(ConnResId, ChanResId),
            Error
    end.

destroy_channel(ConnResId, ChanResId, _ChanState) ->
    destroy_channel_allocated_resources(ConnResId, ChanResId),
    ok.

destroy_channel_allocated_resources(ConnResId, ChanResId) ->
    maps:foreach(
        fun
            (?grpc_source(Id) = Key, Data) when
                ChanResId == '_' orelse ChanResId == Id
            ->
                #{?source_res_id := ResId} = Data,
                ?SOURCE_SUP:ensure_source_stopped(ConnResId, ResId),
                clear_health(ResId),
                deallocate(ConnResId, Key);
            (_, _) ->
                ok
        end,
        emqx_resource:get_allocated_resources(ConnResId)
    ).
