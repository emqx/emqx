%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_streaming_impl).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_snowflake.hrl").
-include_lib("emqx_connector_jwt/include/emqx_connector_jwt_tables.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3
]).

%% Internal exports only for mocking
-export([
    do_get_streaming_hostname/4
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources
-define(setup_pool, setup_pool).
-define(channel_pool(RES_ID), {channel_pool, RES_ID}).
-define(write_pool(RES_ID), {write_pool, RES_ID}).
-define(streaming_jwt(RES_ID), {streaming_jwt, RES_ID}).

-define(HC_TIMEOUT, 15_000).
%% Seconds
-define(AUTO_RECONNECT_INTERVAL, 2).

-ifdef(TEST).
-define(STREAMING_PORT, persistent_term:get({?MODULE, streaming_port}, 443)).
-else.
-define(STREAMING_PORT, 443).
-endif.

-type connector_config() :: #{
    account := account(),
    connect_timeout := emqx_schema:timeout_duration_ms(),
    max_inactive := emqx_schema:timeout_duration_ms(),
    max_retries := non_neg_integer(),
    pipelining := non_neg_integer(),
    pipe_user := pipe_user(),
    private_key := emqx_schema_secret:secret(),
    private_key_password => emqx_schema_secret:secret(),
    proxy := none | proxy_config(),
    %% request_ttl := timeout(),
    server := binary()
}.
-type connector_state() :: #{
    ?installed_actions := #{action_resource_id() => action_state()},
    ?connect_timeout := emqx_schema:timeout_duration_ms(),
    ?jwt_config := emqx_connector_jwt:jwt_config(),
    ?max_inactive := emqx_schema:timeout_duration_ms(),
    ?max_retries := non_neg_integer(),
    ?request_ttl := timeout()
}.

-type action_config() :: #{
    parameters := #{
        database := database(),
        schema := schema(),
        pipe := pipe()
    }
}.
-type action_state() :: #{
    ?health_check_timeout := timeout(),
    ?connect_timeout := timeout(),
    ?setup := setup_pool_state(),
    ?write := write_pool_state(),
    ?channel := chan_pool_state()
}.

%% todo: refine
-type setup_pool_state() :: map().
-type write_pool_state() :: map().
-type chan_pool_state() :: map().

-type account() :: binary().
-type database() :: binary().
-type schema() :: binary().
-type pipe() :: binary().
-type pipe_user() :: binary().

-type proxy_config() :: #{
    host := binary(),
    port := emqx_schema:port_number()
}.

-type query() :: action_query().
-type action_query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    snowflake_streaming.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    case init_setup_pool(ConnResId, ConnConfig) of
        {ok, State0} ->
            State = State0#{?installed_actions => #{}},
            {ok, State};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    streaming_destroy_allocated_resources(ConnResId),
    Res = emqx_resource_pool:stop(ConnResId),
    ?tp("snowflake_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, ConnState) ->
    #{?connect_timeout := ConnectTimeout} = ConnState,
    SetupPoolId = setup_pool(ConnResId),
    maybe
        ok ?= http_pool_workers_healthy(SetupPoolId, ConnectTimeout),
        ?status_connected
    else
        {error, Reason} ->
            {?status_disconnected, Reason}
    end.

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(ConnResId, ConnState0, ActionResId, ActionConfig) ->
    maybe
        {ok, ActionState} ?= create_action(ConnResId, ActionResId, ActionConfig, ConnState0),
        ConnState = emqx_utils_maps:deep_put(
            [?installed_actions, ActionResId], ConnState0, ActionState
        ),
        {ok, ConnState}
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    ConnResId, ConnState0 = #{?installed_actions := InstalledActions0}, ActionResId
) when
    is_map_key(ActionResId, InstalledActions0)
->
    {ActionState, InstalledActions} = maps:take(ActionResId, InstalledActions0),
    destroy_action(ConnResId, ActionResId, ActionState),
    ConnState = ConnState0#{?installed_actions := InstalledActions},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, ActionResId) ->
    ensure_common_action_destroyed(ActionResId),
    {ok, ConnState}.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), action_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    ConnResId,
    ActionResId,
    _ConnState = #{?installed_actions := InstalledActions}
) when is_map_key(ActionResId, InstalledActions) ->
    ActionState = maps:get(ActionResId, InstalledActions),
    action_status(ConnResId, ActionResId, ActionState);
on_get_channel_status(_ConnResId, _ActionResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    _ConnResId, {ActionResId, Data}, #{?installed_actions := InstalledActions} = _ConnState
) when
    is_map_key(ActionResId, InstalledActions)
->
    #{ActionResId := ActionState} = InstalledActions,
    run_streaming_action([Data], ActionResId, ActionState);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_batch_query(_ConnResId, [{ActionResId, _} | _] = Batch0, #{
    ?installed_actions := InstalledActions
}) when
    is_map_key(ActionResId, InstalledActions)
->
    #{ActionResId := ActionState} = InstalledActions,
    Batch = [Data || {_, Data} <- Batch0],
    run_streaming_action(Batch, ActionResId, ActionState);
on_batch_query(_ConnResId, Batch, _ConnState) ->
    {error, {unrecoverable_error, {bad_batch, Batch}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec create_action(
    connector_resource_id(), action_resource_id(), action_config(), connector_state()
) ->
    {ok, action_state()} | {error, term()}.
create_action(ConnResId, ActionResId, ActionConfig, ConnState) ->
    ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, ActionResId),
    ConnectTimeout = emqx_utils_maps:deep_get([parameters, connect_timeout], ActionConfig),
    HCTimeout = emqx_resource:get_health_check_timeout(ActionConfig),
    maybe
        {ok, WritePoolState, ChanPoolState} ?=
            start_streaming_pools(ConnResId, ActionResId, ActionConfig, ConnState),
        ok ?= open_channels(ActionResId),
        ActionState = #{
            ?mode => ?streaming,
            ?health_check_timeout => HCTimeout,
            ?connect_timeout => ConnectTimeout,
            ?write => WritePoolState,
            ?channel => ChanPoolState
        },
        {ok, ActionState}
    else
        Error ->
            streaming_destroy_allocated_resources(ConnResId, ActionResId),
            Error
    end.

start_streaming_pools(ConnResId, ActionResId, ActionConfig, ConnState) ->
    maybe
        {ok, WritePoolState} ?=
            init_write_pool(ConnResId, ActionResId, ActionConfig, ConnState),
        {ok, ChanPoolState} ?=
            init_channel_pool(ConnResId, ConnState, ActionResId, ActionConfig),
        {ok, WritePoolState, ChanPoolState}
    end.

init_setup_pool(ConnResId, ConnConfig) ->
    #{
        account := Account,
        connect_timeout := ConnectTimeout,
        max_inactive := MaxInactive,
        max_retries := MaxRetries,
        pipe_user := PipeUser,
        pipelining := Pipelining,
        private_key := PrivateKey,
        proxy := ProxyConfig,
        server := Server
    } = ConnConfig,
    %% Maybe make this configurable in the future; it's used mostly during initialization
    %% only.
    RequestTTL = maps:get(request_ttl, ConnConfig, timer:seconds(14)),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?SERVER_OPTS),
    PrivateKeyPassword = maps:get(private_key_password, ConnConfig, undefined),
    JWTParams = #{
        account => Account,
        pipe_user => PipeUser,
        private_key => PrivateKey,
        private_key_password => PrivateKeyPassword
    },
    SetupPoolId = setup_pool(ConnResId),
    ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, SetupPoolId),
    JWTConfig = emqx_bridge_snowflake_lib:jwt_config(SetupPoolId, JWTParams),
    CommonPoolOpts = emqx_bridge_snowflake_lib:common_ehttpc_pool_opts(#{
        connect_timeout => ConnectTimeout,
        pipelining => Pipelining,
        max_inactive => MaxInactive,
        proxy => ProxyConfig
    }),
    SetupPoolOpts =
        [
            {host, Host},
            {port, Port},
            {pool_type, random},
            %% Just for setup, so no need for big pool.
            {pool_size, 1}
            | CommonPoolOpts
        ],
    SetupPoolState0 = #{
        ?common_pool_opts => CommonPoolOpts,
        ?connect_timeout => ConnectTimeout,
        ?jwt_config => JWTConfig,
        ?max_inactive => MaxInactive,
        ?max_retries => MaxRetries,
        ?request_ttl => RequestTTL
    },
    allocate(ConnResId, ?setup_pool, SetupPoolId),
    maybe
        {ok, _} ?= ehttpc_sup:start_pool(SetupPoolId, SetupPoolOpts),
        {ok, Hostname} ?= get_streaming_hostname(SetupPoolId, SetupPoolState0),
        SetupPoolState = SetupPoolState0#{?hostname => Hostname},
        {ok, SetupPoolState}
    else
        {error, {already_started, _}} ->
            _ = ehttpc_sup:stop_pool(SetupPoolId),
            init_setup_pool(ConnResId, ConnConfig);
        {error, Reason} ->
            {error, Reason}
    end.

init_write_pool(ConnResId, ActionResId, ActionConfig, ConnState) ->
    #{
        ?common_pool_opts := CommonPoolOpts,
        ?hostname := Hostname
    } = ConnState,
    #{
        parameters := #{
            pool_size := PoolSize,
            max_retries := MaxRetries,
            max_inactive := MaxInactive
        },
        resource_opts := #{request_ttl := RequestTTL}
    } = ActionConfig,
    WritePoolOpts =
        [
            {host, emqx_utils_conv:str(Hostname)},
            {port, ?STREAMING_PORT},
            {pool_type, direct},
            {pool_size, PoolSize}
            | CommonPoolOpts
        ],
    WritePoolState = #{
        ?hostname => Hostname,
        ?max_inactive => MaxInactive,
        ?max_retries => MaxRetries,
        ?request_ttl => RequestTTL
    },
    WritePoolId = write_pool(ActionResId),
    allocate(ConnResId, ?write_pool(ActionResId), WritePoolId),
    maybe
        ?SLOG(debug, #{
            msg => "snowflake_streaming_starting_write_http_pool",
            write_pool_id => WritePoolId,
            hostname => Hostname
        }),
        ok ?= do_start_streaming_write_http_pool(ActionResId, WritePoolOpts),
        {ok, WritePoolState}
    end.

init_channel_pool(ConnResId, ConnState, ActionResId, ActionConfig) ->
    #{
        ?hostname := Hostname,
        ?jwt_config := JWTConfig
    } = ConnState,
    #{
        parameters := #{
            database := Database,
            schema := Schema,
            pipe := Pipe,
            pool_size := PoolSize,
            max_retries := MaxRetries
        },
        resource_opts := #{request_ttl := RequestTTL}
    } = ActionConfig,
    SetupPoolId = setup_pool(ConnResId),
    WritePoolId = write_pool(ActionResId),
    ChanPoolId = channel_pool(ActionResId),
    SetupPoolState = maps:with([?jwt_config, ?max_retries, ?request_ttl], ConnState),
    ChanOpts = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_type, hash},
        {pool_size, PoolSize},
        {?action_res_id, ActionResId},
        {?database, Database},
        {?schema, Schema},
        {?pipe, Pipe},
        {?jwt_config, JWTConfig},
        {?max_retries, MaxRetries},
        {?request_ttl, RequestTTL},
        {?setup_pool_id, SetupPoolId},
        {?setup_pool_state, SetupPoolState},
        {?write_pool_id, WritePoolId}
    ],
    ?SLOG(debug, #{
        msg => "snowflake_streaming_starting_channel_pool",
        channel_pool_id => ChanPoolId,
        hostname => Hostname
    }),
    allocate(ConnResId, ?channel_pool(ActionResId), ChanPoolId),
    allocate(ConnResId, ?streaming_jwt(ActionResId), ActionResId),
    case emqx_resource_pool:start(ChanPoolId, emqx_bridge_snowflake_channel_client, ChanOpts) of
        ok ->
            ChanPoolState = #{?jwt_config => JWTConfig},
            {ok, ChanPoolState};
        {error, Reason} ->
            {error, Reason}
    end.

do_start_streaming_write_http_pool(ActionResId, WritePoolOpts) ->
    WritePoolId = write_pool(ActionResId),
    case ehttpc_sup:start_pool(WritePoolId, WritePoolOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            _ = ehttpc_sup:stop_pool(WritePoolId),
            do_start_streaming_write_http_pool(ActionResId, WritePoolOpts);
        {error, Reason} ->
            {error, Reason}
    end.

-spec destroy_action(connector_resource_id(), action_resource_id(), action_state()) -> ok.
destroy_action(ConnResId, ActionResId, _ActionState) ->
    streaming_destroy_allocated_resources(ConnResId, ActionResId),
    ok = ensure_common_action_destroyed(ActionResId),
    ok.

ensure_common_action_destroyed(ActionResId) ->
    ok = ehttpc_sup:stop_pool(ActionResId),
    ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, ActionResId),
    ok.

run_streaming_action(Batch, ActionResId, _ActionState) ->
    emqx_trace:rendered_action_template(ActionResId, #{records => Batch}),
    ecpool:pick_and_do(
        {channel_pool(ActionResId), self()},
        fun(ChanClient) ->
            emqx_bridge_snowflake_channel_client:append_rows(ChanClient, Batch)
        end,
        handover
    ).

get_streaming_hostname(SetupPoolId, SetupPoolState) ->
    #{
        jwt_config := JWTConfig,
        request_ttl := RequestTTL,
        max_retries := MaxRetries
    } = SetupPoolState,
    JWTToken = emqx_connector_jwt:ensure_jwt(JWTConfig),
    AuthnHeader = [<<"BEARER ">>, JWTToken],
    Headers = [
        {<<"X-Snowflake-Authorization-Token-Type">>, <<"KEYPAIR_JWT">>},
        {<<"Content-Type">>, <<"application/json">>},
        {<<"Accept">>, <<"application/json">>},
        {<<"User-Agent">>, <<"emqx">>},
        {<<"Authorization">>, AuthnHeader}
    ],
    Path = <<"/v2/streaming/hostname">>,
    Req = {Path, Headers},
    ?tp(debug, "snowflake_streaming_get_hostname_request", #{
        setup_pool_id => SetupPoolId
    }),
    maybe
        {ok, 200, _Headers, Hostname} ?=
            ?MODULE:do_get_streaming_hostname(SetupPoolId, Req, RequestTTL, MaxRetries),
        {ok, Hostname}
    else
        Res ->
            {error, #{reason => <<"get_hostname_unexpected_response">>, response => Res}}
    end.

%% Internal export exposed ONLY for mocking
do_get_streaming_hostname(HTTPPool, Req, RequestTTL, MaxRetries) ->
    ehttpc:request(HTTPPool, get, Req, RequestTTL, MaxRetries).

action_status(_ConnResId, ActionResId, ActionState) ->
    #{
        health_check_timeout := HCTimeout,
        connect_timeout := ConnectTimeout
    } = ActionState,
    WritePoolId = write_pool(ActionResId),
    maybe
        ok ?= channel_pool_workers_healthy(ActionResId, HCTimeout),
        ok ?= http_pool_workers_healthy(WritePoolId, ConnectTimeout),
        ?status_connected
    else
        {error, Reason} ->
            {?status_disconnected, Reason}
    end.

http_pool_workers_healthy(HTTPPool, Timeout) ->
    emqx_bridge_snowflake_lib:http_pool_workers_healthy(HTTPPool, Timeout).

channel_pool_workers_healthy(ActionResId, Timeout) ->
    ChanPoolId = channel_pool(ActionResId),
    Fn = fun(_) -> ok end,
    Res0 = emqx_resource_pool:health_check_workers(
        ChanPoolId, Fn, Timeout, #{return_values => true}
    ),
    case Res0 of
        {ok, []} ->
            {error, {<<"channel_pool_initializing">>, ChanPoolId}};
        {ok, Sts} ->
            case lists:filter(fun(X) -> X /= ok end, Sts) of
                [] ->
                    ok;
                [Err | _] ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

setup_pool(ResId) -> <<ResId/binary, ":setup">>.
write_pool(ResId) -> <<ResId/binary, ":write">>.
channel_pool(ResId) -> <<ResId/binary, ":channel">>.

open_channels(ActionResId) ->
    ChannelPoolId = channel_pool(ActionResId),
    emqx_utils:foldl_while(
        fun({_, Worker}, _Acc) ->
            {ok, ChanClient} = ecpool_worker:client(Worker),
            case emqx_bridge_snowflake_channel_client:ensure_channel_opened(ChanClient) of
                ok ->
                    {cont, ok};
                {error,
                    #{response := {_, _, #{<<"error">> := <<"ERR_PIPE_KIND_NOT_SUPPORTED">>}}} =
                        Error} ->
                    Context = #{
                        reason => <<"error_opening_channel">>,
                        hint => <<"specified pipe does not support streaming">>,
                        error => Error
                    },
                    {halt, {error, {unhealthy_target, Context}}};
                Error ->
                    Context = #{reason => <<"error_opening_channel">>, error => Error},
                    {halt, {error, Context}}
            end
        end,
        ok,
        ecpool:workers(ChannelPoolId)
    ).

streaming_destroy_allocated_resources(ConnResId) ->
    streaming_destroy_allocated_resources(ConnResId, _ActionResId = '_').

streaming_destroy_allocated_resources(ConnResId, ActionResId) ->
    maps:foreach(
        fun
            (?channel_pool(Id) = Key, _) when
                ActionResId == '_' orelse Id == ActionResId
            ->
                ChanPoolId = channel_pool(Id),
                _ = emqx_resource_pool:stop(ChanPoolId),
                deallocate(ConnResId, Key),
                ok;
            (?write_pool(Id) = Key, _) when
                ActionResId == '_' orelse Id == ActionResId
            ->
                WritePoolId = write_pool(Id),
                _ = ehttpc_sup:stop_pool(WritePoolId),
                deallocate(ConnResId, Key),
                ok;
            (?setup_pool = Key, SetupPoolId) when
                ActionResId == '_'
            ->
                _ = ehttpc_sup:stop_pool(SetupPoolId),
                deallocate(ConnResId, Key),
                ok;
            (?streaming_jwt(Id) = Key, _) when
                ActionResId == '_' orelse Id == ActionResId
            ->
                ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, Id),
                deallocate(ConnResId, Key),
                ok;
            (_, _) ->
                ok
        end,
        emqx_resource:get_allocated_resources(ConnResId)
    ).

allocate(ConnResId, Key, Value) ->
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, Key, Value).

deallocate(ConnResId, Key) ->
    ok = emqx_resource:deallocate_resource(ConnResId, Key).
