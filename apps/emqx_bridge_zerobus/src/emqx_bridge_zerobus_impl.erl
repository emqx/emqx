%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_impl).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_zerobus.hrl").
-include_lib("emqx_schema_registry/include/emqx_schema_registry.hrl").

%% `emqx_resource` API
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
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4
]).

%% API
-export([]).

%% Internal exports
-export([rest_reply_delegator/3]).
-export([set_health/3]).

-export_type([
    proto_record/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources
-define(grpc_action(ACTIONRESID), {grpc_action, ACTIONRESID}).
-define(oauth2(ACTIONRESID), {oauth2, ACTIONRESID}).

-define(OPTVAR_CHAN_HEALTH(CHANRESID, IDX), {?MODULE, chan_health, CHANRESID, IDX}).

-define(async, async).
-define(authn, authn).
-define(connect_timeout, connect_timeout).
-define(grpc, grpc).
-define(health_check_timeout, health_check_timeout).
-define(installed_channels, installed_channels).
-define(max_retries, max_retries).
-define(path, path).
-define(pool_size, pool_size).
-define(rest, rest).
-define(sync, sync).
-define(transport, transport).
-define(writer_pool, writer_pool).
-define(zerobus_endpoint, zerobus_endpoint).

-type connector_config() :: #{
    authentication := _,
    ssl := _,
    zerobus_endpoint := binary(),
    transport := transport_config(),
    resource_opts := #{health_check_timeout := timeout()}
}.
-type connector_state() :: #{
    ?authn => authn(),
    ?health_check_timeout => timeout(),
    ?installed_channels => #{action_resource_id() => channel_state()},
    ?transport := transport_conn(),
    ?zerobus_endpoint => binary()
}.

-type transport_config() :: grpc_transport_config() | rest_transport_config().
-type grpc_transport_config() :: #{
    type := grpc,
    connect_timeout := pos_integer(),
    pool_size := pos_integer()
}.
-type rest_transport_config() :: #{
    type := rest,
    connect_timeout := pos_integer(),
    pool_size := pos_integer(),
    max_inactive := _,
    pipelining := pos_integer()
}.

-type authn() :: #{}.
-type transport_conn() :: {?rest, rest_transport_conn()} | {?grpc, grpc_transport_conn()}.
-type rest_transport_conn() :: #{}.
-type grpc_transport_conn() :: #{
    ?client_pool => term(),
    ?connect_timeout => timeout(),
    ?pool_size => pos_integer()
}.

-type channel_config() :: #{
    parameters := #{
        catalog := binary(),
        schema := binary(),
        table := binary(),
        record := record_config()
    },
    resource_opts := #{
        request_ttl := timeout(),
        health_check_timeout := timeout()
    }
}.
-type channel_state() :: #{
    ?catalog := binary(),
    ?schema := binary(),
    ?table := binary(),
    ?transport := transport_chan(),
    ?health_check_timeout := timeout(),
    ?request_ttl := timeout(),
    ?record := ?json | {?proto, proto_record()}
}.

-type record_config() ::
    #{type := json}
    | #{type := proto, schema_name := binary(), message_type := binary()}.

-type transport_chan() :: {?rest, rest_transport_chan()} | {?grpc, grpc_transport_chan()}.
-type rest_transport_chan() :: #{?path := binary()}.
-type grpc_transport_chan() :: #{
    ?client_pool => term(),
    ?connect_timeout => timeout(),
    ?pool_size => pos_integer()
}.
-type proto_record() :: #{
    ?serde_name := binary(),
    ?message_type := atom(),
    ?descriptor := binary()
}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    zerobus.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    async_if_possible.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    start_connector(ConnResId, ConnConfig).

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    destroy_channel_allocated_resources(ConnResId, '_'),
    _ = ehttpc_sup:stop_pool(ConnResId),
    emqx_bridge_zerobus_action_sup:ensure_connector_stopped(ConnResId),
    ?tp("zerobus_connector_stop", #{instance_id => ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | {?status_connecting | ?status_disconnected, term()}.
on_get_status(ConnResId, ConnState) ->
    case ConnState of
        #{?transport := {?rest, _}} ->
            ehttpc_connector_health_check(ConnResId);
        #{?transport := {?grpc, _}} ->
            grpc_connector_health_check(ConnState)
    end.

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
    ?status_connected | {?status_connecting | ?status_disconnected, term()}.
on_get_channel_status(
    _ConnResId,
    ChanResId,
    ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    #{ChanResId := ChanState} = InstalledChannels,
    maybe
        {ok, _} ?= emqx_connector_oauth2:get_token(ChanResId),
        case ChanState of
            #{?transport := {?grpc, _}} ->
                grpc_channel_health_check(ChanResId, ChanState, ConnState);
            #{?transport := {?rest, _}} ->
                %% not much we can check?
                ?status_connected
        end
    else
        TokenError ->
            map_token_errors(TokenError)
    end;
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data],
    do_insert_batch(Batch, ?sync, ChanState, ConnState, ConnResId, ChanResId);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_query_async(
    connector_resource_id(),
    {message_tag(), map()},
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) -> {ok, pid()} | {error, term()}.
on_query_async(
    ConnResId,
    {ChanResId, #{} = Data},
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data],
    do_insert_batch(Batch, {?async, ReplyFnAndArgs}, ChanState, ConnState, ConnResId, ChanResId);
on_query_async(_ConnResId, Query, _ReplyFnAndArgs, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    ConnResId,
    [{ChanResId, _Data} | _] = Queries,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data || {_, Data} <- Queries],
    do_insert_batch(Batch, ?sync, ChanState, ConnState, ConnResId, ChanResId);
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

-spec on_batch_query_async(
    connector_resource_id(),
    [query()],
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) ->
    ok | {error, term()}.
on_batch_query_async(
    ConnResId,
    [{ChanResId, _Data} | _] = Queries,
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data || {_, Data} <- Queries],
    do_insert_batch(Batch, {?async, ReplyFnAndArgs}, ChanState, ConnState, ConnResId, ChanResId);
on_batch_query_async(_ConnResId, Queries, _ReplyFnAndArgs, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% adapted from emqx_bridge_http_connector, since we can't reuse it directly.
rest_reply_delegator(
    #{trace_metadata := TraceMetadata} = Context,
    ReplyFunAndArgs,
    Result0
) ->
    spawn(fun() ->
        logger:set_process_metadata(TraceMetadata),
        Result = emqx_bridge_http_connector:transform_result(Result0),
        logger:unset_process_metadata(),
        maybe_retry(Result, Context, ReplyFunAndArgs)
    end).

set_health(ChanResId, Idx, HealthStatus) ->
    optvar:set(?OPTVAR_CHAN_HEALTH(ChanResId, Idx), HealthStatus),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

allocate(ConnResId, Key, Value) ->
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, Key, Value).

deallocate(ConnResId, Key) ->
    ok = emqx_resource:deallocate_resource(ConnResId, Key).

fmt(FmtStr, Context) ->
    Template = emqx_template:parse(FmtStr),
    iolist_to_binary(emqx_template:render_strict(Template, Context)).

clear_health(ChanResId, PoolSize) ->
    lists:foreach(
        fun(Idx) -> optvar:unset(?OPTVAR_CHAN_HEALTH(ChanResId, Idx)) end,
        lists:seq(1, PoolSize)
    ).

gun_opts(ConnConfig) ->
    #{
        ssl := SSL,
        zerobus_endpoint := ZerobusEndpoint
    } = ConnConfig,
    case uri_string:parse(ZerobusEndpoint) of
        #{scheme := "https"} ->
            #{
                transport => ssl,
                tls_opts => emqx_tls_lib:to_client_opts(SSL#{enable => true}),
                retry => 0
            };
        _ ->
            #{transport => tcp, retry => 0}
    end.

map_token_errors({token_endpoint_error, 401, Details}) ->
    Msg = append_error_detail(~"Invalid credentials", Details),
    {?status_disconnected, {unhealthy_target, Msg}};
map_token_errors({token_endpoint_error, 403, Details}) ->
    Msg = append_error_detail(~"Permission denied", Details),
    {?status_disconnected, {unhealthy_target, Msg}};
map_token_errors({token_endpoint_error, Status, Details}) ->
    Msg0 = fmt(~"Bad token status code ${s}", #{s => Status}),
    Msg = append_error_detail(Msg0, Details),
    {?status_disconnected, Msg};
map_token_errors({error, Reason}) ->
    map_token_errors(Reason);
map_token_errors(Reason) ->
    {?status_disconnected, {token_error, Reason}}.

append_error_detail(Msg, ~"") ->
    Msg;
append_error_detail(Msg, Details) when is_binary(Details) ->
    <<Msg/binary, ": ", Details/binary>>;
append_error_detail(Msg, _Details) ->
    Msg.

serialize_records(Records0, #{?record := {?proto, Proto}}) ->
    #{
        ?serde_name := SerdeName,
        ?message_type := MessageType
    } = Proto,
    case emqx_schema_registry:get_serde(SerdeName) of
        {error, not_found} ->
            {error, {unrecoverable_error, {serde_deleted, SerdeName}}};
        {ok, Serde = #serde{type = ?protobuf}} ->
            do_serialize_proto_records(Records0, Serde, MessageType);
        {ok, #serde{type = Type}} ->
            {error, {unrecoverable_error, {serde_wrong_type, SerdeName, Type}}}
    end;
serialize_records(Records0, #{?record := ?json}) ->
    {ok, lists:map(fun emqx_utils_json:encode/1, Records0)}.

do_serialize_proto_records(Records0, Serde, MessageType) ->
    #serde{eval_context = SerdeMod} = Serde,
    try
        Records = lists:map(
            fun(R) ->
                apply(SerdeMod, encode_msg, [R, MessageType])
            end,
            Records0
        ),
        {ok, Records}
    catch
        error:Reason ->
            {error, {unrecoverable_error, {invalid_record, Reason}}}
    end.

grpc_connector_health_check(ConnState) ->
    #{
        ?health_check_timeout := HCTimeout,
        ?transport :=
            {?grpc, #{
                ?client_pool := ClientPool,
                ?connect_timeout := ConnectTimeout
            }}
    } = ConnState,
    Workers = grpc_client_sup:workers(ClientPool),
    Fn = fun({_Id, Worker}) ->
        Opts = #{connect_timeout => ConnectTimeout},
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

map_grpc_health_check_error({shutdown, Reason}) ->
    map_grpc_health_check_error(Reason);
map_grpc_health_check_error({error, Reason}) ->
    map_grpc_health_check_error(Reason);
map_grpc_health_check_error(Reason) ->
    Reason.

ehttpc_connector_health_check(ConnResId) ->
    case ehttpc:check_pool_integrity(ConnResId) of
        ok ->
            health_check_ehttpc_pool_workers(ConnResId);
        {error, Reason} ->
            {?status_disconnected, Reason}
    end.

health_check_ehttpc_pool_workers(ConnResId) ->
    Timeout = emqx_resource_pool:health_check_timeout(),
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(ConnResId)],
    try
        emqx_utils:pmap(fun(Worker) -> ehttpc:health_check(Worker, Timeout) end, Workers, Timeout)
    of
        [] ->
            {?status_connecting, <<"connection_pool_not_initialized">>};
        [_ | _] = Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ?status_connected;
                [{error, Reason} | _] ->
                    ?SLOG(info, #{
                        msg => "zerobus_health_check_failed",
                        reason => emqx_utils:redact(Reason),
                        connector => ConnResId
                    }),
                    {?status_disconnected, Reason}
            end
    catch
        exit:timeout ->
            ?SLOG(info, #{
                msg => "zerobus_health_check_failed",
                reason => timeout,
                connector => ConnResId
            }),
            {?status_disconnected, <<"Health check timeout">>}
    end.

pick_ehttpc_worker(ConnResId, Key) ->
    case ehttpc_pool:pick_worker(ConnResId, Key) of
        false ->
            {error, {recoverable_error, no_pool_worker_available}};
        Worker ->
            {ok, Worker}
    end.

grpc_channel_health_check(ChanResId, ChanState, ConnState) ->
    #{?transport := {?grpc, #{?pool_size := PoolSize}}} = ConnState,
    #{?health_check_timeout := HCTimeout} = ChanState,
    Keys = [?OPTVAR_CHAN_HEALTH(ChanResId, Idx) || Idx <- lists:seq(1, PoolSize)],
    case optvar:wait_vars(Keys, HCTimeout) of
        ok ->
            do_fold_grpc_optvar_results(Keys);
        {timeout, _} ->
            {?status_disconnected, ~"zerobus_health_check_timeout"}
    end.

do_fold_grpc_optvar_results(Keys) ->
    Results = [optvar:peek(Key) || Key <- Keys],
    Errors0 = lists:filtermap(
        fun
            ({ok, ?status_connected}) ->
                false;
            ({ok, {?status_connected, _}}) ->
                false;
            ({ok, {Status, Reason}}) when
                ?IS_HEALTH_CHECK_STATUS(Status),
                Status /= ?status_connected
            ->
                {true, {Status, Reason}};
            ({ok, Status}) when
                ?IS_HEALTH_CHECK_STATUS(Status),
                Status /= ?status_connected
            ->
                {true, Status};
            ({ok, Reason}) ->
                %% shouldn't happen; it's a code bug.
                {true, {?status_disconnected, Reason}};
            (_) ->
                %% not ok should be a very rare race condition, since we wait for all
                %% optvars before peeking.
                false
        end,
        Results
    ),
    case sort_status(Errors0) of
        [{Status, Reason} | _] ->
            {Status, Reason};
        [Status | _] ->
            Status;
        [] ->
            ?status_connected
    end.

%% {disconnected, unhealthy_target} > disconnected > connecting > connected
sort_status(Errors0) ->
    ToStatus = fun
        ({S, {unhealthy_target, _}}) -> {S, unhealthy_target};
        ({S, _Reason}) -> S;
        (S) -> S
    end,
    CompareFn = fun(S1A, S2A) ->
        S1 = ToStatus(S1A),
        S2 = ToStatus(S2A),
        case S1 == S2 of
            true ->
                case {S1A, S2A} of
                    {{_, noproc}, {_, _}} ->
                        %% race: health check might've caught a restarting grpc_client, or
                        %% grpc_client itself might've received `noproc` because it set up
                        %% its monitor after gun died.  try to get the next reason;
                        %% hopefully it's more informative.
                        false;
                    {{_, _}, {_, noproc}} ->
                        true;
                    {{_, _}, _} when not is_tuple(S2A) ->
                        %% prefer those with reason
                        true;
                    _ ->
                        false
                end;
            false ->
                S1 > S2
        end
    end,
    lists:sort(CompareFn, Errors0).

-spec start_connector(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, any()}.
start_connector(ConnResId, #{transport := #{type := rest}} = ConnConfig) ->
    #{
        authentication := Authn,
        ssl := SSL,
        zerobus_endpoint := ZerobusEndpoint,
        resource_opts := #{health_check_timeout := HCTimeout},
        transport := #{
            connect_timeout := ConnectTimeout,
            pool_size := PoolSize,
            max_inactive := MaxInactive,
            pipelining := Pipelining
        }
    } = ConnConfig,
    #{hostname := Host, port := Port} = emqx_schema:parse_server(
        ZerobusEndpoint, ?PARSE_SERVER_OPTS
    ),
    {Transport, TransportOpts0} =
        case SSL of
            #{enable := true} ->
                TLSOpts = emqx_tls_lib:to_client_opts(SSL),
                {tls, TLSOpts};
            _ ->
                {tcp, []}
        end,
    TransportOpts = emqx_utils:ipv6_probe(TransportOpts0),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30_000},
        {pool_type, hash},
        {pool_size, PoolSize},
        {transport, Transport},
        {transport_opts, TransportOpts},
        {max_inactive, MaxInactive},
        {enable_pipelining, Pipelining}
    ],
    ConnState = #{
        ?authn => Authn,
        ?health_check_timeout => HCTimeout,
        ?installed_channels => #{},
        ?transport => {?rest, #{}},
        ?zerobus_endpoint => ZerobusEndpoint
    },
    case ehttpc_sup:start_pool(ConnResId, PoolOpts) of
        {ok, _} ->
            {ok, ConnState};
        {error, {already_started, _}} ->
            _ = ehttpc_sup:stop_pool(ConnResId),
            start_connector(ConnResId, ConnConfig);
        {error, Reason} ->
            {error, Reason}
    end;
start_connector(ConnResId, #{transport := #{type := grpc}} = ConnConfig) ->
    #{
        authentication := Authn,
        ssl := _SSL,
        zerobus_endpoint := ZerobusEndpoint,
        resource_opts := #{health_check_timeout := HCTimeout},
        transport := #{
            connect_timeout := ConnectTimeout,
            pool_size := PoolSize
        }
    } = ConnConfig,
    Opts = #{
        url => ZerobusEndpoint,
        grpc_opts => #{
            pool_size => PoolSize,
            gun_opts => gun_opts(ConnConfig)
        }
    },
    maybe
        {ok, #{client_pool := ClientPool}} ?=
            emqx_bridge_zerobus_action_sup:ensure_connector_started(ConnResId, Opts),
        ConnState = #{
            ?authn => Authn,
            ?health_check_timeout => HCTimeout,
            ?installed_channels => #{},
            ?transport =>
                {?grpc, #{
                    ?client_pool => ClientPool,
                    ?connect_timeout => ConnectTimeout,
                    ?pool_size => PoolSize
                }},
            ?zerobus_endpoint => ZerobusEndpoint
        },
        {ok, ConnState}
    end.

-spec create_channel(
    connector_resource_id(),
    action_resource_id(),
    channel_config(),
    connector_state()
) ->
    {ok, channel_state()} | {error, any()}.
create_channel(
    ConnResId,
    ChanResId,
    #{parameters := #{record := #{type := json}}} = ChanConfig,
    #{?transport := {?rest, _}} = ConnState
) ->
    #{
        parameters := #{
            catalog := Catalog,
            schema := Schema,
            table := Table
        },
        resource_opts := #{
            health_check_timeout := HCTimeout,
            request_ttl := RequestTTL
        }
    } = ChanConfig,
    ok = register_oauth2(ConnResId, ChanResId, ChanConfig, ConnState),
    Path = fmt(~"/zerobus/v1/tables/${c}.${s}.${t}/insert", #{
        c => uri_string:quote(Catalog),
        s => uri_string:quote(Schema),
        t => uri_string:quote(Table)
    }),
    ChanState = #{
        ?catalog => Catalog,
        ?schema => Schema,
        ?table => Table,
        ?transport =>
            {?rest, #{
                ?max_retries => 3,
                ?path => Path
            }},
        ?health_check_timeout => HCTimeout,
        ?request_ttl => RequestTTL,
        ?record => ?json
    },
    {ok, ChanState};
create_channel(
    _ConnResId,
    _ChanResId,
    #{parameters := #{record := #{type := _}}} = _ChanConfig,
    #{?transport := {?rest, _}} = _ConnState
) ->
    {error, ~"REST transport only supports JSON records"};
create_channel(ConnResId, ChanResId, ChanConfig, #{?transport := {?grpc, _}} = ConnState) ->
    #{
        parameters := #{
            catalog := Catalog,
            schema := Schema,
            table := Table,
            record := Record0,
            grpc_open_stream_retry_interval := GRPCOpenStreamRetryInterval
        },
        resource_opts := #{
            health_check_timeout := HCTimeout,
            request_ttl := RequestTTL
        }
    } = ChanConfig,
    #{
        ?transport :=
            {?grpc, #{
                ?client_pool := ClientPool,
                ?pool_size := PoolSize
            }}
    } = ConnState,
    ok = register_oauth2(ConnResId, ChanResId, ChanConfig, ConnState),
    Record =
        case Record0 of
            #{type := json} ->
                ?json;
            #{type := proto} ->
                case emqx_bridge_zerobus_utils:get_serde(Record0) of
                    {ok, Proto} ->
                        {?proto, Proto};
                    {error, schema_not_found} ->
                        throw(~"Schema not found");
                    {error, bad_type} ->
                        throw(~"Invalid schema type; must be protobuf");
                    {error, message_type_not_found} ->
                        throw(~"Message type not found in protobuf schema/bundle")
                end
        end,
    Opts = #{
        action_res_id => ChanResId,
        client_pool => ClientPool,
        record => Record,
        catalog => Catalog,
        schema => Schema,
        table => Table,
        request_ttl => RequestTTL,
        pool_size => PoolSize,
        open_stream_retry_interval => GRPCOpenStreamRetryInterval
    },
    allocate(ConnResId, ?grpc_action(ChanResId), #{
        ?action_res_id => ChanResId,
        ?pool_size => PoolSize
    }),
    clear_health(ChanResId, PoolSize),
    maybe
        {ok, #{writer_pool := WriterPool}} ?=
            emqx_bridge_zerobus_action_sup:ensure_action_started(
                ConnResId, ChanResId, Opts
            ),
        ChanState = #{
            ?catalog => Catalog,
            ?schema => Schema,
            ?table => Table,
            ?transport => {?grpc, #{?writer_pool => WriterPool}},
            ?health_check_timeout => HCTimeout,
            ?request_ttl => RequestTTL,
            ?record => Record
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
            (?oauth2(Id), ResId) when
                ChanResId == '_' orelse ChanResId == Id
            ->
                unregister_oauth2(ConnResId, ResId);
            (?grpc_action(Id) = Key, Data) when
                ChanResId == '_' orelse ChanResId == Id
            ->
                #{?action_res_id := ResId, ?pool_size := PoolSize} = Data,
                emqx_bridge_zerobus_action_sup:ensure_action_stopped(ConnResId, ResId),
                clear_health(ResId, PoolSize),
                deallocate(ConnResId, Key);
            (_, _) ->
                ok
        end,
        emqx_resource:get_allocated_resources(ConnResId)
    ).

register_oauth2(ConnResId, ChanResId, ChanConfig, ConnState) ->
    #{
        parameters := #{
            catalog := Catalog,
            schema := Schema,
            table := Table
        }
    } = ChanConfig,
    #{
        ?authn := #{
            workspace_id := WorkspaceId,
            workspace_url := WorkspaceURL,
            client_id := ClientId,
            client_secret := ClientSecret,
            timeout := AuthnTimeout,
            ssl := AuthnSSL
        }
    } = ConnState,
    Resource = fmt(
        <<"api://databricks/workspaces/${workspace_id}/zerobusDirectWriteApi">>,
        #{workspace_id => WorkspaceId}
    ),
    AuthnDetails = [
        #{
            ~"type" => ~"unity_catalog_privileges",
            ~"privileges" => [~"USE CATALOG"],
            ~"object_type" => ~"CATALOG",
            ~"object_full_path" => Catalog
        },
        #{
            ~"type" => ~"unity_catalog_privileges",
            ~"privileges" => [~"USE SCHEMA"],
            ~"object_type" => ~"SCHEMA",
            ~"object_full_path" => fmt(~"${c}.${s}", #{c => Catalog, s => Schema})
        },
        #{
            ~"type" => ~"unity_catalog_privileges",
            ~"privileges" => [~"SELECT", ~"MODIFY"],
            ~"object_type" => ~"TABLE",
            ~"object_full_path" => fmt(~"${c}.${s}.${t}", #{c => Catalog, s => Schema, t => Table})
        }
    ],
    TokenEndpoint = fmt(<<"${u}/oidc/v1/token">>, #{u => WorkspaceURL}),
    OAuthOpts = #{
        token_endpoint => TokenEndpoint,
        client_id => ClientId,
        client_secret => ClientSecret,
        scope => <<"all-apis">>,
        extra_keys => #{
            <<"resource">> => Resource,
            <<"authorization_details">> => emqx_utils_json:encode(AuthnDetails)
        },
        timeout => AuthnTimeout,
        ssl => AuthnSSL
    },
    allocate(ConnResId, ?oauth2(ChanResId), ChanResId),
    emqx_connector_oauth2:register(ChanResId, OAuthOpts).

unregister_oauth2(ConnResId, ChanResId) ->
    emqx_connector_oauth2:unregister(ChanResId),
    deallocate(ConnResId, ?oauth2(ChanResId)),
    ok.

do_insert_batch(
    Records0, ?sync, ChanState, #{?transport := {?rest, _}} = _ConnState, ConnResId, ChanResId
) ->
    #{
        ?request_ttl := RequestTTL,
        ?transport :=
            {?rest, #{
                ?max_retries := MaxRetries
            }}
    } = ChanState,
    maybe
        {ok, Token} ?= emqx_connector_oauth2:get_token(ChanResId),
        Body = emqx_utils_json:encode(Records0),
        emqx_trace:rendered_action_template(ChanResId, #{body => Body}),
        Request = mk_ehttpc_request(Body, Token, ChanState),
        {ok, Worker} ?= pick_ehttpc_worker(ConnResId, self()),
        Res = ehttpc:request(Worker, post, Request, RequestTTL, MaxRetries),
        emqx_bridge_http_connector:transform_result(Res)
    end;
do_insert_batch(
    Records0,
    {?async, ReplyFnAndArgs},
    ChanState,
    #{?transport := {?rest, _}} = _ConnState,
    ConnResId,
    ChanResId
) ->
    #{
        ?request_ttl := RequestTTL
    } = ChanState,
    maybe
        {ok, Worker} ?= pick_ehttpc_worker(ConnResId, self()),
        {ok, Token} ?= emqx_connector_oauth2:get_token(ChanResId),
        Body = emqx_utils_json:encode(Records0),
        emqx_trace:rendered_action_template(ChanResId, #{body => Body}),
        Request = mk_ehttpc_request(Body, Token, ChanState),
        Context = #{
            conn_res_id => ConnResId,
            chan_res_id => ChanResId,
            attempt => 1,
            max_attempts => 3,
            key => self(),
            method => post,
            request => Request,
            request_ttl => RequestTTL,
            trace_metadata => logger:get_process_metadata()
        },
        ok = ehttpc:request_async(
            Worker,
            post,
            Request,
            RequestTTL,
            {fun ?MODULE:rest_reply_delegator/3, [Context, ReplyFnAndArgs]}
        ),
        {ok, Worker}
    end;
do_insert_batch(
    Records0,
    {?async, ReplyFnAndArgs},
    ChanState,
    #{?transport := {?grpc, _}} = _ConnState,
    _ConnResId,
    ChanResId
) ->
    #{
        ?transport := {?grpc, #{?writer_pool := Pool}}
    } = ChanState,
    maybe
        {ok, Records} ?= serialize_records(Records0, ChanState),
        emqx_trace:rendered_action_template(ChanResId, #{records => Records}),
        emqx_bridge_zerobus_stream_writer_worker:ingest_record_batch_async(
            Pool, Records, ReplyFnAndArgs
        )
    end;
do_insert_batch(
    Records0,
    ?sync,
    ChanState,
    #{?transport := {?grpc, _}} = _ConnState,
    _ConnResId,
    ChanResId
) ->
    #{
        ?transport := {?grpc, #{?writer_pool := Pool}},
        ?request_ttl := RequestTTL
    } = ChanState,
    maybe
        {ok, Records} ?= serialize_records(Records0, ChanState),
        emqx_trace:rendered_action_template(ChanResId, #{records => Records}),
        emqx_bridge_zerobus_stream_writer_worker:ingest_record_batch_sync(
            Pool, Records, RequestTTL
        )
    end.

mk_ehttpc_request(Body, Token, ChanState) ->
    #{
        ?transport := {?rest, #{?path := Path}}
    } = ChanState,
    {
        Path,
        [
            {~"Authorization", <<"Bearer ", Token/binary>>},
            {~"Content-Type", ~"application/json"}
        ],
        Body
    }.

%% adapted from emqx_bridge_http_connector, since we can't reuse it directly.
maybe_retry(Result, _Context = #{attempt := N, max_attempts := Max}, ReplyFunAndArgs) when
    N >= Max
->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry(
    {error, {unrecoverable_error, #{status_code := _}}} = Result, _Context, ReplyFunAndArgs
) ->
    %% request was successful, but we got an error response; no need to retry
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry({error, Reason}, Context, ReplyFunAndArgs) ->
    #{
        conn_res_id := ConnResId,
        chan_res_id := ChanResId,
        attempt := Attempt,
        key := Key,
        method := Method,
        request := Request,
        request_ttl := RequestTTL
    } = Context,
    %% TODO: reset the expiration time for free retries?
    IsFreeRetry =
        case Reason of
            {recoverable_error, normal} -> true;
            {recoverable_error, {shutdown, normal}} -> true;
            _ -> false
        end,
    NContext =
        case IsFreeRetry of
            true -> Context;
            false -> Context#{attempt := Attempt + 1}
        end,
    case pick_ehttpc_worker(ConnResId, Key) of
        {ok, Worker} ->
            NRequest = reinject_token(Request, ChanResId),
            ok = ehttpc:request_async(
                Worker,
                Method,
                NRequest,
                RequestTTL,
                {fun ?MODULE:rest_reply_delegator/3, [NContext, ReplyFunAndArgs]}
            );
        {error, _} ->
            %% should be a really rare race condition...
            timer:sleep(1_000),
            NContext1 = maps:update_with(attempt, fun(N) -> N + 1 end, NContext),
            maybe_retry({error, Reason}, NContext1, ReplyFunAndArgs)
    end;
maybe_retry(Result, _Context, ReplyFunAndArgs) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

reinject_token({Path, Headers0, Body} = Request0, ChanResId) ->
    maybe
        {ok, Token} ?= emqx_connector_oauth2:get_token(ChanResId),
        Headers1 = lists:keydelete(~"Authorization", 1, Headers0),
        Headers = [{~"Authorization", <<"Bearer ", Token/binary>>} | Headers1],
        {Path, Headers, Body}
    else
        _ ->
            %% if we can't get the token right now... tough luck.  let it expire.
            Request0
    end.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

status_ordering_test() ->
    Status0 = [
        ?status_connecting,
        ?status_connected,
        {?status_connecting, ~"stream closed"},
        {?status_connected, ~"shouldn't happen, but..."},
        {?status_disconnected, noproc},
        {?status_disconnected, ~"boom"},
        {?status_disconnected, {unhealthy_target, ~"needs manual intervention"}},
        ?status_disconnected
    ],
    ?assertEqual(
        [
            {?status_disconnected, {unhealthy_target, ~"needs manual intervention"}},
            {?status_disconnected, ~"boom"},
            {?status_disconnected, noproc},
            ?status_disconnected,
            {?status_connecting, ~"stream closed"},
            ?status_connecting,
            {?status_connected, ~"shouldn't happen, but..."},
            ?status_connected
        ],
        sort_status(Status0)
    ),
    ok.
-endif.
