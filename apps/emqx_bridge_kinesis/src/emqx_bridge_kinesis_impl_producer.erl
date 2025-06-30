%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis_impl_producer).

-feature(maybe_expr, enable).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_resource/include/emqx_resource_runtime.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_format_query_result/1
]).

-export([
    connect/1,
    do_connector_health_check/3,
    do_channel_health_check/4
]).

%% BPAPI RPC Targets
-export([
    try_consume_connector_limiter_v1/3,
    try_consume_action_limiter_v1/4
]).

%%-------------------------------------------------------------------------------------------------
%% Type declarations
%%-------------------------------------------------------------------------------------------------

-define(BPAPI_NAME, emqx_bridge_kinesis).
-define(HEALTH_CHECK_TIMEOUT, 15000).
-define(TOPIC_MESSAGE,
    "Kinesis stream is invalid. Please check if the stream exist in Kinesis account."
).

-define(connector_hc, connector_hc).
-define(action_hc, action_hc).
-define(connector_account, connector_account).
-define(action_account, action_account).

-type config_connector() :: #{
    aws_access_key_id := binary(),
    aws_secret_access_key := emqx_secret:t(binary()),
    endpoint := binary(),
    max_retries := non_neg_integer(),
    pool_size := non_neg_integer(),
    instance_id => resource_id(),
    any() => term()
}.
-type state() :: #{
    pool_name := resource_id(),
    health_check_timeout := timeout(),
    installed_channels := map()
}.
-export_type([config_connector/0]).

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------

resource_type() -> kinesis_producer.

callback_mode() -> always_sync.

-spec on_start(resource_id(), config_connector()) -> {ok, state()} | {error, term()}.
on_start(
    InstanceId,
    #{
        pool_size := PoolSize
    } = Config0
) ->
    ?SLOG(info, #{
        msg => "starting_kinesis_bridge",
        connector => InstanceId,
        config => redact(Config0)
    }),
    Config = Config0#{instance_id => InstanceId},
    Options = [
        {config, Config},
        {pool_size, PoolSize}
    ],
    HealthCheckTimeout = emqx_utils_maps:deep_get(
        [resource_opts, health_check_timeout],
        Config0,
        60_000
    ),
    State = #{
        pool_name => InstanceId,
        health_check_timeout => HealthCheckTimeout,
        installed_channels => #{}
    },
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            ?tp(emqx_bridge_kinesis_impl_producer_start_ok, #{config => Config}),
            {ok, State};
        Error ->
            ?tp(emqx_bridge_kinesis_impl_producer_start_failed, #{config => Config}),
            Error
    end.

-spec on_stop(resource_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, _State) ->
    Res = emqx_resource_pool:stop(InstanceId),
    ensure_limiter_group_deleted(InstanceId),
    Res.

-spec on_get_status(resource_id(), state()) ->
    ?status_connected
    | ?status_disconnected
    | {?status_disconnected, state(), {unhealthy_target, string()}}.
on_get_status(ConnResId, ConnState) ->
    #{pool_name := Pool} = ConnState,
    case
        emqx_resource_pool:health_check_workers_optimistic(
            Pool,
            {?MODULE, do_connector_health_check, [ConnResId, ConnState]},
            ?HEALTH_CHECK_TIMEOUT
        )
    of
        {ok, Status} ->
            Status;
        {error, {nxdomain = Posix, _}} ->
            {error, emqx_utils:explain_posix(Posix)};
        {error, {econnrefused = Posix, _}} ->
            {error, emqx_utils:explain_posix(Posix)};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "kinesis_producer_connector_get_status_failed",
                reason => Reason
            }),
            {?status_disconnected, Reason}
    end.

do_connector_health_check(WorkerPid, ConnResId, ConnState) ->
    ?tp("kinesis_producer_connector_hc_enter", #{}),
    #{
        status := StatusBefore,
        error := ErrorBefore
    } = emqx_resource_cache:read_status(ConnResId),
    %% Guard against race conditions
    true = StatusBefore /= ?rm_status_stopped,
    maybe
        ok ?= try_consume_connector_limiter(ConnResId, ConnState),
        {ok, ?status_connected} ?=
            emqx_bridge_kinesis_connector_client:connection_status(WorkerPid),
        {halt, {ok, ?status_connected}}
    else
        timeout ->
            %% Timeout in erpc while attempting to acquire limiter tokens.
            ?tp(info, "kinesis_producer_connector_hc_timeout", #{
                hint => "repeating last status",
                connector => ConnResId,
                status => StatusBefore,
                error => ErrorBefore
            }),
            {halt, {ok, {StatusBefore, ErrorBefore}}};
        {error, timeout} ->
            %% Timeout in client `gen_server:call`.
            ?tp(info, "kinesis_producer_connector_hc_client_call_timeout", #{
                hint => "repeating last status",
                connector => ConnResId,
                status => StatusBefore,
                error => ErrorBefore
            }),
            {halt, {ok, {StatusBefore, ErrorBefore}}};
        no_cores ->
            %% No cores, so no coordinator to handle the limiter
            ?tp(info, "kinesis_producer_connector_hc_no_core", #{
                hint => "repeating last status",
                connector => ConnResId,
                status => StatusBefore,
                error => ErrorBefore
            }),
            {halt, {ok, {StatusBefore, ErrorBefore}}};
        {error, {<<"LimitExceededException">>, _} = Error} ->
            ?tp(info, "kinesis_producer_connector_hc_throttled", #{
                hint => "repeating last status",
                connector => ConnResId,
                status => StatusBefore,
                error => Error
            }),
            {halt, {ok, {StatusBefore, ErrorBefore}}};
        {error, Reason} ->
            {error, Reason}
    end.

do_channel_health_check(WorkerPid, ChanResId, ChanState, ConnResId) ->
    #{stream_name := StreamName} = ChanState,
    StatusBefore =
        case emqx_resource_cache:get_runtime(ChanResId) of
            {ok, #rt{channel_status = StatusBefore0}} ->
                StatusBefore0;
            {error, not_found} ->
                %% Starting this channel just now
                ?status_connecting
        end,
    %% Guard against race conditions
    true = StatusBefore /= no_channel,
    maybe
        ok ?= try_consume_action_limiter(ConnResId, ChanResId, ChanState),
        ok ?= do_channel_health_check1(WorkerPid, StreamName),
        {halt, {ok, ?status_connected}}
    else
        timeout ->
            %% Timeout in erpc while attempting to acquire limiter tokens.
            ?tp(info, "kinesis_producer_action_hc_timeout", #{
                hint => "repeating last status",
                action => ChanResId,
                status => StatusBefore
            }),
            {halt, {ok, StatusBefore}};
        {error, timeout} ->
            %% Timeout in client `gen_server:call`.
            ?tp(info, "kinesis_producer_action_hc_client_call_timeout", #{
                hint => "repeating last status",
                action => ChanResId,
                status => StatusBefore
            }),
            {halt, {ok, StatusBefore}};
        no_cores ->
            %% No cores, so no coordinator to handle the limiter
            ?tp(info, "kinesis_producer_action_hc_no_core", #{
                hint => "repeating last status",
                connector => ConnResId,
                status => StatusBefore
            }),
            {halt, {ok, StatusBefore}};
        {halt, Res} ->
            Res;
        {error, {<<"LimitExceededException">>, _} = Error} ->
            ?tp(info, "kinesis_producer_action_hc_throttled", #{
                hint => "repeating last status",
                action => ChanResId,
                status => StatusBefore,
                error => Error
            }),
            {halt, {ok, StatusBefore}};
        {error, Reason} ->
            {error, Reason}
    end.

do_channel_health_check1(WorkerPid, StreamName) ->
    case emqx_bridge_kinesis_connector_client:connection_status(WorkerPid, StreamName) of
        {ok, ?status_connected} ->
            ok;
        {error, unhealthy_target} ->
            {halt, {error, unhealthy_target}};
        {error, Reason} ->
            {error, Reason}
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(ChannelConfig),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

create_channel_state(
    #{parameters := Parameters} = ChannelConfig
) ->
    #{
        stream_name := StreamName,
        partition_key := PartitionKey
    } = Parameters,
    HealthCheckTimeout = emqx_utils_maps:deep_get(
        [resource_opts, health_check_timeout],
        ChannelConfig,
        60_000
    ),
    {ok, #{
        health_check_timeout => HealthCheckTimeout,
        templates => parse_template(Parameters),
        stream_name => StreamName,
        partition_key => PartitionKey
    }}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    ConnResId,
    ChanResId,
    #{
        pool_name := Pool,
        installed_channels := Channels
    } = _ConnState
) ->
    ChanState = maps:get(ChanResId, Channels),
    case
        emqx_resource_pool:health_check_workers_optimistic(
            Pool,
            {?MODULE, do_channel_health_check, [ChanResId, ChanState, ConnResId]},
            ?HEALTH_CHECK_TIMEOUT
        )
    of
        {ok, Status} ->
            %% Note: may actually be status and reason tuple.
            Status;
        {error, unhealthy_target} ->
            {?status_disconnected, {unhealthy_target, ?TOPIC_MESSAGE}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "kinesis_producer_channel_get_status_failed",
                reason => Reason
            }),
            {?status_disconnected, Reason}
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

-spec on_query(
    resource_id(),
    {channel_id(), map()},
    state()
) ->
    {ok, map()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
on_query(ResourceId, {ChannelId, Message}, State) ->
    Requests = [{ChannelId, Message}],
    ?tp(emqx_bridge_kinesis_impl_producer_sync_query, #{message => Message}),
    do_send_requests_sync(ResourceId, Requests, State, ChannelId).

-spec on_batch_query(
    resource_id(),
    [{channel_id(), map()}],
    state()
) ->
    {ok, map()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
%% we only support batch insert
on_batch_query(ResourceId, [{ChannelId, _} | _] = Requests, State) ->
    ?tp(emqx_bridge_kinesis_impl_producer_sync_batch_query, #{requests => Requests}),
    do_send_requests_sync(ResourceId, Requests, State, ChannelId).

connect(Opts) ->
    Options = proplists:get_value(config, Opts),
    emqx_bridge_kinesis_connector_client:start_link(Options).

%%-------------------------------------------------------------------------------------------------
%% BPAPI RPC Targets
%%-------------------------------------------------------------------------------------------------

-spec try_consume_connector_limiter_v1(
    connector_resource_id(), [{emqx_limiter:name(), emqx_limiter:options()}], timeout()
) ->
    ok | timeout.
try_consume_connector_limiter_v1(ConnResId, LimiterConfig, Timeout) ->
    Deadline =
        case Timeout of
            infinity -> infinity;
            _ -> max(0, now_ms() + Timeout - 100)
        end,
    do_ensure_limiter_group_created(ConnResId, LimiterConfig),
    Limiter = connect_limiter(?connector_hc, ConnResId),
    LogMsg = "kinesis_connector_hc_rate_limited",
    LogMeta = #{conn_res_id => ConnResId},
    do_try_consume_limiter_v1(Limiter, Deadline, ConnResId, LogMsg, LogMeta).

-spec try_consume_action_limiter_v1(
    connector_resource_id(),
    action_resource_id(),
    [{emqx_limiter:name(), emqx_limiter:options()}],
    timeout()
) ->
    ok | timeout.
try_consume_action_limiter_v1(ConnResId, ChanResId, LimiterConfig, Timeout) ->
    Deadline =
        case Timeout of
            infinity -> infinity;
            _ -> max(0, now_ms() + Timeout - 100)
        end,
    do_ensure_limiter_group_created(ConnResId, LimiterConfig),
    Limiter = connect_limiter(?action_hc, ConnResId),
    LogMsg = "kinesis_action_hc_rate_limited",
    LogMeta = #{action_res_id => ChanResId},
    do_try_consume_limiter_v1(Limiter, Deadline, ConnResId, LogMsg, LogMeta).

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

-spec do_send_requests_sync(
    resource_id(),
    [{channel_id(), map()}],
    state(),
    channel_id()
) ->
    {ok, jsx:json_term() | binary()}
    | {error, {recoverable_error, term()}}
    | {error, {unrecoverable_error, {invalid_request, term()}}}
    | {error, {unrecoverable_error, {unhealthy_target, string()}}}
    | {error, {unrecoverable_error, term()}}
    | {error, term()}.
do_send_requests_sync(
    InstanceId,
    Requests,
    #{
        pool_name := PoolName,
        installed_channels := InstalledChannels
    } = _State,
    ChannelId
) ->
    #{
        templates := Templates,
        stream_name := StreamName
    } = maps:get(ChannelId, InstalledChannels),
    Records = render_records(Requests, Templates),
    StructuredRecords = [
        #{data => Data, partition_key => PartitionKey}
     || {Data, PartitionKey} <- Records
    ],
    emqx_trace:rendered_action_template(ChannelId, StructuredRecords),
    Result = ecpool:pick_and_do(
        PoolName,
        {emqx_bridge_kinesis_connector_client, query, [Records, StreamName]},
        no_handover
    ),
    handle_result(Result, Requests, InstanceId).

handle_result({ok, _} = Result, _Requests, _InstanceId) ->
    Result;
handle_result({error, {<<"ResourceNotFoundException">>, _} = Reason}, Requests, InstanceId) ->
    ?SLOG(error, #{
        msg => "kinesis_error_response",
        request => Requests,
        connector => InstanceId,
        reason => Reason
    }),
    {error, {unrecoverable_error, {unhealthy_target, ?TOPIC_MESSAGE}}};
handle_result(
    {error, {<<"ProvisionedThroughputExceededException">>, _} = Reason}, Requests, InstanceId
) ->
    ?SLOG(error, #{
        msg => "kinesis_error_response",
        request => Requests,
        connector => InstanceId,
        reason => Reason
    }),
    {error, {recoverable_error, Reason}};
handle_result({error, {<<"InvalidArgumentException">>, _} = Reason}, Requests, InstanceId) ->
    ?SLOG(error, #{
        msg => "kinesis_error_response",
        request => Requests,
        connector => InstanceId,
        reason => Reason
    }),
    {error, {unrecoverable_error, Reason}};
handle_result({error, {econnrefused = Reason, _}}, Requests, InstanceId) ->
    ?SLOG(error, #{
        msg => "kinesis_error_response",
        request => Requests,
        connector => InstanceId,
        reason => Reason
    }),
    {error, {recoverable_error, Reason}};
handle_result({error, Reason} = Error, Requests, InstanceId) ->
    ?SLOG(error, #{
        msg => "kinesis_error_response",
        request => Requests,
        connector => InstanceId,
        reason => Reason
    }),
    Error.

on_format_query_result({ok, Result}) ->
    #{result => ok, info => Result};
on_format_query_result(Result) ->
    Result.

parse_template(Config) ->
    #{payload_template := PayloadTemplate, partition_key := PartitionKeyTemplate} = Config,
    Templates = #{send_message => PayloadTemplate, partition_key => PartitionKeyTemplate},
    maps:map(fun(_K, V) -> emqx_placeholder:preproc_tmpl(V) end, Templates).

render_records(Items, Templates) ->
    PartitionKeyTemplate = maps:get(partition_key, Templates),
    MsgTemplate = maps:get(send_message, Templates),
    render_messages(Items, {MsgTemplate, PartitionKeyTemplate}, []).

render_messages([], _Templates, RenderedMsgs) ->
    RenderedMsgs;
render_messages(
    [{_, Msg} | Others],
    {MsgTemplate, PartitionKeyTemplate} = Templates,
    RenderedMsgs
) ->
    Data = emqx_placeholder:proc_tmpl(MsgTemplate, Msg),
    PartitionKey = emqx_placeholder:proc_tmpl(PartitionKeyTemplate, Msg),
    RenderedMsg = {Data, PartitionKey},
    render_messages(Others, Templates, [RenderedMsg | RenderedMsgs]).

redact(Config) ->
    emqx_utils:redact(Config, fun(Any) -> Any =:= aws_secret_access_key end).

limiter_group(ConnResId) ->
    {connector, kinesis_producer, ConnResId}.

limiter_group_config() ->
    Types = [?action_hc, ?connector_hc],
    LimiterConfigs0 = lists:flatmap(fun limiter_config/1, Types),
    lists:sort(LimiterConfigs0).

do_ensure_limiter_group_created(ConnResId, LimiterConfigs) ->
    case find_limiter_group(ConnResId) of
        {ok, LimiterConfigs} ->
            %% No changes in config
            ok;
        {ok, _OtherConfigs} ->
            %% Needs update
            emqx_limiter:update_group(limiter_group(ConnResId), LimiterConfigs);
        undefined ->
            emqx_limiter:create_group(shared, limiter_group(ConnResId), LimiterConfigs)
    end.

try_consume_connector_limiter(ConnResId, ConnState) ->
    LimiterConfig = limiter_group_config(),
    CompatibleNodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI_NAME, 1),
    maybe
        {ok, Coordinator} ?= get_coordinator(CompatibleNodes),
        HealthCheckTimeout = maps:get(health_check_timeout, ConnState, 60_000),
        try
            emqx_bridge_kinesis_proto_v1:try_consume_connector_limiter(
                Coordinator, ConnResId, LimiterConfig, HealthCheckTimeout, HealthCheckTimeout
            )
        catch
            error:{erpc, _} ->
                %% Treating erpc errors as a timeout
                timeout
        end
    end.

get_coordinator(CompatibleNodes) ->
    try
        {ok, mria_membership:coordinator(CompatibleNodes)}
    catch
        error:badarg ->
            %% Empty list of up cores
            no_cores
    end.

try_consume_action_limiter(ConnResId, ChanResId, ChanState) ->
    LimiterConfig = limiter_group_config(),
    CompatibleNodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI_NAME, 1),
    maybe
        {ok, Coordinator} ?= get_coordinator(CompatibleNodes),
        HealthCheckTimeout = maps:get(health_check_timeout, ChanState, 60_000),
        try
            emqx_bridge_kinesis_proto_v1:try_consume_action_limiter(
                Coordinator,
                ConnResId,
                ChanResId,
                LimiterConfig,
                HealthCheckTimeout,
                HealthCheckTimeout
            )
        catch
            error:{erpc, _} ->
                %% Treating erpc errors as a timeout
                timeout
        end
    end.

connect_limiter(Kind, ConnResId) ->
    Clients =
        lists:map(
            fun(Name) ->
                emqx_limiter:connect({limiter_group(ConnResId), Name})
            end,
            limiter_names(Kind)
        ),
    emqx_limiter_composite:new(Clients).

limiter_names(?connector_hc) ->
    [?connector_account];
limiter_names(?action_hc) ->
    [?action_account].

find_limiter_group(ConnResId) ->
    maybe
        {_, Options} ?= emqx_limiter_registry:find_group(limiter_group(ConnResId)),
        {ok, lists:sort(Options)}
    end.

%% https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
limiter_config(?connector_hc) ->
    %% AWS quota: 5 TPS per account
    Capacity = 5,
    [{?connector_account, emqx_limiter:config_from_rate({Capacity, 1_000})}];
limiter_config(?action_hc) ->
    %% AWS quota: 10 TPS per account
    Capacity = 10,
    [{?action_account, emqx_limiter:config_from_rate({Capacity, 1_000})}].

ensure_limiter_group_deleted(ConnResId) ->
    try
        emqx_limiter:delete_group(limiter_group(ConnResId))
    catch
        error:{limiter_group_not_found, _} ->
            ok
    end.

now_ms() ->
    erlang:system_time(millisecond).

do_try_consume_limiter_v1(Limiter0, Deadline, ConnResId, LogMsg, LogMeta) ->
    maybe
        {false, Limiter1, _Reason} ?=
            emqx_limiter_client:try_consume(Limiter0, 1),
        ?tp(debug, LogMsg, LogMeta),
        true ?= now_ms() < Deadline orelse timeout,
        timer:sleep(100),
        do_try_consume_limiter_v1(Limiter1, Deadline, ConnResId, LogMsg, LogMeta)
    else
        timeout ->
            timeout;
        {true, _Limiter} ->
            ok
    end.
