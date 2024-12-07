%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis_impl_producer).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HEALTH_CHECK_TIMEOUT, 15000).
-define(TOPIC_MESSAGE,
    "Kinesis stream is invalid. Please check if the stream exist in Kinesis account."
).

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
    installed_channels := map()
}.
-export_type([config_connector/0]).

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
    connect/1
]).

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
    State = #{
        pool_name => InstanceId,
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
    emqx_resource_pool:stop(InstanceId).

-spec on_get_status(resource_id(), state()) ->
    ?status_connected
    | ?status_disconnected
    | {?status_disconnected, state(), {unhealthy_target, string()}}.
on_get_status(_InstanceId, #{pool_name := _Pool} = State) ->
    do_get_status(State, []).

-spec do_get_status(state(), nil() | [_Stream]) -> _.
do_get_status(#{pool_name := Pool}, StreamArgs) ->
    case
        emqx_resource_pool:health_check_workers(
            Pool,
            {emqx_bridge_kinesis_connector_client, connection_status, StreamArgs},
            ?HEALTH_CHECK_TIMEOUT,
            #{return_values => true}
        )
    of
        {ok, Values} ->
            AllOk = lists:all(fun(S) -> S =:= {ok, ?status_connected} end, Values),
            case AllOk of
                true ->
                    ?status_connected;
                false ->
                    Unhealthy = lists:any(fun(S) -> S =:= {error, unhealthy_target} end, Values),
                    case Unhealthy of
                        true -> {?status_disconnected, {unhealthy_target, ?TOPIC_MESSAGE}};
                        false -> ?status_disconnected
                    end
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "kinesis_producer_get_status_failed",
                reason => Reason
            }),
            ?status_disconnected
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
    #{parameters := Parameters} = _ChannelConfig
) ->
    #{
        stream_name := StreamName,
        partition_key := PartitionKey
    } = Parameters,
    {ok, #{
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
    _ResId,
    ChannelId,
    #{
        pool_name := _PoolName,
        installed_channels := Channels
    } = State
) ->
    #{stream_name := StreamName} = maps:get(ChannelId, Channels),
    do_get_status(State, [StreamName]).

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
