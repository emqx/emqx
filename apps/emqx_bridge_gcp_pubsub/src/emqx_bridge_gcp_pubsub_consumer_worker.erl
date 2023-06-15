%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_worker).

-behaviour(ecpool_worker).
-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `ecpool_worker' API
-export([connect/1, health_check/1]).

%% `gen_server' API
-export([
    init/1,
    handle_info/2,
    handle_cast/2,
    handle_call/3,
    handle_continue/2,
    terminate/2
]).

-export([get_subscription/1]).
-export([reply_delegator/3, pull_async/1, process_pull_response/2, ensure_subscription/1]).

-type subscription_id() :: binary().
-type bridge_name() :: atom() | binary().
-type ack_id() :: binary().
-type config() :: #{
    ack_retry_interval := emqx_schema:timeout_duration_ms(),
    connector_state := emqx_bridge_gcp_pubsub_connector:state(),
    ecpool_worker_id => non_neg_integer(),
    hookpoint := binary(),
    instance_id := binary(),
    mqtt_config => emqx_bridge_gcp_pubsub_impl_consumer:mqtt_config(),
    pull_max_messages := non_neg_integer(),
    subscription_id => subscription_id(),
    topic => emqx_bridge_gcp_pubsub_connector:topic()
}.
-type state() :: #{
    ack_retry_interval := emqx_schema:timeout_duration_ms(),
    ack_timer := undefined | reference(),
    async_workers := #{pid() => reference()},
    connector_state := emqx_bridge_gcp_pubsub_connector:state(),
    ecpool_worker_id := non_neg_integer(),
    hookpoint := binary(),
    instance_id := binary(),
    mqtt_config => emqx_bridge_gcp_pubsub_impl_consumer:mqtt_config(),
    pending_acks => [ack_id()],
    pull_max_messages := non_neg_integer(),
    subscription_id => subscription_id(),
    topic => emqx_bridge_gcp_pubsub_connector:topic()
}.
-type decoded_message() :: map().

-define(HEALTH_CHECK_TIMEOUT, 10_000).
-define(OPTVAR_SUB_OK(PID), {?MODULE, PID}).

%%-------------------------------------------------------------------------------------------------
%% API used by `reply_delegator'
%%-------------------------------------------------------------------------------------------------

-spec pull_async(pid()) -> ok.
pull_async(WorkerPid) ->
    gen_server:cast(WorkerPid, pull_async).

-spec process_pull_response(pid(), binary()) -> ok.
process_pull_response(WorkerPid, RespBody) ->
    gen_server:cast(WorkerPid, {process_pull_response, RespBody}).

-spec ensure_subscription(pid()) -> ok.
ensure_subscription(WorkerPid) ->
    gen_server:cast(WorkerPid, ensure_subscription).

-spec reply_delegator(pid(), binary(), {ok, map()} | {error, timeout | term()}) -> ok.
reply_delegator(WorkerPid, InstanceId, Result) ->
    case Result of
        {error, timeout} ->
            ?MODULE:pull_async(WorkerPid);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "gcp_pubsub_consumer_worker_pull_error",
                instance_id => InstanceId,
                reason => Reason
            }),
            case Reason of
                #{status_code := 409} ->
                    %% the subscription was not found; deleted?!
                    ?MODULE:ensure_subscription(WorkerPid);
                _ ->
                    ?MODULE:pull_async(WorkerPid)
            end;
        {ok, #{status_code := 200, body := RespBody}} ->
            ?MODULE:process_pull_response(WorkerPid, RespBody)
    end.

%%-------------------------------------------------------------------------------------------------
%% Debugging API
%%-------------------------------------------------------------------------------------------------

-spec get_subscription(pid()) -> {ok, map()} | {error, term()}.
get_subscription(WorkerPid) ->
    gen_server:call(WorkerPid, get_subscription, 5_000).

%%-------------------------------------------------------------------------------------------------
%% `ecpool' health check
%%-------------------------------------------------------------------------------------------------

-spec health_check(pid()) -> boolean().
health_check(WorkerPid) ->
    case optvar:read(?OPTVAR_SUB_OK(WorkerPid), ?HEALTH_CHECK_TIMEOUT) of
        {ok, _} ->
            true;
        timeout ->
            false
    end.

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------

connect(Opts0) ->
    Opts = maps:from_list(Opts0),
    #{
        ack_retry_interval := AckRetryInterval,
        bridge_name := BridgeName,
        connector_state := ConnectorState,
        ecpool_worker_id := WorkerId,
        hookpoint := Hookpoint,
        instance_id := InstanceId,
        pull_max_messages := PullMaxMessages,
        topic_mapping := TopicMapping
    } = Opts,
    TopicMappingList = lists:keysort(1, maps:to_list(TopicMapping)),
    Index = 1 + (WorkerId rem map_size(TopicMapping)),
    {Topic, MQTTConfig} = lists:nth(Index, TopicMappingList),
    Config = #{
        ack_retry_interval => AckRetryInterval,
        connector_state => ConnectorState,
        hookpoint => Hookpoint,
        instance_id => InstanceId,
        mqtt_config => MQTTConfig,
        pull_max_messages => PullMaxMessages,
        topic => Topic,
        subscription_id => subscription_id(BridgeName, Topic)
    },
    start_link(Config).

%%-------------------------------------------------------------------------------------------------
%% `gen_server' API
%%-------------------------------------------------------------------------------------------------

-spec init(config()) -> {ok, state(), {continue, ensure_subscription}}.
init(Config) ->
    process_flag(trap_exit, true),
    State = Config#{
        ack_timer => undefined,
        async_workers => #{},
        pending_acks => []
    },
    {ok, State, {continue, ensure_subscription}}.

handle_continue(ensure_subscription, State0) ->
    case ensure_subscription_exists(State0) of
        ok ->
            #{instance_id := InstanceId} = State0,
            ?tp(
                debug,
                "gcp_pubsub_consumer_worker_subscription_ready",
                #{instance_id => InstanceId}
            ),
            ?MODULE:pull_async(self()),
            optvar:set(?OPTVAR_SUB_OK(self()), subscription_ok),
            {noreply, State0};
        error ->
            %% FIXME: add delay if topic does not exist?!
            %% retry
            {noreply, State0, {continue, ensure_subscription}}
    end.

handle_call(get_subscription, _From, State0) ->
    Res = do_get_subscription(State0),
    {reply, Res, State0};
handle_call(_Request, _From, State0) ->
    {reply, {error, unknown_call}, State0}.

handle_cast(pull_async, State0) ->
    State = do_pull_async(State0),
    {noreply, State};
handle_cast({process_pull_response, RespBody}, State0) ->
    State = do_process_pull_response(State0, RespBody),
    {noreply, State};
handle_cast(ensure_subscription, State0) ->
    {noreply, State0, {continue, ensure_subscription}};
handle_cast(_Request, State0) ->
    {noreply, State0}.

handle_info({timeout, TRef, ack}, State0 = #{ack_timer := TRef}) ->
    State1 = acknowledge(State0),
    State = ensure_ack_timer(State1),
    {noreply, State};
handle_info(
    {'DOWN', _Ref, process, AsyncWorkerPid, _Reason}, State0 = #{async_workers := Workers0}
) when
    is_map_key(AsyncWorkerPid, Workers0)
->
    Workers = maps:remove(AsyncWorkerPid, Workers0),
    State1 = State0#{async_workers := Workers},
    State = do_pull_async(State1),
    {noreply, State};
handle_info(Msg, State0) ->
    #{
        instance_id := InstanceId,
        topic := Topic
    } = State0,
    ?SLOG(debug, #{
        msg => "gcp_pubsub_consumer_worker_unexpected_message",
        unexpected_msg => Msg,
        instance_id => InstanceId,
        topic => Topic
    }),
    {noreply, State0}.

terminate(_Reason, _State) ->
    optvar:unset(?OPTVAR_SUB_OK(self())),
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

-spec start_link(config()) -> gen_server:start_ret().
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec ensure_ack_timer(state()) -> state().
ensure_ack_timer(State = #{pending_acks := []}) ->
    State;
ensure_ack_timer(State = #{ack_timer := TRef}) when is_reference(TRef) ->
    State;
ensure_ack_timer(State = #{ack_retry_interval := AckRetryInterval}) ->
    State#{ack_timer := emqx_utils:start_timer(AckRetryInterval, ack)}.

-spec ensure_subscription_exists(state()) -> ok | error.
ensure_subscription_exists(State) ->
    #{
        connector_state := ConnectorState,
        instance_id := InstanceId,
        subscription_id := SubscriptionId,
        topic := Topic
    } = State,
    Method = put,
    Path = path(State, create),
    Body = body(State, create),
    PreparedRequest = {prepared_request, {Method, Path, Body}},
    Res = emqx_bridge_gcp_pubsub_connector:on_query(InstanceId, PreparedRequest, ConnectorState),
    case Res of
        {error, #{status_code := 409}} ->
            %% already exists
            ?SLOG(debug, #{
                msg => "gcp_pubsub_consumer_worker_subscription_already_exists",
                instance_id => InstanceId,
                topic => Topic,
                subscription_id => SubscriptionId
            }),
            Method1 = patch,
            Path1 = path(State, create),
            Body1 = body(State, patch_subscription),
            PreparedRequest1 = {prepared_request, {Method1, Path1, Body1}},
            Res1 = emqx_bridge_gcp_pubsub_connector:on_query(
                InstanceId, PreparedRequest1, ConnectorState
            ),
            ?SLOG(debug, #{
                msg => "gcp_pubsub_consumer_worker_subscription_patch",
                instance_id => InstanceId,
                topic => Topic,
                subscription_id => SubscriptionId,
                result => Res1
            }),
            ok;
        {ok, #{status_code := 200}} ->
            ?SLOG(debug, #{
                msg => "gcp_pubsub_consumer_worker_subscription_created",
                instance_id => InstanceId,
                topic => Topic,
                subscription_id => SubscriptionId
            }),
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "gcp_pubsub_consumer_worker_subscription_error",
                instance_id => InstanceId,
                topic => Topic,
                reason => Reason
            }),
            error
    end.

%% We use async requests so that this process will be more responsive to system messages.
do_pull_async(State) ->
    #{
        connector_state := ConnectorState,
        instance_id := InstanceId
    } = State,
    Method = post,
    Path = path(State, pull),
    Body = body(State, pull),
    PreparedRequest = {prepared_request, {Method, Path, Body}},
    ReplyFunAndArgs = {fun ?MODULE:reply_delegator/3, [self(), InstanceId]},
    {ok, AsyncWorkerPid} = emqx_bridge_gcp_pubsub_connector:on_query_async(
        InstanceId,
        PreparedRequest,
        ReplyFunAndArgs,
        ConnectorState
    ),
    ensure_async_worker_monitored(State, AsyncWorkerPid).

-spec ensure_async_worker_monitored(state(), pid()) -> state().
ensure_async_worker_monitored(State = #{async_workers := Workers0}, AsyncWorkerPid) ->
    case is_map_key(AsyncWorkerPid, Workers0) of
        true ->
            State;
        false ->
            Ref = monitor(process, AsyncWorkerPid),
            Workers = Workers0#{AsyncWorkerPid => Ref},
            State#{async_workers := Workers}
    end.

-spec do_process_pull_response(state(), binary()) -> state().
do_process_pull_response(State0, RespBody) ->
    Messages = decode_response(RespBody),
    AckIds = lists:map(fun(Msg) -> handle_message(State0, Msg) end, Messages),
    State1 = maps:update_with(pending_acks, fun(AckIds0) -> AckIds0 ++ AckIds end, State0),
    State2 = acknowledge(State1),
    pull_async(self()),
    ensure_ack_timer(State2).

-spec acknowledge(state()) -> state().
acknowledge(State0 = #{pending_acks := []}) ->
    State0;
acknowledge(State0) ->
    State1 = State0#{ack_timer := undefined},
    #{
        connector_state := ConnectorState,
        instance_id := InstanceId,
        pending_acks := AckIds
    } = State1,
    Method = post,
    Path = path(State1, ack),
    Body = body(State1, ack, #{ack_ids => AckIds}),
    PreparedRequest = {prepared_request, {Method, Path, Body}},
    Res = emqx_bridge_gcp_pubsub_connector:on_query(InstanceId, PreparedRequest, ConnectorState),
    case Res of
        {error, Reason} ->
            ?SLOG(warning, #{msg => "gcp_pubsub_consumer_worker_ack_error", reason => Reason}),
            State1;
        {ok, #{status_code := 200}} ->
            ?tp(gcp_pubsub_consumer_worker_acknowledged, #{ack_ids => AckIds}),
            State1#{pending_acks := []};
        {ok, Details} ->
            ?SLOG(warning, #{msg => "gcp_pubsub_consumer_worker_ack_error", details => Details}),
            State1
    end.

do_get_subscription(State) ->
    #{
        connector_state := ConnectorState,
        instance_id := InstanceId
    } = State,
    Method = get,
    Path = path(State, get_subscription),
    Body = body(State, get_subscription),
    PreparedRequest = {prepared_request, {Method, Path, Body}},
    Res = emqx_bridge_gcp_pubsub_connector:on_query(InstanceId, PreparedRequest, ConnectorState),
    case Res of
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "gcp_pubsub_consumer_worker_get_subscription_error",
                reason => Reason
            }),
            {error, Reason};
        {ok, #{status_code := 200, body := RespBody}} ->
            DecodedBody = emqx_utils_json:decode(RespBody, [return_maps]),
            {ok, DecodedBody};
        {ok, Details} ->
            ?SLOG(warning, #{
                msg => "gcp_pubsub_consumer_worker_get_subscription_unexpected_response",
                details => Details
            }),
            {error, Details}
    end.

-spec subscription_id(bridge_name(), emqx_bridge_gcp_pubsub_connector:topic()) -> subscription_id().
subscription_id(BridgeName0, Topic) ->
    %% The real GCP PubSub accepts colons in subscription names, but its emulator
    %% doesn't...  We currently validate bridge names to not include that character.  The
    %% exception is the prefix from the probe API.
    BridgeName1 = to_bin(BridgeName0),
    BridgeName = binary:replace(BridgeName1, <<":">>, <<"-">>),
    to_bin(uri_string:quote(<<"emqx-sub-", BridgeName/binary, "-", Topic/binary>>)).

-spec path(state(), pull | create | ack | get_subscription) -> binary().
path(State, Type) ->
    #{
        connector_state := #{project_id := ProjectId},
        subscription_id := SubscriptionId
    } = State,
    SubscriptionResource = subscription_resource(ProjectId, SubscriptionId),
    case Type of
        pull ->
            <<"/v1/", SubscriptionResource/binary, ":pull">>;
        create ->
            <<"/v1/", SubscriptionResource/binary>>;
        ack ->
            <<"/v1/", SubscriptionResource/binary, ":acknowledge">>;
        get_subscription ->
            <<"/v1/", SubscriptionResource/binary>>
    end.

-spec body(state(), pull | create | patch_subscription | get_subscription) -> binary().
body(State, pull) ->
    #{pull_max_messages := PullMaxMessages} = State,
    emqx_utils_json:encode(#{<<"maxMessages">> => PullMaxMessages});
body(State, create) ->
    #{
        ack_retry_interval := AckRetryInterval,
        connector_state := #{project_id := ProjectId},
        topic := PubSubTopic
    } = State,
    TopicResource = <<"projects/", ProjectId/binary, "/topics/", PubSubTopic/binary>>,
    AckDeadlineSeconds = 5 + erlang:convert_time_unit(AckRetryInterval, millisecond, second),
    JSON = #{
        <<"topic">> => TopicResource,
        <<"ackDeadlineSeconds">> => AckDeadlineSeconds
    },
    emqx_utils_json:encode(JSON);
body(State, patch_subscription) ->
    #{
        ack_retry_interval := AckRetryInterval,
        connector_state := #{project_id := ProjectId},
        topic := PubSubTopic,
        subscription_id := SubscriptionId
    } = State,
    TopicResource = <<"projects/", ProjectId/binary, "/topics/", PubSubTopic/binary>>,
    SubscriptionResource = subscription_resource(ProjectId, SubscriptionId),
    AckDeadlineSeconds = 5 + erlang:convert_time_unit(AckRetryInterval, millisecond, second),
    JSON = #{
        <<"subscription">> =>
            #{
                <<"ackDeadlineSeconds">> => AckDeadlineSeconds,
                <<"name">> => SubscriptionResource,
                <<"topic">> => TopicResource
            },
        %% topic is immutable; don't add it here.
        <<"updateMask">> => <<"ackDeadlineSeconds">>
    },
    emqx_utils_json:encode(JSON);
body(_State, get_subscription) ->
    <<>>.

-spec body(state(), ack, map()) -> binary().
body(_State, ack, Opts) ->
    #{ack_ids := AckIds} = Opts,
    JSON = #{<<"ackIds">> => AckIds},
    emqx_utils_json:encode(JSON).

-spec subscription_resource(emqx_bridge_gcp_pubsub_connector:project_id(), subscription_id()) ->
    binary().
subscription_resource(ProjectId, SubscriptionId) ->
    <<"projects/", ProjectId/binary, "/subscriptions/", SubscriptionId/binary>>.

-spec decode_response(binary()) -> [decoded_message()].
decode_response(RespBody) ->
    case emqx_utils_json:decode(RespBody, [return_maps]) of
        #{<<"receivedMessages">> := Msgs0} ->
            lists:map(
                fun(Msg0 = #{<<"message">> := InnerMsg0}) ->
                    InnerMsg = emqx_utils_maps:update_if_present(
                        <<"data">>, fun base64:decode/1, InnerMsg0
                    ),
                    Msg0#{<<"message">> := InnerMsg}
                end,
                Msgs0
            );
        #{} ->
            []
    end.

-spec handle_message(state(), decoded_message()) -> [ack_id()].
handle_message(State, #{<<"ackId">> := AckId, <<"message">> := InnerMsg} = _Message) ->
    ?tp(
        debug,
        "gcp_pubsub_consumer_worker_handle_message",
        #{message_id => maps:get(<<"messageId">>, InnerMsg), message => _Message, ack_id => AckId}
    ),
    #{
        instance_id := InstanceId,
        hookpoint := Hookpoint,
        mqtt_config := #{
            payload_template := PayloadTemplate,
            qos := MQTTQoS,
            mqtt_topic := MQTTTopic
        },
        topic := Topic
    } = State,
    #{
        <<"messageId">> := MessageId,
        <<"publishTime">> := PublishTime
    } = InnerMsg,
    FullMessage0 = #{
        message_id => MessageId,
        publish_time => PublishTime,
        topic => Topic
    },
    FullMessage =
        lists:foldl(
            fun({FromKey, ToKey}, Acc) ->
                add_if_present(FromKey, InnerMsg, ToKey, Acc)
            end,
            FullMessage0,
            [
                {<<"data">>, value},
                {<<"attributes">>, attributes},
                {<<"orderingKey">>, ordering_key}
            ]
        ),
    Payload = render(FullMessage, PayloadTemplate),
    MQTTMessage = emqx_message:make(InstanceId, MQTTQoS, MQTTTopic, Payload),
    _ = emqx:publish(MQTTMessage),
    emqx:run_hook(Hookpoint, [FullMessage]),
    emqx_resource_metrics:received_inc(InstanceId),
    AckId.

-spec add_if_present(any(), map(), any(), map()) -> map().
add_if_present(FromKey, Message, ToKey, Map) ->
    case maps:get(FromKey, Message, undefined) of
        undefined ->
            Map;
        Value ->
            Map#{ToKey => Value}
    end.

render(FullMessage, PayloadTemplate) ->
    Opts = #{return => full_binary},
    emqx_placeholder:proc_tmpl(PayloadTemplate, FullMessage, Opts).

to_bin(A) when is_atom(A) -> atom_to_binary(A);
to_bin(L) when is_list(L) -> iolist_to_binary(L);
to_bin(B) when is_binary(B) -> B.
