%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([reply_delegator/4, pull_async/1, process_pull_response/2, ensure_subscription/1]).

-type subscription_id() :: binary().
-type bridge_name() :: atom() | binary().
-type ack_id() :: binary().
-type message_id() :: binary().
-type duration() :: non_neg_integer().
-type config() :: #{
    ack_deadline := emqx_schema:timeout_duration_s(),
    ack_retry_interval := emqx_schema:timeout_duration_ms(),
    client := emqx_bridge_gcp_pubsub_client:state(),
    ecpool_worker_id => non_neg_integer(),
    forget_interval := duration(),
    hookpoints := [binary()],
    connector_resource_id := binary(),
    source_resource_id := binary(),
    mqtt_config => emqx_bridge_gcp_pubsub_impl_consumer:mqtt_config(),
    project_id := emqx_bridge_gcp_pubsub_client:project_id(),
    pull_max_messages := non_neg_integer(),
    pull_retry_interval := emqx_schema:timeout_duration_ms(),
    request_ttl := emqx_schema:duration_ms() | infinity,
    subscription_id => subscription_id(),
    topic => emqx_bridge_gcp_pubsub_client:topic()
}.
-type state() :: #{
    ack_deadline := emqx_schema:timeout_duration_s(),
    ack_retry_interval := emqx_schema:timeout_duration_ms(),
    ack_timer := undefined | reference(),
    async_workers := #{pid() => reference()},
    client := emqx_bridge_gcp_pubsub_client:state(),
    ecpool_worker_id := non_neg_integer(),
    forget_interval := duration(),
    hookpoints := [binary()],
    connector_resource_id := binary(),
    source_resource_id := binary(),
    mqtt_config := #{} | emqx_bridge_gcp_pubsub_impl_consumer:mqtt_config(),
    pending_acks := #{message_id() => ack_id()},
    project_id := emqx_bridge_gcp_pubsub_client:project_id(),
    pull_max_messages := non_neg_integer(),
    pull_retry_interval := emqx_schema:timeout_duration_ms(),
    pull_timer := undefined | reference(),
    request_ttl := emqx_schema:duration_ms() | infinity,
    %% In order to avoid re-processing the same message twice due to race conditions
    %% between acknlowledging a message and receiving a duplicate pulled message, we need
    %% to keep the seen message IDs for a while...
    seen_message_ids := sets:set(message_id()),
    subscription_id := subscription_id(),
    topic := emqx_bridge_gcp_pubsub_client:topic()
}.
-type decoded_message() :: map().

%% initialization states
-define(ensure_subscription, ensure_subscription).
-define(patch_subscription, patch_subscription).

-define(HEALTH_CHECK_TIMEOUT, 10_000).
-define(OPTVAR_SUB_OK(PID), {?MODULE, subscription_ok, PID}).

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

-spec reply_delegator(pid(), pull_async, binary(), {ok, map()} | {error, timeout | term()}) -> ok.
reply_delegator(WorkerPid, pull_async = _Action, SourceResId, Result) ->
    ?tp(gcp_pubsub_consumer_worker_reply_delegator, #{result => Result}),
    case Result of
        {error, timeout} ->
            ?MODULE:pull_async(WorkerPid);
        {error, Reason} ->
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_pull_error",
                #{
                    instance_id => SourceResId,
                    reason => Reason
                }
            ),
            case Reason of
                #{status_code := 404} ->
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

-spec health_check(pid()) -> subscription_ok | topic_not_found | timeout.
health_check(WorkerPid) ->
    case optvar:read(?OPTVAR_SUB_OK(WorkerPid), ?HEALTH_CHECK_TIMEOUT) of
        {ok, Status} ->
            Status;
        timeout ->
            timeout
    end.

%%-------------------------------------------------------------------------------------------------
%% `ecpool' API
%%-------------------------------------------------------------------------------------------------

connect(Opts0) ->
    Opts = maps:from_list(Opts0),
    #{
        ack_deadline := AckDeadlineSeconds,
        ack_retry_interval := AckRetryInterval,
        bridge_name := BridgeName,
        client := Client,
        ecpool_worker_id := WorkerId,
        forget_interval := ForgetInterval,
        hookpoints := Hookpoints,
        connector_resource_id := ConnectorResId,
        source_resource_id := SourceResId,
        project_id := ProjectId,
        pull_max_messages := PullMaxMessages,
        pull_retry_interval := PullRetryInterval,
        request_ttl := RequestTTL,
        topic_mapping := TopicMapping
    } = Opts,
    TopicMappingList = lists:keysort(1, maps:to_list(TopicMapping)),
    Index = 1 + (WorkerId rem map_size(TopicMapping)),
    {Topic, MQTTConfig} = lists:nth(Index, TopicMappingList),
    Config = #{
        ack_deadline => AckDeadlineSeconds,
        ack_retry_interval => AckRetryInterval,
        %% Note: the `client' value here must be immutable and not changed by the
        %% bridge during `on_get_status', since we have handed it over to the pull
        %% workers.
        client => Client,
        forget_interval => ForgetInterval,
        hookpoints => Hookpoints,
        connector_resource_id => ConnectorResId,
        source_resource_id => SourceResId,
        mqtt_config => MQTTConfig,
        project_id => ProjectId,
        pull_max_messages => PullMaxMessages,
        pull_retry_interval => PullRetryInterval,
        request_ttl => RequestTTL,
        topic => Topic,
        subscription_id => subscription_id(BridgeName, Topic)
    },
    ?tp(gcp_pubsub_consumer_worker_about_to_spawn, #{}),
    start_link(Config).

%%-------------------------------------------------------------------------------------------------
%% `gen_server' API
%%-------------------------------------------------------------------------------------------------

-spec init(config()) -> {ok, state(), {continue, ?ensure_subscription}}.
init(Config) ->
    process_flag(trap_exit, true),
    State = Config#{
        ack_timer => undefined,
        async_workers => #{},
        pending_acks => #{},
        pull_timer => undefined,
        seen_message_ids => sets:new([{version, 2}])
    },
    ?tp(gcp_pubsub_consumer_worker_init, #{topic => maps:get(topic, State)}),
    {ok, State, {continue, ?ensure_subscription}}.

handle_continue(?ensure_subscription, State0) ->
    case ensure_subscription_exists(State0) of
        already_exists ->
            {noreply, State0, {continue, ?patch_subscription}};
        continue ->
            #{source_resource_id := SourceResId} = State0,
            ?MODULE:pull_async(self()),
            optvar:set(?OPTVAR_SUB_OK(self()), subscription_ok),
            ?tp(
                debug,
                "gcp_pubsub_consumer_worker_subscription_ready",
                #{instance_id => SourceResId}
            ),
            {noreply, State0};
        retry ->
            {noreply, State0, {continue, ?ensure_subscription}};
        not_found ->
            %% there's nothing much to do if the topic suddenly doesn't exist anymore.
            {stop, {error, topic_not_found}, State0};
        bad_credentials ->
            {stop, {error, bad_credentials}, State0};
        permission_denied ->
            {stop, {error, permission_denied}, State0}
    end;
handle_continue(?patch_subscription, State0) ->
    ?tp(gcp_pubsub_consumer_worker_patch_subscription_enter, #{}),
    case patch_subscription(State0) of
        ok ->
            #{source_resource_id := SourceResId} = State0,
            ?MODULE:pull_async(self()),
            optvar:set(?OPTVAR_SUB_OK(self()), subscription_ok),
            ?tp(
                debug,
                "gcp_pubsub_consumer_worker_subscription_ready",
                #{instance_id => SourceResId}
            ),
            {noreply, State0};
        error ->
            %% retry; add a random delay for the case where multiple workers step on each
            %% other's toes before retrying.
            RandomMS = rand:uniform(500),
            timer:sleep(RandomMS),
            {noreply, State0, {continue, ?patch_subscription}}
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
    ?tp(gcp_pubsub_consumer_worker_pull_response_received, #{}),
    State = do_process_pull_response(State0, RespBody),
    {noreply, State};
handle_cast(ensure_subscription, State0) ->
    {noreply, State0, {continue, ?ensure_subscription}};
handle_cast(_Request, State0) ->
    {noreply, State0}.

handle_info({timeout, TRef, ack}, State0 = #{ack_timer := TRef}) ->
    State = acknowledge(State0),
    {noreply, State};
handle_info({timeout, TRef, pull}, State0 = #{pull_timer := TRef}) ->
    State1 = State0#{pull_timer := undefined},
    State = do_pull_async(State1),
    {noreply, State};
handle_info(
    {'DOWN', _Ref, process, AsyncWorkerPid, _Reason}, State0 = #{async_workers := Workers0}
) when
    is_map_key(AsyncWorkerPid, Workers0)
->
    Workers = maps:remove(AsyncWorkerPid, Workers0),
    State1 = State0#{async_workers := Workers},
    State = do_pull_async(State1),
    ?tp(gcp_pubsub_consumer_worker_handled_async_worker_down, #{async_worker_pid => AsyncWorkerPid}),
    {noreply, State};
handle_info({forget_message_ids, MsgIds}, State0) ->
    State = maps:update_with(
        seen_message_ids, fun(Seen) -> sets:subtract(Seen, MsgIds) end, State0
    ),
    ?tp(gcp_pubsub_consumer_worker_message_ids_forgotten, #{message_ids => MsgIds}),
    {noreply, State};
handle_info(Msg, State0) ->
    #{
        source_resource_id := SoureceResId,
        topic := Topic
    } = State0,
    ?SLOG(debug, #{
        msg => "gcp_pubsub_consumer_worker_unexpected_message",
        unexpected_msg => Msg,
        instance_id => SoureceResId,
        topic => Topic
    }),
    {noreply, State0}.

terminate({error, Reason}, State) when
    Reason =:= topic_not_found;
    Reason =:= bad_credentials;
    Reason =:= permission_denied
->
    #{
        source_resource_id := SourceResId,
        topic := _Topic
    } = State,
    optvar:unset(?OPTVAR_SUB_OK(self())),
    emqx_bridge_gcp_pubsub_impl_consumer:mark_as_unhealthy(SourceResId, Reason),
    ?tp(gcp_pubsub_consumer_worker_terminate, #{reason => {error, Reason}, topic => _Topic}),
    ok;
terminate(_Reason, _State) ->
    optvar:unset(?OPTVAR_SUB_OK(self())),
    ?tp(gcp_pubsub_consumer_worker_terminate, #{reason => _Reason, topic => maps:get(topic, _State)}),
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

-spec start_link(config()) -> gen_server:start_ret().
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec ensure_ack_timer(state()) -> state().
ensure_ack_timer(State = #{ack_timer := TRef, pending_acks := PendingAcks}) ->
    case {map_size(PendingAcks) =:= 0, is_reference(TRef)} of
        {false, false} ->
            #{ack_retry_interval := AckRetryInterval} = State,
            State#{ack_timer := emqx_utils:start_timer(AckRetryInterval, ack)};
        {_, _} ->
            State
    end.

-spec ensure_pull_timer(state()) -> state().
ensure_pull_timer(State = #{pull_timer := TRef}) when is_reference(TRef) ->
    State;
ensure_pull_timer(State = #{pull_retry_interval := PullRetryInterval}) ->
    State#{pull_timer := emqx_utils:start_timer(PullRetryInterval, pull)}.

-spec ensure_subscription_exists(state()) ->
    continue | retry | not_found | permission_denied | bad_credentials | already_exists.
ensure_subscription_exists(State) ->
    ?tp(gcp_pubsub_consumer_worker_create_subscription_enter, #{}),
    #{
        client := Client,
        source_resource_id := SourceResId,
        request_ttl := RequestTTL,
        subscription_id := SubscriptionId,
        topic := Topic
    } = State,
    Method = put,
    Path = path(State, create),
    Body = body(State, create),
    ReqOpts = #{request_ttl => RequestTTL},
    PreparedRequest = {prepared_request, {Method, Path, Body}, ReqOpts},
    Res = emqx_bridge_gcp_pubsub_client:query_sync(PreparedRequest, Client),
    case Res of
        {error, #{status_code := 409}} ->
            %% already exists
            ?tp(
                debug,
                "gcp_pubsub_consumer_worker_subscription_already_exists",
                #{
                    instance_id => SourceResId,
                    topic => Topic,
                    subscription_id => SubscriptionId
                }
            ),
            already_exists;
        {error, #{status_code := 404}} ->
            %% nonexistent topic
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_nonexistent_topic",
                #{
                    instance_id => SourceResId,
                    topic => Topic
                }
            ),
            not_found;
        {error, #{status_code := 403}} ->
            %% permission denied
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_permission_denied",
                #{
                    instance_id => SourceResId,
                    topic => Topic
                }
            ),
            permission_denied;
        {error, #{status_code := 401}} ->
            %% bad credentials
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_bad_credentials",
                #{
                    instance_id => SourceResId,
                    topic => Topic
                }
            ),
            bad_credentials;
        {ok, #{status_code := 200}} ->
            ?tp(
                debug,
                "gcp_pubsub_consumer_worker_subscription_created",
                #{
                    instance_id => SourceResId,
                    topic => Topic,
                    subscription_id => SubscriptionId
                }
            ),
            continue;
        {error, Reason} ->
            ?tp(
                error,
                "gcp_pubsub_consumer_worker_subscription_error",
                #{
                    instance_id => SourceResId,
                    topic => Topic,
                    reason => Reason
                }
            ),
            retry
    end.

-spec patch_subscription(state()) -> ok | error.
patch_subscription(State) ->
    #{
        client := Client,
        source_resource_id := SourceResId,
        subscription_id := SubscriptionId,
        request_ttl := RequestTTL,
        topic := Topic
    } = State,
    Method1 = patch,
    Path1 = path(State, create),
    Body1 = body(State, patch_subscription),
    ReqOpts = #{request_ttl => RequestTTL},
    PreparedRequest1 = {prepared_request, {Method1, Path1, Body1}, ReqOpts},
    Res = emqx_bridge_gcp_pubsub_client:query_sync(PreparedRequest1, Client),
    case Res of
        {ok, _} ->
            ?tp(
                debug,
                "gcp_pubsub_consumer_worker_subscription_patched",
                #{
                    instance_id => SourceResId,
                    topic => Topic,
                    subscription_id => SubscriptionId,
                    result => Res
                }
            ),
            ok;
        {error, Reason} ->
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_subscription_patch_error",
                #{
                    instance_id => SourceResId,
                    topic => Topic,
                    subscription_id => SubscriptionId,
                    reason => Reason
                }
            ),
            error
    end.

%% We use async requests so that this process will be more responsive to system messages.
-spec do_pull_async(state()) -> state().
do_pull_async(State0) ->
    ?tp_span(
        gcp_pubsub_consumer_worker_pull_async,
        #{topic => maps:get(topic, State0), subscription_id => maps:get(subscription_id, State0)},
        begin
            #{
                client := Client,
                source_resource_id := SourceResId,
                request_ttl := RequestTTL
            } = State0,
            Method = post,
            Path = path(State0, pull),
            Body = body(State0, pull),
            ReqOpts = #{request_ttl => RequestTTL},
            PreparedRequest = {prepared_request, {Method, Path, Body}, ReqOpts},
            ReplyFunAndArgs = {fun ?MODULE:reply_delegator/4, [self(), pull_async, SourceResId]},
            Res = emqx_bridge_gcp_pubsub_client:query_async(
                PreparedRequest,
                ReplyFunAndArgs,
                Client
            ),
            case Res of
                {ok, AsyncWorkerPid} ->
                    State1 = ensure_pull_timer(State0),
                    ensure_async_worker_monitored(State1, AsyncWorkerPid);
                {error, no_pool_worker_available} ->
                    ensure_pull_timer(State0)
            end
        end
    ).

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
    #{
        pending_acks := PendingAcks,
        seen_message_ids := SeenMsgIds
    } = State0,
    Messages = decode_response(RespBody),
    ?tp(gcp_pubsub_consumer_worker_decoded_messages, #{messages => Messages}),
    {NewPendingAcks, NewSeenMsgIds} =
        lists:foldl(
            fun(
                Msg = #{
                    <<"ackId">> := AckId,
                    <<"message">> := #{<<"messageId">> := MsgId}
                },
                {AccAck, AccSeen}
            ) ->
                case is_map_key(MsgId, PendingAcks) or sets:is_element(MsgId, SeenMsgIds) of
                    true ->
                        ?tp(message_redelivered, #{message => Msg}),
                        %% even though it was redelivered, pubsub might change the ack
                        %% id...  we should ack this latest value.
                        {AccAck#{MsgId => AckId}, AccSeen};
                    false ->
                        _ = handle_message(State0, Msg),
                        {AccAck#{MsgId => AckId}, sets:add_element(MsgId, AccSeen)}
                end
            end,
            {PendingAcks, SeenMsgIds},
            Messages
        ),
    State1 = State0#{pending_acks := NewPendingAcks, seen_message_ids := NewSeenMsgIds},
    State2 = acknowledge(State1),
    pull_async(self()),
    State2.

-spec acknowledge(state()) -> state().
acknowledge(State0 = #{pending_acks := PendingAcks}) ->
    case map_size(PendingAcks) =:= 0 of
        true ->
            State0;
        false ->
            do_acknowledge(State0)
    end.

do_acknowledge(State0) ->
    ?tp(gcp_pubsub_consumer_worker_acknowledge_enter, #{}),
    State1 = State0#{ack_timer := undefined},
    #{
        client := Client,
        forget_interval := ForgetInterval,
        request_ttl := RequestTTL,
        pending_acks := PendingAcks
    } = State1,
    AckIds = maps:values(PendingAcks),
    Method = post,
    Path = path(State1, ack),
    Body = body(State1, ack, #{ack_ids => AckIds}),
    ReqOpts = #{request_ttl => RequestTTL},
    PreparedRequest = {prepared_request, {Method, Path, Body}, ReqOpts},
    ?tp(gcp_pubsub_consumer_worker_will_acknowledge, #{acks => PendingAcks}),
    Res = emqx_bridge_gcp_pubsub_client:query_sync(PreparedRequest, Client),
    case Res of
        {error, Reason} ->
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_ack_error",
                #{reason => Reason}
            ),
            ensure_ack_timer(State1);
        {ok, #{status_code := 200}} ->
            ?tp(gcp_pubsub_consumer_worker_acknowledged, #{acks => PendingAcks}),
            MsgIds = maps:keys(PendingAcks),
            forget_message_ids_after(MsgIds, ForgetInterval),
            State1#{pending_acks := #{}};
        {ok, Details} ->
            ?tp(
                warning,
                "gcp_pubsub_consumer_worker_ack_error",
                #{details => Details}
            ),
            ensure_ack_timer(State1)
    end.

-spec do_get_subscription(state()) -> {ok, emqx_utils_json:json_term()} | {error, term()}.
do_get_subscription(State) ->
    #{
        client := Client,
        request_ttl := RequestTTL
    } = State,
    Method = get,
    Path = path(State, get_subscription),
    Body = body(State, get_subscription),
    ReqOpts = #{request_ttl => RequestTTL},
    PreparedRequest = {prepared_request, {Method, Path, Body}, ReqOpts},
    Res = emqx_bridge_gcp_pubsub_client:query_sync(PreparedRequest, Client),
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

-spec subscription_id(bridge_name(), emqx_bridge_gcp_pubsub_client:topic()) -> subscription_id().
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
        client := #{project_id := ProjectId},
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
        ack_deadline := AckDeadlineSeconds,
        project_id := ProjectId,
        topic := PubSubTopic
    } = State,
    TopicResource = <<"projects/", ProjectId/binary, "/topics/", PubSubTopic/binary>>,
    JSON = #{
        <<"topic">> => TopicResource,
        <<"ackDeadlineSeconds">> => AckDeadlineSeconds
    },
    emqx_utils_json:encode(JSON);
body(State, patch_subscription) ->
    #{
        ack_deadline := AckDeadlineSeconds,
        project_id := ProjectId,
        topic := PubSubTopic,
        subscription_id := SubscriptionId
    } = State,
    TopicResource = <<"projects/", ProjectId/binary, "/topics/", PubSubTopic/binary>>,
    SubscriptionResource = subscription_resource(ProjectId, SubscriptionId),
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

-spec subscription_resource(emqx_bridge_gcp_pubsub_client:project_id(), subscription_id()) ->
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

-spec handle_message(state(), decoded_message()) -> ok.
handle_message(State, #{<<"ackId">> := AckId, <<"message">> := InnerMsg} = _Message) ->
    ?tp_span(
        debug,
        "gcp_pubsub_consumer_worker_handle_message",
        #{message_id => maps:get(<<"messageId">>, InnerMsg), message => _Message, ack_id => AckId},
        begin
            #{
                source_resource_id := SourceResId,
                hookpoints := Hookpoints,
                mqtt_config := MQTTConfig,
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
            legacy_maybe_publish_mqtt_message(MQTTConfig, SourceResId, FullMessage),
            lists:foreach(
                fun(Hookpoint) -> emqx_hooks:run(Hookpoint, [FullMessage]) end,
                Hookpoints
            ),
            emqx_resource_metrics:received_inc(SourceResId),
            ok
        end
    ).

legacy_maybe_publish_mqtt_message(
    _MQTTConfig = #{
        payload_template := PayloadTemplate,
        qos := MQTTQoS,
        mqtt_topic := MQTTTopic
    },
    SourceResId,
    FullMessage
) when MQTTTopic =/= <<>> ->
    Payload = render(FullMessage, PayloadTemplate),
    MQTTMessage = emqx_message:make(SourceResId, MQTTQoS, MQTTTopic, Payload),
    _ = emqx:publish(MQTTMessage),
    ok;
legacy_maybe_publish_mqtt_message(_MQTTConfig, _SourceResId, _FullMessage) ->
    ok.

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

forget_message_ids_after(MsgIds0, Timeout) ->
    MsgIds = sets:from_list(MsgIds0, [{version, 2}]),
    _ = erlang:send_after(Timeout, self(), {forget_message_ids, MsgIds}),
    ok.

to_bin(A) when is_atom(A) -> atom_to_binary(A);
to_bin(L) when is_list(L) -> iolist_to_binary(L);
to_bin(B) when is_binary(B) -> B.
