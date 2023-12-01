%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_impl_consumer).

-behaviour(emqx_resource).

%% `emqx_resource' API
-export([
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_get_status/2
]).

%% `brod_group_consumer' API
-export([
    init/2,
    handle_message/2
]).

-ifdef(TEST).
-export([consumer_group_id/1]).
-endif.

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
%% needed for the #kafka_message record definition
-include_lib("brod/include/brod.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-type config() :: #{
    authentication := term(),
    bootstrap_hosts := binary(),
    bridge_name := atom(),
    kafka := #{
        max_batch_bytes := emqx_schema:bytesize(),
        max_rejoin_attempts := non_neg_integer(),
        offset_commit_interval_seconds := pos_integer(),
        offset_reset_policy := offset_reset_policy(),
        topic := binary()
    },
    topic_mapping := nonempty_list(
        #{
            kafka_topic := kafka_topic(),
            mqtt_topic := emqx_types:topic(),
            qos := emqx_types:qos(),
            payload_template := string()
        }
    ),
    ssl := _,
    any() => term()
}.
-type subscriber_id() :: emqx_bridge_kafka_consumer_sup:child_id().
-type kafka_topic() :: brod:topic().
-type kafka_message() :: #kafka_message{}.
-type state() :: #{
    kafka_topics := nonempty_list(kafka_topic()),
    subscriber_id := subscriber_id(),
    kafka_client_id := brod:client_id()
}.
-type offset_reset_policy() :: latest | earliest.
-type encoding_mode() :: none | base64.
-type consumer_init_data() :: #{
    hookpoint := binary(),
    key_encoding_mode := encoding_mode(),
    resource_id := resource_id(),
    topic_mapping := #{
        kafka_topic() := #{
            payload_template := emqx_placeholder:tmpl_token(),
            mqtt_topic_template => emqx_placeholder:tmpl_token(),
            qos => emqx_types:qos()
        }
    },
    value_encoding_mode := encoding_mode()
}.
-type consumer_state() :: #{
    hookpoint := binary(),
    kafka_topic := binary(),
    key_encoding_mode := encoding_mode(),
    resource_id := resource_id(),
    topic_mapping := #{
        kafka_topic() := #{
            payload_template := emqx_placeholder:tmpl_token(),
            mqtt_topic_template => emqx_placeholder:tmpl_token(),
            qos => emqx_types:qos()
        }
    },
    value_encoding_mode := encoding_mode()
}.
-type subscriber_init_info() :: #{
    topic => brod:topic(),
    parition => brod:partition(),
    group_id => brod:group_id(),
    commit_fun => brod_group_subscriber_v2:commit_fun()
}.

-define(CLIENT_DOWN_MESSAGE,
    "Failed to start Kafka client. Please check the logs for errors and check"
    " the connection parameters."
).

%% Allocatable resources
-define(kafka_client_id, kafka_client_id).
-define(kafka_subscriber_id, kafka_subscriber_id).

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------

callback_mode() ->
    async_if_possible.

%% consumer bridges don't need resource workers
query_mode(_Config) ->
    no_queries.

-spec on_start(resource_id(), config()) -> {ok, state()}.
on_start(ResourceId, Config) ->
    #{
        authentication := Auth,
        bootstrap_hosts := BootstrapHosts0,
        bridge_type := BridgeType,
        bridge_name := BridgeName,
        hookpoint := _,
        kafka := #{
            max_batch_bytes := _,
            max_rejoin_attempts := _,
            offset_commit_interval_seconds := _,
            offset_reset_policy := _
        },
        socket_opts := SocketOpts0,
        ssl := SSL,
        topic_mapping := _
    } = Config,
    BootstrapHosts = emqx_bridge_kafka_impl:hosts(BootstrapHosts0),
    %% Note: this is distinct per node.
    ClientID = make_client_id(ResourceId, BridgeType, BridgeName),
    ClientOpts0 =
        case Auth of
            none -> [];
            Auth -> [{sasl, emqx_bridge_kafka_impl:sasl(Auth)}]
        end,
    ClientOpts = add_ssl_opts(ClientOpts0, SSL),
    SocketOpts = emqx_bridge_kafka_impl:socket_opts(SocketOpts0),
    ClientOpts1 = [{extra_sock_opts, SocketOpts} | ClientOpts],
    ok = emqx_resource:allocate_resource(ResourceId, ?kafka_client_id, ClientID),
    case brod:start_client(BootstrapHosts, ClientID, ClientOpts1) of
        ok ->
            ?tp(
                kafka_consumer_client_started,
                #{client_id => ClientID, resource_id => ResourceId}
            ),
            ?SLOG(info, #{
                msg => "kafka_consumer_client_started",
                resource_id => ResourceId,
                kafka_hosts => BootstrapHosts
            });
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer_client",
                resource_id => ResourceId,
                kafka_hosts => BootstrapHosts,
                reason => emqx_utils:redact(Reason)
            }),
            throw(?CLIENT_DOWN_MESSAGE)
    end,
    start_consumer(Config, ResourceId, ClientID).

-spec on_stop(resource_id(), state()) -> ok.
on_stop(ResourceId, _State = undefined) ->
    case emqx_resource:get_allocated_resources(ResourceId) of
        #{?kafka_client_id := ClientID, ?kafka_subscriber_id := SubscriberId} ->
            stop_subscriber(SubscriberId),
            stop_client(ClientID),
            ?tp(kafka_consumer_subcriber_and_client_stopped, #{}),
            ok;
        #{?kafka_client_id := ClientID} ->
            stop_client(ClientID),
            ?tp(kafka_consumer_just_client_stopped, #{}),
            ok;
        _ ->
            ok
    end;
on_stop(_ResourceId, State) ->
    #{
        subscriber_id := SubscriberId,
        kafka_client_id := ClientID
    } = State,
    stop_subscriber(SubscriberId),
    stop_client(ClientID),
    ok.

-spec on_get_status(resource_id(), state()) -> connected | disconnected.
on_get_status(_ResourceID, State) ->
    #{
        subscriber_id := SubscriberId,
        kafka_client_id := ClientID,
        kafka_topics := KafkaTopics
    } = State,
    case do_get_status(ClientID, KafkaTopics, SubscriberId) of
        {disconnected, Message} ->
            {disconnected, State, Message};
        Res ->
            Res
    end.

%%-------------------------------------------------------------------------------------
%% `brod_group_subscriber' API
%%-------------------------------------------------------------------------------------

-spec init(subscriber_init_info(), consumer_init_data()) -> {ok, consumer_state()}.
init(GroupData, State0) ->
    ?tp(kafka_consumer_subscriber_init, #{group_data => GroupData, state => State0}),
    #{topic := KafkaTopic} = GroupData,
    State = State0#{kafka_topic => KafkaTopic},
    {ok, State}.

-spec handle_message(kafka_message(), consumer_state()) -> {ok, commit, consumer_state()}.
handle_message(Message, State) ->
    ?tp_span(
        kafka_consumer_handle_message,
        #{message => Message, state => State},
        do_handle_message(Message, State)
    ).

do_handle_message(Message, State) ->
    #{
        hookpoint := Hookpoint,
        kafka_topic := KafkaTopic,
        key_encoding_mode := KeyEncodingMode,
        resource_id := ResourceId,
        topic_mapping := TopicMapping,
        value_encoding_mode := ValueEncodingMode
    } = State,
    #{
        mqtt_topic_template := MQTTTopicTemplate,
        qos := MQTTQoS,
        payload_template := PayloadTemplate
    } = maps:get(KafkaTopic, TopicMapping),
    FullMessage = #{
        headers => maps:from_list(Message#kafka_message.headers),
        key => encode(Message#kafka_message.key, KeyEncodingMode),
        offset => Message#kafka_message.offset,
        topic => KafkaTopic,
        ts => Message#kafka_message.ts,
        ts_type => Message#kafka_message.ts_type,
        value => encode(Message#kafka_message.value, ValueEncodingMode)
    },
    Payload = render(FullMessage, PayloadTemplate),
    MQTTTopic = render(FullMessage, MQTTTopicTemplate),
    MQTTMessage = emqx_message:make(ResourceId, MQTTQoS, MQTTTopic, Payload),
    _ = emqx_broker:safe_publish(MQTTMessage),
    emqx_hooks:run(Hookpoint, [FullMessage]),
    emqx_resource_metrics:received_inc(ResourceId),
    %% note: just `ack' does not commit the offset to the
    %% kafka consumer group.
    {ok, commit, State}.

%%-------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------

add_ssl_opts(ClientOpts, #{enable := false}) ->
    ClientOpts;
add_ssl_opts(ClientOpts, SSL) ->
    [{ssl, emqx_tls_lib:to_client_opts(SSL)} | ClientOpts].

-spec make_subscriber_id(atom() | binary()) -> emqx_bridge_kafka_consumer_sup:child_id().
make_subscriber_id(BridgeName) ->
    BridgeNameBin = to_bin(BridgeName),
    <<"kafka_subscriber:", BridgeNameBin/binary>>.

ensure_consumer_supervisor_started() ->
    Mod = emqx_bridge_kafka_consumer_sup,
    ChildSpec =
        #{
            id => Mod,
            start => {Mod, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [Mod]
        },
    case supervisor:start_child(emqx_bridge_sup, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, already_present} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok
    end.

-spec start_consumer(config(), resource_id(), brod:client_id()) -> {ok, state()}.
start_consumer(Config, ResourceId, ClientID) ->
    #{
        bootstrap_hosts := BootstrapHosts0,
        bridge_name := BridgeName,
        hookpoint := Hookpoint,
        kafka := #{
            max_batch_bytes := MaxBatchBytes,
            max_rejoin_attempts := MaxRejoinAttempts,
            offset_commit_interval_seconds := OffsetCommitInterval,
            offset_reset_policy := OffsetResetPolicy0
        },
        key_encoding_mode := KeyEncodingMode,
        topic_mapping := TopicMapping0,
        value_encoding_mode := ValueEncodingMode
    } = Config,
    ok = ensure_consumer_supervisor_started(),
    TopicMapping = convert_topic_mapping(TopicMapping0),
    InitialState = #{
        key_encoding_mode => KeyEncodingMode,
        hookpoint => Hookpoint,
        resource_id => ResourceId,
        topic_mapping => TopicMapping,
        value_encoding_mode => ValueEncodingMode
    },
    %% note: the group id should be the same for all nodes in the
    %% cluster, so that the load gets distributed between all
    %% consumers and we don't repeat messages in the same cluster.
    GroupID = consumer_group_id(BridgeName),
    %% earliest or latest
    BeginOffset = OffsetResetPolicy0,
    OffsetResetPolicy =
        case OffsetResetPolicy0 of
            latest -> reset_to_latest;
            earliest -> reset_to_earliest
        end,
    ConsumerConfig = [
        {begin_offset, BeginOffset},
        {max_bytes, MaxBatchBytes},
        {offset_reset_policy, OffsetResetPolicy}
    ],
    GroupConfig = [
        {max_rejoin_attempts, MaxRejoinAttempts},
        {offset_commit_interval_seconds, OffsetCommitInterval}
    ],
    KafkaTopics = maps:keys(TopicMapping),
    GroupSubscriberConfig =
        #{
            client => ClientID,
            group_id => GroupID,
            topics => KafkaTopics,
            cb_module => ?MODULE,
            init_data => InitialState,
            message_type => message,
            consumer_config => ConsumerConfig,
            group_config => GroupConfig
        },
    %% Below, we spawn a single `brod_group_consumer_v2' worker, with
    %% no option for a pool of those. This is because that worker
    %% spawns one worker for each assigned topic-partition
    %% automatically, so we should not spawn duplicate workers.
    SubscriberId = make_subscriber_id(BridgeName),
    ?tp(kafka_consumer_about_to_start_subscriber, #{}),
    ok = emqx_resource:allocate_resource(ResourceId, ?kafka_subscriber_id, SubscriberId),
    ?tp(kafka_consumer_subscriber_allocated, #{}),
    case emqx_bridge_kafka_consumer_sup:start_child(SubscriberId, GroupSubscriberConfig) of
        {ok, _ConsumerPid} ->
            ?tp(
                kafka_consumer_subscriber_started,
                #{resource_id => ResourceId, subscriber_id => SubscriberId}
            ),
            {ok, #{
                subscriber_id => SubscriberId,
                kafka_client_id => ClientID,
                kafka_topics => KafkaTopics
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer",
                resource_id => ResourceId,
                kafka_hosts => emqx_bridge_kafka_impl:hosts(BootstrapHosts0),
                reason => emqx_utils:redact(Reason2)
            }),
            stop_client(ClientID),
            throw(failed_to_start_kafka_consumer)
    end.

-spec stop_subscriber(emqx_bridge_kafka_consumer_sup:child_id()) -> ok.
stop_subscriber(SubscriberId) ->
    _ = log_when_error(
        fun() ->
            try
                emqx_bridge_kafka_consumer_sup:ensure_child_deleted(SubscriberId)
            catch
                exit:{noproc, _} ->
                    %% may happen when node is shutting down
                    ok
            end
        end,
        #{
            msg => "failed_to_delete_kafka_subscriber",
            subscriber_id => SubscriberId
        }
    ),
    ok.

-spec stop_client(brod:client_id()) -> ok.
stop_client(ClientID) ->
    _ = log_when_error(
        fun() ->
            brod:stop_client(ClientID)
        end,
        #{
            msg => "failed_to_delete_kafka_consumer_client",
            client_id => ClientID
        }
    ),
    ok.

do_get_status(ClientID, [KafkaTopic | RestTopics], SubscriberId) ->
    case brod:get_partitions_count(ClientID, KafkaTopic) of
        {ok, NPartitions} ->
            case do_get_topic_status(ClientID, KafkaTopic, SubscriberId, NPartitions) of
                connected -> do_get_status(ClientID, RestTopics, SubscriberId);
                disconnected -> disconnected
            end;
        {error, {client_down, Context}} ->
            case infer_client_error(Context) of
                auth_error ->
                    Message = "Authentication error. " ++ ?CLIENT_DOWN_MESSAGE,
                    {disconnected, Message};
                {auth_error, Message0} ->
                    Message = binary_to_list(Message0) ++ "; " ++ ?CLIENT_DOWN_MESSAGE,
                    {disconnected, Message};
                connection_refused ->
                    Message = "Connection refused. " ++ ?CLIENT_DOWN_MESSAGE,
                    {disconnected, Message};
                _ ->
                    {disconnected, ?CLIENT_DOWN_MESSAGE}
            end;
        {error, leader_not_available} ->
            Message =
                "Leader connection not available. Please check the Kafka topic used,"
                " the connection parameters and Kafka cluster health",
            {disconnected, Message};
        _ ->
            disconnected
    end;
do_get_status(_ClientID, _KafkaTopics = [], _SubscriberId) ->
    connected.

-spec do_get_topic_status(brod:client_id(), binary(), subscriber_id(), pos_integer()) ->
    connected | disconnected.
do_get_topic_status(ClientID, KafkaTopic, SubscriberId, NPartitions) ->
    Results =
        lists:map(
            fun(N) ->
                brod_client:get_leader_connection(ClientID, KafkaTopic, N)
            end,
            lists:seq(0, NPartitions - 1)
        ),
    AllLeadersOk =
        length(Results) > 0 andalso
            lists:all(
                fun
                    ({ok, _}) ->
                        true;
                    (_) ->
                        false
                end,
                Results
            ),
    WorkersAlive = are_subscriber_workers_alive(SubscriberId),
    case AllLeadersOk andalso WorkersAlive of
        true ->
            connected;
        false ->
            disconnected
    end.

are_subscriber_workers_alive(SubscriberId) ->
    try
        Children = supervisor:which_children(emqx_bridge_kafka_consumer_sup),
        case lists:keyfind(SubscriberId, 1, Children) of
            false ->
                false;
            {_, Pid, _, _} ->
                Workers = brod_group_subscriber_v2:get_workers(Pid),
                %% we can't enforce the number of partitions on a single
                %% node, as the group might be spread across an emqx
                %% cluster.
                lists:all(fun is_process_alive/1, maps:values(Workers))
        end
    catch
        exit:{shutdown, _} ->
            %% may happen if node is shutting down
            false
    end.

log_when_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.

-spec consumer_group_id(atom() | binary()) -> binary().
consumer_group_id(BridgeName0) ->
    BridgeName = to_bin(BridgeName0),
    <<"emqx-kafka-consumer-", BridgeName/binary>>.

-spec is_dry_run(resource_id()) -> boolean().
is_dry_run(ResourceId) ->
    TestIdStart = string:find(ResourceId, ?TEST_ID_PREFIX),
    case TestIdStart of
        nomatch ->
            false;
        _ ->
            string:equal(TestIdStart, ResourceId)
    end.

-spec make_client_id(resource_id(), binary(), atom() | binary()) -> atom().
make_client_id(ResourceId, BridgeType, BridgeName) ->
    case is_dry_run(ResourceId) of
        false ->
            ClientID0 = emqx_bridge_kafka_impl:make_client_id(BridgeType, BridgeName),
            binary_to_atom(ClientID0);
        true ->
            %% It is a dry run and we don't want to leak too many
            %% atoms.
            probing_brod_consumers
    end.

convert_topic_mapping(TopicMappingList) ->
    lists:foldl(
        fun(Fields, Acc) ->
            #{
                kafka_topic := KafkaTopic,
                mqtt_topic := MQTTTopicTemplate0,
                qos := QoS,
                payload_template := PayloadTemplate0
            } = Fields,
            PayloadTemplate = emqx_placeholder:preproc_tmpl(PayloadTemplate0),
            MQTTTopicTemplate = emqx_placeholder:preproc_tmpl(MQTTTopicTemplate0),
            Acc#{
                KafkaTopic => #{
                    payload_template => PayloadTemplate,
                    mqtt_topic_template => MQTTTopicTemplate,
                    qos => QoS
                }
            }
        end,
        #{},
        TopicMappingList
    ).

render(FullMessage, PayloadTemplate) ->
    Opts = #{
        return => full_binary,
        var_trans => fun
            (undefined) ->
                <<>>;
            (X) ->
                emqx_utils_conv:bin(X)
        end
    },
    emqx_placeholder:proc_tmpl(PayloadTemplate, FullMessage, Opts).

encode(Value, none) ->
    Value;
encode(Value, base64) ->
    base64:encode(Value).

to_bin(B) when is_binary(B) -> B;
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).

infer_client_error(Error) ->
    case Error of
        [{_BrokerEndpoint, {econnrefused, _}} | _] ->
            connection_refused;
        [{_BrokerEndpoint, {{sasl_auth_error, Message}, _}} | _] when is_binary(Message) ->
            {auth_error, Message};
        [{_BrokerEndpoint, {{sasl_auth_error, _}, _}} | _] ->
            auth_error;
        _ ->
            undefined
    end.
