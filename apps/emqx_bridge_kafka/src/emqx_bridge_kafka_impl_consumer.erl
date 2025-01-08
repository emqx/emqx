%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_impl_consumer).

-behaviour(emqx_resource).

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

%% `brod_group_consumer' API
-export([
    init/2,
    handle_message/2
]).

-ifdef(TEST).
-export([consumer_group_id/2]).
-endif.

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
%% needed for the #kafka_message record definition
-include_lib("brod/include/brod.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-type connector_config() :: #{
    authentication := term(),
    bootstrap_hosts := binary(),
    connector_name := atom() | binary(),
    connector_type := atom() | binary(),
    socket_opts := _,
    ssl := _,
    any() => term()
}.
-type source_config() :: #{
    bridge_name := atom(),
    hookpoints := [binary()],
    parameters := source_parameters()
}.
-type source_parameters() :: #{
    group_id => binary(),
    key_encoding_mode := encoding_mode(),
    max_batch_bytes := emqx_schema:bytesize(),
    max_wait_time := non_neg_integer(),
    max_rejoin_attempts := non_neg_integer(),
    offset_commit_interval_seconds := pos_integer(),
    offset_reset_policy := offset_reset_policy(),
    topic := kafka_topic(),
    value_encoding_mode := encoding_mode(),
    topic_mapping => [one_topic_mapping()]
}.
-type one_topic_mapping() :: #{
    kafka_topic => kafka_topic(),
    mqtt_topic => emqx_types:topic(),
    qos => emqx_types:qos(),
    payload_template => string()
}.
-type subscriber_id() :: emqx_bridge_kafka_consumer_sup:child_id().
-type kafka_topic() :: brod:topic().
-type kafka_message() :: #kafka_message{}.
-type connector_state() :: #{
    kafka_client_id := brod:client_id(),
    installed_sources := #{source_resource_id() => source_state()}
}.
-type source_state() :: #{
    subscriber_id := subscriber_id(),
    kafka_client_id := brod:client_id(),
    kafka_topics := [kafka_topic()]
}.
-type offset_reset_policy() :: latest | earliest.
-type encoding_mode() :: none | base64.
-type consumer_init_data() :: #{
    hookpoints := [binary()],
    key_encoding_mode := encoding_mode(),
    resource_id := source_resource_id(),
    topic_mapping := #{
        kafka_topic() := #{
            payload_template => emqx_placeholder:tmpl_token(),
            mqtt_topic_template => emqx_placeholder:tmpl_token(),
            qos => emqx_types:qos()
        }
    },
    value_encoding_mode := encoding_mode()
}.
-type consumer_state() :: #{
    hookpoints := [binary()],
    kafka_topic := kafka_topic(),
    key_encoding_mode := encoding_mode(),
    resource_id := source_resource_id(),
    topic_mapping := #{
        kafka_topic() := #{
            payload_template => emqx_placeholder:tmpl_token(),
            mqtt_topic_template => emqx_placeholder:tmpl_token(),
            qos => emqx_types:qos()
        }
    },
    value_encoding_mode := encoding_mode()
}.
-type subscriber_init_info() :: #{
    topic := brod:topic(),
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
resource_type() -> kafka_consumer.

callback_mode() ->
    async_if_possible.

%% consumer bridges don't need resource workers
query_mode(_Config) ->
    no_queries.

-spec on_start(connector_resource_id(), connector_config()) -> {ok, connector_state()}.
on_start(ConnectorResId, Config) ->
    #{
        authentication := Auth,
        bootstrap_hosts := BootstrapHosts0,
        connector_type := ConnectorType,
        connector_name := ConnectorName,
        socket_opts := SocketOpts0,
        ssl := SSL
    } = Config,
    BootstrapHosts = emqx_bridge_kafka_impl:hosts(BootstrapHosts0),
    %% Note: this is distinct per node.
    ClientID = make_client_id(ConnectorResId, ConnectorType, ConnectorName),
    ClientOpts0 =
        case Auth of
            none -> [];
            Auth -> [{sasl, emqx_bridge_kafka_impl:sasl(Auth)}]
        end,
    ClientOpts = add_ssl_opts(ClientOpts0, SSL),
    SocketOpts = emqx_bridge_kafka_impl:socket_opts(SocketOpts0),
    ClientOpts1 = [{extra_sock_opts, SocketOpts} | ClientOpts],
    ok = emqx_resource:allocate_resource(ConnectorResId, ?kafka_client_id, ClientID),
    case brod:start_client(BootstrapHosts, ClientID, ClientOpts1) of
        ok ->
            ?tp(
                kafka_consumer_client_started,
                #{client_id => ClientID, resource_id => ConnectorResId}
            ),
            ?SLOG(info, #{
                msg => "kafka_consumer_client_started",
                resource_id => ConnectorResId,
                kafka_hosts => BootstrapHosts
            });
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer_client",
                resource_id => ConnectorResId,
                kafka_hosts => BootstrapHosts,
                reason => emqx_utils:redact(Reason)
            }),
            throw(?CLIENT_DOWN_MESSAGE)
    end,
    {ok, #{
        kafka_client_id => ClientID,
        installed_sources => #{}
    }}.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnectorResId, _State = undefined) ->
    SubscribersStopped =
        maps:fold(
            fun
                (?kafka_client_id, ClientID, Acc) ->
                    stop_client(ClientID),
                    Acc;
                ({?kafka_subscriber_id, _SourceResId}, SubscriberId, Acc) ->
                    stop_subscriber(SubscriberId),
                    Acc + 1
            end,
            0,
            emqx_resource:get_allocated_resources(ConnectorResId)
        ),
    case SubscribersStopped > 0 of
        true ->
            ?tp(kafka_consumer_subcriber_and_client_stopped, #{}),
            ok;
        false ->
            ?tp(kafka_consumer_just_client_stopped, #{}),
            ok
    end;
on_stop(ConnectorResId, State) ->
    #{
        installed_sources := InstalledSources,
        kafka_client_id := ClientID
    } = State,
    maps:foreach(
        fun(_SourceResId, #{subscriber_id := SubscriberId}) ->
            stop_subscriber(SubscriberId)
        end,
        InstalledSources
    ),
    stop_client(ClientID),
    ?tp(kafka_consumer_subcriber_and_client_stopped, #{instance_id => ConnectorResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(_ConnectorResId, #{kafka_client_id := ClientID}) ->
    case whereis(ClientID) of
        Pid when is_pid(Pid) ->
            check_client_connectivity(Pid);
        _ ->
            ?status_disconnected
    end;
on_get_status(_ConnectorResId, _State) ->
    ?status_disconnected.

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    source_resource_id(),
    source_config()
) ->
    {ok, connector_state()}.
on_add_channel(ConnectorResId, ConnectorState0, SourceResId, SourceConfig) ->
    #{
        kafka_client_id := ClientID,
        installed_sources := InstalledSources0
    } = ConnectorState0,
    case start_consumer(SourceConfig, ConnectorResId, SourceResId, ClientID, ConnectorState0) of
        {ok, SourceState} ->
            InstalledSources = InstalledSources0#{SourceResId => SourceState},
            ConnectorState = ConnectorState0#{installed_sources := InstalledSources},
            {ok, ConnectorState};
        Error = {error, _} ->
            Error
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    source_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(ConnectorResId, ConnectorState0, SourceResId) ->
    #{installed_sources := InstalledSources0} = ConnectorState0,
    case maps:take(SourceResId, InstalledSources0) of
        {SourceState, InstalledSources} ->
            #{subscriber_id := SubscriberId} = SourceState,
            stop_subscriber(SubscriberId),
            deallocate_subscriber_id(ConnectorResId, SourceResId),
            ok;
        error ->
            InstalledSources = InstalledSources0
    end,
    ConnectorState = ConnectorState0#{installed_sources := InstalledSources},
    {ok, ConnectorState}.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), source_config()}].
on_get_channels(ConnectorResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnectorResId).

-spec on_get_channel_status(
    connector_resource_id(),
    source_resource_id(),
    connector_state()
) ->
    ?status_connected | {?status_disconnected | ?status_connecting, _Msg :: binary()}.
on_get_channel_status(
    _ConnectorResId,
    SourceResId,
    ConnectorState = #{installed_sources := InstalledSources}
) when is_map_key(SourceResId, InstalledSources) ->
    #{kafka_client_id := ClientID} = ConnectorState,
    #{
        kafka_topics := KafkaTopics,
        subscriber_id := SubscriberId
    } = maps:get(SourceResId, InstalledSources),
    do_get_status(ClientID, KafkaTopics, SubscriberId);
on_get_channel_status(_ConnectorResId, _SourceResId, _ConnectorState) ->
    ?status_disconnected.

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
        hookpoints := Hookpoints,
        kafka_topic := KafkaTopic,
        key_encoding_mode := KeyEncodingMode,
        resource_id := SourceResId,
        topic_mapping := TopicMapping,
        value_encoding_mode := ValueEncodingMode
    } = State,
    FullMessage = #{
        headers => maps:from_list(Message#kafka_message.headers),
        key => encode(Message#kafka_message.key, KeyEncodingMode),
        offset => Message#kafka_message.offset,
        topic => KafkaTopic,
        ts => Message#kafka_message.ts,
        ts_type => Message#kafka_message.ts_type,
        value => encode(Message#kafka_message.value, ValueEncodingMode)
    },
    LegacyMQTTConfig = maps:get(KafkaTopic, TopicMapping, #{}),
    legacy_maybe_publish_mqtt_message(LegacyMQTTConfig, SourceResId, FullMessage),
    lists:foreach(fun(Hookpoint) -> emqx_hooks:run(Hookpoint, [FullMessage]) end, Hookpoints),
    emqx_resource_metrics:received_inc(SourceResId),
    %% note: just `ack' does not commit the offset to the
    %% kafka consumer group.
    {ok, commit, State}.

legacy_maybe_publish_mqtt_message(
    _MQTTConfig = #{
        payload_template := PayloadTemplate,
        qos := MQTTQoS,
        mqtt_topic_template := MQTTTopicTemplate
    },
    SourceResId,
    FullMessage
) when MQTTTopicTemplate =/= <<>> ->
    Payload = render(FullMessage, PayloadTemplate),
    MQTTTopic = render(FullMessage, MQTTTopicTemplate),
    MQTTMessage = emqx_message:make(SourceResId, MQTTQoS, MQTTTopic, Payload),
    _ = emqx_broker:safe_publish(MQTTMessage),
    ok;
legacy_maybe_publish_mqtt_message(_MQTTConfig, _SourceResId, _FullMessage) ->
    ok.

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

-spec start_consumer(
    source_config(),
    connector_resource_id(),
    source_resource_id(),
    brod:client_id(),
    connector_state()
) ->
    {ok, source_state()} | {error, term()}.
start_consumer(Config, ConnectorResId, SourceResId, ClientID, ConnState) ->
    #{
        bridge_name := BridgeName,
        hookpoints := Hookpoints,
        parameters := #{
            key_encoding_mode := KeyEncodingMode,
            max_batch_bytes := MaxBatchBytes,
            max_wait_time := MaxWaitTime,
            max_rejoin_attempts := MaxRejoinAttempts,
            offset_commit_interval_seconds := OffsetCommitInterval,
            offset_reset_policy := OffsetResetPolicy0,
            topic := _Topic,
            value_encoding_mode := ValueEncodingMode
        } = Params0
    } = Config,
    ?tp(kafka_consumer_sup_started, #{}),
    TopicMapping = ensure_topic_mapping(Params0),
    InitialState = #{
        key_encoding_mode => KeyEncodingMode,
        hookpoints => Hookpoints,
        resource_id => SourceResId,
        topic_mapping => TopicMapping,
        value_encoding_mode => ValueEncodingMode
    },
    %% note: the group id should be the same for all nodes in the
    %% cluster, so that the load gets distributed between all
    %% consumers and we don't repeat messages in the same cluster.
    GroupID = consumer_group_id(Params0, BridgeName),
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
        {max_wait_time, MaxWaitTime},
        {offset_reset_policy, OffsetResetPolicy}
    ],
    GroupConfig = [
        {max_rejoin_attempts, MaxRejoinAttempts},
        {offset_commit_interval_seconds, OffsetCommitInterval}
    ],
    KafkaTopics = maps:keys(TopicMapping),
    ensure_no_repeated_topics(KafkaTopics, ConnState),
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
    ok = allocate_subscriber_id(ConnectorResId, SourceResId, SubscriberId),
    ?tp(kafka_consumer_subscriber_allocated, #{}),
    case emqx_bridge_kafka_consumer_sup:start_child(SubscriberId, GroupSubscriberConfig) of
        {ok, _ConsumerPid} ->
            ?tp(
                kafka_consumer_subscriber_started,
                #{resource_id => SourceResId, subscriber_id => SubscriberId}
            ),
            {ok, #{
                subscriber_id => SubscriberId,
                kafka_client_id => ClientID,
                kafka_topics => KafkaTopics
            }};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer",
                resource_id => SourceResId,
                reason => emqx_utils:redact(Reason)
            }),
            {error, Reason}
    end.

%% Currently, brod treats a consumer process to a specific topic as a singleton (per
%% client id / connector), meaning that the first subscriber to a given topic will define
%% the consumer options for all other consumers, and those options persist even after the
%% original consumer group is terminated.  We enforce that, if the user wants to consume
%% multiple times from the same topic, then they must create a different connector.
ensure_no_repeated_topics(KafkaTopics, ConnState) ->
    #{installed_sources := Sources} = ConnState,
    InstalledTopics = lists:flatmap(fun(#{kafka_topics := Ts}) -> Ts end, maps:values(Sources)),
    case KafkaTopics -- InstalledTopics of
        KafkaTopics ->
            %% all new topics
            ok;
        NewTopics ->
            ExistingTopics0 = KafkaTopics -- NewTopics,
            ExistingTopics = lists:join(<<", ">>, ExistingTopics0),
            Message = iolist_to_binary([
                <<"Topics ">>,
                ExistingTopics,
                <<" already exist in other sources associated with this connector.">>,
                <<" If you want to repeat topics, create new connector and source(s).">>
            ]),
            throw(Message)
    end.

%% This is to ensure backwards compatibility with the deprectated topic mapping.
-spec ensure_topic_mapping(source_parameters()) -> #{kafka_topic() := map()}.
ensure_topic_mapping(#{topic_mapping := [_ | _] = TM}) ->
    %% There is an existing topic mapping: legacy config.  We use it and ignore the single
    %% pubsub topic so that the bridge keeps working as before.
    convert_topic_mapping(TM);
ensure_topic_mapping(#{topic := KafkaTopic}) ->
    %% No topic mapping: generate one without MQTT templates.
    #{KafkaTopic => #{}}.

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
                ?status_connected ->
                    do_get_status(ClientID, RestTopics, SubscriberId);
                {Status, Message} when Status =/= ?status_connected ->
                    {Status, Message}
            end;
        {error, {client_down, Context}} ->
            case infer_client_error(Context) of
                auth_error ->
                    Message = "Authentication error. " ++ ?CLIENT_DOWN_MESSAGE,
                    {?status_disconnected, Message};
                {auth_error, Message0} ->
                    Message = binary_to_list(Message0) ++ "; " ++ ?CLIENT_DOWN_MESSAGE,
                    {?status_disconnected, Message};
                connection_refused ->
                    Message = "Connection refused. " ++ ?CLIENT_DOWN_MESSAGE,
                    {?status_disconnected, Message};
                _ ->
                    {?status_disconnected, ?CLIENT_DOWN_MESSAGE}
            end;
        {error, leader_not_available} ->
            Message =
                "Leader connection not available. Please check the Kafka topic used,"
                " the connection parameters and Kafka cluster health",
            {?status_disconnected, Message};
        _ ->
            ?status_disconnected
    end;
do_get_status(_ClientID, _KafkaTopics = [], _SubscriberId) ->
    ?status_connected.

-spec do_get_topic_status(brod:client_id(), binary(), subscriber_id(), pos_integer()) ->
    ?status_connected | {?status_disconnected | ?status_connecting, _Msg :: binary()}.
do_get_topic_status(ClientID, KafkaTopic, SubscriberId, NPartitions) ->
    Results =
        lists:map(
            fun(N) ->
                {N, brod_client:get_leader_connection(ClientID, KafkaTopic, N)}
            end,
            lists:seq(0, NPartitions - 1)
        ),
    WorkersAlive = are_subscriber_workers_alive(SubscriberId),
    case check_leader_connection_results(Results) of
        ok when WorkersAlive ->
            ?status_connected;
        {error, no_leaders} ->
            {?status_disconnected, <<"No leaders available (no partitions?)">>};
        {error, {N, Reason}} ->
            Msg = iolist_to_binary(
                io_lib:format(
                    "Leader for partition ~b unavailable; reason: ~0p",
                    [N, emqx_utils:redact(Reason)]
                )
            ),
            {?status_disconnected, Msg};
        ok when not WorkersAlive ->
            {?status_connecting, <<"Subscription workers restarting">>}
    end.

check_leader_connection_results(Results) ->
    emqx_utils:foldl_while(
        fun
            ({_N, {ok, _}}, _Acc) ->
                {cont, ok};
            ({N, {error, Reason}}, _Acc) ->
                {halt, {error, {N, Reason}}}
        end,
        {error, no_leaders},
        Results
    ).

are_subscriber_workers_alive(SubscriberId) ->
    try
        Children = supervisor:which_children(emqx_bridge_kafka_consumer_sup),
        case lists:keyfind(SubscriberId, 1, Children) of
            false ->
                false;
            {_, undefined, _, _} ->
                false;
            {_, Pid, _, _} when is_pid(Pid) ->
                Workers = brod_group_subscriber_v2:get_workers(Pid),
                %% we can't enforce the number of partitions on a single
                %% node, as the group might be spread across an emqx
                %% cluster.
                lists:all(fun is_process_alive/1, maps:values(Workers))
        end
    catch
        exit:{noproc, _} ->
            false;
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

-spec consumer_group_id(#{group_id => binary(), any() => term()}, atom() | binary()) -> binary().
consumer_group_id(#{group_id := GroupId}, _BridgeName) when
    is_binary(GroupId) andalso GroupId =/= <<"">>
->
    GroupId;
consumer_group_id(_ConsumerParams, BridgeName0) ->
    BridgeName = to_bin(BridgeName0),
    <<"emqx-kafka-consumer-", BridgeName/binary>>.

-spec check_client_connectivity(pid()) ->
    ?status_connected
    | ?status_disconnected
    | {?status_disconnected, term()}.
check_client_connectivity(ClientPid) ->
    %% We use a fake group id just to probe the connection, as `get_group_coordinator'
    %% will ensure a connection to the broker.
    FakeGroupId = <<"____emqx_consumer_probe">>,
    case brod_client:get_group_coordinator(ClientPid, FakeGroupId) of
        {error, client_down} ->
            ?status_disconnected;
        {error, {client_down, Reason}} ->
            %% `brod' should have already logged the client being down.
            {?status_disconnected, maybe_clean_error(Reason)};
        {error, Reason} ->
            %% `brod' should have already logged the client being down.
            {?status_disconnected, maybe_clean_error(Reason)};
        {ok, _Metadata} ->
            ?status_connected
    end.

%% Attempt to make the returned error a bit more friendly.
maybe_clean_error(Reason) ->
    case Reason of
        [{{Host, Port}, {nxdomain, _Stacktrace}} | _] when is_integer(Port) ->
            HostPort = iolist_to_binary([Host, ":", integer_to_binary(Port)]),
            {HostPort, nxdomain};
        [{error_code, Code}, {error_msg, Msg} | _] ->
            {Code, Msg};
        _ ->
            Reason
    end.

-spec make_client_id(connector_resource_id(), binary(), atom() | binary()) -> atom().
make_client_id(ConnectorResId, BridgeType, BridgeName) ->
    case emqx_resource:is_dry_run(ConnectorResId) of
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

allocate_subscriber_id(ConnectorResId, SourceResId, SubscriberId) ->
    ok = emqx_resource:allocate_resource(
        ConnectorResId,
        {?kafka_subscriber_id, SourceResId},
        SubscriberId
    ).

deallocate_subscriber_id(ConnectorResId, SourceResId) ->
    ok = emqx_resource:deallocate_resource(
        ConnectorResId,
        {?kafka_subscriber_id, SourceResId}
    ).
