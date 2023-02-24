%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_impl_kafka_consumer).

-behaviour(emqx_resource).

%% `emqx_resource' API
-export([
    callback_mode/0,
    is_buffer_supported/0,
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
        offset_reset_policy := offset_reset_policy(),
        topic := binary()
    },
    mqtt := #{
        topic := emqx_types:topic(),
        qos := emqx_types:qos(),
        payload := mqtt_payload()
    },
    ssl := _,
    any() => term()
}.
-type subscriber_id() :: emqx_ee_bridge_kafka_consumer_sup:child_id().
-type state() :: #{
    kafka_topic := binary(),
    subscriber_id := subscriber_id(),
    kafka_client_id := brod:client_id()
}.
-type offset_reset_policy() :: reset_to_latest | reset_to_earliest | reset_by_subscriber.
-type mqtt_payload() :: full_message | message_value.
-type consumer_state() :: #{
    resource_id := resource_id(),
    mqtt := #{
        payload := mqtt_payload(),
        topic => emqx_types:topic(),
        qos => emqx_types:qos()
    },
    hookpoint := binary(),
    kafka_topic := binary()
}.

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------

callback_mode() ->
    async_if_possible.

%% there are no queries to be made to this bridge, so we say that
%% buffer is supported so we don't spawn unused resource buffer
%% workers.
is_buffer_supported() ->
    true.

-spec on_start(manager_id(), config()) -> {ok, state()}.
on_start(InstanceId, Config) ->
    ensure_consumer_supervisor_started(),
    #{
        authentication := Auth,
        bootstrap_hosts := BootstrapHosts0,
        bridge_name := BridgeName,
        hookpoint := Hookpoint,
        kafka := #{
            max_batch_bytes := MaxBatchBytes,
            max_rejoin_attempts := MaxRejoinAttempts,
            offset_reset_policy := OffsetResetPolicy,
            topic := KafkaTopic
        },
        mqtt := #{topic := MQTTTopic, qos := MQTTQoS, payload := MQTTPayload},
        ssl := SSL
    } = Config,
    BootstrapHosts = emqx_bridge_impl_kafka:hosts(BootstrapHosts0),
    GroupConfig = [{max_rejoin_attempts, MaxRejoinAttempts}],
    ConsumerConfig = [
        {max_bytes, MaxBatchBytes},
        {offset_reset_policy, OffsetResetPolicy}
    ],
    InitialState = #{
        resource_id => emqx_bridge_resource:resource_id(kafka_consumer, BridgeName),
        mqtt => #{
            payload => MQTTPayload,
            topic => MQTTTopic,
            qos => MQTTQoS
        },
        hookpoint => Hookpoint,
        kafka_topic => KafkaTopic
    },
    KafkaType = kafka_consumer,
    %% Note: this is distinct per node.
    ClientID0 = emqx_bridge_impl_kafka:make_client_id(KafkaType, BridgeName),
    ClientID = binary_to_atom(ClientID0),
    ClientOpts0 =
        case Auth of
            none -> [];
            Auth -> [{sasl, emqx_bridge_impl_kafka:sasl(Auth)}]
        end,
    ClientOpts = add_ssl_opts(ClientOpts0, SSL),
    case brod:start_client(BootstrapHosts, ClientID, ClientOpts) of
        ok ->
            ?tp(
                kafka_consumer_client_started,
                #{client_id => ClientID, instance_id => InstanceId}
            ),
            ?SLOG(info, #{
                msg => "kafka_consumer_client_started",
                instance_id => InstanceId,
                kafka_hosts => BootstrapHosts
            });
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer_client",
                instance_id => InstanceId,
                kafka_hosts => BootstrapHosts,
                reason => Reason
            }),
            throw(failed_to_start_kafka_client)
    end,
    %% note: the group id should be the same for all nodes in the
    %% cluster, so that the load gets distributed between all
    %% consumers and we don't repeat messages in the same cluster.
    GroupID = consumer_group_id(BridgeName),
    GroupSubscriberConfig =
        #{
            client => ClientID,
            group_id => GroupID,
            topics => [KafkaTopic],
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
    case emqx_ee_bridge_kafka_consumer_sup:start_child(SubscriberId, GroupSubscriberConfig) of
        {ok, _ConsumerPid} ->
            ?tp(
                kafka_consumer_subscriber_started,
                #{instance_id => InstanceId, subscriber_id => SubscriberId}
            ),
            {ok, #{
                subscriber_id => SubscriberId,
                kafka_client_id => ClientID,
                kafka_topic => KafkaTopic
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer",
                instance_id => InstanceId,
                kafka_hosts => BootstrapHosts,
                kafka_topic => KafkaTopic,
                reason => Reason2
            }),
            stop_client(ClientID),
            throw(failed_to_start_kafka_consumer)
    end.

-spec on_stop(manager_id(), state()) -> ok.
on_stop(_InstanceID, State) ->
    #{
        subscriber_id := SubscriberId,
        kafka_client_id := ClientID
    } = State,
    stop_subscriber(SubscriberId),
    stop_client(ClientID),
    ok.

-spec on_get_status(manager_id(), state()) -> connected | disconnected.
on_get_status(_InstanceID, State) ->
    #{
        subscriber_id := SubscriberId,
        kafka_client_id := ClientID,
        kafka_topic := KafkaTopic
    } = State,
    case brod:get_partitions_count(ClientID, KafkaTopic) of
        {ok, NPartitions} ->
            do_get_status(ClientID, KafkaTopic, SubscriberId, NPartitions);
        _ ->
            disconnected
    end.

%%-------------------------------------------------------------------------------------
%% `brod_group_subscriber' API
%%-------------------------------------------------------------------------------------

-spec init(_, consumer_state()) -> {ok, consumer_state()}.
init(_GroupData, State) ->
    ?tp(kafka_consumer_subscriber_init, #{group_data => _GroupData, state => State}),
    {ok, State}.

-spec handle_message(#kafka_message{}, consumer_state()) -> {ok, commit, consumer_state()}.
handle_message(Message, State) ->
    ?tp_span(
        kafka_consumer_handle_message,
        #{message => Message, state => State},
        begin
            #{
                resource_id := ResourceId,
                hookpoint := Hookpoint,
                kafka_topic := KafkaTopic,
                mqtt := #{
                    topic := MQTTTopic,
                    payload := MQTTPayload,
                    qos := MQTTQoS
                }
            } = State,
            FullMessage = #{
                offset => Message#kafka_message.offset,
                key => Message#kafka_message.key,
                value => Message#kafka_message.value,
                ts => Message#kafka_message.ts,
                ts_type => Message#kafka_message.ts_type,
                headers => maps:from_list(Message#kafka_message.headers),
                topic => KafkaTopic
            },
            Payload =
                case MQTTPayload of
                    full_message ->
                        FullMessage;
                    message_value ->
                        Message#kafka_message.value
                end,
            EncodedPayload = emqx_json:encode(Payload),
            MQTTMessage = emqx_message:make(ResourceId, MQTTQoS, MQTTTopic, EncodedPayload),
            _ = emqx:publish(MQTTMessage),
            emqx:run_hook(Hookpoint, [FullMessage]),
            emqx_resource_metrics:received_inc(ResourceId),
            %% note: just `ack' does not commit the offset to the
            %% kafka consumer group.
            {ok, commit, State}
        end
    ).

%%-------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------

add_ssl_opts(ClientOpts, #{enable := false}) ->
    ClientOpts;
add_ssl_opts(ClientOpts, SSL) ->
    [{ssl, emqx_tls_lib:to_client_opts(SSL)} | ClientOpts].

-spec make_subscriber_id(atom()) -> emqx_ee_bridge_kafka_consumer_sup:child_id().
make_subscriber_id(BridgeName) ->
    BridgeNameBin = atom_to_binary(BridgeName),
    <<"kafka_subscriber:", BridgeNameBin/binary>>.

ensure_consumer_supervisor_started() ->
    Mod = emqx_ee_bridge_kafka_consumer_sup,
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

-spec stop_subscriber(emqx_ee_bridge_kafka_consumer_sup:child_id()) -> ok.
stop_subscriber(SubscriberId) ->
    _ = log_when_error(
        fun() ->
            emqx_ee_bridge_kafka_consumer_sup:ensure_child_deleted(SubscriberId)
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

-spec do_get_status(brod:client_id(), binary(), subscriber_id(), pos_integer()) ->
    connected | disconnected.
do_get_status(ClientID, KafkaTopic, SubscriberId, NPartitions) ->
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
    Children = supervisor:which_children(emqx_ee_bridge_kafka_consumer_sup),
    case lists:keyfind(SubscriberId, 1, Children) of
        false ->
            false;
        {_, Pid, _, _} ->
            Workers = brod_group_subscriber_v2:get_workers(Pid),
            %% we can't enforce the number of partitions on a single
            %% node, as the group might be spread across an emqx
            %% cluster.
            lists:all(fun is_process_alive/1, maps:values(Workers))
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

-spec consumer_group_id(atom()) -> binary().
consumer_group_id(BridgeName0) ->
    BridgeName = atom_to_binary(BridgeName0),
    <<"emqx-kafka-consumer:", BridgeName/binary>>.
