%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_impl_kafka_consumer).

-behaviour(emqx_resource).

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2
]).

%% `brod_group_consumer' API
-export([
    init/2,
    handle_message/2
]).

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
        offset_reset_policy := offset_reset_policy(),
        topic := binary()
    },
    mqtt := #{
        topic := emqx_types:topic(),
        qos := emqx_types:qos(),
        payload := mqtt_payload()
    },
    pool_size := pos_integer(),
    ssl := _,
    any() => term()
}.
-type kafka_client_id() :: atom().
-type state() :: #{
    children_ids := #{emqx_ee_bridge_kafka_consumer_sup:child_id() => pid()},
    kafka_client_id := kafka_client_id()
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
    kafka_topic => binary()
}.

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------

callback_mode() ->
    async_if_possible.

-spec on_start(manager_id(), config()) -> {ok, state()}.
on_start(InstanceId, Config) ->
    ensure_consumer_supervisor_started(),
    #{
        authentication := Auth,
        bootstrap_hosts := BootstrapHosts0,
        bridge_name := BridgeName,
        kafka := #{
            max_batch_bytes := MaxBatchBytes,
            offset_reset_policy := OffsetResetPolicy,
            topic := KafkaTopic
        },
        mqtt := #{topic := MQTTTopic, qos := MQTTQoS, payload := MQTTPayload},
        pool_size := PoolSize,
        ssl := SSL
    } = Config,
    BootstrapHosts = emqx_bridge_impl_kafka:hosts(BootstrapHosts0),
    GroupConfig = [],
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
        kafka_topic => KafkaTopic
    },
    ClientID0 = emqx_bridge_impl_kafka:make_client_id(BridgeName),
    ClientID = binary_to_atom(ClientID0),
    ClientOpts0 =
        case Auth of
            none -> [];
            Auth -> [{sasl, emqx_bridge_impl_kafka:sasl(Auth)}]
        end,
    ClientOpts = add_ssl_opts(ClientOpts0, SSL),
    case brod:start_client(BootstrapHosts, ClientID, ClientOpts) of
        ok ->
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
    GroupID = ClientID0,
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
    case start_pool(InstanceId, PoolSize, GroupSubscriberConfig) of
        {ok, ChildrenIds} ->
            ?tp(
                kafka_consumer_start_pool_started,
                #{
                    pool => ChildrenIds,
                    instance_id => InstanceId
                }
            ),
            {ok, #{
                children_ids => ChildrenIds,
                kafka_client_id => ClientID
            }};
        {error, #{error := Reason2, started := Started}} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_consumer",
                instance_id => InstanceId,
                kafka_hosts => BootstrapHosts,
                kafka_topic => KafkaTopic,
                reason => Reason2
            }),
            stop_consumer_pool(Started),
            stop_client(ClientID),
            throw(failed_to_start_kafka_consumer)
    end.

-spec on_stop(manager_id(), state()) -> ok.
on_stop(_InstanceID, State) ->
    #{
        children_ids := ChildrenIds,
        kafka_client_id := ClientID
    } = State,
    stop_consumer_pool(ChildrenIds),
    stop_client(ClientID),
    ok.

-spec on_get_status(manager_id(), state()) -> connected.
on_get_status(_InstanceID, _State) ->
    connected.

%%-------------------------------------------------------------------------------------
%% `brod_group_subscriber' API
%%-------------------------------------------------------------------------------------

-spec init(_, consumer_state()) -> {ok, consumer_state()}.
init(_GroupID, State) ->
    {ok, State}.

-spec handle_message(#kafka_message{}, consumer_state()) -> {ok, ack, consumer_state()}.
handle_message(Message, State) ->
    ?tp(
        kafka_consumer_handle_message_enter,
        #{message => Message, state => State}
    ),
    #{
        resource_id := ResourceId,
        kafka_topic := KafkaTopic,
        mqtt := #{
            topic := MQTTTopic,
            payload := MQTTPayload,
            qos := MQTTQoS
        }
    } = State,
    Payload =
        case MQTTPayload of
            full_message ->
                #{
                    offset => Message#kafka_message.offset,
                    key => Message#kafka_message.key,
                    value => Message#kafka_message.value,
                    ts => Message#kafka_message.ts,
                    ts_type => Message#kafka_message.ts_type,
                    headers => maps:from_list(Message#kafka_message.headers),
                    topic => KafkaTopic
                };
            message_value ->
                Message#kafka_message.value
        end,
    EncodedPayload = emqx_json:encode(Payload),
    MQTTMessage = emqx_message:make(ResourceId, MQTTQoS, MQTTTopic, EncodedPayload),
    _ = emqx:publish(MQTTMessage),
    emqx_resource:inc_received(ResourceId),
    {ok, ack, State}.

%%-------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------

add_ssl_opts(ClientOpts, #{enable := false}) ->
    ClientOpts;
add_ssl_opts(ClientOpts, SSL) ->
    [{ssl, emqx_tls_lib:to_client_opts(SSL)} | ClientOpts].

make_consumer_worker_id(InstanceId, N) ->
    <<"kafka_consumer:", InstanceId/binary, ":", (integer_to_binary(N))/binary>>.

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

start_pool(InstanceId, PoolSize, GroupSubscriberConfig) ->
    do_start_pool(PoolSize, InstanceId, GroupSubscriberConfig, #{}).

do_start_pool(N, _InstanceId, _GroupSubscriberConfig, Acc) when N =< 0 ->
    {ok, Acc};
do_start_pool(N, InstanceId, GroupSubscriberConfig, Acc) ->
    ChildId = make_consumer_worker_id(InstanceId, N),
    case emqx_ee_bridge_kafka_consumer_sup:start_child(ChildId, GroupSubscriberConfig) of
        {ok, Pid} ->
            do_start_pool(N - 1, InstanceId, GroupSubscriberConfig, Acc#{ChildId => Pid});
        {error, Reason} ->
            {error, #{error => Reason, started => Acc}}
    end.

stop_consumer_pool(Children) ->
    lists:foreach(
        fun(Id) ->
            log_when_error(
                fun() ->
                    emqx_ee_bridge_kafka_consumer_sup:ensure_child_deleted(Id)
                end,
                #{
                    msg => "failed_to_delete_kafka_consumer",
                    consumer_id => Id
                }
            )
        end,
        maps:keys(Children)
    ).

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
