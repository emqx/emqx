%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_impl_consumer).

-behaviour(emqx_resource).

%% `emqx_resource' API
-export([
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_get_status/2
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-type mqtt_config() :: #{
    mqtt_topic := emqx_types:topic(),
    qos := emqx_types:qos(),
    payload_template := string()
}.
-type config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    pool_size := non_neg_integer(),
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    service_account_json := emqx_bridge_gcp_pubsub_connector:service_account_json(),
    any() => term()
}.
-type state() :: #{
    connector_state := emqx_bridge_gcp_pubsub_connector:state()
}.

-export_type([mqtt_config/0]).

-define(AUTO_RECONNECT_S, 2).

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------

-spec callback_mode() -> callback_mode().
callback_mode() -> async_if_possible.

-spec query_mode(any()) -> query_mode().
query_mode(_Config) -> no_queries.

-spec on_start(resource_id(), config()) -> {ok, state()} | {error, term()}.
on_start(InstanceId, Config) ->
    case emqx_bridge_gcp_pubsub_connector:on_start(InstanceId, Config) of
        {ok, ConnectorState} ->
            start_consumers(InstanceId, ConnectorState, Config);
        Error ->
            Error
    end.

-spec on_stop(resource_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, #{connector_state := ConnectorState}) ->
    ok = stop_consumers(InstanceId),
    emqx_bridge_gcp_pubsub_connector:on_stop(InstanceId, ConnectorState);
on_stop(InstanceId, undefined = _State) ->
    ok = stop_consumers(InstanceId),
    emqx_bridge_gcp_pubsub_connector:on_stop(InstanceId, undefined).

-spec on_get_status(resource_id(), state()) -> connected | disconnected.
on_get_status(InstanceId, _State) ->
    %% Note: do *not* alter the `connector_state' value here.  It must be immutable, since
    %% we have handed it over to the pull workers.
    case
        emqx_resource_pool:health_check_workers(
            InstanceId,
            fun emqx_bridge_gcp_pubsub_consumer_worker:health_check/1
        )
    of
        true -> connected;
        false -> connecting
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

start_consumers(InstanceId, ConnectorState, Config) ->
    #{
        bridge_name := BridgeName,
        consumer := ConsumerConfig0,
        hookpoint := Hookpoint
    } = Config,
    ConsumerConfig1 = maps:update_with(topic_mapping, fun convert_topic_mapping/1, ConsumerConfig0),
    TopicMapping = maps:get(topic_mapping, ConsumerConfig1),
    ConsumerWorkersPerTopic = maps:get(consumer_workers_per_topic, ConsumerConfig1),
    PoolSize = map_size(TopicMapping) * ConsumerWorkersPerTopic,
    ConsumerConfig = ConsumerConfig1#{
        auto_reconnect => ?AUTO_RECONNECT_S,
        bridge_name => BridgeName,
        connector_state => ConnectorState,
        hookpoint => Hookpoint,
        instance_id => InstanceId,
        pool_size => PoolSize
    },
    ConsumerOpts = maps:to_list(ConsumerConfig),
    %% FIXME: mark as unhealthy if topics do not exist!
    case validate_pubsub_topics(InstanceId, TopicMapping, ConnectorState) of
        ok ->
            ok;
        error ->
            _ = emqx_bridge_gcp_pubsub_connector:on_stop(InstanceId, ConnectorState),
            throw(
                "GCP PubSub topics are invalid.  Please check the logs, check if the "
                "topic exists in GCP and if the service account has permissions to use them."
            )
    end,
    case
        emqx_resource_pool:start(InstanceId, emqx_bridge_gcp_pubsub_consumer_worker, ConsumerOpts)
    of
        ok ->
            State = #{
                connector_state => ConnectorState,
                pool_name => InstanceId
            },
            {ok, State};
        {error, Reason} ->
            _ = emqx_bridge_gcp_pubsub_connector:on_stop(InstanceId, ConnectorState),
            {error, Reason}
    end.

stop_consumers(InstanceId) ->
    _ = log_when_error(
        fun() ->
            ok = emqx_resource_pool:stop(InstanceId)
        end,
        #{
            msg => "failed_to_stop_pull_worker_pool",
            instance_id => InstanceId
        }
    ),
    ok.

convert_topic_mapping(TopicMappingList) ->
    lists:foldl(
        fun(Fields, Acc) ->
            #{
                pubsub_topic := PubSubTopic,
                mqtt_topic := MQTTTopic,
                qos := QoS,
                payload_template := PayloadTemplate0
            } = Fields,
            PayloadTemplate = emqx_placeholder:preproc_tmpl(PayloadTemplate0),
            Acc#{
                PubSubTopic => #{
                    payload_template => PayloadTemplate,
                    mqtt_topic => MQTTTopic,
                    qos => QoS
                }
            }
        end,
        #{},
        TopicMappingList
    ).

validate_pubsub_topics(InstanceId, TopicMapping, ConnectorState) ->
    PubSubTopics = maps:keys(TopicMapping),
    do_validate_pubsub_topics(InstanceId, ConnectorState, PubSubTopics).

do_validate_pubsub_topics(InstanceId, ConnectorState, [Topic | Rest]) ->
    case check_for_topic_existence(InstanceId, Topic, ConnectorState) of
        ok ->
            do_validate_pubsub_topics(InstanceId, ConnectorState, Rest);
        {error, _} ->
            error
    end;
do_validate_pubsub_topics(_InstanceId, _ConnectorState, []) ->
    %% we already validate that the mapping is not empty in the config schema.
    ok.

check_for_topic_existence(InstanceId, Topic, ConnectorState) ->
    Res = emqx_bridge_gcp_pubsub_connector:get_topic(InstanceId, Topic, ConnectorState),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 404}} ->
            {error, not_found};
        {error, Details} ->
            ?tp(warning, "gcp_pubsub_consumer_check_topic_error", Details),
            {error, Details}
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
