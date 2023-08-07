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

%% health check API
-export([
    mark_topic_as_nonexistent/1,
    unset_nonexistent_topic/1,
    is_nonexistent_topic/1
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
    service_account_json := emqx_bridge_gcp_pubsub_client:service_account_json(),
    any() => term()
}.
-type state() :: #{
    client := emqx_bridge_gcp_pubsub_client:state()
}.

-export_type([mqtt_config/0]).

-define(AUTO_RECONNECT_S, 2).
-define(DEFAULT_FORGET_INTERVAL, timer:seconds(60)).
-define(OPTVAR_TOPIC_NOT_FOUND(INSTANCE_ID), {?MODULE, topic_not_found, INSTANCE_ID}).
-define(TOPIC_MESSAGE,
    "GCP PubSub topics are invalid.  Please check the logs, check if the "
    "topics exist in GCP and if the service account has permissions to use them."
).

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------

-spec callback_mode() -> callback_mode().
callback_mode() -> async_if_possible.

-spec query_mode(any()) -> query_mode().
query_mode(_Config) -> no_queries.

-spec on_start(resource_id(), config()) -> {ok, state()} | {error, term()}.
on_start(InstanceId, Config0) ->
    %% ensure it's a binary key map
    Config = maps:update_with(service_account_json, fun emqx_utils_maps:binary_key_map/1, Config0),
    case emqx_bridge_gcp_pubsub_client:start(InstanceId, Config) of
        {ok, Client} ->
            start_consumers(InstanceId, Client, Config);
        Error ->
            Error
    end.

-spec on_stop(resource_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, _State) ->
    ?tp(gcp_pubsub_consumer_stop_enter, #{}),
    unset_nonexistent_topic(InstanceId),
    ok = stop_consumers(InstanceId),
    emqx_bridge_gcp_pubsub_client:stop(InstanceId).

-spec on_get_status(resource_id(), state()) -> connected | connecting | {disconnected, state(), _}.
on_get_status(InstanceId, State) ->
    %% We need to check this flag separately because the workers might be gone when we
    %% check them.
    case is_nonexistent_topic(InstanceId) of
        true ->
            {disconnected, State, {unhealthy_target, ?TOPIC_MESSAGE}};
        false ->
            #{client := Client} = State,
            check_workers(InstanceId, Client)
    end.

%%-------------------------------------------------------------------------------------------------
%% Health check API (signalled by consumer worker)
%%-------------------------------------------------------------------------------------------------

-spec mark_topic_as_nonexistent(resource_id()) -> ok.
mark_topic_as_nonexistent(InstanceId) ->
    optvar:set(?OPTVAR_TOPIC_NOT_FOUND(InstanceId), true),
    ok.

-spec unset_nonexistent_topic(resource_id()) -> ok.
unset_nonexistent_topic(InstanceId) ->
    optvar:unset(?OPTVAR_TOPIC_NOT_FOUND(InstanceId)),
    ?tp(gcp_pubsub_consumer_unset_nonexistent_topic, #{}),
    ok.

-spec is_nonexistent_topic(resource_id()) -> boolean().
is_nonexistent_topic(InstanceId) ->
    case optvar:peek(?OPTVAR_TOPIC_NOT_FOUND(InstanceId)) of
        {ok, true} ->
            true;
        _ ->
            false
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

start_consumers(InstanceId, Client, Config) ->
    #{
        bridge_name := BridgeName,
        consumer := ConsumerConfig0,
        hookpoint := Hookpoint,
        resource_opts := #{request_ttl := RequestTTL},
        service_account_json := #{<<"project_id">> := ProjectId}
    } = Config,
    ConsumerConfig1 = maps:update_with(topic_mapping, fun convert_topic_mapping/1, ConsumerConfig0),
    TopicMapping = maps:get(topic_mapping, ConsumerConfig1),
    ConsumerWorkersPerTopic = maps:get(consumer_workers_per_topic, ConsumerConfig1),
    PoolSize = map_size(TopicMapping) * ConsumerWorkersPerTopic,
    ConsumerConfig = ConsumerConfig1#{
        auto_reconnect => ?AUTO_RECONNECT_S,
        bridge_name => BridgeName,
        client => Client,
        forget_interval => forget_interval(RequestTTL),
        hookpoint => Hookpoint,
        instance_id => InstanceId,
        pool_size => PoolSize,
        project_id => ProjectId,
        pull_retry_interval => RequestTTL
    },
    ConsumerOpts = maps:to_list(ConsumerConfig),
    case validate_pubsub_topics(TopicMapping, Client) of
        ok ->
            ok;
        {error, not_found} ->
            _ = emqx_bridge_gcp_pubsub_client:stop(InstanceId),
            throw(
                {unhealthy_target, ?TOPIC_MESSAGE}
            );
        {error, _} ->
            %% connection might be down; we'll have to check topic existence during health
            %% check, or the workers will kill themselves when they realized there's no
            %% topic when upserting their subscription.
            ok
    end,
    case
        emqx_resource_pool:start(InstanceId, emqx_bridge_gcp_pubsub_consumer_worker, ConsumerOpts)
    of
        ok ->
            State = #{
                client => Client,
                pool_name => InstanceId
            },
            {ok, State};
        {error, Reason} ->
            _ = emqx_bridge_gcp_pubsub_client:stop(InstanceId),
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

validate_pubsub_topics(TopicMapping, Client) ->
    PubSubTopics = maps:keys(TopicMapping),
    do_validate_pubsub_topics(Client, PubSubTopics).

do_validate_pubsub_topics(Client, [Topic | Rest]) ->
    case check_for_topic_existence(Topic, Client) of
        ok ->
            do_validate_pubsub_topics(Client, Rest);
        {error, _} = Err ->
            Err
    end;
do_validate_pubsub_topics(_Client, []) ->
    %% we already validate that the mapping is not empty in the config schema.
    ok.

check_for_topic_existence(Topic, Client) ->
    Res = emqx_bridge_gcp_pubsub_client:get_topic(Topic, Client),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 404}} ->
            {error, not_found};
        {error, Reason} ->
            ?tp(warning, "gcp_pubsub_consumer_check_topic_error", #{reason => Reason}),
            {error, Reason}
    end.

-spec get_client_status(emqx_bridge_gcp_pubsub_client:state()) -> connected | connecting.
get_client_status(Client) ->
    case emqx_bridge_gcp_pubsub_client:get_status(Client) of
        disconnected -> connecting;
        connected -> connected
    end.

-spec check_workers(resource_id(), emqx_bridge_gcp_pubsub_client:state()) -> connected | connecting.
check_workers(InstanceId, Client) ->
    case
        emqx_resource_pool:health_check_workers(
            InstanceId,
            fun emqx_bridge_gcp_pubsub_consumer_worker:health_check/1,
            emqx_resource_pool:health_check_timeout(),
            #{return_values => true}
        )
    of
        {ok, []} ->
            connecting;
        {ok, Values} ->
            AllOk = lists:all(fun(S) -> S =:= subscription_ok end, Values),
            case AllOk of
                true ->
                    get_client_status(Client);
                false ->
                    connecting
            end;
        {error, _} ->
            connecting
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

forget_interval(infinity) -> ?DEFAULT_FORGET_INTERVAL;
forget_interval(Timeout) -> 2 * Timeout.
