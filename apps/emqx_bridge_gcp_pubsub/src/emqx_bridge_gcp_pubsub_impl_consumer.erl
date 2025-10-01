%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_impl_consumer).

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

%% health check API
-export([
    mark_as_unhealthy/2,
    clear_unhealthy/1,
    check_if_unhealthy/1
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-type mqtt_config() :: #{
    mqtt_topic := emqx_types:topic(),
    qos := emqx_types:qos(),
    payload_template := string()
}.
-type connector_config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    pool_size := non_neg_integer(),
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    service_account_json := emqx_bridge_gcp_pubsub_client:service_account_json(),
    any() => term()
}.
-type connector_state() :: #{
    client := emqx_bridge_gcp_pubsub_client:state(),
    installed_sources := #{source_resource_id() => source_state()},
    project_id := binary()
}.
-type source_config() :: #{
    bridge_name := binary(),
    hookpoints := [binary()],
    parameters := #{
        consumer_workers_per_topic := pos_integer(),
        topic := binary()
    },
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()}
}.
-type source_state() :: #{}.

-export_type([mqtt_config/0]).

-define(AUTO_RECONNECT_S, 2).
-define(DEFAULT_FORGET_INTERVAL, timer:seconds(60)).
-define(OPTVAR_UNHEALTHY(INSTANCE_ID), {?MODULE, topic_not_found, INSTANCE_ID}).
-define(TOPIC_MESSAGE,
    "GCP PubSub topics are invalid.  Please check the logs, check if the "
    "topics exist in GCP and if the service account has permissions to use them."
).
-define(PERMISSION_MESSAGE,
    "Permission denied while verifying topic existence.  Please check that the "
    "provided service account has the correct permissions configured."
).

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------
-spec resource_type() -> resource_type().
resource_type() -> gcp_pubsub_consumer.

-spec callback_mode() -> callback_mode().
callback_mode() -> async_if_possible.

-spec query_mode(any()) -> query_mode().
query_mode(_Config) -> no_queries.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, term()}.
on_start(ConnectorResId, Config0) ->
    Config1 = maps:update_with(
        service_account_json,
        fun(X) ->
            JsonBin = emqx_secret:unwrap(X),
            emqx_utils_json:decode(JsonBin)
        end,
        Config0
    ),
    {Transport, HostPort} = emqx_bridge_gcp_pubsub_client:get_transport(pubsub),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(HostPort, #{default_port => 443}),
    Config = Config1#{
        jwt_opts => #{
            %% fixed for pubsub; trailing slash is important.
            aud => <<"https://pubsub.googleapis.com/">>
        },
        transport => Transport,
        host => Host,
        port => Port
    },
    #{service_account_json := #{<<"project_id">> := ProjectId}} = Config,
    case emqx_bridge_gcp_pubsub_client:start(ConnectorResId, Config) of
        {ok, Client} ->
            ConnectorState = #{
                client => Client,
                installed_sources => #{},
                project_id => ProjectId
            },
            {ok, ConnectorState};
        Error ->
            Error
    end.

-spec on_stop(resource_id(), connector_state()) -> ok | {error, term()}.
on_stop(ConnectorResId, ConnectorState) ->
    ?tp(gcp_pubsub_consumer_stop_enter, #{}),
    clear_unhealthy(ConnectorState),
    ok = stop_consumers(ConnectorState),
    emqx_bridge_gcp_pubsub_client:stop(ConnectorResId).

-spec on_get_status(resource_id(), connector_state()) ->
    ?status_connected | ?status_connecting.
on_get_status(_ConnectorResId, ConnectorState) ->
    #{client := Client} = ConnectorState,
    get_client_status(Client).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    source_resource_id(),
    source_config()
) ->
    {ok, connector_state()}.
on_add_channel(ConnectorResId, ConnectorState0, SourceResId, SourceConfig) ->
    #{
        client := Client,
        installed_sources := InstalledSources0,
        project_id := ProjectId
    } = ConnectorState0,
    case start_consumers(ConnectorResId, SourceResId, Client, ProjectId, SourceConfig) of
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
on_remove_channel(_ConnectorResId, ConnectorState0, SourceResId) ->
    #{installed_sources := InstalledSources0} = ConnectorState0,
    case maps:take(SourceResId, InstalledSources0) of
        {SourceState, InstalledSources} ->
            stop_consumers1(SourceState),
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

-spec on_get_channel_status(connector_resource_id(), source_resource_id(), connector_state()) ->
    health_check_status().
on_get_channel_status(
    _ConnectorResId,
    SourceResId,
    ConnectorState = #{installed_sources := InstalledSources}
) when is_map_key(SourceResId, InstalledSources) ->
    %% We need to check this flag separately because the workers might be gone when we
    %% check them.
    case check_if_unhealthy(SourceResId) of
        {error, topic_not_found} ->
            {?status_disconnected, {unhealthy_target, ?TOPIC_MESSAGE}};
        {error, permission_denied} ->
            {?status_disconnected, {unhealthy_target, ?PERMISSION_MESSAGE}};
        {error, bad_credentials} ->
            {?status_disconnected, {unhealthy_target, ?PERMISSION_MESSAGE}};
        ok ->
            #{client := Client} = ConnectorState,
            #{SourceResId := #{pool_name := PoolName}} = InstalledSources,
            check_workers(PoolName, Client)
    end;
on_get_channel_status(_ConnectorResId, _SourceResId, _ConnectorState) ->
    ?status_disconnected.

%%-------------------------------------------------------------------------------------------------
%% Health check API (signalled by consumer worker)
%%-------------------------------------------------------------------------------------------------

-spec mark_as_unhealthy(
    source_resource_id(),
    topic_not_found
    | permission_denied
    | bad_credentials
) -> ok.
mark_as_unhealthy(SourceResId, Reason) ->
    optvar:set(?OPTVAR_UNHEALTHY(SourceResId), Reason),
    ok.

-spec clear_unhealthy(connector_state()) -> ok.
clear_unhealthy(ConnectorState) ->
    #{installed_sources := InstalledSources} = ConnectorState,
    maps:foreach(
        fun(SourceResId, _SourceState) ->
            optvar:unset(?OPTVAR_UNHEALTHY(SourceResId))
        end,
        InstalledSources
    ),
    ?tp(gcp_pubsub_consumer_clear_unhealthy, #{}),
    ok.

-spec check_if_unhealthy(source_resource_id()) ->
    ok
    | {error,
        topic_not_found
        | permission_denied
        | bad_credentials}.
check_if_unhealthy(SourceResId) ->
    case optvar:peek(?OPTVAR_UNHEALTHY(SourceResId)) of
        {ok, Reason} ->
            {error, Reason};
        undefined ->
            ok
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

start_consumers(ConnectorResId, SourceResId, Client, ProjectId, SourceConfig) ->
    #{
        bridge_name := BridgeName,
        parameters := #{topic := PubsubTopic} = ConsumerConfig0,
        hookpoints := Hookpoints,
        resource_opts := #{request_ttl := RequestTTL}
    } = SourceConfig,
    #{namespace := Namespace} = emqx_resource:parse_channel_id(SourceResId),
    ConsumerWorkersPerTopic = maps:get(consumer_workers_per_topic, ConsumerConfig0),
    PoolSize = ConsumerWorkersPerTopic,
    ConsumerConfig = ConsumerConfig0#{
        auto_reconnect => ?AUTO_RECONNECT_S,
        bridge_name => BridgeName,
        client => Client,
        forget_interval => forget_interval(RequestTTL),
        hookpoints => Hookpoints,
        namespace => Namespace,
        connector_resource_id => ConnectorResId,
        source_resource_id => SourceResId,
        pool_size => PoolSize,
        project_id => ProjectId,
        pull_retry_interval => RequestTTL,
        request_ttl => RequestTTL
    },
    ConsumerOpts = maps:to_list(ConsumerConfig),
    ReqOpts = #{request_ttl => RequestTTL},
    PubsubTopics = [PubsubTopic],
    case validate_pubsub_topics(PubsubTopics, Client, ReqOpts) of
        ok ->
            ok;
        {error, not_found} ->
            throw(
                {unhealthy_target, ?TOPIC_MESSAGE}
            );
        {error, permission_denied} ->
            throw(
                {unhealthy_target, ?PERMISSION_MESSAGE}
            );
        {error, bad_credentials} ->
            throw(
                {unhealthy_target, ?PERMISSION_MESSAGE}
            );
        {error, _} ->
            %% connection might be down; we'll have to check topic existence during health
            %% check, or the workers will kill themselves when they realized there's no
            %% topic when upserting their subscription.
            ok
    end,
    case
        emqx_resource_pool:start(SourceResId, emqx_bridge_gcp_pubsub_consumer_worker, ConsumerOpts)
    of
        ok ->
            State = #{pool_name => SourceResId},
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

stop_consumers(ConnectorState) ->
    #{installed_sources := InstalledSources} = ConnectorState,
    maps:foreach(
        fun(_SourceResId, SourceState) ->
            stop_consumers1(SourceState)
        end,
        InstalledSources
    ).

stop_consumers1(SourceState) ->
    #{pool_name := PoolName} = SourceState,
    _ = log_when_error(
        fun() ->
            ok = emqx_resource_pool:stop(PoolName)
        end,
        #{
            msg => "failed_to_stop_pull_worker_pool",
            pool_name => PoolName
        }
    ),
    ok.

validate_pubsub_topics(PubsubTopics, Client, ReqOpts) ->
    do_validate_pubsub_topics(Client, PubsubTopics, ReqOpts).

do_validate_pubsub_topics(Client, [Topic | Rest], ReqOpts) ->
    case check_for_topic_existence(Topic, Client, ReqOpts) of
        ok ->
            do_validate_pubsub_topics(Client, Rest, ReqOpts);
        {error, _} = Err ->
            Err
    end;
do_validate_pubsub_topics(_Client, [], _ReqOpts) ->
    ok.

check_for_topic_existence(Topic, Client, ReqOpts) ->
    Res = emqx_bridge_gcp_pubsub_client:pubsub_get_topic(Topic, Client, ReqOpts),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 404}} ->
            {error, not_found};
        {error, #{status_code := 403}} ->
            {error, permission_denied};
        {error, #{status_code := 401}} ->
            {error, bad_credentials};
        {error, Reason} ->
            ?tp(warning, "gcp_pubsub_consumer_check_topic_error", #{reason => Reason}),
            {error, Reason}
    end.

-spec get_client_status(emqx_bridge_gcp_pubsub_client:state()) ->
    ?status_connected | {?status_connecting, term()}.
get_client_status(Client) ->
    case emqx_bridge_gcp_pubsub_client:get_status(Client) of
        {?status_disconnected, Reason} -> {?status_connecting, Reason};
        ?status_connected -> ?status_connected
    end.

-spec check_workers(source_resource_id(), emqx_bridge_gcp_pubsub_client:state()) ->
    ?status_connected | ?status_connecting.
check_workers(SourceResId, Client) ->
    Opts = #{
        check_fn => fun emqx_bridge_gcp_pubsub_consumer_worker:health_check/1,
        is_success_fn => fun
            (subscription_ok) -> false;
            (_) -> true
        end,
        on_success_fn => fun() -> get_client_status(Client) end
    },
    emqx_resource_pool:common_health_check_workers(SourceResId, Opts).

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
