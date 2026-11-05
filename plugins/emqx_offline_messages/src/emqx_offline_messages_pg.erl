%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_pg).
-moduledoc "PostgreSQL offline message lifecycle and MQTT hooks.".
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_offline_messages.hrl").

-export([on_config_changed/2, on_health_check/1, config/0, is_managed/2, count/2, start/1, stop/0]).
-export([on_publish/1, on_connected/2, on_session/2, on_authorize/4]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Restart the configured backend or remove its hooks when PostgreSQL is disabled.".
-spec on_config_changed(emqx_omp_pg_config:raw_config(), emqx_omp_pg_config:raw_config()) ->
    ok.
on_config_changed(Old, Old) ->
    ok;
on_config_changed(_Old, #{<<"enable">> := true} = New) ->
    Config = emqx_omp_pg_config:parse(New),
    emqx_omp_pg_sup:stop(),
    %% A failed start keeps the hooks, logs the error and fails the health check.
    _ = start(Config),
    ok;
on_config_changed(_Old, _New) ->
    stop().

-doc """
Start the backend for `Config`.
The config and the hooks are active before the runtime starts. Managed QoS 1/2 publishes
fail until the runtime is running, and they keep failing when the start fails.
""".
-spec start(emqx_omp_pg_config:config()) -> ok | {error, {atom(), term()}}.
start(Config) ->
    %% Publish the configuration and the metrics before any hook can use them.
    persistent_term:put(?MODULE, Config),
    emqx_metrics_worker:create_metrics(
        ?METRICS_WORKER, postgresql, [write_failed, rejected, skipped]
    ),
    case
        emqx_extsub_handler_registry:register(
            emqx_omp_pg_extsub,
            #{multi_topic => false, handle_generic_messages => true}
        )
    of
        ok -> ok;
        {error, extsub_handler_already_registered} -> ok
    end,
    %% Install the hooks before the runtime, so managed publishes fail instead of being routed.
    lists:foreach(
        fun({Hook, Function, Priority}) ->
            ok = emqx_hooks:put(Hook, {?MODULE, Function, []}, Priority)
        end,
        hooks()
    ),
    %% Start the pools, writers and cleanup worker.
    try
        start_runtime(Config)
    catch
        Class:Reason ->
            ?SLOG(error, #{
                msg => offline_messages_pg_start_failed, class => Class, reason => Reason
            }),
            {error, {Class, Reason}}
    end.

-doc "Remove the hooks and handler registration before stopping the database runtime.".
-spec stop() -> ok.
stop() ->
    %% Detach from the broker first, so no new work reaches the stopping runtime.
    lists:foreach(
        fun({Hook, Function, _}) -> emqx_hooks:del(Hook, {?MODULE, Function}) end, hooks()
    ),
    case config() of
        undefined -> ok;
        _ -> emqx_extsub_handler_registry:unregister(emqx_omp_pg_extsub)
    end,
    persistent_term:erase(?MODULE),
    %% Stop the pools, writers and cleanup worker.
    case whereis(emqx_omp_pg_sup) of
        undefined -> ok;
        _ -> emqx_omp_pg_sup:stop()
    end.

-doc "Report an unavailable database or an incompatible schema as a failed health check.".
-spec on_health_check(emqx_omp_pg_config:raw_config()) -> ok | {error, binary()}.
on_health_check(#{<<"enable">> := true}) ->
    Healthy =
        try
            ok = emqx_omp_pg_store:health(config()),
            true
        catch
            _:_ -> false
        end,
    case Healthy of
        true -> ok;
        false -> {error, <<"PostgreSQL storage is unavailable">>}
    end;
on_health_check(_) ->
    ok.

-doc "Return the configuration of the running backend.".
-spec config() -> emqx_omp_pg_config:config() | undefined.
config() -> persistent_term:get(?MODULE, undefined).

-doc "Check whether a topic matches one of the configured topic filters.".
-spec is_managed(emqx_types:topic(), emqx_omp_pg_config:config() | undefined) -> boolean().
is_managed(Topic, #{topics := Filters}) when is_binary(Topic) ->
    lists:any(fun(Filter) -> emqx_topic:match(Topic, Filter) end, Filters);
is_managed(_, _) ->
    false.

-doc "Increase a PostgreSQL backend metric.".
-spec count(write_failed | rejected | skipped, pos_integer()) -> ok.
count(Name, N) -> emqx_metrics_worker:inc(?METRICS_WORKER, postgresql, Name, N).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-doc "Store managed QoS 1/2 publishes, either before acknowledgement or through the batch writer.".
-spec on_publish(emqx_types:message()) -> {ok | stop, emqx_types:message()}.
on_publish(#message{qos = ?QOS_0} = Msg) ->
    {ok, Msg};
on_publish(#message{headers = #{allow_publish := false}} = Msg) ->
    {ok, Msg};
on_publish(#message{headers = #{should_disconnect := true}} = Msg) ->
    {ok, Msg};
on_publish(#message{topic = Topic} = Msg) ->
    Config = config(),
    case is_managed(Topic, Config) of
        false ->
            {ok, Msg};
        true ->
            #{namespace := Namespace, max_topic_length := Max, async_batched := Async} = Config,
            Result =
                case byte_size(Topic) =< Max of
                    false ->
                        {error, topic_too_long};
                    true when Async -> emqx_omp_pg_store:append(Namespace, Msg, Config);
                    true ->
                        emqx_omp_pg_store:append_sync(Namespace, Msg, Config)
                end,
            publish_result(Result, Msg)
    end.

-doc "Suppress regular dispatch after admission or commit, and disconnect on write failure.".
publish_result(ok, Msg) ->
    {stop, persisted(Msg)};
publish_result({error, Reason}, Msg) ->
    count(write_failed, 1),
    ?SLOG_THROTTLE(error, #{msg => offline_messages_pg_write_failed, reason => Reason}),
    {stop, emqx_message:set_headers(#{should_disconnect => true, allow_publish => false}, Msg)}.

persisted(Msg) ->
    emqx_message:set_headers(#{allow_publish => false, message_persisted => true}, Msg).

-doc "Remember the session implementation for the authorization and connect hooks.".
-spec on_session(emqx_types:clientinfo(), #{impl := module(), _ => _}) -> ok.
on_session(_ClientInfo, #{impl := Impl}) ->
    put(omp_session_impl, Impl),
    ok.

-doc "Restore managed subscriptions for MQTT clients with in-memory sessions.".
-spec on_connected(emqx_types:clientinfo(), emqx_types:conninfo()) -> ok.
on_connected(#{clientid := ClientId, protocol := mqtt} = ClientInfo, _ConnInfo) ->
    #{namespace := Namespace} = Config = config(),
    case get(omp_session_impl) of
        emqx_session_mem ->
            case emqx_omp_pg_store:restore(Namespace, ClientId, Config) of
                {ok, Rows} ->
                    %% Skip stored subscriptions whose topics are no longer managed.
                    Subs = [
                        {unmount(ClientInfo, Topic), subopts(Options)}
                     || {Topic, Options} <- Rows,
                        is_managed(Topic, Config)
                    ],
                    emqx_offline_messages_utils:induce_subscriptions(Subs);
                {error, Reason} ->
                    ?SLOG_THROTTLE(error, #{
                        msg => offline_messages_pg_restore_failed, reason => Reason
                    })
            end;
        _ ->
            ok
    end,
    ok;
on_connected(_ClientInfo, _ConnInfo) ->
    ok.

-doc "Deny unsupported subscriptions to managed topics.".
-spec on_authorize(
    emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic(), map()
) -> {ok, map()} | {stop, map()}.
on_authorize(ClientInfo, #{action_type := subscribe} = Action, Topic0, Result) ->
    Config = config(),
    Topic = authz_topic(ClientInfo, Topic0),
    case is_overlapping(Topic, Config) andalso is_unsupported(ClientInfo, Action, Topic, Config) of
        true -> {stop, #{result => deny, from => ?MODULE, is_cacheable => false}};
        false -> {ok, Result}
    end;
on_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

-doc "Install the schema when requested and check the started database pools.".
start_runtime(#{init_default_schema := Install} = Config) ->
    case Install of
        true -> ok = emqx_omp_pg_store:install(Config);
        false -> ok
    end,
    {ok, _} = emqx_omp_pg_sup:start(Config),
    ok = emqx_omp_pg_store:health(Config).

hooks() ->
    [
        {'message.publish', on_publish, ?HP_LOWEST - 1},
        {'client.connected', on_connected, ?HP_LOWEST},
        {'session.created', on_session, ?HP_LOWEST + 1},
        {'session.resumed', on_session, ?HP_LOWEST + 1},
        {'client.authorize', on_authorize, ?HP_AUTHZ + 1}
    ].

is_overlapping(Topic, #{topics := Filters}) ->
    lists:any(fun(Filter) -> emqx_topic:intersection(Topic, Filter) =/= false end, Filters);
is_overlapping(_, _) ->
    false.

-doc """
Return `true` for a subscription that a managed topic does not support.
Managed topics accept only exact QoS 1 subscriptions from in-memory MQTT sessions.
Authorization cannot see the share prefix, so shared subscriptions pass but receive
no managed messages.
""".
is_unsupported(#{protocol := mqtt}, #{qos := QoS}, Topic, #{max_topic_length := Max}) ->
    QoS =/= ?QOS_1 orelse emqx_topic:wildcard(Topic) orelse
        get(omp_session_impl) =/= emqx_session_mem orelse byte_size(Topic) > Max;
is_unsupported(_ClientInfo, _Action, _Topic, _Config) ->
    true.

-doc "Return the mounted topic. With `include_mountpoint`, authorization passes it mounted.".
authz_topic(ClientInfo, Topic) ->
    case emqx:get_config([authorization, include_mountpoint], false) of
        true -> Topic;
        false -> mount(ClientInfo, Topic)
    end.

mount(#{mountpoint := Mountpoint}, Topic) -> emqx_mountpoint:mount(Mountpoint, Topic);
mount(_, Topic) -> Topic.

unmount(#{mountpoint := Mountpoint}, Topic) -> emqx_mountpoint:unmount(Mountpoint, Topic);
unmount(_, Topic) -> Topic.

-doc "Restore only the MQTT subscription options understood by the broker.".
subopts(JSON) ->
    Raw = emqx_utils_json:decode(JSON),
    maps:from_list([
        {Key, Value}
     || Key <- [qos, nl, rap, rh, subid],
        {ok, Value} <- [maps:find(atom_to_binary(Key), Raw)]
    ]).
