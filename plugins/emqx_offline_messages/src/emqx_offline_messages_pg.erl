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

-export([on_config_changed/2, on_health_check/1, config/0, managed/2, count/2, start/1, stop/0]).
-export([on_publish/1, on_connected/2, on_session/2, on_authorize/4]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

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
The config and the hooks are active before the runtime starts. Managed publishes
fail until the runtime is running, and they keep failing when the start fails.
""".
start(Config) ->
    true = erlang:function_exported(emqx_broker, dispatch_info, 2),
    persistent_term:put(?MODULE, Config),
    emqx_metrics_worker:create_metrics(
        ?METRICS_WORKER, postgresql, [write_failed, rejected, skipped]
    ),
    case
        emqx_extsub_handler_registry:register(
            emqx_offline_messages_pg_extsub,
            #{multi_topic => false, handle_generic_messages => true}
        )
    of
        ok -> ok;
        {error, extsub_handler_already_registered} -> ok
    end,
    lists:foreach(
        fun({Hook, Function, Priority}) ->
            ok = emqx_hooks:put(Hook, {?MODULE, Function, []}, Priority)
        end,
        hooks()
    ),
    try
        start_runtime(Config)
    catch
        Class:Reason ->
            ?SLOG(error, #{
                msg => offline_messages_pg_start_failed, class => Class, reason => Reason
            }),
            {error, {Class, Reason}}
    end.

stop() ->
    lists:foreach(
        fun({Hook, Function, _}) -> emqx_hooks:del(Hook, {?MODULE, Function}) end, hooks()
    ),
    case config() of
        undefined -> ok;
        _ -> emqx_extsub_handler_registry:unregister(emqx_offline_messages_pg_extsub)
    end,
    persistent_term:erase(?MODULE),
    case whereis(emqx_omp_pg_sup) of
        undefined -> ok;
        _ -> emqx_omp_pg_sup:stop()
    end.

on_health_check(#{<<"enable">> := true}) ->
    Healthy =
        try
            ok = emqx_omp_pg_store:call(omp_pg_read, health, health, config()),
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

config() -> persistent_term:get(?MODULE, undefined).

managed(Topic, #{topics := Filters}) when is_binary(Topic) ->
    lists:any(fun(Filter) -> emqx_topic:match(Topic, Filter) end, Filters);
managed(_, _) ->
    false.

count(Name, N) -> emqx_metrics_worker:inc(?METRICS_WORKER, postgresql, Name, N).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

on_publish(#message{headers = #{allow_publish := false}} = Msg) ->
    {ok, Msg};
on_publish(#message{headers = #{should_disconnect := true}} = Msg) ->
    {ok, Msg};
on_publish(#message{topic = Topic, qos = QoS} = Msg) ->
    Config = config(),
    case managed(Topic, Config) of
        false ->
            {ok, Msg};
        true ->
            #{namespace := Namespace, max_topic_length := Max} = Config,
            Result =
                case byte_size(Topic) =< Max of
                    false ->
                        {error, topic_too_long};
                    true when QoS =:= ?QOS_0 -> emqx_omp_pg_writer:append(Namespace, Msg, Config);
                    true ->
                        Operation = {append, Namespace, Topic, [emqx_omp_pg_store:encode(Msg)]},
                        emqx_omp_pg_store:call(omp_pg_write, Topic, Operation, Config)
                end,
            publish_result(Result, Namespace, Msg)
    end.

publish_result(ok, _Namespace, Msg) ->
    {stop, emqx_message:set_header(allow_publish, false, Msg)};
publish_result({ok, {Id, Offset}}, Namespace, Msg) ->
    ok = emqx_omp_pg_store:dispatch(Msg, {Namespace, Id}, Offset),
    {stop, emqx_message:set_headers(#{allow_publish => false, message_persisted => true}, Msg)};
publish_result({error, Reason}, _Namespace, Msg) ->
    count(write_failed, 1),
    ?SLOG_THROTTLE(error, #{msg => offline_messages_pg_write_failed, reason => Reason}),
    {stop, emqx_message:set_headers(#{should_disconnect => true, allow_publish => false}, Msg)}.

on_session(_ClientInfo, #{impl := Impl}) ->
    put(omp_session_impl, Impl),
    ok.

on_connected(#{clientid := ClientId, protocol := mqtt} = ClientInfo, _ConnInfo) ->
    #{namespace := Namespace} = Config = config(),
    case get(omp_session_impl) of
        emqx_session_mem ->
            Operation = {restore, Namespace, ClientId},
            case emqx_omp_pg_store:call(omp_pg_read, ClientId, Operation, Config) of
                {ok, Rows} ->
                    Subs = [
                        {unmount(ClientInfo, Topic), subopts(Options)}
                     || {Topic, Options} <- Rows,
                        managed(Topic, Config)
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

on_authorize(ClientInfo, #{action_type := subscribe} = Action, Topic0, Result) ->
    Config = config(),
    Topic = mount(ClientInfo, Topic0),
    case overlaps(Topic, Config) andalso unsupported(ClientInfo, Action, Topic, Config) of
        true -> {stop, #{result => deny, from => ?MODULE, is_cacheable => false}};
        false -> {ok, Result}
    end;
on_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

start_runtime(#{init_default_schema := Install} = Config) ->
    case Install of
        true -> ok = emqx_omp_pg_store:install(Config);
        false -> ok
    end,
    {ok, _} = emqx_omp_pg_sup:start(Config),
    ok = emqx_omp_pg_store:call(omp_pg_read, health, health, Config).

hooks() ->
    [
        {'message.publish', on_publish, ?HP_LOWEST - 1},
        {'client.connected', on_connected, ?HP_LOWEST},
        {'session.created', on_session, ?HP_LOWEST + 1},
        {'session.resumed', on_session, ?HP_LOWEST + 1},
        {'client.authorize', on_authorize, ?HP_AUTHZ + 1}
    ].

overlaps(Topic, #{topics := Filters}) ->
    lists:any(fun(Filter) -> emqx_topic:intersection(Topic, Filter) =/= false end, Filters);
overlaps(_, _) ->
    false.

%% Managed topics accept only exact QoS 1 subscriptions from in-memory MQTT sessions.
%% Authorization cannot see the share prefix, so shared subscriptions pass and receive
%% no managed messages.
unsupported(#{protocol := mqtt}, #{qos := QoS}, Topic, #{max_topic_length := Max}) ->
    QoS =/= ?QOS_1 orelse emqx_topic:wildcard(Topic) orelse
        get(omp_session_impl) =/= emqx_session_mem orelse byte_size(Topic) > Max;
unsupported(_ClientInfo, _Action, _Topic, _Config) ->
    true.

mount(#{mountpoint := Mountpoint}, Topic) -> emqx_mountpoint:mount(Mountpoint, Topic);
mount(_, Topic) -> Topic.

unmount(#{mountpoint := Mountpoint}, Topic) -> emqx_mountpoint:unmount(Mountpoint, Topic);
unmount(_, Topic) -> Topic.

subopts(JSON) ->
    Raw = emqx_utils_json:decode(JSON),
    maps:from_list([
        {Key, Value}
     || Key <- [qos, nl, rap, rh, subid],
        {ok, Value} <- [maps:find(atom_to_binary(Key), Raw)]
    ]).
