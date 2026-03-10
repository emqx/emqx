%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-include("emqx_bridge_mqtt_dq.hrl").

-export([start/2, stop/1]).
-export([on_config_changed/2, on_handle_api_call/4]).
-export([sync_bridges/0]).

start(_StartType, _StartArgs) ->
    ok = emqx_bridge_mqtt_dq_config:load(),
    case check_queue_dirs() of
        ok ->
            {ok, Sup} = emqx_bridge_mqtt_dq_sup:start_link(),
            ok = emqx_bridge_mqtt_dq:hook(),
            ok = start_bridges(),
            {ok, Sup};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok = emqx_bridge_mqtt_dq:unhook(),
    ok.

on_config_changed(_OldConfig, NewConfig) ->
    ok = emqx_bridge_mqtt_dq_config:update(NewConfig),
    ok = sync_bridges().

on_handle_api_call(Method, Path, Request, _Context) ->
    emqx_bridge_mqtt_dq_api:handle(Method, Path, Request).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_bridges() ->
    Bridges = emqx_bridge_mqtt_dq_config:get_bridges(),
    lists:foreach(fun maybe_start_bridge/1, Bridges),
    ok.

maybe_start_bridge(#{enable := true} = BridgeConfig) ->
    start_bridge(BridgeConfig);
maybe_start_bridge(_) ->
    ok.

bridge_child_spec(BridgeConfig) ->
    #{name := Name} = BridgeConfig,
    #{
        id => {bridge, Name},
        start => {emqx_bridge_mqtt_dq_bridge_sup, start_link, [BridgeConfig]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [emqx_bridge_mqtt_dq_bridge_sup]
    }.

start_bridge(BridgeConfig) ->
    ChildSpec = bridge_child_spec(BridgeConfig),
    case supervisor:start_child(emqx_bridge_mqtt_dq_sup, ChildSpec) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Synchronize running bridges with the current config.
%% Only restarts bridges whose child spec actually changed.
%% Removes bridges no longer in config, starts new ones.
sync_bridges() ->
    Sup = emqx_bridge_mqtt_dq_sup,
    Bridges = emqx_bridge_mqtt_dq_config:get_bridges(),
    DisabledBridges = maps:from_list(
        [{bridge_id(B), B} || B <- Bridges, not maps:get(enable, B, false)]
    ),
    DesiredSpecs = maps:from_list(
        [{bridge_id(B), bridge_child_spec(B)} || B <- Bridges, maps:get(enable, B, false)]
    ),
    CurrentChildren = supervisor:which_children(Sup),
    CurrentIds = [Id || {Id, _Pid, _Type, _Mods} <- CurrentChildren, is_bridge_child_id(Id)],
    lists:foreach(
        fun(Id) -> sync_existing_child(Sup, Id, DesiredSpecs, DisabledBridges) end,
        CurrentIds
    ),
    StartNew = maps:without(CurrentIds, DesiredSpecs),
    maps:foreach(fun(_Id, Spec) -> start_child(Sup, Spec) end, StartNew),
    ok.

bridge_id(#{name := Name}) ->
    {bridge, Name}.

is_bridge_child_id({bridge, Name}) when is_binary(Name) ->
    true;
is_bridge_child_id(_) ->
    false.

sync_existing_child(Sup, Id, DesiredSpecs, DisabledBridges) ->
    case maps:find(Id, DesiredSpecs) of
        {ok, DesiredSpec} ->
            maybe_restart_child(Sup, Id, DesiredSpec);
        error ->
            disable_and_purge_bridge(Sup, Id, DisabledBridges)
    end.

disable_and_purge_bridge(Sup, Id, DisabledBridges) ->
    case maps:find(Id, DisabledBridges) of
        {ok, BridgeConfig} ->
            do_disable_and_purge_bridge(Sup, Id, BridgeConfig);
        error ->
            %% Bridge removed from config entirely — purge its queue too.
            BridgeConfig = get_bridge_config_from_spec(Sup, Id),
            do_disable_and_purge_bridge(Sup, Id, BridgeConfig)
    end.

maybe_restart_child(Sup, Id, DesiredSpec) ->
    case spec_changed(Sup, Id, DesiredSpec) of
        true ->
            stop_bridge_child(Sup, Id),
            start_child(Sup, DesiredSpec);
        false ->
            ok
    end.

spec_changed(Sup, Id, #{start := DesiredStart} = _DesiredSpec) ->
    case supervisor:get_childspec(Sup, Id) of
        {ok, #{start := CurrentStart}} ->
            CurrentStart =/= DesiredStart;
        {error, _} ->
            true
    end.

start_child(Sup, Spec) ->
    case supervisor:start_child(Sup, Spec) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

stop_bridge_child(Sup, Id) ->
    _ = supervisor:terminate_child(Sup, Id),
    _ = supervisor:delete_child(Sup, Id),
    maybe_delete_metrics(Id).

do_disable_and_purge_bridge(Sup, Id, BridgeConfig) ->
    stop_bridge_child(Sup, Id),
    maybe_purge_queue(BridgeConfig).

get_bridge_config_from_spec(Sup, Id) ->
    case supervisor:get_childspec(Sup, Id) of
        {ok, #{start := {_, _, [BridgeConfig]}}} -> BridgeConfig;
        _ -> #{}
    end.

maybe_delete_metrics({bridge, Name}) when is_binary(Name) ->
    ok = emqx_bridge_mqtt_dq_metrics:delete_bridge(Name);
maybe_delete_metrics(_Id) ->
    ok.

maybe_purge_queue(#{name := Name, queue_dir := QueueDir}) ->
    Dir = binary_to_list(QueueDir),
    case file:del_dir_r(Dir) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?LOG(warning, #{
                msg => "mqtt_dq_queue_purge_failed",
                bridge => Name,
                dir => Dir,
                reason => Reason
            }),
            ok
    end;
maybe_purge_queue(_BridgeConfig) ->
    ok.

%%--------------------------------------------------------------------
%% Queue dir pre-flight check
%%--------------------------------------------------------------------

check_queue_dirs() ->
    Bridges = emqx_bridge_mqtt_dq_config:get_bridges(),
    Enabled = [B || #{enable := true} = B <- Bridges],
    check_queue_dirs(Enabled).

check_queue_dirs([]) ->
    ok;
check_queue_dirs([#{name := Name, queue_dir := QueueDir} | Rest]) ->
    Dir = binary_to_list(QueueDir),
    case check_one_queue_dir(Dir) of
        ok ->
            check_queue_dirs(Rest);
        {error, Reason} ->
            ?LOG(error, #{
                msg => "mqtt_dq_queue_dir_not_writable",
                bridge => Name,
                dir => Dir,
                reason => Reason
            }),
            {error, {queue_dir_not_writable, Name, Dir, Reason}}
    end.

check_one_queue_dir(Dir) ->
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            TestFile = filename:join(Dir, ".write_test"),
            case file:write_file(TestFile, <<>>) of
                ok ->
                    _ = file:delete(TestFile),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
