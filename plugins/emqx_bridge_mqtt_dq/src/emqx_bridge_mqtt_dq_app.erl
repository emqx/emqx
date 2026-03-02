%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2, on_handle_api_call/4]).
-export([sync_bridges/0]).

start(_StartType, _StartArgs) ->
    emqx_bridge_mqtt_dq_conns = ets:new(emqx_bridge_mqtt_dq_conns, [
        public, set, named_table, {read_concurrency, true}
    ]),
    {ok, Sup} = emqx_bridge_mqtt_dq_sup:start_link(),
    ok = emqx_bridge_mqtt_dq_config:load(),
    ok = emqx_bridge_mqtt_dq:hook(),
    ok = start_bridges(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_bridge_mqtt_dq:unhook(),
    ok.

on_config_changed(_OldConfig, NewConfig) ->
    ok = emqx_bridge_mqtt_dq_config:update(NewConfig),
    ok = sync_bridges().

on_handle_api_call(_Method, _Path, _Request, _Context) ->
    {error, not_found}.

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
    DesiredSpecs = maps:from_list(
        [{bridge_id(B), bridge_child_spec(B)} || B <- Bridges, maps:get(enable, B, false)]
    ),
    CurrentChildren = supervisor:which_children(Sup),
    CurrentIds = [Id || {Id, _Pid, _Type, _Mods} <- CurrentChildren],
    lists:foreach(
        fun(Id) -> sync_existing_child(Sup, Id, DesiredSpecs) end,
        CurrentIds
    ),
    StartNew = maps:without(CurrentIds, DesiredSpecs),
    maps:foreach(fun(_Id, Spec) -> start_child(Sup, Spec) end, StartNew),
    ok.

bridge_id(#{name := Name}) ->
    {bridge, Name}.

sync_existing_child(Sup, Id, DesiredSpecs) ->
    case maps:find(Id, DesiredSpecs) of
        {ok, DesiredSpec} ->
            maybe_restart_child(Sup, Id, DesiredSpec);
        error ->
            stop_bridge_child(Sup, Id)
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
    _ = supervisor:delete_child(Sup, Id).
