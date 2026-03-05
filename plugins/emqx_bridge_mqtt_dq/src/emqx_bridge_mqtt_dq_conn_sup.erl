%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_conn_sup).

-behaviour(brod_supervisor3).

-export([start_link/1, reg_name/1]).
-export([init/1]).

start_link(BridgeConfig) ->
    #{name := Name} = BridgeConfig,
    brod_supervisor3:start_link({local, reg_name(Name)}, ?MODULE, [BridgeConfig]).

-spec reg_name(binary()) -> atom().
reg_name(BridgeName) ->
    list_to_atom("emqx_bridge_mqtt_dq_conn_" ++ binary_to_list(BridgeName)).

init([BridgeConfig]) ->
    #{pool_size := PoolSize} = BridgeConfig,
    Children = [conn_child_spec(BridgeConfig, I) || I <- lists:seq(0, PoolSize - 1)],
    {ok, {{one_for_one, 10, 60}, Children}}.

conn_child_spec(BridgeConfig, Index) ->
    #{name := Name} = BridgeConfig,
    %% brod_supervisor3 uses old-style 6-tuple child spec
    {
        {conn, Name, Index},
        {emqx_bridge_mqtt_dq_connector, start_link, [BridgeConfig, Index]},
        {permanent, 5},
        5000,
        worker,
        [emqx_bridge_mqtt_dq_connector]
    }.
