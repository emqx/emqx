%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_conn_sup).

-behaviour(supervisor).

-export([start_link/1, reg_name/1]).
-export([init/1]).

start_link(BridgeConfig) ->
    #{name := Name} = BridgeConfig,
    supervisor:start_link({local, reg_name(Name)}, ?MODULE, [BridgeConfig]).

-spec reg_name(binary()) -> atom().
reg_name(BridgeName) ->
    list_to_atom("emqx_bridge_mqtt_dq_conn_" ++ binary_to_list(BridgeName)).

init([BridgeConfig]) ->
    #{pool_size := PoolSize} = BridgeConfig,
    Children = [conn_child_spec(BridgeConfig, I) || I <- lists:seq(0, PoolSize - 1)],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

conn_child_spec(BridgeConfig, Index) ->
    #{name := Name} = BridgeConfig,
    #{
        id => {conn, Name, Index},
        start => {emqx_bridge_mqtt_dq_connector, start_link, [BridgeConfig, Index]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_bridge_mqtt_dq_connector]
    }.
