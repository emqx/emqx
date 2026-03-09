%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_buffer_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(BridgeConfig) ->
    supervisor:start_link(?MODULE, [BridgeConfig]).

init([BridgeConfig]) ->
    #{buffer_pool_size := PoolSize} = BridgeConfig,
    Children = [buffer_child_spec(BridgeConfig, I) || I <- lists:seq(0, PoolSize - 1)],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    {ok, {SupFlags, Children}}.

buffer_child_spec(BridgeConfig, Index) ->
    #{name := Name} = BridgeConfig,
    #{
        id => {buffer, Name, Index},
        start => {emqx_bridge_mqtt_dq_buffer, start_link, [BridgeConfig, Index]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_bridge_mqtt_dq_buffer]
    }.
