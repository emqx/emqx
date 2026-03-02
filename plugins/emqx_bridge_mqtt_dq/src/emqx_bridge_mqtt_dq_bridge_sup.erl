%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_bridge_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(BridgeConfig) ->
    #{name := Name} = BridgeConfig,
    RegName = list_to_atom("emqx_bridge_mqtt_dq_bridge_" ++ binary_to_list(Name)),
    supervisor:start_link({local, RegName}, ?MODULE, [BridgeConfig]).

init([BridgeConfig]) ->
    Children = [
        #{
            id => buffer_pool,
            start => {emqx_bridge_mqtt_dq_buffer_sup, start_link, [BridgeConfig]},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [emqx_bridge_mqtt_dq_buffer_sup]
        },
        #{
            id => conn_pool,
            start => {emqx_bridge_mqtt_dq_conn_sup, start_link, [BridgeConfig]},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [emqx_bridge_mqtt_dq_conn_sup]
        }
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 60
    },
    {ok, {SupFlags, Children}}.
