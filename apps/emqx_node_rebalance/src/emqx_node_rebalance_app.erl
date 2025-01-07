%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_node_rebalance_sup:start_link(),
    ok = emqx_node_rebalance_cli:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_node_rebalance_cli:unload().
