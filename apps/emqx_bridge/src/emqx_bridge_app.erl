%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_app).

-behaviour(application).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_bridge_sup:start_link(),
    ok = emqx_bridge_v2:load(),
    ?tp(emqx_bridge_app_started, #{}),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_bridge_v2:unload(),
    emqx_action_info:clean_cache(),
    ok.
