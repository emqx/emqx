%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auto_subscribe_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_auto_subscribe:load(),
    {ok, Sup} = emqx_auto_subscribe_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_auto_subscribe:unload(),
    ok.

%% internal functions
