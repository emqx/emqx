%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_slow_subs_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_slow_subs_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.
