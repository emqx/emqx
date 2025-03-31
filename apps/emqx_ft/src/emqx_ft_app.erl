%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_app).

-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_ft_sup:start_link(),
    ok = emqx_ft_conf:load(),
    {ok, Sup}.

prep_stop(State) ->
    ok = emqx_ft_conf:unload(),
    State.

stop(_State) ->
    ok.
