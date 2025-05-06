%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    ok = emqx_retainer_cli:load(),
    ok = emqx_retainer_limiter:create(),
    emqx_retainer_sup:start_link().

stop(_State) ->
    ok = emqx_retainer_cli:unload(),
    ok = emqx_retainer_limiter:delete(),
    ok.
