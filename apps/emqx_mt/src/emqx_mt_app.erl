%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_mt_sup:start_link(),
    ok = emqx_mt_hookcb:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_mt_hookcb:unregister_hooks(),
    ok.
