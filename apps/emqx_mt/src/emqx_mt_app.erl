%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_mt_sup:start_link(),
    ok = emqx_mt_hookcb:register_hooks(),
    ok = emqx_channel:set_limiter_adjustment_fn(fun emqx_mt_limiter:adjust_limiter/1),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_channel:unset_limiter_adjustment_fn(),
    ok = emqx_mt_hookcb:unregister_hooks(),
    ok.
