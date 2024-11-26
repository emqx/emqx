%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_mt_state:create_tables(),
    ok = emqx_mt_hookcb:register_hooks(),
    emqx_mt_sup:start_link().

stop(_State) ->
    ok.
