%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_durable_timer_app).

-export([start/2]).

start(_Type, _Args) ->
    emqx_durable_timer_sup:start_top().
