%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_machine_app).

-include_lib("emqx/include/logger.hrl").

-export([
    start/2,
    stop/1
]).

-behaviour(application).

start(_Type, _Args) ->
    ok = emqx_machine:start(),
    _ = emqx_restricted_shell:set_prompt_func(),
    emqx_machine_sup:start_link().

stop(_State) ->
    ok.
