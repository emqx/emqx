%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_exhook_app).

-behaviour(application).

-include("emqx_exhook.hrl").

-export([
    start/2,
    stop/1
]).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_exhook_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------
