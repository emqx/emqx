%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_app).

-behaviour(application).
-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_bcast_sup:start_link(),
    ok = emqx_bcast_config:load(),
    ok = emqx_bcast_metrics:init(),
    ok = emqx_bcast:init_tables(),
    ok = emqx_bcast:rebuild_index(),
    ok = emqx_bcast:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_bcast:unhook(),
    ok.

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_bcast_api:handle(Method, PathRemainder, Request).
