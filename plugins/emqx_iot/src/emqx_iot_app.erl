%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_app).

-behaviour(application).
-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_iot_sup:start_link(),
    ok = emqx_iot_config:load(),
    ok = emqx_iot_metrics:init(),
    ok = emqx_iot:init_tables(),
    ok = emqx_iot:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_iot:unhook(),
    ok.

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_iot_api:handle(Method, PathRemainder, Request).
