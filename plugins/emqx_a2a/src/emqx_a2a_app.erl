%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2, on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_a2a_sup:start_link(),
    ok = emqx_a2a_config:load(),
    ok = emqx_a2a:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_a2a:unhook().

on_config_changed(_OldConfig, NewConfig) ->
    emqx_a2a_config:update(NewConfig).

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_a2a_api:handle(Method, PathRemainder, Request).
