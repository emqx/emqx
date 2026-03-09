%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2, on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_unsgov_sup:start_link(),
    ok = emqx_unsgov_config:load(),
    ok = emqx_unsgov:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_unsgov:unhook().

on_config_changed(_OldConfig, NewConfig) ->
    emqx_unsgov_config:update(NewConfig).

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_unsgov_api:handle(Method, PathRemainder, Request).
