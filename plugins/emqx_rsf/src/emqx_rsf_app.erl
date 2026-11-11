%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rsf_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2, on_health_check/1, on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    ok = emqx_rsf_store:create_tables(),
    {ok, Sup} = emqx_rsf_sup:start_link(),
    ok = emqx_rsf:register_sql_function(),
    {ok, Sup}.

stop(_State) ->
    emqx_rsf:unregister_sql_function().

on_config_changed(_OldConfig, NewConfig) ->
    emqx_rsf_client:update_config(NewConfig).

on_health_check(_Options) ->
    case emqx_rsf_client:server_info() of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, iolist_to_binary(io_lib:format("function server: ~0p", [Reason]))}
    end.

on_handle_api_call(Method, Path, Request, Context) ->
    emqx_rsf_api:handle(Method, Path, Request, Context).
