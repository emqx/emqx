%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%%%-------------------------------------------------------------------
%% @doc emqx_authz public API
%% @end
%%%-------------------------------------------------------------------

%% TODO: delete this module

-module(emqx_authz_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_authz_mnesia:init_tables(),
    {ok, Sup} = emqx_authz_sup:start_link(),
    ok = emqx_authz:init(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authz:deinit(),
    ok.
