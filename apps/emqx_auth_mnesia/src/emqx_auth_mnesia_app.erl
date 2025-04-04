%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_mnesia_app).

-include("emqx_auth_mnesia.hrl").

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_authz_mnesia:init_tables(),
    ok = emqx_authn_mnesia:init_tables(),
    ok = emqx_authn_scram_mnesia:init_tables(),
    ok = emqx_authz:register_source(?AUTHZ_TYPE, emqx_authz_mnesia),
    ok = emqx_authn:register_provider(?AUTHN_TYPE_SIMPLE, emqx_authn_mnesia),
    ok = emqx_authn:register_provider(?AUTHN_TYPE_SCRAM, emqx_authn_scram_mnesia),
    {ok, Sup} = emqx_auth_mnesia_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authn:deregister_provider(?AUTHN_TYPE_SIMPLE),
    ok = emqx_authn:deregister_provider(?AUTHN_TYPE_SCRAM),
    ok = emqx_authz:unregister_source(?AUTHZ_TYPE),
    ok.
