%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_mysql_app).

-include("emqx_auth_mysql.hrl").

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_authz:register_source(?AUTHZ_TYPE, emqx_authz_mysql),
    ok = emqx_authn:register_provider(?AUTHN_TYPE, emqx_authn_mysql),
    {ok, Sup} = emqx_auth_mysql_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authn:deregister_provider(?AUTHN_TYPE),
    ok = emqx_authz:unregister_source(?AUTHZ_TYPE),
    ok.
