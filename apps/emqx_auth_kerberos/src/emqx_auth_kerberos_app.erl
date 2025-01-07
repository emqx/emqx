%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_kerberos_app).

-include("emqx_auth_kerberos.hrl").

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_authn:register_provider(?AUTHN_TYPE_KERBEROS, emqx_authn_kerberos),
    {ok, Sup} = emqx_auth_kerberos_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authn:deregister_provider(?AUTHN_TYPE_KERBEROS),
    ok.
