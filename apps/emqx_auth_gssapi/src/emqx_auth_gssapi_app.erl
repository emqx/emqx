%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_gssapi_app).

-include("emqx_auth_gssapi.hrl").

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_authn:register_provider(?AUTHN_TYPE_GSSAPI, emqx_authn_gssapi),
    {ok, Sup} = emqx_auth_gssapi_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authn:deregister_provider(?AUTHN_TYPE_GSSAPI),
    ok.
