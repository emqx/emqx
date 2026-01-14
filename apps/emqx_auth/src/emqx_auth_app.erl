%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_app).

-behaviour(application).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

-dialyzer({nowarn_function, [start/2]}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    %% required by test cases, ensure the injection of schema
    _ = emqx_conf_schema:roots(),
    {ok, Sup} = emqx_auth_sup:start_link(),
    ok = emqx_authz:init(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authn_utils:cleanup_resources(),
    ok = emqx_authz:deinit().
