%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_username_quota_sup:start_link(),
    ok = emqx_username_quota:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_username_quota:unhook().
