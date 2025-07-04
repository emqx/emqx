%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

-include("emqx_dashboard.hrl").

start(_StartType, _StartArgs) ->
    Tables = lists:append([
        emqx_dashboard_admin:create_tables(),
        emqx_dashboard_token:create_tables(),
        emqx_dashboard_monitor:create_tables(),
        emqx_dashboard_login_lock:create_tables()
    ]),
    ok = mria:wait_for_tables(Tables),
    {ok, Sup} = emqx_dashboard_sup:start_link(),
    case emqx_dashboard:start_listeners() of
        ok ->
            emqx_dashboard_cli:load(),
            {ok, _} = emqx_dashboard_admin:add_default_user(),
            {ok, Sup};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok = emqx_dashboard:stop_listeners(),
    emqx_dashboard_cli:unload(),
    ok.
