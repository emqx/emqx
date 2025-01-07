%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_oidc_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/2, stop_child/1]).

-export([init/1]).

-define(CHILD(I, Args, Restart), {I, {I, start_link, Args}, Restart, 5000, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Mod, Args) ->
    supervisor:start_child(?MODULE, ?CHILD(Mod, Args, transient)).

stop_child(Mod) ->
    _ = supervisor:terminate_child(?MODULE, Mod),
    _ = supervisor:delete_child(?MODULE, Mod),
    ok.

init([]) ->
    {ok, {{one_for_one, 0, 1}, []}}.
