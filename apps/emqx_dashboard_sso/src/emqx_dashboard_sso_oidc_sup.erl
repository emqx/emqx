%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    %% The oidcc worker has its own soft-failure backoff (random 5-10 s) for
    %% unreachable or malformed providers; this intensity budget only burns
    %% when the worker actually crashes, and is sized to survive a handful of
    %% such crashes within a minute before giving up.
    {ok, {{one_for_one, 10, 60}, []}}.
