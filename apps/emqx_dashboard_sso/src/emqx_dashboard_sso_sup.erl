%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/2, stop_child/1]).

-export([init/1]).

-define(CHILD(I, Args), {I, {I, start_link, Args}, permanent, 5000, worker, [I]}).
-define(CHILD(I), ?CHILD(I, [])).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Mod, Args) ->
    supervisor:start_child(?MODULE, ?CHILD(Mod, Args)).

stop_child(Mod) ->
    supervisor:terminate_child(?MODULE, Mod).

init([]) ->
    {ok,
        {{one_for_one, 5, 100}, [
            ?CHILD(emqx_dashboard_sso_manager)
        ]}}.
