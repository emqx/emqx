%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok,
        {{one_for_one, 10, 100}, [
            sup_spec(emqx_dashboard_sso_oidc_sup),
            child_spec(emqx_dashboard_sso_manager, permanent)
        ]}}.

sup_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [Mod]
    }.

child_spec(Mod, Restart) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => Restart,
        shutdown => 15000,
        type => worker,
        modules => [Mod]
    }.
