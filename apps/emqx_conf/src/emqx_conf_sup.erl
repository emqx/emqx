%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_sup).

-behaviour(supervisor).

-export([start_link/0, start_link_rpc/0]).
-export([on_run_level/2]).

-export([init/1]).

-include_lib("emqx/include/emqx_machine_rl.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, top).

start_link_rpc() ->
    supervisor:start_link({local, emqx_conf_rpc_sup}, ?MODULE, rpc).

on_run_level(enter, ?emqx_rl_cluster_rpc) ->
    ChildSpec = #{
        id => rpc,
        type => supervisor,
        start => {?MODULE, start_link_rpc, []},
        restart => permanent,
        shutdown => infinity
    },
    case supervisor:start_child(?SERVER, ChildSpec) of
        {ok, _} ->
            ok;
        {ok, _, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, already_present} ->
            supervisor:restart_child(?SERVER, rpc)
    end;
on_run_level(leave, ?emqx_rl_cluster_rpc) ->
    _ = supervisor:terminate_child(?SERVER, rpc),
    supervisor:delete_child(?SERVER, rpc);
on_run_level(_, _) ->
    ok.

init(top) ->
    %% Initially the application starts without the workers. The
    %% workers are grafted into the supervisiton tree at run level
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 100
    },
    {ok, {SupFlags, []}};
init(rpc) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 100
    },
    ChildSpecs =
        [
            child_spec(emqx_cluster_rpc, []),
            child_spec(emqx_cluster_rpc_cleaner, [])
        ],
    {ok, {SupFlags, ChildSpecs}}.

child_spec(Mod, Args) ->
    #{
        id => Mod,
        start => {Mod, start_link, Args},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [Mod]
    }.
