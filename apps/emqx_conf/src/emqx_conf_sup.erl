%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_sup).

-behaviour(supervisor).

-export([start_link/0, on_run_level/2, start_link_rpc/0]).

-export([init/1]).

-define(top, ?MODULE).
-define(cluster_rpc, emqx_conf_sup_cluster_rpc).

start_link() ->
    supervisor:start_link({local, ?top}, ?MODULE, ?top).

start_link_rpc() ->
    supervisor:start_link({local, ?cluster_rpc}, ?MODULE, ?cluster_rpc).

on_run_level(single, cluster) ->
    Result = supervisor:start_child(
        ?top,
        #{
            id => ?cluster_rpc,
            start => {?MODULE, start_link_rpc, []},
            type => supervisor,
            shutdown => infinity,
            modules => [?MODULE]
        }
    ),
    case Result of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, already_present} ->
            {ok, _} = supervisor:restart_child(?top, ?cluster_rpc),
            ok
    end;
on_run_level(cluster, single) ->
    ok = supervisor:terminate_child(?top, ?cluster_rpc),
    ok = supervisor:delete_child(?top, ?cluster_rpc);
on_run_level(_, _) ->
    ok.

init(?top) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 100
    },
    {ok, {SupFlags, []}};
init(?cluster_rpc) ->
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
