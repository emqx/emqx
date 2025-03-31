%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Workers =
        case os:type() of
            {unix, linux} ->
                [child_spec(emqx_mgmt_cache, 5000, worker)];
            _ ->
                []
        end,
    Cluster = child_spec(emqx_mgmt_cluster, 5000, worker),
    {ok, {{one_for_one, 1, 5}, [Cluster | Workers]}}.

child_spec(Mod, Shutdown, Type) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => Shutdown,
        type => Type,
        modules => [Mod]
    }.
