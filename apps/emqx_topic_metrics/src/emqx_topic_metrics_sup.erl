%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_sup).

-behaviour(supervisor).

-include("emqx_topic_metrics.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The supervisor itself creates and owns the local ETS tables, so a
%% registry-process crash doesn't take the tables (and every reader's
%% lookups) down with it. The registry process only rehydrates the
%% tables from mria on init and otherwise stays out of the read path.
init([]) ->
    _ = ets:new(?REGISTRY_TAB, [
        ordered_set, named_table, public, {read_concurrency, true}
    ]),
    _ = ets:new(?INDEX_TAB, [
        ordered_set, named_table, public, {read_concurrency, true}
    ]),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 3600
    },
    ChildSpecs = [
        #{
            id => registry,
            start => {emqx_topic_metrics_registry, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_topic_metrics_registry]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
