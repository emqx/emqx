%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_sup).

-behaviour(supervisor).

-include("rule_engine.hrl").

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    _ = ets:new(?RULE_TAB, [named_table, public, ordered_set, {read_concurrency, true}]),
    _ = ets:new(?RULE_TOPIC_INDEX, [named_table, public, ordered_set, {read_concurrency, true}]),
    _ = ets:new(?KV_TAB, [
        named_table,
        set,
        public,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    RuleEngineRegistry = worker_spec(emqx_rule_engine),
    RuleEngineMetrics = emqx_metrics_worker:child_spec(rule_metrics),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    Children = [
        RuleEngineRegistry,
        RuleEngineMetrics
    ],
    {ok, {SupFlags, Children}}.

worker_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker,
        modules => [Mod]
    }.
