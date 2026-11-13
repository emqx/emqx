%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_udf_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Children = [worker(emqx_rule_udf_store), worker(emqx_rule_udf_client)],
    {ok, {SupFlags, Children}}.

worker(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [Mod]
    }.
