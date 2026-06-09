%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok,
        {{one_for_one, 10, 3600}, [
            #{
                id => registry,
                start => {emqx_topic_metrics_registry, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_topic_metrics_registry]
            }
        ]}}.
