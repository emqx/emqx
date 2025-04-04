%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_modules_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod), #{
    id => Mod,
    start => {Mod, start_link, []},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [Mod]
}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------
init([]) ->
    {ok,
        {{one_for_one, 10, 3600}, [
            ?CHILD(emqx_topic_metrics),
            ?CHILD(emqx_trace),
            ?CHILD(emqx_delayed)
        ]}}.
