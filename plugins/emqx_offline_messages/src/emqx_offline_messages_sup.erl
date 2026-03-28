%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_sup).

-include("emqx_offline_messages.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ChildSpecs = [
        emqx_metrics_worker:child_spec(?METRICS_WORKER),
        emqx_offline_messages:child_spec()
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, ChildSpecs}}.
