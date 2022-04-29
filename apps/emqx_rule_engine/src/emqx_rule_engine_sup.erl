%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_sup).

-behaviour(supervisor).

-include("rule_engine.hrl").

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Registry = #{
        id => emqx_rule_engine,
        start => {emqx_rule_engine, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_rule_engine]
    },
    Metrics = emqx_metrics_worker:child_spec(rule_metrics),
    {ok, {{one_for_one, 10, 10}, [Registry, Metrics]}}.
