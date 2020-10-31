%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Opts = [public, named_table, set, {read_concurrency, true}],
    ets:new(?ACTION_INST_PARAMS_TAB, [{keypos, #action_instance_params.id}|Opts]),
    ets:new(?RES_PARAMS_TAB, [{keypos, #resource_params.id}|Opts]),
    Registry = #{id => emqx_rule_registry,
                 start => {emqx_rule_registry, start_link, []},
                 restart => permanent,
                 shutdown => 5000,
                 type => worker,
                 modules => [emqx_rule_registry]},
    Metrics = #{id => emqx_rule_metrics,
                start => {emqx_rule_metrics, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_rule_metrics]},
    {ok, {{one_for_all, 10, 100}, [Registry, Metrics]}}.

