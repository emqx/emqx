%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_node_rebalance_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

start_link(Env) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, [Env]).

init([_Env]) ->
    Childs = [child_spec(emqx_node_rebalance_evacuation, []),
              child_spec(emqx_node_rebalance_agent, []),
              child_spec(emqx_node_rebalance, [])],
    {ok, {
       {one_for_one, 10, 3600},
       Childs}
    }.

child_spec(Mod, Args) ->
    #{id => Mod,
      start => {Mod, start_link, Args},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [Mod]
     }.
