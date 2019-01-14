%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_portal_sup).
-behavior(supervisor).

-export([start_link/0, start_link/1]).

-export([init/1]).

-define(SUP, ?MODULE).
-define(WORKER_SUP, emqx_portal_worker_sup).

start_link() -> start_link(?SUP).

start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, Name).

init(?SUP) ->
    Sp = fun(Name) ->
                 #{id => Name,
                   start => {?MODULE, start_link, [Name]},
                   restart => permanent,
                   shutdown => 5000,
                   type => supervisor,
                   modules => [?MODULE]
                  }
         end,
    {ok, {{one_for_one, 5, 10}, [Sp(?WORKER_SUP)]}};
init(?WORKER_SUP) ->
    BridgesConf = emqx_config:get_env(bridges, []),
    BridgesSpec = lists:map(fun portal_spec/1, BridgesConf),
    {ok, {{one_for_one, 10, 100}, BridgesSpec}}.

portal_spec({Name, Config}) ->
    #{id => Name,
      start => {emqx_portal, start_link, [Name, Config]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [emqx_portal]
     }.

