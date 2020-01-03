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

-module(emqx_sys_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Childs = [child_spec(emqx_sys),
              child_spec(emqx_sys_mon, [config(sysmon)]),
              child_spec(emqx_os_mon,  [config(os_mon)]),
              child_spec(emqx_vm_mon,  [config(vm_mon)])],
    {ok, {{one_for_one, 10, 100}, Childs}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

child_spec(Mod) ->
    child_spec(Mod, []).

child_spec(Mod, Args) ->
    #{id => Mod,
      start => {Mod, start_link, Args},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [Mod]
     }.

config(Name) -> emqx:get_env(Name, []).

