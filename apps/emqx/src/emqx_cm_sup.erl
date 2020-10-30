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

-module(emqx_cm_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Banned = #{id => banned,
               start => {emqx_banned, start_link, []},
               restart => permanent,
               shutdown => 1000,
               type => worker,
               modules => [emqx_banned]},
    Flapping = #{id => flapping,
                 start => {emqx_flapping, start_link, []},
                 restart => permanent,
                 shutdown => 1000,
                 type => worker,
                 modules => [emqx_flapping]},
    %% Channel locker
    Locker = #{id => locker,
               start => {emqx_cm_locker, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker,
               modules => [emqx_cm_locker]
              },
    %% Channel registry
    Registry = #{id => registry,
                 start => {emqx_cm_registry, start_link, []},
                 restart => permanent,
                 shutdown => 5000,
                 type => worker,
                 modules => [emqx_cm_registry]
                },
    %% Channel Manager
    Manager = #{id => manager,
                start => {emqx_cm, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_cm]
               },
    SupFlags = #{strategy => one_for_one,
                 intensity => 100,
                 period => 10
                },
    {ok, {SupFlags, [Banned, Flapping, Locker, Registry, Manager]}}.

