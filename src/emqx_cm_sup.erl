%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sm_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Session locker
    Locker = #{id => locker,
               start => {emqx_sm_locker, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker,
               modules => [emqx_sm_locker]
              },
    %% Session registry
    Registry = #{id => registry,
                 start => {emqx_sm_registry, start_link, []},
                 restart => permanent,
                 shutdown => 5000,
                 type => worker,
                 modules => [emqx_sm_registry]
                },
    %% Session Manager
    Manager = #{id => manager,
                start => {emqx_sm, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_sm]
               },
    %% Session Sup
    SessSpec = #{start => {emqx_session, start_link, []},
                 shutdown => brutal_kill,
                 clean_down => fun emqx_sm:clean_down/1
                },
    SessionSup = #{id => session_sup,
                   start => {emqx_session_sup, start_link, [SessSpec ]},
                   restart => transient,
                   shutdown => infinity,
                   type => supervisor,
                   modules => [emqx_session_sup]
                  },
    {ok, {{rest_for_one, 10, 3600}, [Locker, Registry, Manager, SessionSup]}}.

