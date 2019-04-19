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
    FlappingOption = emqx_config:get_env(flapping_clean_interval, 3600000),
    Flapping = #{id => flapping,
                 start => {emqx_flapping, start_link, [FlappingOption]},
                 restart => permanent,
                 shutdown => 1000,
                 type => worker,
                 modules => [emqx_flapping]},
    Manager = #{id => manager,
                start => {emqx_cm, start_link, []},
                restart => permanent,
                shutdown => 2000,
                type => worker,
                modules => [emqx_cm]},
    SupFlags = #{strategy => one_for_one,
                 intensity => 100,
                 period => 10},
    {ok, {SupFlags, [Banned, Manager, Flapping]}}.
