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
-module(emqx_resource_manager_sup).

-behaviour(supervisor).

-export([ensure_child/6]).

-export([start_link/0]).

-export([init/1]).

ensure_child(MgrId, ResId, Group, ResourceType, Config, Opts) ->
    _ = supervisor:start_child(?MODULE, [MgrId, ResId, Group, ResourceType, Config, Opts]),
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    TabOpts = [named_table, set, public, {read_concurrency, true}],
    _ = ets:new(emqx_resource_manager, TabOpts),

    ChildSpecs = [
        #{
            id => emqx_resource_manager,
            start => {emqx_resource_manager, start_link, []},
            restart => transient,
            shutdown => brutal_kill,
            type => worker,
            modules => [emqx_resource_manager]
        }
    ],

    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    {ok, {SupFlags, ChildSpecs}}.
