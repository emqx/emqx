%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% for test
-export([restart_flapping/0]).

-include("emqx_cm.hrl").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    ok = mria:wait_for_tables(emqx_banned:create_tables()),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },
    Banned = child_spec(emqx_banned, 1000, worker),
    Flapping = child_spec(emqx_flapping, 1000, worker),
    Locker = child_spec(emqx_cm_locker, 5000, worker),
    CmPool = emqx_pool_sup:spec(emqx_cm_pool_sup, [?CM_POOL, random, {emqx_pool, start_link, []}]),
    Registry = child_spec(emqx_cm_registry, 5000, worker),
    RegistryKeeper = child_spec(emqx_cm_registry_keeper, 5000, worker),
    Manager = child_spec(emqx_cm, 5000, worker),
    DSSessionSup = child_spec(emqx_persistent_session_ds_sup, infinity, supervisor),
    DSSessionBookkeeper = child_spec(emqx_persistent_session_bookkeeper, 5_000, worker),
    Children =
        [
            Banned,
            Flapping,
            Locker,
            CmPool,
            Registry,
            RegistryKeeper,
            Manager,
            DSSessionSup,
            DSSessionBookkeeper
        ],
    {ok, {SupFlags, Children}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

child_spec(Mod, Shutdown, Type) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => Shutdown,
        type => Type,
        modules => [Mod]
    }.

restart_flapping() ->
    ok = supervisor:terminate_child(?MODULE, emqx_flapping),
    {ok, _} = supervisor:restart_child(?MODULE, emqx_flapping),
    ok.
