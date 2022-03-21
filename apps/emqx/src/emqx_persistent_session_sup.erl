%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% We want this supervisor to own the table for restarts
    SessionTab = emqx_session_router:create_init_tab(),

    %% Resume worker sup
    ResumeSup = #{
        id => router_worker_sup,
        start => {emqx_session_router_worker_sup, start_link, [SessionTab]},
        restart => permanent,
        shutdown => 2000,
        type => supervisor,
        modules => [emqx_session_router_worker_sup]
    },

    SessionRouterPool = emqx_pool_sup:spec(
        session_router_pool,
        [
            session_router_pool,
            hash,
            {emqx_session_router, start_link, []}
        ]
    ),

    GCWorker = child_spec(emqx_persistent_session_gc, worker),

    Spec = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },

    {ok, {Spec, [ResumeSup, SessionRouterPool, GCWorker]}}.

child_spec(Mod, worker) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 15000,
        type => worker,
        modules => [Mod]
    }.
