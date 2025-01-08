%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_sup).

-export([start_link/0]).

-export([start_gc/2]).

-behaviour(supervisor).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_gc(emqx_retainer:context(), emqx_retainer_gc:opts()) ->
    supervisor:startchild_ret().
start_gc(Context, Opts) ->
    ChildSpec = #{
        id => gc,
        start => {emqx_retainer_gc, start_link, [Context, Opts]},
        restart => temporary,
        type => worker
    },
    supervisor:start_child(?MODULE, ChildSpec).

%%

init([]) ->
    PoolSpec = emqx_pool_sup:spec([
        emqx_retainer_dispatcher,
        hash,
        emqx_vm:schedulers(),
        {emqx_retainer_dispatcher, start_link, []}
    ]),
    {ok,
        {{one_for_one, 10, 3600}, [
            #{
                id => retainer,
                start => {emqx_retainer, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_retainer]
            },
            PoolSpec
        ]}}.
