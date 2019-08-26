%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------

-module(ecpool_worker_sup).

-behaviour(supervisor).

-export([start_link/3]).

-export([init/1]).

start_link(Pool, Mod, Opts) when is_atom(Pool) ->
    supervisor:start_link(?MODULE, [Pool, Mod, Opts]).

init([Pool, Mod, Opts]) ->
    WorkerSpec = fun(Id) ->
                     #{id => {worker, Id},
                       start => {ecpool_worker, start_link, [Pool, Id, Mod, Opts]},
                       restart => transient,
                       shutdown => 5000,
                       type => worker,
                       modules => [ecpool_worker, Mod]}
                 end,
    Workers = [WorkerSpec(I) || I <- lists:seq(1, pool_size(Opts))],
    {ok, { {one_for_one, 10, 60}, Workers} }.

pool_size(Opts) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Opts, Schedulers).

