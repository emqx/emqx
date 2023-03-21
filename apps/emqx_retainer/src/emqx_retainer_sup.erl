%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(supervisor).

-include("emqx_retainer.hrl").

-export([start_link/1]).
-export([init/1]).

start_link(Env) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, [Env]).

init([Env]) ->
    Retainer = #{
        id => retainer,
        start => {emqx_retainer, start_link, [Env]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_retainer]
    },
    WorkerPool = #{
      id => ?POOL,
      start => {emqx_pool_sup, start_link, [?POOL, random, {emqx_pool, start_link, []}]},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => [emqx_pool_sup]
    },
    ChildSpecs = case is_managed_by_modules() of
        false -> [Retainer, WorkerPool];
        true -> [WorkerPool]
    end,
	{ok, {{one_for_one, 10, 3600}, ChildSpecs}}.

-ifdef(EMQX_ENTERPRISE).

is_managed_by_modules() ->
    try
        case supervisor:get_childspec(emqx_modules_sup, emqx_retainer) of
            {ok, _} -> true;
            _ -> false
        end
    catch
        exit : {noproc, _} ->
            false
    end.

-else.

is_managed_by_modules() ->
    %% always false for opensource edition
    false.

-endif.
