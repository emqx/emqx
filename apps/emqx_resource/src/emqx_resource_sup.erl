%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_resource_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(RESOURCE_INST_MOD, emqx_resource_instance).
-define(POOL_SIZE, 64). %% set a very large pool size in case all the workers busy

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    TabOpts = [named_table, set, public, {read_concurrency, true}],
    _ = ets:new(emqx_resource_instance, TabOpts),

    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Pool = ?RESOURCE_INST_MOD,
    Mod = ?RESOURCE_INST_MOD,
    ensure_pool(Pool, hash, [{size, ?POOL_SIZE}]),
    {ok, {SupFlags, [
        begin
            ensure_pool_worker(Pool, {Pool, Idx}, Idx),
            #{id => {Mod, Idx},
              start => {Mod, start_link, [Pool, Idx]},
              restart => transient,
              shutdown => 5000, type => worker, modules => [Mod]}
        end || Idx <- lists:seq(1, ?POOL_SIZE)]}}.

%% internal functions
ensure_pool(Pool, Type, Opts) ->
    try gproc_pool:new(Pool, Type, Opts)
    catch
        error:exists -> ok
    end.

ensure_pool_worker(Pool, Name, Slot) ->
    try gproc_pool:add_worker(Pool, Name, Slot)
    catch
        error:exists -> ok
    end.