%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_replay_local_store_sup).

-behavior(supervisor).

%% API:
-export([start_link/0, start_shard/1, stop_shard/1]).

%% behavior callbacks:
-export([init/1]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP, ?MODULE).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

-spec start_shard(emqx_replay:shard()) -> supervisor:startchild_ret().
start_shard(Shard) ->
    supervisor:start_child(?SUP, shard_child_spec(Shard)).

-spec stop_shard(emqx_replay:shard()) -> ok | {error, _}.
stop_shard(Shard) ->
    ok = supervisor:terminate_child(?SUP, Shard),
    ok = supervisor:delete_child(?SUP, Shard).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([]) ->
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec shard_child_spec(emqx_replay:shard()) -> supervisor:child_spec().
shard_child_spec(Shard) ->
    #{
        id => Shard,
        start => {emqx_replay_local_store, start_link, [Shard]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.
