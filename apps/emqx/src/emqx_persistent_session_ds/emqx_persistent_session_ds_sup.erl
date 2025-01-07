%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_persistent_session_ds_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0
]).

%% `supervisor' API
-export([
    init/1
]).

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------------------
%% `supervisor' API
%%--------------------------------------------------------------------------------

init(Opts) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            do_init(Opts);
        false ->
            ignore
    end.

do_init(_Opts) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 2,
        auto_shutdown => never
    },
    CoreNodeChildren = [
        worker(session_gc_worker, emqx_persistent_session_ds_gc_worker, []),
        worker(message_gc_worker, emqx_persistent_message_ds_gc_worker, [])
    ],
    AnyNodeChildren = [
        worker(node_heartbeat, emqx_persistent_session_ds_node_heartbeat_worker, [])
    ],
    Children =
        case mria_rlog:role() of
            core -> CoreNodeChildren ++ AnyNodeChildren;
            replicant -> AnyNodeChildren
        end,
    {ok, {SupFlags, Children}}.

%%--------------------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------------------

worker(Id, Mod, Args) ->
    #{
        id => Id,
        start => {Mod, start_link, Args},
        type => worker,
        restart => permanent,
        shutdown => 10_000,
        significant => false
    }.
