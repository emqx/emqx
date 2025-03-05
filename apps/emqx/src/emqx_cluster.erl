%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cluster).

-export([
    join/1,
    leave/0,
    force_leave/1,
    ensure_normal_mode/0,
    ensure_singleton_mode/0
]).

-define(CLUSTER_MODE_NORMAL, normal).
-define(CLUSTER_MODE_SINGLE, singleton).

%% Allow cluster when running tests
-ifdef(TEST).
-define(DEFAULT_MODE, ?CLUSTER_MODE_NORMAL).
-else.
-define(DEFAULT_MODE, ?CLUSTER_MODE_SINGLE).
-endif.

join(Node) ->
    case is_single_node_mode() of
        true ->
            {error, single_node_mode};
        false ->
            ekka:join(Node)
    end.

leave() ->
    ekka:leave().

force_leave(Node) ->
    ekka:force_leave(Node).

is_single_node_mode() ->
    case application:get_env(emqx, cluster_mode, ?DEFAULT_MODE) of
        ?CLUSTER_MODE_SINGLE -> true;
        _ -> false
    end.

%% @doc Set the cluster mode to single node mode.
%% Called by license checker for community license.
ensure_singleton_mode() ->
    ensure_mode(?DEFAULT_MODE).

%% @doc Allow clustering.
ensure_normal_mode() ->
    ensure_mode(?CLUSTER_MODE_NORMAL).

ensure_mode(Mode) ->
    _ = application:set_env(emqx, cluster_mode, Mode),
    ok.
