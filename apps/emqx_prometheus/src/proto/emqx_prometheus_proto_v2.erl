%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_prometheus_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    start/1,
    stop/1,

    raw_prom_data/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.5.0".

-spec start([node()]) -> emqx_rpc:multicall_result().
start(Nodes) ->
    rpc:multicall(Nodes, emqx_prometheus, do_start, [], 5000).

-spec stop([node()]) -> emqx_rpc:multicall_result().
stop(Nodes) ->
    rpc:multicall(Nodes, emqx_prometheus, do_stop, [], 5000).

-type key() :: atom().
-type arg() :: list(term()).

-spec raw_prom_data([node()], key(), key(), arg()) -> emqx_rpc:erpc_multicall(term()).
raw_prom_data(Nodes, M, F, A) ->
    erpc:multicall(
        Nodes,
        emqx_prometheus_api,
        lookup_from_local_nodes,
        [M, F, A],
        5000
    ).
