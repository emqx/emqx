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
-module(emqx_mgmt_api_relup_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,
    run_upgrade/1,
    get_upgrade_status_from_nodes/1,
    get_upgrade_status/1
]).

-define(RPC_TIMEOUT_OP, 180_000).
-define(RPC_TIMEOUT_INFO, 15_000).

introduced_in() ->
    "5.8.0".

-spec run_upgrade([node()]) -> emqx_rpc:multicall_result().
run_upgrade(Nodes) ->
    rpc:multicall(Nodes, emqx_mgmt_api_relup, emqx_relup_upgrade, [], ?RPC_TIMEOUT_OP).

-spec get_upgrade_status_from_nodes([node()]) -> emqx_rpc:multicall_result().
get_upgrade_status_from_nodes(Nodes) ->
    rpc:multicall(Nodes, emqx_mgmt_api_relup, get_upgrade_status, [], ?RPC_TIMEOUT_INFO).

-spec get_upgrade_status(node()) -> emqx_rpc:call_result(map()).
get_upgrade_status(Node) ->
    rpc:call(Node, emqx_mgmt_api_relup, get_upgrade_status, [], ?RPC_TIMEOUT_INFO).
