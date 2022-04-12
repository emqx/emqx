%%--------------------------------------------------------------------
%%Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_telemetry_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_node_uuid/1,
    get_cluster_uuid/1
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec get_node_uuid(node()) -> {ok, binary()} | emqx_rpc:badrpc().
get_node_uuid(Node) ->
    rpc:call(Node, emqx_telemetry, get_node_uuid, []).

-spec get_cluster_uuid(node()) -> {ok, binary()} | emqx_rpc:badrpc().
get_cluster_uuid(Node) ->
    rpc:call(Node, emqx_telemetry, get_cluster_uuid, []).
