%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([
    get_routing_schema_vsn/1
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 3_000).

introduced_in() ->
    "5.6.0".

-spec get_routing_schema_vsn([node()]) ->
    [emqx_rpc:erpc(emqx_router:schemavsn())].
get_routing_schema_vsn(Nodes) ->
    erpc:multicall(Nodes, emqx_router, get_schema_vsn, [], ?TIMEOUT).
