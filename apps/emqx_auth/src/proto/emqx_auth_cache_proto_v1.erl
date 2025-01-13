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

-module(emqx_auth_cache_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    metrics/2,
    reset/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.9.0".

-spec metrics([node()], emqx_auth_cache:name()) ->
    emqx_rpc:erpc_multicall({node(), map()}).
metrics(Nodes, Name) ->
    erpc:multicall(Nodes, emqx_auth_cache, metrics_v1, [Name], ?TIMEOUT).

-spec reset([node()], emqx_auth_cache:name()) ->
    emqx_rpc:erpc_multicall(ok).
reset(Nodes, Name) ->
    erpc:multicall(Nodes, emqx_auth_cache, reset_v1, [Name], ?TIMEOUT).
