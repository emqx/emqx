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

-module(emqx_delayed_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_delayed_message/2,
    delete_delayed_message/2,
    clean_by_clientid/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.0.10".

-spec get_delayed_message(node(), binary()) ->
    emqx_delayed:with_id_return(map()) | emqx_rpc:badrpc().
get_delayed_message(Node, Id) ->
    rpc:call(Node, emqx_delayed, get_delayed_message, [Id]).

-spec delete_delayed_message(node(), binary()) -> emqx_delayed:with_id_return() | emqx_rpc:badrpc().
delete_delayed_message(Node, Id) ->
    rpc:call(Node, emqx_delayed, delete_delayed_message, [Id]).

-spec clean_by_clientid(list(node()), emqx_types:clientid()) ->
    emqx_rpc:erpc_multicall().
clean_by_clientid(Nodes, ClientID) ->
    erpc:multicall(Nodes, emqx_delayed, do_clean_by_clientid, [ClientID], ?TIMEOUT).
