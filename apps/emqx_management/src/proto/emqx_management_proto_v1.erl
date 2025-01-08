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

-module(emqx_management_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    node_info/1,
    broker_info/1,
    list_subscriptions/1,

    list_listeners/1,
    subscribe/3,
    unsubscribe/3,

    call_client/3,

    get_full_config/1
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec node_info(node()) -> map() | {badrpc, _}.
node_info(Node) ->
    rpc:call(Node, emqx_mgmt, node_info, []).

-spec broker_info(node()) -> map() | {badrpc, _}.
broker_info(Node) ->
    rpc:call(Node, emqx_mgmt, broker_info, []).

-spec list_subscriptions(node()) -> [map()] | {badrpc, _}.
list_subscriptions(Node) ->
    rpc:call(Node, emqx_mgmt, do_list_subscriptions, []).

-spec list_listeners(node()) -> map() | {badrpc, _}.
list_listeners(Node) ->
    rpc:call(Node, emqx_mgmt_api_listeners, do_list_listeners, []).

-spec subscribe(node(), emqx_types:clientid(), emqx_types:topic_filters()) ->
    {subscribe, _} | {error, atom()} | {badrpc, _}.
subscribe(Node, ClientId, TopicTables) ->
    rpc:call(Node, emqx_mgmt, do_subscribe, [ClientId, TopicTables]).

-spec unsubscribe(node(), emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, _} | {badrpc, _}.
unsubscribe(Node, ClientId, Topic) ->
    rpc:call(Node, emqx_mgmt, do_unsubscribe, [ClientId, Topic]).

-spec call_client(node(), emqx_types:clientid(), term()) -> term().
call_client(Node, ClientId, Req) ->
    rpc:call(Node, emqx_mgmt, do_call_client, [ClientId, Req]).

-spec get_full_config(node()) -> map() | list() | {badrpc, _}.
get_full_config(Node) ->
    rpc:call(Node, emqx_mgmt_api_configs, get_full_config, []).
