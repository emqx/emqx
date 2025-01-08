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

-module(emqx_broker_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    forward/3,
    forward_async/3,
    list_client_subscriptions/2,
    list_subscriptions_via_topic/2
]).

-include("bpapi.hrl").
-include("emqx.hrl").

introduced_in() ->
    "5.0.0".

-spec forward(node(), emqx_types:topic(), emqx_types:delivery()) ->
    emqx_types:deliver_result()
    | emqx_rpc:badrpc().
forward(Node, Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    emqx_rpc:call(Topic, Node, emqx_broker, dispatch, [Topic, Delivery]).

-spec forward_async(node(), emqx_types:topic(), emqx_types:delivery()) -> true.
forward_async(Node, Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    emqx_rpc:cast(Topic, Node, emqx_broker, dispatch, [Topic, Delivery]).

-spec list_client_subscriptions(node(), emqx_types:clientid()) ->
    [{emqx_types:topic(), emqx_types:subopts()}]
    | emqx_rpc:badrpc().
list_client_subscriptions(Node, ClientId) ->
    rpc:call(Node, emqx_broker, subscriptions, [ClientId]).

-spec list_subscriptions_via_topic(node(), emqx_types:topic()) -> [emqx_types:subopts()].
list_subscriptions_via_topic(Node, Topic) ->
    rpc:call(Node, emqx_broker, subscriptions_via_topic, [Topic]).
