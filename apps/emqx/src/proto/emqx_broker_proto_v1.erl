%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ introduced_in/0

        , forward/3
        , forward_async/3
        , client_subscriptions/2

        , lookup_client/2
        , kickout_client/2
        ]).

-include("bpapi.hrl").
-include("emqx.hrl").

introduced_in() ->
    "5.0.0".

-spec forward(node(), emqx_types:topic(), emqx_types:delivery()) -> emqx_types:deliver_result()
                                                                  | emqx_rpc:badrpc().
forward(Node, Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    emqx_rpc:call(Topic, Node, emqx_broker, dispatch, [Topic, Delivery]).

-spec forward_async(node(), emqx_types:topic(), emqx_types:delivery()) -> true.
forward_async(Node, Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    emqx_rpc:cast(Topic, Node, emqx_broker, dispatch, [Topic, Delivery]).

-spec client_subscriptions(node(), emqx_types:clientid()) ->
                [{emqx_types:topic(), emqx_types:subopts()}]
              | emqx_rpc:badrpc().
client_subscriptions(Node, ClientId) ->
    rpc:call(Node, emqx_broker, subscriptions, [ClientId]).

-spec kickout_client(node(), emqx_types:clientid()) -> ok | {badrpc, _}.
kickout_client(Node, ClientId) ->
    rpc:call(Node, emqx_cm, kick_session, [ClientId]).

-spec lookup_client(node(), {clientid, emqx_types:clientid()} | {username, emqx_types:username()}) ->
          [emqx_cm:channel_info()] | {badrpc, _}.
lookup_client(Node, Key) ->
    rpc:call(Node, emqx_cm, lookup_client, [Key]).
