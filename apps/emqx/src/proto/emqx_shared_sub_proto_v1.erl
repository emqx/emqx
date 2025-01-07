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
-module(emqx_shared_sub_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    send/4,
    dispatch_with_ack/5
]).

-include("bpapi.hrl").

%%================================================================================
%% behaviour callbacks
%%================================================================================

introduced_in() ->
    "5.0.8".

%%================================================================================
%% API functions
%%================================================================================

-spec send(node(), pid(), emqx_types:topic(), term()) -> true.
send(Node, Pid, Topic, Msg) ->
    emqx_rpc:cast(Topic, Node, erlang, send, [Pid, Msg]).

-spec dispatch_with_ack(
    pid(), emqx_types:group(), emqx_types:topic(), emqx_types:message(), timeout()
) ->
    ok | {error, _}.
dispatch_with_ack(Pid, Group, Topic, Msg, Timeout) ->
    emqx_rpc:call(
        Topic, node(Pid), emqx_shared_sub, do_dispatch_with_ack, [Pid, Group, Topic, Msg], Timeout
    ).
