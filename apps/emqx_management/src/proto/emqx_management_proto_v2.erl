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

-module(emqx_management_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    unsubscribe_batch/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.1".

-spec unsubscribe_batch(node(), emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe, _} | {error, _} | {badrpc, _}.
unsubscribe_batch(Node, ClientId, Topics) ->
    rpc:call(Node, emqx_mgmt, do_unsubscribe_batch, [ClientId, Topics]).
