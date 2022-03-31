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

-module(emqx_persistent_session_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    resume_begin/3,
    resume_end/3
]).

-include("bpapi.hrl").
-include("emqx.hrl").

introduced_in() ->
    "5.0.0".

-spec resume_begin([node()], pid(), binary()) ->
    emqx_rpc:erpc_multicall([{node(), emqx_guid:guid()}]).
resume_begin(Nodes, Pid, SessionID) when is_pid(Pid), is_binary(SessionID) ->
    erpc:multicall(Nodes, emqx_session_router, resume_begin, [Pid, SessionID]).

-spec resume_end([node()], pid(), binary()) ->
    emqx_rpc:erpc_multicall({'ok', [emqx_types:message()]} | {'error', term()}).
resume_end(Nodes, Pid, SessionID) when is_pid(Pid), is_binary(SessionID) ->
    erpc:multicall(Nodes, emqx_session_router, resume_end, [Pid, SessionID]).
