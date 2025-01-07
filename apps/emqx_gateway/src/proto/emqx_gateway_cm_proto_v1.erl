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

-module(emqx_gateway_cm_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    get_chan_info/3,
    set_chan_info/4,
    get_chan_stats/3,
    set_chan_stats/4,
    kick_session/4,
    get_chann_conn_mod/3,
    lookup_by_clientid/3,
    takeover_session/3,
    call/4,
    call/5,
    cast/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec lookup_by_clientid([node()], emqx_gateway_cm:gateway_name(), emqx_types:clientid()) ->
    emqx_rpc:multicall_result([pid()]).
lookup_by_clientid(Nodes, GwName, ClientId) ->
    rpc:multicall(Nodes, emqx_gateway_cm, do_lookup_by_clientid, [GwName, ClientId]).

-spec get_chan_info(emqx_gateway_cm:gateway_name(), emqx_types:clientid(), pid()) ->
    emqx_types:infos() | undefined | {badrpc, _}.
get_chan_info(GwName, ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_get_chan_info, [GwName, ClientId, ChanPid]).

-spec set_chan_info(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:infos()
) -> boolean() | {badrpc, _}.
set_chan_info(GwName, ClientId, ChanPid, Infos) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_set_chan_info, [GwName, ClientId, ChanPid, Infos]).

-spec get_chan_stats(emqx_gateway_cm:gateway_name(), emqx_types:clientid(), pid()) ->
    emqx_types:stats() | undefined | {badrpc, _}.
get_chan_stats(GwName, ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_get_chan_stats, [GwName, ClientId, ChanPid]).

-spec set_chan_stats(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:stats()
) -> boolean() | {badrpc, _}.
set_chan_stats(GwName, ClientId, ChanPid, Stats) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_set_chan_stats, [GwName, ClientId, ChanPid, Stats]).

-spec kick_session(
    emqx_gateway_cm:gateway_name(),
    kick | discard,
    emqx_types:clientid(),
    pid()
) -> _.
kick_session(GwName, Action, ClientId, ChanPid) ->
    rpc:call(
        node(ChanPid),
        emqx_gateway_cm,
        do_kick_session,
        [GwName, Action, ClientId, ChanPid]
    ).

-spec get_chann_conn_mod(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid()
) -> atom() | {badrpc, _}.
get_chann_conn_mod(GwName, ClientId, ChanPid) ->
    rpc:call(
        node(ChanPid),
        emqx_gateway_cm,
        do_get_chann_conn_mod,
        [GwName, ClientId, ChanPid]
    ).

-spec takeover_session(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid()
) -> boolean() | {badrpc, _}.
takeover_session(GwName, ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_takeover_session, [GwName, ClientId, ChanPid]).

-spec call(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    term(),
    timeout()
) -> term() | {badrpc, _}.
call(GwName, ClientId, ChanPid, Req, Timeout) ->
    rpc:call(
        node(ChanPid),
        emqx_gateway_cm,
        do_call,
        [GwName, ClientId, ChanPid, Req, Timeout]
    ).

-spec call(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    term()
) -> term() | {badrpc, _}.
call(GwName, ClientId, ChanPid, Req) ->
    rpc:call(
        node(ChanPid),
        emqx_gateway_cm,
        do_call,
        [GwName, ClientId, ChanPid, Req]
    ).

-spec cast(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    term()
) -> term() | {badrpc, _}.
cast(GwName, ClientId, ChanPid, Req) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_cast, [GwName, ClientId, ChanPid, Req]).
