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

-module(emqx_cm_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,

    lookup_client/2,
    kickout_client/2,

    get_chan_stats/2,
    get_chan_info/2,
    get_chann_conn_mod/2,

    takeover_session/2,
    takeover_finish/2,
    kick_session/3
]).

-include("bpapi.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

introduced_in() ->
    "5.0.0".

deprecated_since() ->
    "5.7.0".

-spec kickout_client(node(), emqx_types:clientid()) -> ok | {badrpc, _}.
kickout_client(Node, ClientId) ->
    rpc:call(Node, emqx_cm, kick_session, [ClientId]).

-spec lookup_client(node(), {clientid, emqx_types:clientid()} | {username, emqx_types:username()}) ->
    [emqx_cm:channel_info()] | {badrpc, _}.
lookup_client(Node, Key) ->
    rpc:call(Node, emqx_cm, lookup_client, [Key]).

-spec get_chan_stats(emqx_types:clientid(), emqx_cm:chan_pid()) ->
    emqx_types:stats() | undefined | {badrpc, _}.
get_chan_stats(ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_cm, do_get_chan_stats, [ClientId, ChanPid], ?T_GET_INFO * 2).

-spec get_chan_info(emqx_types:clientid(), emqx_cm:chan_pid()) ->
    emqx_types:infos() | undefined | {badrpc, _}.
get_chan_info(ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_cm, do_get_chan_info, [ClientId, ChanPid], ?T_GET_INFO * 2).

-spec get_chann_conn_mod(emqx_types:clientid(), emqx_cm:chan_pid()) ->
    module() | undefined | {badrpc, _}.
get_chann_conn_mod(ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_cm, do_get_chann_conn_mod, [ClientId, ChanPid], ?T_GET_INFO * 2).

-spec takeover_session(emqx_types:clientid(), emqx_cm:chan_pid()) ->
    none
    | {living, _ConnMod :: atom(), emqx_cm:chan_pid(), emqx_session:session()}
    %% NOTE: v5.3.0
    | {living, _ConnMod :: atom(), emqx_session:session()}
    | {expired | persistent, emqx_session:session()}
    | {badrpc, _}.
takeover_session(ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_cm, takeover_session, [ClientId, ChanPid], ?T_TAKEOVER * 2).

-spec takeover_finish(module(), emqx_cm:chan_pid()) ->
    {ok, emqx_types:takeover_data()}
    | {ok, list(emqx_types:deliver())}
    | {error, term()}
    | {badrpc, _}.
takeover_finish(ConnMod, ChanPid) ->
    erpc:call(
        node(ChanPid),
        emqx_cm,
        takeover_finish,
        [ConnMod, ChanPid],
        ?T_TAKEOVER * 2
    ).

-spec kick_session(kick | discard, emqx_types:clientid(), emqx_cm:chan_pid()) -> ok | {badrpc, _}.
kick_session(Action, ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_cm, do_kick_session, [Action, ClientId, ChanPid], ?T_KICK * 2).
