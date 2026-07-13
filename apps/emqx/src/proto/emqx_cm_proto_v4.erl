%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_proto_v4).

-moduledoc """
Changes since v3:

1. Takeover RPCs now carry information of the takeover protocol supported
   by the requester. This is a forward-compatibility measure. Affects:
   * `takeover_begin/3`
   * `takeover_finish/3`
""".

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    lookup_client/2,
    kickout_client/2,

    get_chan_stats/2,
    get_chan_info/2,
    get_chann_conn_mod/2,

    takeover_begin/3,
    takeover_finish/3,

    kick_session/3,
    takeover_kick_session/2
]).

-include("bpapi.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

introduced_in() ->
    "6.3.0".

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

-spec takeover_begin(
    emqx_types:clientid(),
    emqx_cm:chan_pid(),
    emqx_cm_takeover:protocol()
) ->
    {ok, emqx_cm_takeover:channelref(), emqx_cm_takeover:state()}
    | none.
takeover_begin(ClientId, ChanPid, Protocol) ->
    erpc:call(
        node(ChanPid),
        emqx_cm_takeover,
        begin_rpc,
        [ClientId, ChanPid, Protocol],
        ?T_TAKEOVER * 2
    ).

-spec takeover_finish(
    module(),
    emqx_cm:chan_pid(),
    emqx_cm_takeover:protocol()
) ->
    {ok, list(emqx_types:deliver())}
    | {error, term()}.
takeover_finish(ConnMod, ChanPid, Protocol) ->
    erpc:call(
        node(ChanPid),
        emqx_cm_takeover,
        finish_rpc,
        [ConnMod, ChanPid, Protocol],
        ?T_TAKEOVER * 2
    ).

-spec kick_session(kick | discard, emqx_types:clientid(), emqx_cm:chan_pid()) -> ok | {badrpc, _}.
kick_session(Action, ClientId, ChanPid) ->
    rpc:call(node(ChanPid), emqx_cm, do_kick_session, [Action, ClientId, ChanPid], ?T_KICK * 2).

-spec takeover_kick_session(emqx_types:clientid(), emqx_cm:chan_pid()) ->
    ok | {badrpc, _}.
takeover_kick_session(ClientId, ChanPid) ->
    rpc:call(
        node(ChanPid),
        emqx_cm,
        do_takeover_kick_session_v3,
        [ClientId, ChanPid],
        ?T_KICK * 2
    ).
