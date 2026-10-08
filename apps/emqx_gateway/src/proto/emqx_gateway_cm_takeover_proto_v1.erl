%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_cm_takeover_proto_v1).

-moduledoc """
Gateway takeover RPCs. Begin and finish carry the requester's takeover protocol
so the owner can adapt transferred session state and pending deliveries.
""".

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    takeover_begin/6,
    takeover_finish/6
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.3.2".

-spec takeover_begin(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_gateway_cm_takeover:request(),
    emqx_gateway_cm_takeover:protocol(),
    timeout()
) ->
    {ok, emqx_gateway_cm_takeover:channelref(), map()} | {error, term()}.
takeover_begin(GwName, ClientId, ChanPid, Request, Protocol, Timeout) ->
    erpc:call(
        node(ChanPid),
        emqx_gateway_cm_takeover,
        begin_rpc,
        [GwName, ClientId, ChanPid, Request, Protocol],
        Timeout
    ).

-spec takeover_finish(
    emqx_gateway_cm:gateway_name(),
    module(),
    pid(),
    emqx_gateway_cm_takeover:request(),
    emqx_gateway_cm_takeover:protocol(),
    timeout()
) ->
    {ok, [emqx_types:deliver()]} | {error, term()}.
takeover_finish(GwName, ConnMod, ChanPid, Request, Protocol, Timeout) ->
    erpc:call(
        node(ChanPid),
        emqx_gateway_cm_takeover,
        finish_rpc,
        [GwName, ConnMod, ChanPid, Request, Protocol],
        Timeout
    ).
