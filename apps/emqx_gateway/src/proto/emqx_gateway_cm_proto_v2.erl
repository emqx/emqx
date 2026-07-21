%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_cm_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_chan_info/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.0.4".

-spec get_chan_info(emqx_gateway_cm:gateway_name(), emqx_types:clientid(), pid(), timeout()) ->
    emqx_types:infos() | undefined | {badrpc, _}.
get_chan_info(GwName, ClientId, ChanPid, Timeout) ->
    rpc:call(
        node(ChanPid), emqx_gateway_cm, do_get_chan_info, [GwName, ClientId, ChanPid], Timeout
    ).
