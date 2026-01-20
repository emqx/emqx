%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cm_proto_v4).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    takeover_session_with_context/3
]).

-include("bpapi.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

introduced_in() ->
    "6.1.1".

-spec takeover_session_with_context(emqx_types:clientid(), emqx_cm:chan_pid(), timeout()) ->
    emqx_cm:remote_ctx().
takeover_session_with_context(ClientId, ChanPid, Timeout) ->
    erpc:call(
        node(ChanPid), emqx_cm, takeover_session_with_context_v4, [ClientId, ChanPid], Timeout
    ).
