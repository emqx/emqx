%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_cm_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    takeover_session/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.3.2".

-doc """
Begin a session takeover with options for the owning channel. The session
module of the requesting channel sets the options; `session_format` selects
the layout of the returned session.
""".
-spec takeover_session(
    emqx_gateway_cm:gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_gateway_cm:takeover_opts()
) -> {ok, module(), pid(), _Session} | {error, _} | {badrpc, _}.
takeover_session(GwName, ClientId, ChanPid, Opts) ->
    rpc:call(node(ChanPid), emqx_gateway_cm, do_takeover_session, [GwName, ClientId, ChanPid, Opts]).
