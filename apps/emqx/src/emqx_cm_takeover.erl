%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_takeover).

-include("emqx_cm.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    begin_/2,
    finish/1,
    begin_rpc/3,
    begin_rpc_legacy/2,
    begin_local/2,
    finish_rpc/3,
    finish_rpc_legacy/2,
    finish_local/2
]).

-export_type([
    protocol/0,
    channelref/0,
    state/0
]).

-export_type([session_legacy/0]).

-record(chanref, {
    proto :: local | protocol() | legacy,
    connmod :: module(),
    pid :: emqx_cm:chan_pid()
}).

-type protocol() :: #{vsn := pos_integer(), atom() := _}.
-type channelref() :: #chanref{}.
-type session() :: emqx_session_mem:exported().

-type state() :: session().

-type session_legacy() :: emqx_session_mem_compat:legacy_session().

-define(BPAPI, emqx_cm).
-define(BPAPI_VSN_BASELINE, 4).

-define(VSN_TAKEOVER, 1).

%% v3 nodes:
%% -> emqx_cm_proto_v3 -> emqx_cm:takeover_session/2 ->
%%    {living, _ConnMod :: atom(), pid(), emqx_session:session()}

-spec current() -> protocol().
current() ->
    #{vsn => ?VSN_TAKEOVER}.

-doc "Begin a two-phase session takeover process".
-spec begin_(emqx_types:clientid(), pid()) ->
    {ok, channelref(), session()} | none.
begin_(ClientId, ChanPid) when node(ChanPid) =:= node() ->
    begin_local(ClientId, ChanPid);
begin_(ClientId, ChanPid) ->
    TargetNode = node(ChanPid),
    case emqx_bpapi:supported_version(TargetNode, ?BPAPI) of
        Vsn when is_integer(Vsn), Vsn >= ?BPAPI_VSN_BASELINE ->
            RequesterProto = current(),
            ?tp(emqx_cm_takeover_begin, #{
                clientid => ClientId,
                target_node => TargetNode,
                requester_proto => RequesterProto
            }),
            Ret = emqx_cm_proto_v4:takeover_begin(ClientId, ChanPid, RequesterProto),
            from_begin_ret(Ret);
        _ ->
            ?tp(emqx_cm_takeover_begin_legacy, #{
                clientid => ClientId,
                target_node => TargetNode
            }),
            Ret = emqx_cm_proto_v3:takeover_session(ClientId, ChanPid),
            upgrade_begin_ret(Ret)
    end.

-doc "Direct RPC target for `emqx_cm_proto_v4:takeover_begin/3`.".
-spec begin_rpc(emqx_types:clientid(), pid(), protocol()) ->
    {ok, channelref(), session()} | none.
begin_rpc(ClientId, ChanPid, RequesterProto) ->
    ?tp(emqx_cm_takeover_begin_rpc, #{
        clientid => ClientId,
        chanpid => ChanPid,
        requester_proto => RequesterProto
    }),
    Ret = begin_local(ClientId, ChanPid),
    to_begin_ret(RequesterProto, Ret).

-doc """
Indirect RPC target for `emqx_cm_proto_v{1..3}:takeover_session/2`.
See `emqx_cm:takeover_session/2`.
""".
-spec begin_rpc_legacy(emqx_types:clientid(), pid()) ->
    {living, module(), emqx_cm:chan_pid(), session_legacy()} | none.
begin_rpc_legacy(ClientId, ChanPid) ->
    ?tp(emqx_cm_takeover_begin_rpc_legacy, #{
        clientid => ClientId,
        chanpid => ChanPid
    }),
    case emqx_cm:do_get_chan_info(ClientId, ChanPid) of
        undefined ->
            none;
        ChanInfo ->
            Ret = begin_local(ClientId, ChanPid),
            downgrade_begin_ret(ClientId, ChanInfo, Ret)
    end.

begin_local(ClientId, ChanPid) when node(ChanPid) =:= node() ->
    case emqx_cm:do_get_chann_conn_mod(ClientId, ChanPid) of
        undefined ->
            none;
        ConnMod when is_atom(ConnMod) ->
            ChanRef = #chanref{proto = local, connmod = ConnMod, pid = ChanPid},
            case emqx_cm:request_stepdown({takeover, 'begin'}, ConnMod, ChanPid, ?T_TAKEOVER) of
                {ok, Session} ->
                    {ok, ChanRef, Session};
                {error, _Reason} ->
                    none
            end
    end.

-doc "Adapt takeover result received from remote node".
from_begin_ret(none) ->
    none;
from_begin_ret({ok, _ChanRef, _Session} = Ret) ->
    %% NOTE
    %% Any logic regarding adapting response from nodes running older EMQX version
    %% (according to `ChanRef#chanref.proto`) goes here. Currently, this is a no-op.
    Ret.

upgrade_begin_ret(none) ->
    none;
upgrade_begin_ret({living, ConnMod, ChanPid, Session}) ->
    %% NOTE: Convert pre-6.3.0 `#session{}` record into "exported" form.
    ChanRef = #chanref{proto = legacy, connmod = ConnMod, pid = ChanPid},
    {ok, ChanRef, emqx_session_mem_compat:to_exported(Session)};
upgrade_begin_ret({expired, _} = Ret) ->
    %% NOTE: Unsupported pre-5.3.0 stuff.
    error({unsupported, Ret});
upgrade_begin_ret({persistent, _} = Ret) ->
    %% NOTE: Unsupported pre-5.3.0 stuff.
    error({unsupported, Ret}).

to_begin_ret(#{vsn := _}, {ok, ChanRef, Session}) ->
    {ok, ChanRef#chanref{proto = current()}, Session};
to_begin_ret(_RequesterProto, none) ->
    none.

downgrade_begin_ret(ClientId, ChanInfo, {ok, ChanRef, Session}) ->
    %% NOTE: Turn back into pre-6.3.0 `#session{}` record.
    #chanref{connmod = ConnMod, pid = ChanPid} = ChanRef,
    ClientInfo = (maps:get(clientinfo, ChanInfo))#{clientid => ClientId},
    {living, ConnMod, ChanPid, emqx_session_mem_compat:from_exported(pre63, ClientInfo, Session)};
downgrade_begin_ret(_ClientId, _ChanInfo, none) ->
    none.

%%

-doc """
Conclude a two-phase session takeover process, of a channel specified by `channelref()`
obtained through `begin_/2`.
""".
-spec finish(channelref()) ->
    {ok, _ReplayContext} | {error, _Reason}.
finish(#chanref{proto = local, connmod = ConnMod, pid = ChanPid}) when node(ChanPid) =:= node() ->
    finish_local(ConnMod, ChanPid);
finish(#chanref{proto = #{} = ServerProto, connmod = ConnMod, pid = ChanPid}) ->
    RequesterProto = current(),
    ?tp(emqx_cm_takeover_finish, #{
        target_node => node(ChanPid),
        target_proto => ServerProto,
        requester_proto => RequesterProto
    }),
    Ret = finish_remote(fun() ->
        emqx_cm_proto_v4:takeover_finish(ConnMod, ChanPid, RequesterProto)
    end),
    from_finish_ret(ServerProto, Ret);
finish(#chanref{proto = legacy, connmod = ConnMod, pid = ChanPid}) ->
    ?tp(emqx_cm_takeover_finish_legacy, #{target_node => node(ChanPid)}),
    Ret = finish_remote(fun() ->
        emqx_cm_proto_v3:takeover_finish(ConnMod, ChanPid)
    end),
    from_finish_ret(legacy, Ret).

%% The proto calls are erpc-backed: a node dying between takeover-begin and
%% takeover-end raises instead of returning, which would propagate through the
%% new channel's CONNECT rather than hit the session-open branches that degrade
%% to local-only replay.  Convert the raises to `{error, _}'.
finish_remote(ProtoCall) ->
    try
        ProtoCall()
    catch
        error:{erpc, Reason} ->
            {error, {erpc, Reason}};
        error:{exception, Reason, _Stack} ->
            {error, Reason}
    end.

-doc "Direct RPC target for `emqx_cm_proto_v4:takeover_finish/3`.".
-spec finish_rpc(module(), emqx_cm:chan_pid(), legacy | protocol()) ->
    {ok, _Pendings} | {error, term()}.
finish_rpc(ConnMod, ChanPid, RequesterProto) ->
    ?tp(emqx_cm_takeover_finish_rpc, #{
        chanpid => ChanPid,
        requester_proto => RequesterProto
    }),
    Ret = finish_local(ConnMod, ChanPid),
    to_finish_ret(RequesterProto, Ret).

-doc """
Indirect RPC target for `emqx_cm_proto_v{1..3}:takeover_finish/2`.
See `emqx_cm:takeover_finish/2`.
""".
-spec finish_rpc_legacy(module(), emqx_cm:chan_pid()) ->
    {ok, _Pendings} | {error, term()}.
finish_rpc_legacy(ConnMod, ChanPid) ->
    ?tp(emqx_cm_takeover_finish_rpc_legacy, #{chanpid => ChanPid}),
    Ret = finish_local(ConnMod, ChanPid),
    to_finish_ret(legacy, Ret).

-spec finish_local(module(), emqx_cm:chan_pid()) ->
    {ok, _ReplayContext} | {error, _Reason}.
finish_local(ConnMod, ChanPid) ->
    emqx_cm:request_stepdown({takeover, 'end'}, ConnMod, ChanPid, ?T_TAKEOVER).

from_finish_ret(_Proto, {ok, ReplayContext}) ->
    {ok, ReplayContext};
from_finish_ret(_Proto, {error, Reason}) ->
    {error, Reason}.

to_finish_ret(_Proto, {ok, ReplayContext}) ->
    {ok, ReplayContext};
to_finish_ret(_Proto, {error, Reason}) ->
    {error, Reason}.
