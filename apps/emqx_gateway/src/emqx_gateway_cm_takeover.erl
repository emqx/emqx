%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_cm_takeover).
-moduledoc """
Gateway counterpart of `emqx_cm_takeover`.
""".

-export([
    current/0,

    begin_/5,
    finish/4,
    begin_rpc/5,
    begin_rpc_legacy/3,
    begin_local/4,
    finish_rpc/5,
    finish_local/3,

    from_session/3,
    to_session/4,
    to_legacy_reply/1
]).

-export_type([mode/0, protocol/0, channelref/0, request/0]).

-type gateway_name() :: emqx_gateway_cm:gateway_name().
-type mode() :: force | {resume, term()}.
-type protocol() :: #{vsn := pos_integer(), atom() := _}.
-type request() :: #{owner := pid(), attempt := reference(), mode := mode()}.

-record(chanref, {
    proto :: local | protocol() | legacy,
    connmod :: module(),
    pid :: pid()
}).

-type channelref() :: #chanref{}.
-type begin_result() :: {ok, channelref(), map()} | {error, term()}.

-define(BPAPI, emqx_gateway_cm_takeover).
-define(VSN_TAKEOVER, 1).
-define(T_TAKEOVER, 15000).
-define(RPC_TIMEOUT, ?T_TAKEOVER * 2).

-doc "Return the gateway takeover protocol supported by this node.".
-spec current() -> protocol().
current() ->
    #{vsn => ?VSN_TAKEOVER}.

-doc "Begin takeover with a caller-supplied attempt token.".
-spec begin_(gateway_name(), emqx_types:clientid(), pid(), mode(), reference()) ->
    begin_result().
begin_(GwName, ClientId, ChanPid, Mode, Attempt) when node(ChanPid) =:= node() ->
    begin_local(GwName, ClientId, ChanPid, takeover_request(Mode, Attempt));
begin_(GwName, ClientId, ChanPid, Mode, Attempt) ->
    Request = takeover_request(Mode, Attempt),
    case emqx_bpapi:supported_version(node(ChanPid), ?BPAPI) of
        Vsn when is_integer(Vsn) andalso Vsn >= 1 ->
            try
                emqx_gateway_cm_takeover_proto_v1:takeover_begin(
                    GwName,
                    ClientId,
                    ChanPid,
                    Request,
                    current(),
                    ?RPC_TIMEOUT
                )
            of
                Ret ->
                    from_begin_ret(GwName, Ret)
            catch
                error:{erpc, Reason} ->
                    {error, {erpc, Reason}};
                error:{exception, Reason, _Stack} ->
                    {error, Reason}
            end;
        _ ->
            from_begin_ret(GwName, begin_legacy(GwName, ClientId, ChanPid, Request))
    end.

-doc "Adapt received takeover back to the local requester protocol.".
from_begin_ret(GwName, {ok, Ref = #chanref{proto = Proto}, Data = #{session := Session}}) ->
    {ok, Ref, Data#{session := from_session(GwName, Proto, Session)}};
from_begin_ret(_GwName, {error, _} = Error) ->
    Error.

-doc """
Direct RPC target @ `emqx_gateway_cm_takeover_proto_v1:takeover_begin/6`.
Should respect requester protocol version.
""".
-spec begin_rpc(gateway_name(), emqx_types:clientid(), pid(), request(), protocol()) ->
    begin_result().
begin_rpc(GwName, ClientId, ChanPid, Request, RequesterProto) ->
    Ret = begin_local(GwName, ClientId, ChanPid, Request),
    to_begin_ret(RequesterProto, Ret).

-doc "Adapt RPC begin result to remote requester protocol / advertise server protocol in chanref.".
to_begin_ret(#{vsn := _}, {ok, Ref, Data}) ->
    {ok, Ref#chanref{proto = current()}, Data};
to_begin_ret(_RequesterProto, {error, _} = Error) ->
    Error.

-doc """
Begin takeover locally.
Supplies original requester identity in `Request` so it can be monitored.
""".
-spec begin_local(gateway_name(), emqx_types:clientid(), pid(), request()) ->
    begin_result().
begin_local(GwName, ClientId, ChanPid, Request) when node(ChanPid) =:= node() ->
    ConnMod = emqx_gateway_cm:do_get_chann_conn_mod(GwName, ClientId, ChanPid),
    ChanRef = #chanref{proto = local, connmod = ConnMod, pid = ChanPid},
    begin_channel(ChanRef, Request, begin_call(Request)).

begin_legacy(GwName, ClientId, ChanPid, #{mode := force}) ->
    case emqx_gateway_cm_proto_v1:takeover_session(GwName, ClientId, ChanPid) of
        {ok, ConnMod, ChanPid, Session} ->
            ChanRef = #chanref{proto = legacy, connmod = ConnMod, pid = ChanPid},
            {ok, ChanRef, #{session => Session}};
        {error, _} = Error ->
            Error;
        {badrpc, Reason} ->
            {error, Reason};
        Reply ->
            {error, {unexpected_takeover_reply, Reply}}
    end;
begin_legacy(GwName, ClientId, ChanPid, Request = #{mode := {resume, Info}}) ->
    %% NOTE
    %% For current→legacy resume takeover, channel is called directly because legacy
    %% implementation expects initiator channel `OwnerPid` to be in `From`.
    case emqx_gateway_cm_proto_v1:get_chann_conn_mod(GwName, ClientId, ChanPid) of
        ConnMod when is_atom(ConnMod) ->
            ChanRef = #chanref{proto = legacy, connmod = ConnMod, pid = ChanPid},
            begin_channel(ChanRef, Request, {takeover, 'begin', Info});
        {badrpc, Reason} ->
            {error, Reason}
    end.

-doc """
Legacy RPC target @ `emqx_gateway_cm:do_takeover_session/3`.
Adapts takeover result to the legacy protocol.
""".
begin_rpc_legacy(GwName, ClientId, ChanPid) ->
    Request = takeover_request(force, make_ref()),
    Ret = begin_local(GwName, ClientId, ChanPid, Request),
    downgrade_begin_ret(GwName, Ret).

downgrade_begin_ret(
    GwName,
    {ok, #chanref{connmod = ConnMod, pid = ChanPid}, Data = #{session := Session}}
) ->
    ClientInfo = maps:get(clientinfo, Data, #{}),
    {ok, ConnMod, ChanPid, to_session(GwName, ClientInfo, legacy, Session)};
downgrade_begin_ret(_GwName, {error, _} = Error) ->
    Error.

-doc "Complete takeover from the initiating process with the same attempt token supplied at begin.".
-spec finish(gateway_name(), channelref(), mode(), reference()) ->
    {ok, [emqx_types:deliver()]} | {error, term()}.
finish(GwName, Ref, Mode, Attempt) ->
    finish(GwName, Ref, takeover_request(Mode, Attempt)).

finish(_GwName, #chanref{proto = local, connmod = ConnMod, pid = ChanPid}, Request) ->
    finish_local(ConnMod, ChanPid, Request);
finish(GwName, #chanref{proto = #{} = ServerProto, connmod = ConnMod, pid = ChanPid}, Request) ->
    try
        emqx_gateway_cm_takeover_proto_v1:takeover_finish(
            GwName,
            ConnMod,
            ChanPid,
            Request,
            current(),
            ?RPC_TIMEOUT
        )
    of
        Ret ->
            from_finish_ret(ServerProto, Ret)
    catch
        error:{erpc, Reason} ->
            {error, {erpc, Reason}};
        error:{exception, Reason, _Stack} ->
            {error, Reason}
    end;
finish(_GwName, Ref = #chanref{proto = legacy}, Request) ->
    Ret = finish_channel(Ref, Request, {takeover, 'end'}),
    from_finish_ret(legacy, Ret).

-doc """
Direct RPC target @ `emqx_gateway_cm_takeover_proto_v1:takeover_finish/6`.
Should respect requester protocol version.
""".
-spec finish_rpc(gateway_name(), module(), pid(), request(), protocol()) ->
    {ok, [emqx_types:deliver()]} | {error, term()}.
finish_rpc(_GwName, ConnMod, ChanPid, Request, RequesterProto) ->
    Ret = finish_local(ConnMod, ChanPid, Request),
    to_finish_ret(RequesterProto, Ret).

finish_local(ConnMod, ChanPid, Request) when node(ChanPid) =:= node() ->
    ChanRef = #chanref{proto = local, connmod = ConnMod, pid = ChanPid},
    finish_channel(ChanRef, Request, finish_call(Request)).

-doc "Adapt received completion from the server protocol. Currently a pass-through.".
from_finish_ret(_ServerProto, Ret) ->
    Ret.

-doc "Adapt outgoing RPC completion to the requester protocol. Currently a pass-through.".
to_finish_ret(_RequesterProto, Ret) ->
    Ret.

takeover_request(Mode, Attempt) ->
    #{owner => self(), attempt => Attempt, mode => Mode}.

%% Channel calls
%% Request shape determines channel semantics, not protocol selection.

begin_channel(Ref, Request, Call) ->
    Ret = channel_call(Ref, Request, Call),
    maybe
        {ok, Data} ?= begin_ret(Ret),
        {ok, Ref, Data}
    end.

begin_call(#{owner := Owner, attempt := Attempt, mode := force}) ->
    {takeover, 'begin', {Owner, Attempt}, undefined};
begin_call(#{owner := Owner, attempt := Attempt, mode := {resume, Info}}) ->
    {takeover, 'begin', {Owner, Attempt}, Info}.

-doc "Normalize channel begin replies to `{ok, Data}` or `{error, Reason}`.".
begin_ret({ok, Data = #{session := _}}) ->
    {ok, Data};
begin_ret({error, _} = Error) ->
    Error;
begin_ret(ignored) ->
    {error, unsupported};
begin_ret(undefined) ->
    {error, not_found};
begin_ret(Reply) ->
    {error, {unexpected_takeover_reply, Reply}}.

finish_channel(Ref, Request, Call) ->
    finish_ret(channel_call(Ref, Request, Call)).

finish_call(#{owner := Owner, attempt := Attempt}) ->
    {takeover, 'end', {Owner, Attempt}}.

-doc "Normalize channel completion replies to `{ok, Pendings}` or `{error, Reason}`.".
finish_ret(Pendings) when is_list(Pendings) ->
    {ok, Pendings};
finish_ret({ok, Pendings}) when is_list(Pendings) ->
    {ok, Pendings};
finish_ret({error, _} = Error) ->
    Error;
finish_ret(Reply) ->
    {error, {unexpected_takeover_reply, Reply}}.

channel_call(#chanref{connmod = undefined}, _Mode, _Request) ->
    {error, not_found};
channel_call(#chanref{connmod = ConnMod, pid = ChanPid}, #{mode := Mode}, Request) ->
    %% NOTE
    %% * Ordinary (force) takeover retains stepdown force-kill policy.
    %% * Resume-takeover keep the old channel alive on failure so it can roll
    %%   incomplete takeover back.
    FailurePolicy =
        case Mode of
            force -> kill;
            {resume, _} -> keep
        end,
    case emqx_gateway_cm:request_stepdown(Request, ConnMod, ChanPid, FailurePolicy) of
        {ok, Reply} ->
            Reply;
        Other ->
            Other
    end.

%% Compatibility

-doc "Encode the legacy MQTT-SN begin reply, preserving only its supported fields.".
to_legacy_reply(#{
    session := Session,
    conninfo := ConnInfo,
    clientinfo := ClientInfo,
    asleep_timer_duration := SleepDuration
}) ->
    #{
        session => to_session(mqttsn, ClientInfo, legacy, Session),
        conninfo => ConnInfo,
        clientinfo => ClientInfo,
        asleep_timer_duration => SleepDuration
    }.

-doc "Decode received session state back to the local requester takeover protocol.".
-spec from_session(gateway_name(), protocol() | legacy, term()) -> term().
from_session(mqttsn, _ServerProto = legacy, #{registry := Registry, session := Legacy}) ->
    #{
        registry => Registry,
        session => emqx_cm_takeover:from_legacy_session(Legacy)
    };
from_session(mqttsn, #{vsn := _}, Session) ->
    Session;
from_session(_GwName, _ServerProto, Session) ->
    Session.

-doc "Encode session state for the remote requester takeover protocol.".
-spec to_session(gateway_name(), emqx_types:clientinfo(), protocol() | legacy, term()) ->
    term().
to_session(mqttsn, ClientInfo, _RequesterProto = legacy, #{
    registry := Registry,
    session := Exported
}) ->
    ClientId = maps:get(clientid, ClientInfo),
    ChanInfo = #{clientinfo => ClientInfo},
    Conf = (emqx_session:get_session_conf(ClientInfo))#{receive_maximum => 1},
    #{
        registry => Registry,
        session => emqx_cm_takeover:to_legacy_session(ClientId, ChanInfo, Exported, Conf)
    };
to_session(mqttsn, _ClientInfo, #{vsn := _}, Session) ->
    Session;
to_session(_GwName, _ClientInfo, _RequesterProto, Session) ->
    Session.
