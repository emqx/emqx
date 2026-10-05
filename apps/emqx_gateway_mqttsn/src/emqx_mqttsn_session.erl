%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqttsn_session).

-include_lib("emqx/include/emqx_mqtt.hrl").

-export([registry/1, set_registry/2]).

-export([
    init/2,
    info/1,
    info/2,
    stats/1
]).

-export([
    publish/4,
    subscribe/4,
    unsubscribe/4,
    puback/3,
    pubrec/3,
    pubrel/3,
    pubcomp/3
]).

-export([
    replay/2,
    deliver/3,
    handle_timeout/3,
    obtain_next_pkt_id/1,
    takeover/1,
    takeover_request/0,
    export_for_takeover/3,
    resume/2,
    resume_clientinfo/2,
    disconnect/2,
    enqueue/3
]).

-type session() :: #{
    registry := emqx_mqttsn_registry:registry(),
    session := emqx_session_mem:session()
}.

-doc """
The session as it crosses nodes in a takeover: the inner session is the
map of `emqx_session_mem:export/1`, or the `#session{}` of the version that
asked for it.
""".
-type takeover_session() :: #{
    registry := emqx_mqttsn_registry:registry(),
    session :=
        emqx_session_mem:exported()
        | emqx_session_mem_compat:legacy_session()
}.

-type takeover_format() :: emqx_session_mem_compat:format().

-export_type([session/0, takeover_session/0, takeover_format/0]).

init(ClientInfo, MaybeWillMsg) ->
    #{
        registry => emqx_mqttsn_registry:init(),
        session => emqx_session_mem:create(
            ClientInfo, conn_info(), MaybeWillMsg, session_conf(ClientInfo)
        )
    }.

conn_info() ->
    #{receive_maximum => 1, expiry_interval => 0}.

session_conf(ClientInfo) ->
    maps:merge(
        emqx_session:get_session_conf(ClientInfo),
        #{
            %% TODO: Handle quota-related timer effects.
            enable_quota => false
        }
    ).

registry(#{registry := Registry}) ->
    Registry.

set_registry(Registry, Session) ->
    Session#{registry := Registry}.

info(#{session := Session}) ->
    emqx_session:info(Session).

info(Key, #{session := Session}) ->
    emqx_session:info(Key, Session).

stats(#{session := Session}) ->
    emqx_session:stats(Session).

puback(ClientInfo, MsgId, Session = #{session := S}) ->
    wrap_result(emqx_session:puback(ClientInfo, MsgId, ?RC_SUCCESS, [], S), Session).

pubrec(ClientInfo, MsgId, Session = #{session := S}) ->
    wrap_result(emqx_session:pubrec(ClientInfo, MsgId, S), Session).

pubrel(ClientInfo, MsgId, Session = #{session := S}) ->
    wrap_result(emqx_session:pubrel(ClientInfo, MsgId, S), Session).

pubcomp(ClientInfo, MsgId, Session = #{session := S}) ->
    wrap_result(emqx_session:pubcomp(ClientInfo, MsgId, ?RC_SUCCESS, [], S), Session).

publish(ClientInfo, MsgId, Msg, Session = #{session := S}) ->
    wrap_result(emqx_session:publish(ClientInfo, MsgId, Msg, S), Session).

subscribe(ClientInfo, Topic, SubOpts, Session = #{session := S}) ->
    wrap_result(emqx_session:subscribe(ClientInfo, Topic, SubOpts, S), Session).

unsubscribe(ClientInfo, Topic, SubOpts, Session = #{session := S}) ->
    wrap_result(emqx_session:unsubscribe(ClientInfo, Topic, SubOpts, S), Session).

deliver(ClientInfo, Delivers, Session = #{session := S}) ->
    wrap_result(emqx_session:deliver(ClientInfo, Delivers, [], S), Session).

handle_timeout(ClientInfo, Name, Session = #{session := S}) ->
    wrap_result(emqx_session:handle_timeout(ClientInfo, Name, [], S), Session).

obtain_next_pkt_id(Session = #{session := Sess}) ->
    {Id, Sess1} = emqx_session_mem:obtain_next_pkt_id(Sess),
    {Id, Session#{session := Sess1}}.

takeover(_Session = #{session := Sess}) ->
    emqx_session_mem:takeover(Sess).

-doc "Options a channel of this version sends when it takes over a session.".
-spec takeover_request() -> emqx_gateway_cm:takeover_opts().
takeover_request() ->
    #{session_format => exported}.

-doc """
The session as the channel replies it to a takeover, in the layout of
`emqx_session_mem_compat:from_exported/4`. A `#session{}` layout gets the
inflight window and the session configuration of an MQTT-SN session,
because the receiving gateway uses the record as it is.
""".
-spec export_for_takeover(takeover_format(), emqx_types:clientinfo(), session()) ->
    takeover_session().
export_for_takeover(Format, ClientInfo, Session = #{session := Sess}) ->
    #{receive_maximum := ReceiveMax} = conn_info(),
    Opts = #{receive_maximum => ReceiveMax, conf => session_conf(ClientInfo)},
    Exported = emqx_session_mem:export(Sess),
    Session#{
        session := emqx_session_mem_compat:from_exported(Format, ClientInfo, Exported, Opts)
    }.

-doc """
Resume a session taken over from a channel, in any layout of
`emqx_session_mem_compat`. The session gets this node's inflight window and
session configuration.
""".
-spec resume(emqx_types:clientinfo(), takeover_session()) -> session().
resume(ClientInfo, Session = #{session := SessIn}) ->
    Exported = emqx_session_mem_compat:to_exported(SessIn),
    Sess = emqx_session_mem:import(ClientInfo, conn_info(), session_conf(ClientInfo), Exported),
    Session#{session := emqx_session_mem:resume(ClientInfo, Sess)}.

disconnect(ConnInfo, Session = #{session := S}) ->
    wrap_result(emqx_session_mem:disconnect(S, ConnInfo), Session).

-spec resume_clientinfo(
    emqx_types:clientinfo(),
    emqx_types:clientinfo()
) -> emqx_types:clientinfo().
resume_clientinfo(NewClientInfo, OldClientInfo) ->
    %% Keep session-scoped authorization and topic namespace attributes from
    %% the authenticated session; transport-specific fields come from the new
    %% association.
    PreservedKeys = [
        username,
        password,
        auth_result,
        auth_expire_at,
        is_superuser,
        mountpoint,
        dn,
        cn,
        client_attrs
    ],
    maps:merge(NewClientInfo, maps:with(PreservedKeys, OldClientInfo)).

replay(ClientInfo, Session = #{session := S}) ->
    wrap_result(emqx_session_mem:replay(ClientInfo, S), Session).

enqueue(ClientInfo, Delivers, Session = #{session := Sess}) ->
    Msgs = emqx_session:enrich_delivers(ClientInfo, Delivers, Sess),
    Session#{session := emqx_session_mem:enqueue(ClientInfo, Msgs, Sess)}.

%%--------------------------------------------------------------------
%% internal funcs

%% for subscribe / unsubscribe / pubrel
wrap_result({ok, S}, Session) ->
    {ok, Session#{session := S}};
%% for publish / pubrec / pubcomp / deliver
wrap_result({OkEffects, ResultReplies, S}, Session) ->
    {OkEffects, ResultReplies, Session#{session := S}};
%% for puback / handle_timeout
wrap_result({OkEffects, Msgs, Replies, S}, Session) ->
    {OkEffects, Msgs, Replies, Session#{session := S}};
%% for any errors
wrap_result({error, Reason}, _Session) ->
    {error, Reason}.
