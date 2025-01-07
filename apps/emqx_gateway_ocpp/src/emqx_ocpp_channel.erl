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

-module(emqx_ocpp_channel).

-behaviour(emqx_gateway_channel).

-include("emqx_ocpp.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[OCPP-Chann]").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    init/2,
    authenticate/2,
    handle_in/2,
    handle_frame_error/2,
    handle_deliver/2,
    handle_out/3,
    handle_timeout/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Exports for CT
-export([set_field/3]).

-export_type([channel/0]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% ConnInfo
    conninfo :: emqx_types:conninfo(),
    %% ClientInfo
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: option(map()),
    %% ClientInfo override specs
    clientinfo_override :: map(),
    %% Keepalive
    keepalive :: option(emqx_ocpp_keepalive:keepalive()),
    %% Stores all unsent messages.
    mqueue :: queue:queue(),
    %% Timers
    timers :: #{atom() => disabled | option(reference())},
    %% Conn State
    conn_state :: conn_state()
}).

-type channel() :: #channel{}.

-type conn_state() :: idle | connecting | connected | disconnected.

-type reply() ::
    {outgoing, emqx_ocpp_frame:frame()}
    | {outgoing, [emqx_ocpp_frame:frame()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

-define(TIMER_TABLE, #{
    alive_timer => keepalive,
    connection_expire_timer => connection_expire
}).

-define(INFO_KEYS, [
    conninfo,
    conn_state,
    clientinfo,
    session
]).

-define(CHANNEL_METRICS, [
    recv_pkt,
    recv_msg,
    'recv_msg.qos0',
    'recv_msg.qos1',
    'recv_msg.qos2',
    'recv_msg.dropped',
    'recv_msg.dropped.await_pubrel_timeout',
    send_pkt,
    send_msg,
    'send_msg.qos0',
    'send_msg.qos1',
    'send_msg.qos2',
    'send_msg.dropped',
    'send_msg.dropped.expired',
    'send_msg.dropped.queue_full',
    'send_msg.dropped.too_large'
]).

-define(DEFAULT_OVERRIDE,
    %% Generate clientid by default
    #{
        clientid => <<"">>,
        username => <<"">>,
        password => <<"">>
    }
).

-define(DEFAULT_OCPP_DN_SUBOPTS, #{rh => 0, rap => 0, nl => 0, qos => ?QOS_1}).

-dialyzer(no_match).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(socktype, #channel{conninfo = ConnInfo}) ->
    maps:get(socktype, ConnInfo, undefined);
info(peername, #channel{conninfo = ConnInfo}) ->
    maps:get(peername, ConnInfo, undefined);
info(sockname, #channel{conninfo = ConnInfo}) ->
    maps:get(sockname, ConnInfo, undefined);
info(proto_name, #channel{conninfo = ConnInfo}) ->
    maps:get(proto_name, ConnInfo, undefined);
info(proto_ver, #channel{conninfo = ConnInfo}) ->
    maps:get(proto_ver, ConnInfo, undefined);
info(connected_at, #channel{conninfo = ConnInfo}) ->
    maps:get(connected_at, ConnInfo, undefined);
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(zone, #channel{clientinfo = ClientInfo}) ->
    maps:get(zone, ClientInfo, undefined);
info(clientid, #channel{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(username, #channel{clientinfo = ClientInfo}) ->
    maps:get(username, ClientInfo, undefined);
info(session, #channel{conninfo = ConnInfo}) ->
    %% XXX:
    #{
        created_at => maps:get(connected_at, ConnInfo, undefined),
        is_persistent => false,
        subscriptions => #{},
        upgrade_qos => false,
        retry_interval => 0,
        await_rel_timeout => 0
    };
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(keepalive, #channel{keepalive = Keepalive}) ->
    emqx_utils:maybe_apply(fun emqx_ocpp_keepalive:info/1, Keepalive);
info(ctx, #channel{ctx = Ctx}) ->
    Ctx;
info(timers, #channel{timers = Timers}) ->
    Timers.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{mqueue = MQueue}) ->
    %% XXX: A fake stats for managed by emqx_management
    SessionStats = [
        {subscriptions_cnt, 1},
        {subscriptions_max, 1},
        {inflight_cnt, 0},
        {inflight_max, 0},
        {mqueue_len, queue:len(MQueue)},
        {mqueue_max, queue:len(MQueue)},
        {mqueue_dropped, 0},
        {next_pkt_id, 0},
        {awaiting_rel_cnt, 0},
        {awaiting_rel_max, 0}
    ],
    lists:append(SessionStats, emqx_pd:get_counters(?CHANNEL_METRICS)).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec init(emqx_types:conninfo(), map()) -> channel().
init(
    ConnInfo = #{
        peername := {PeerHost, _Port} = PeerName,
        sockname := {_Host, SockPort}
    },
    Options
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Options, undefined),
    ListenerId =
        case maps:get(listener, Options, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    EnableAuthn = maps:get(enable_authn, Options, true),

    ClientInfo = setting_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => ocpp,
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            clientid => undefined,
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            enalbe_authn => EnableAuthn,
            mountpoint => Mountpoint
        }
    ),
    ConnInfo1 = ConnInfo#{
        keepalive => emqx_ocpp_conf:default_heartbeat_interval()
    },
    {NClientInfo, NConnInfo} = take_ws_cookie(ClientInfo, ConnInfo1),
    Ctx = maps:get(ctx, Options),
    Override = maps:merge(
        ?DEFAULT_OVERRIDE,
        maps:get(clientinfo_override, Options, #{})
    ),
    #channel{
        ctx = Ctx,
        conninfo = NConnInfo,
        clientinfo = NClientInfo,
        clientinfo_override = Override,
        mqueue = queue:new(),
        timers = #{},
        conn_state = idle
    }.

setting_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
setting_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

take_ws_cookie(ClientInfo, ConnInfo) ->
    case maps:take(ws_cookie, ConnInfo) of
        {WsCookie, NConnInfo} ->
            {ClientInfo#{ws_cookie => WsCookie}, NConnInfo};
        _ ->
            {ClientInfo, ConnInfo}
    end.

authenticate(UserInfo, Channel) ->
    case
        emqx_utils:pipeline(
            [
                fun enrich_client/2,
                fun run_conn_hooks/2,
                fun check_banned/2,
                fun auth_connect/2
            ],
            UserInfo,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, _, NChannel} ->
            {ok, NChannel};
        {error, Reason, _NChannel} ->
            {error, Reason}
    end.

enrich_client(
    #{
        clientid := ClientId,
        username := Username,
        proto_name := ProtoName,
        proto_ver := ProtoVer
    },
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{
        clientid => ClientId,
        username => Username,
        proto_name => ProtoName,
        proto_ver => ProtoVer,
        clean_start => true,
        conn_props => #{},
        expiry_interval => 0,
        receive_maximum => 1
    },
    NClientInfo =
        ClientInfo#{
            clientid => ClientId,
            username => Username
        },
    {ok, Channel#channel{conninfo = NConnInfo, clientinfo = NClientInfo}}.

set_log_meta(#channel{
    clientinfo = #{clientid := ClientId},
    conninfo = #{peername := PeerName}
}) ->
    emqx_logger:set_metadata_peername(esockd:format(PeerName)),
    emqx_logger:set_metadata_clientid(ClientId).

run_conn_hooks(_UserInfo, Channel = #channel{conninfo = ConnInfo}) ->
    case run_hooks('client.connect', [ConnInfo], #{}) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Channel}
    end.

check_banned(_UserInfo, #channel{clientinfo = ClientInfo}) ->
    case emqx_banned:check(ClientInfo) of
        true -> {error, banned};
        false -> ok
    end.

auth_connect(
    #{password := Password},
    #channel{ctx = Ctx, clientinfo = ClientInfo} = Channel
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo#{password => Password}) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                clientid => ClientId,
                username => Username,
                reason => Reason
            }),
            {error, Reason}
    end.

publish(
    Frame,
    Channel = #channel{
        clientinfo =
            #{
                clientid := ClientId,
                username := Username,
                protocol := Protocol,
                peerhost := PeerHost,
                mountpoint := Mountpoint
            },
        conninfo = #{proto_ver := ProtoVer}
    }
) when
    is_map(Frame)
->
    Topic0 = upstream_topic(Frame, Channel),
    Topic = emqx_mountpoint:mount(Mountpoint, Topic0),
    Payload = frame2payload(Frame),
    emqx_broker:publish(
        emqx_message:make(
            ClientId,
            ?QOS_2,
            Topic,
            Payload,
            #{},
            #{
                protocol => Protocol,
                proto_ver => ProtoVer,
                username => Username,
                peerhost => PeerHost
            }
        )
    ).

upstream_topic(
    Frame = #{id := Id, type := Type},
    #channel{clientinfo = #{clientid := ClientId}}
) ->
    Vars = #{id => Id, type => Type, clientid => ClientId, cid => ClientId},
    case Type of
        ?OCPP_MSG_TYPE_ID_CALL ->
            Action = maps:get(action, Frame),
            proc_tmpl(
                emqx_ocpp_conf:uptopic(Action),
                Vars#{action => Action}
            );
        ?OCPP_MSG_TYPE_ID_CALLRESULT ->
            proc_tmpl(emqx_ocpp_conf:up_reply_topic(), Vars);
        ?OCPP_MSG_TYPE_ID_CALLERROR ->
            proc_tmpl(emqx_ocpp_conf:up_error_topic(), Vars)
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(emqx_ocpp_frame:frame(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_in(?IS_REQ(Frame), Channel) ->
    %% TODO: strit mode
    _ = publish(Frame, Channel),
    {ok, Channel};
handle_in(Frame = #{type := Type}, Channel) when
    Type == ?OCPP_MSG_TYPE_ID_CALLRESULT;
    Type == ?OCPP_MSG_TYPE_ID_CALLERROR
->
    _ = publish(Frame, Channel),
    try_deliver(Channel);
handle_in(Frame, Channel) ->
    ?SLOG(error, #{msg => "unexpected_frame", frame => Frame}),
    {ok, Channel}.

handle_frame_error({badjson, ReasonStr}, Channel) ->
    shutdown({frame_error, {badjson, iolist_to_binary(ReasonStr)}}, Channel);
handle_frame_error({validation_failure, Id, ReasonStr}, Channel) ->
    handle_out(
        dnstream,
        ?ERR_FRAME(Id, ?OCPP_ERR_FormationViolation, iolist_to_binary(ReasonStr)),
        Channel
    );
handle_frame_error(Reason, Channel) ->
    ?SLOG(error, #{msg => "ocpp_frame_error", reason => Reason}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Process Disconnect
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()} | {ok, replies(), channel()}.
handle_deliver(Delivers, Channel) ->
    NChannel =
        lists:foldl(
            fun({deliver, _, Msg}, Acc) ->
                enqueue(Msg, Acc)
            end,
            Channel,
            Delivers
        ),
    try_deliver(NChannel).

enqueue(Msg, Channel = #channel{mqueue = MQueue}) ->
    case queue:len(MQueue) > emqx_ocpp_conf:max_mqueue_len() of
        false ->
            try payload2frame(Msg#message.payload) of
                Frame ->
                    Channel#channel{mqueue = queue:in(Frame, MQueue)}
            catch
                _:_ ->
                    ?SLOG(error, #{msg => "drop_invalid_message", message => Msg}),
                    Channel
            end;
        true ->
            ?SLOG(error, #{msg => "drop_message", message => Msg, reason => message_queue_full}),
            Channel
    end.

try_deliver(Channel = #channel{mqueue = MQueue}) ->
    case queue:is_empty(MQueue) of
        false ->
            %% TODO: strit_mode
            Frames = queue:to_list(MQueue),
            handle_out(dnstream, Frames, Channel#channel{mqueue = queue:new()});
        true ->
            {ok, Channel}
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec handle_out(atom(), term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_out(dnstream, Frames, Channel) ->
    {Outgoings, NChannel} = apply_frame(Frames, Channel),
    {ok, [{outgoing, Frames} | Outgoings], NChannel};
handle_out(disconnect, keepalive_timeout, Channel) ->
    {shutdown, keepalive_timeout, Channel};
handle_out(Type, Data, Channel) ->
    ?SLOG(error, #{msg => "unexpected_outgoing", type => Type, data => Data}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Apply Response frame to channel state machine
%%--------------------------------------------------------------------

apply_frame(Frames, Channel) when is_list(Frames) ->
    {Outgoings, NChannel} = lists:foldl(fun do_apply_frame/2, {[], Channel}, Frames),
    {lists:reverse(Outgoings), NChannel};
apply_frame(Frames, Channel) ->
    ?SLOG(error, #{msg => "unexpected_frame_list", frames => Frames}),
    Channel.

do_apply_frame(?IS_BootNotification_RESP(Status, Interval), {Outgoings, Channel}) ->
    case Status of
        <<"Accepted">> ->
            ?SLOG(info, #{msg => "adjust_heartbeat_timer", new_interval_s => Interval}),
            {[{event, updated} | Outgoings], reset_keepalive(Interval, Channel)};
        _ ->
            {Outgoings, Channel}
    end;
do_apply_frame(Frame, Acc = {_Outgoings, _Channel}) ->
    ?SLOG(info, #{msg => "skip_to_apply_frame", frame => Frame}),
    Acc.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: any(), From :: emqx_gateway_channel:gen_server_from(), channel()) ->
    {reply, Reply :: any(), channel()}
    | {shutdown, Reason :: any(), Reply :: any(), channel()}.
handle_call(kick, _From, Channel) ->
    shutdown(kicked, ok, Channel);
handle_call(discard, _From, Channel) ->
    shutdown(discarded, ok, Channel);
handle_call(
    subscriptions,
    _From,
    Channel = #channel{clientinfo = #{clientid := ClientId, mountpoint := Mountpoint}}
) ->
    Subs = [{dntopic(ClientId, Mountpoint), ?DEFAULT_OCPP_DN_SUBOPTS}],
    reply({ok, Subs}, Channel);
handle_call(Req, From, Channel) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req, from => From}),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle Cast
%%--------------------------------------------------------------------

-spec handle_cast(Req :: any(), channel()) ->
    ok
    | {ok, channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_cast(Req, Channel) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Req}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_info(after_init, Channel0) ->
    set_log_meta(Channel0),
    case process_connect(Channel0) of
        {ok, Channel} ->
            NChannel = ensure_keepalive(
                ensure_connected(
                    ensure_subscribe_dn_topics(Channel)
                )
            ),
            {ok, [{event, connected}], NChannel};
        {error, Reason} ->
            shutdown(Reason, Channel0)
    end;
handle_info({sock_closed, Reason}, Channel) ->
    NChannel = ensure_disconnected({sock_closed, Reason}, Channel),
    shutdown(Reason, NChannel);
handle_info(Info, Channel) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {ok, Channel}.

process_connect(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    SessFun = fun(_, _) -> #{} end,
    case
        emqx_gateway_ctx:open_session(
            Ctx,
            true,
            ClientInfo,
            ConnInfo,
            SessFun
        )
    of
        {ok, #{session := Session}} ->
            NChannel = Channel#channel{session = Session},
            {ok, NChannel};
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_open_session", reason => Reason}),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_timeout(
    _TRef,
    {keepalive, _StatVal},
    Channel = #channel{keepalive = undefined}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    {keepalive, _StatVal},
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    {keepalive, StatVal},
    Channel = #channel{keepalive = Keepalive}
) ->
    case emqx_ocpp_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            handle_out(disconnect, keepalive_timeout, Channel)
    end;
handle_timeout(_TRef, connection_expire, Channel) ->
    %% No take over implemented, so just shutdown
    shutdown(expired, Channel);
handle_timeout(_TRef, Msg, Channel) ->
    ?SLOG(error, #{msg => "unexpected_timeout", timeout_msg => Msg}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Ensure timers
%%--------------------------------------------------------------------

ensure_timer([Name], Channel) ->
    ensure_timer(Name, Channel);
ensure_timer([Name | Rest], Channel) ->
    ensure_timer(Rest, ensure_timer(Name, Channel));
ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_utils:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(alive_timer, #channel{keepalive = KeepAlive}) ->
    emqx_ocpp_keepalive:info(interval, KeepAlive).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

-spec terminate(any(), channel()) -> ok.
terminate(_, #channel{conn_state = idle}) ->
    ok;
terminate(normal, Channel) ->
    run_terminate_hook(normal, Channel);
terminate({shutdown, Reason}, Channel) when
    Reason =:= kicked; Reason =:= discarded
->
    run_terminate_hook(Reason, Channel);
terminate(Reason, Channel) ->
    run_terminate_hook(Reason, Channel).

run_terminate_hook(Reason, Channel = #channel{clientinfo = ClientInfo}) ->
    emqx_hooks:run('session.terminated', [ClientInfo, Reason, info(session, Channel)]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Frame

frame2payload(Frame = #{type := ?OCPP_MSG_TYPE_ID_CALL}) ->
    emqx_utils_json:encode(
        #{
            <<"MessageTypeId">> => ?OCPP_MSG_TYPE_ID_CALL,
            <<"UniqueId">> => maps:get(id, Frame),
            <<"Action">> => maps:get(action, Frame),
            <<"Payload">> => maps:get(payload, Frame)
        }
    );
frame2payload(Frame = #{type := ?OCPP_MSG_TYPE_ID_CALLRESULT}) ->
    emqx_utils_json:encode(
        #{
            <<"MessageTypeId">> => ?OCPP_MSG_TYPE_ID_CALLRESULT,
            <<"UniqueId">> => maps:get(id, Frame),
            <<"Payload">> => maps:get(payload, Frame)
        }
    );
frame2payload(Frame = #{type := ?OCPP_MSG_TYPE_ID_CALLERROR}) ->
    emqx_utils_json:encode(
        #{
            <<"MessageTypeId">> => maps:get(type, Frame),
            <<"UniqueId">> => maps:get(id, Frame),
            <<"ErrorCode">> => maps:get(error_code, Frame),
            <<"ErrorDescription">> => maps:get(error_desc, Frame)
        }
    ).

payload2frame(Payload) when is_binary(Payload) ->
    payload2frame(emqx_utils_json:decode(Payload, [return_maps]));
payload2frame(#{
    <<"MessageTypeId">> := ?OCPP_MSG_TYPE_ID_CALL,
    <<"UniqueId">> := Id,
    <<"Action">> := Action,
    <<"Payload">> := Payload
}) ->
    #{
        type => ?OCPP_MSG_TYPE_ID_CALL,
        id => Id,
        action => Action,
        payload => Payload
    };
payload2frame(#{
    <<"MessageTypeId">> := ?OCPP_MSG_TYPE_ID_CALLRESULT,
    <<"UniqueId">> := Id,
    <<"Payload">> := Payload
}) ->
    #{
        type => ?OCPP_MSG_TYPE_ID_CALLRESULT,
        id => Id,
        action => undefined,
        payload => Payload
    };
payload2frame(#{
    <<"MessageTypeId">> := ?OCPP_MSG_TYPE_ID_CALLERROR,
    <<"UniqueId">> := Id,
    <<"ErrorCode">> := ErrorCode,
    <<"ErrorDescription">> := ErrorDescription
}) ->
    #{
        type => ?OCPP_MSG_TYPE_ID_CALLERROR,
        id => Id,
        error_code => ErrorCode,
        error_desc => ErrorDescription
    }.

%%--------------------------------------------------------------------
%% Ensure connected

ensure_connected(
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo]),
    schedule_connection_expire(Channel#channel{
        conninfo = NConnInfo,
        conn_state = connected
    }).

schedule_connection_expire(Channel = #channel{ctx = Ctx, clientinfo = ClientInfo}) ->
    case emqx_gateway_ctx:connection_expire_interval(Ctx, ClientInfo) of
        undefined ->
            Channel;
        Interval ->
            ensure_timer(connection_expire_timer, Interval, Channel)
    end.

ensure_disconnected(
    Reason,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo]),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

%%--------------------------------------------------------------------
%% Ensure Keepalive

ensure_keepalive(Channel = #channel{conninfo = ConnInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ConnInfo), Channel).

ensure_keepalive_timer(0, Channel) ->
    Channel;
ensure_keepalive_timer(Interval, Channel) ->
    Keepalive = emqx_ocpp_keepalive:init(
        timer:seconds(Interval),
        heartbeat_checking_times_backoff()
    ),
    ensure_timer(alive_timer, Channel#channel{keepalive = Keepalive}).

reset_keepalive(Interval, Channel = #channel{conninfo = ConnInfo, timers = Timers}) ->
    case maps:get(alive_timer, Timers, undefined) of
        undefined ->
            Channel;
        TRef ->
            NConnInfo = ConnInfo#{keepalive => Interval},
            emqx_utils:cancel_timer(TRef),
            ensure_keepalive_timer(
                Interval,
                Channel#channel{
                    conninfo = NConnInfo,
                    timers = maps:without([alive_timer], Timers)
                }
            )
    end.

heartbeat_checking_times_backoff() ->
    max(0, emqx_ocpp_conf:heartbeat_checking_times_backoff() - 1).

%%--------------------------------------------------------------------
%% Ensure Subscriptions

ensure_subscribe_dn_topics(
    Channel = #channel{clientinfo = #{clientid := ClientId, mountpoint := Mountpoint} = ClientInfo}
) ->
    SubOpts = ?DEFAULT_OCPP_DN_SUBOPTS,
    Topic = dntopic(ClientId, Mountpoint),
    ok = emqx_broker:subscribe(Topic, ClientId, SubOpts),
    ok = emqx_hooks:run('session.subscribed', [ClientInfo, Topic, SubOpts]),
    Channel.

dntopic(ClientId, Mountpoint) ->
    Topic0 = proc_tmpl(
        emqx_ocpp_conf:dntopic(),
        #{
            clientid => ClientId,
            cid => ClientId
        }
    ),
    emqx_mountpoint:mount(Mountpoint, Topic0).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [run_hooks/3]}).
run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run_fold(Name, Args, Acc).

-compile({inline, [reply/2, shutdown/2, shutdown/3]}).

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(success, Channel) ->
    shutdown(normal, Channel);
shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(success, Reply, Channel) ->
    shutdown(normal, Reply, Channel);
shutdown(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

proc_tmpl(Tmpl, Vars) ->
    Tokens = emqx_placeholder:preproc_tmpl(Tmpl),
    emqx_placeholder:proc_tmpl(Tokens, Vars).

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, Channel) ->
    Pos = emqx_utils:index_of(Name, record_info(fields, channel)),
    setelement(Pos + 1, Channel, Value).
