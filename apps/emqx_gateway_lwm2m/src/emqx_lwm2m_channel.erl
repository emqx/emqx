%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_channel).
-behaviour(emqx_gateway_channel).

-include("emqx_lwm2m.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx_gateway_coap/include/emqx_coap.hrl").

%% API
-export([
    info/1,
    info/2,
    stats/1,
    with_context/2,
    do_takeover/3,
    lookup_cmd/3,
    send_cmd/2
]).

-export([
    init/2,
    handle_in/2,
    handle_frame_error/2,
    handle_deliver/2,
    handle_timeout/3,
    terminate/2
]).

-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% Connection Info
    conninfo :: emqx_types:conninfo(),
    %% Client Info
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: emqx_lwm2m_session:session() | undefined,
    %% Channel State
    %% TODO: is there need
    conn_state :: conn_state(),
    %% Timer
    timers :: #{atom() => disable | undefined | reference()},
    %% FIXME: don't store anonymous func
    with_context :: function()
}).

-type channel() :: #channel{}.

-type conn_state() :: idle | connecting | connected | disconnected.

-type reply() ::
    {outgoing, coap_message()}
    | {outgoing, [coap_message()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

%% TODO:
-define(DEFAULT_OVERRIDE,
    %% Generate clientid by default
    #{
        clientid => <<"">>,
        username => <<"${Packet.uri_query.ep}">>,
        password => <<"">>
    }
).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session]).

-import(emqx_coap_medium, [reply/2, reply/3, reply/4, iter/3, iter/4]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    emqx_utils:maybe_apply(fun emqx_lwm2m_session:info/1, Session);
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

stats(#channel{session = Session}) ->
    emqx_lwm2m_session:stats(Session).

init(
    ConnInfo = #{
        peername := {PeerHost, _} = PeerName,
        sockname := {_, SockPort}
    },
    #{ctx := Ctx} = Config
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Config, undefined),
    ListenerId =
        case maps:get(listener, Config, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    EnableAuthn = maps:get(enable_authn, Config, true),
    ClientInfo = set_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => lwm2m,
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            username => undefined,
            clientid => undefined,
            is_bridge => false,
            is_superuser => false,
            enable_authn => EnableAuthn,
            mountpoint => Mountpoint
        }
    ),

    #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        timers = #{},
        session = emqx_lwm2m_session:new(),
        conn_state = idle,
        with_context = with_context(Ctx, ClientInfo)
    }.

lookup_cmd(Channel, Path, Action) ->
    gen_server:call(Channel, {?FUNCTION_NAME, Path, Action}).

send_cmd(Channel, Cmd) ->
    gen_server:call(Channel, {?FUNCTION_NAME, Cmd}).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(coap_message() | {frame_error, any()}, channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_in(Msg, Channle) ->
    NChannel = update_life_timer(Channle),
    call_session(handle_coap_in, Msg, NChannel).

handle_frame_error(Error, Channel) ->
    {shutdown, Error, Channel}.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------
handle_deliver(Delivers, Channel) ->
    call_session(handle_deliver, Delivers, Channel).

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------
handle_timeout(
    _,
    lifetime,
    #channel{
        ctx = Ctx,
        clientinfo = ClientInfo,
        conninfo = ConnInfo
    } = Channel
) ->
    ok = run_hooks(Ctx, 'client.disconnected', [ClientInfo, timeout, ConnInfo]),
    {shutdown, timeout, Channel};
handle_timeout(_, {transport, _} = Msg, Channel) ->
    call_session(timeout, Msg, Channel);
handle_timeout(_, disconnect, Channel) ->
    {shutdown, normal, Channel};
handle_timeout(_, connection_expire, Channel) ->
    {shutdown, expired, Channel};
handle_timeout(_, _, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

handle_call(
    {lookup_cmd, Path, Type},
    _From,
    Channel = #channel{session = Session}
) ->
    Result = emqx_lwm2m_session:find_cmd_record(Path, Type, Session),
    {reply, {ok, Result}, Channel};
handle_call({send_cmd, Cmd}, _From, Channel) ->
    {ok, Outs, Channel2} = call_session(send_cmd, Cmd, Channel),
    {reply, ok, Outs, Channel2};
handle_call(
    {subscribe, Topic, SubOpts},
    _From,
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{
                    clientid := ClientId,
                    mountpoint := Mountpoint
                },
        session = Session
    }
) ->
    NSubOpts = maps:merge(
        emqx_gateway_utils:default_subopts(),
        SubOpts
    ),
    MountedTopic = emqx_mountpoint:mount(Mountpoint, Topic),
    _ = emqx_broker:subscribe(MountedTopic, ClientId, NSubOpts),

    _ = run_hooks(
        Ctx,
        'session.subscribed',
        [ClientInfo, MountedTopic, NSubOpts]
    ),
    %% modify session state
    Subs = emqx_lwm2m_session:info(subscriptions, Session),
    NSubs = maps:put(MountedTopic, NSubOpts, Subs),
    NSession = emqx_lwm2m_session:set_subscriptions(NSubs, Session),
    {reply, {ok, {MountedTopic, NSubOpts}}, [{event, updated}], Channel#channel{session = NSession}};
handle_call(
    {unsubscribe, Topic},
    _From,
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint},
        session = Session
    }
) ->
    MountedTopic = emqx_mountpoint:mount(Mountpoint, Topic),
    ok = emqx_broker:unsubscribe(MountedTopic),
    _ = run_hooks(
        Ctx,
        'session.unsubscribed',
        [ClientInfo, MountedTopic, #{}]
    ),
    %% modify session state
    Subs = emqx_lwm2m_session:info(subscriptions, Session),
    NSubs = maps:remove(MountedTopic, Subs),
    NSession = emqx_lwm2m_session:set_subscriptions(NSubs, Session),
    {reply, ok, [{event, updated}], Channel#channel{session = NSession}};
handle_call(subscriptions, _From, Channel = #channel{session = Session}) ->
    Subs = maps:to_list(emqx_lwm2m_session:info(subscriptions, Session)),
    {reply, {ok, Subs}, Channel};
handle_call(kick, _From, Channel) ->
    NChannel = ensure_disconnected(kicked, Channel),
    shutdown_and_reply(kicked, ok, NChannel);
handle_call(discard, _From, Channel) ->
    shutdown_and_reply(discarded, ok, Channel);
%% TODO: No Session Takeover
%handle_call({takeover, 'begin'}, _From, Channel = #channel{session = Session}) ->
%    reply(Session, Channel#channel{takeover = true});
%
%handle_call({takeover, 'end'}, _From, Channel = #channel{session  = Session,
%                                                  pendings = Pendings}) ->
%    ok = emqx_session:takeover(Session),
%    %% TODO: Should not drain deliver here (side effect)
%    Delivers = emqx_utils:drain_deliver(),
%    AllPendings = lists:append(Delivers, Pendings),
%    shutdown_and_reply(takenover, AllPendings, Channel);

handle_call(Req, _From, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_call",
        call => Req
    }),
    {reply, ignored, Channel}.

%%--------------------------------------------------------------------
%% Handle Cast
%%--------------------------------------------------------------------
handle_cast(Req, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_cast",
        cast => Req
    }),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------
handle_info({subscribe, _AutoSubs}, Channel) ->
    %% not need handle this message
    {ok, Channel};
handle_info(Info, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_info",
        info => Info
    }),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------
terminate(Reason, #channel{
    ctx = Ctx,
    clientinfo = ClientInfo,
    session = Session
}) ->
    MountedTopic = emqx_lwm2m_session:on_close(Session),
    _ = run_hooks(Ctx, 'session.unsubscribed', [ClientInfo, MountedTopic, #{}]),
    run_hooks(Ctx, 'session.terminated', [ClientInfo, Reason, Session]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Ensure connected

ensure_connected(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    _ = run_hooks(Ctx, 'client.connack', [ConnInfo, connection_accepted, #{}]),

    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    schedule_connection_expire(Channel#channel{
        conninfo = NConnInfo,
        conn_state = connected
    }).

schedule_connection_expire(Channel = #channel{ctx = Ctx, clientinfo = ClientInfo}) ->
    case emqx_gateway_ctx:connection_expire_interval(Ctx, ClientInfo) of
        undefined ->
            Channel;
        Interval ->
            make_timer(connection_expire, Interval, connection_expire, Channel)
    end.

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(
    Reason,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks(
        Ctx,
        'client.disconnected',
        [ClientInfo, Reason, NConnInfo]
    ),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

shutdown_and_reply(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

%shutdown_and_reply(Reason, Reply, OutPkt, Channel) ->
%    {shutdown, Reason, Reply, OutPkt, Channel}.

set_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
set_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

make_timer(Name, Time, Msg, Channel = #channel{timers = Timers}) ->
    TRef = emqx_utils:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

update_life_timer(#channel{session = Session, timers = Timers} = Channel) ->
    LifeTime = emqx_lwm2m_session:info(lifetime, Session),
    _ =
        case maps:get(lifetime, Timers, undefined) of
            undefined -> ok;
            Ref -> erlang:cancel_timer(Ref)
        end,
    make_timer(lifetime, LifeTime, lifetime, Channel).

check_location(Location, #channel{session = Session}) ->
    SLocation = emqx_lwm2m_session:info(location_path, Session),
    Location =:= SLocation.

do_takeover(_DesireId, Msg, Channel) ->
    %% TODO completed the takeover, now only reset the message
    Reset = emqx_coap_message:reset(Msg),
    call_session(handle_out, Reset, Channel).

do_connect(Req, Result, Channel, Iter) ->
    case
        emqx_utils:pipeline(
            [
                fun check_lwm2m_version/2,
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2,
                fun auth_connect/2
            ],
            Req,
            Channel
        )
    of
        {ok, _Input,
            #channel{
                session = Session,
                with_context = WithContext
            } = NChannel} ->
            case emqx_lwm2m_session:info(reg_info, Session) of
                undefined ->
                    process_connect(ensure_connected(NChannel), Req, Result, Iter);
                _ ->
                    NewResult = emqx_lwm2m_session:reregister(Req, WithContext, Session),
                    iter(Iter, maps:merge(Result, NewResult), NChannel)
            end;
        {error, ReasonCode, NChannel} ->
            ErrMsg = io_lib:format("Login Failed: ~ts", [ReasonCode]),
            Payload = erlang:list_to_binary(lists:flatten(ErrMsg)),
            iter(
                Iter,
                reply({error, bad_request}, Payload, Req, Result),
                NChannel
            )
    end.

check_lwm2m_version(
    #coap_message{options = Opts},
    #channel{conninfo = ConnInfo} = Channel
) ->
    Ver = gets([uri_query, <<"lwm2m">>], Opts),
    IsValid =
        case Ver of
            <<"1.0">> ->
                true;
            <<"1">> ->
                true;
            <<"1.1">> ->
                true;
            _ ->
                false
        end,
    case IsValid of
        true ->
            NConnInfo = ConnInfo#{
                connected_at => erlang:system_time(millisecond),
                proto_ver => Ver
            },
            {ok, Channel#channel{conninfo = NConnInfo}};
        _ ->
            ?SLOG(error, #{
                msg => "reject_REGISTRE_request",
                reason => {unsupported_version, Ver}
            }),
            {error, "invalid lwm2m version", Channel}
    end.

run_conn_hooks(
    Input,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Input, Channel}
    end.

enrich_conninfo(
    #coap_message{options = Options},
    Channel = #channel{
        conninfo = ConnInfo
    }
) ->
    Query = maps:get(uri_query, Options, #{}),
    case Query of
        #{<<"ep">> := Epn, <<"lt">> := Lifetime} ->
            ClientId = maps:get(<<"device_id">>, Query, Epn),
            NConnInfo = ConnInfo#{
                clientid => ClientId,
                proto_name => <<"LwM2M">>,
                proto_ver => <<"1.0.1">>,
                clean_start => true,
                keepalive => binary_to_integer(Lifetime),
                expiry_interval => 0
            },
            {ok, Channel#channel{conninfo = NConnInfo}};
        _ ->
            {error, "invalid queries", Channel}
    end.

enrich_clientinfo(
    #coap_message{options = Options} = Msg,
    Channel = #channel{clientinfo = ClientInfo0}
) ->
    Query = maps:get(uri_query, Options, #{}),
    case Query of
        #{<<"ep">> := Epn, <<"lt">> := Lifetime} ->
            %% FIXME: the following keys is not belong standrad protocol
            Username = maps:get(<<"imei">>, Query, Epn),
            Password = maps:get(<<"password">>, Query, undefined),
            ClientId = maps:get(<<"device_id">>, Query, Epn),
            ClientInfo =
                ClientInfo0#{
                    endpoint_name => Epn,
                    lifetime => binary_to_integer(Lifetime),
                    username => Username,
                    password => Password,
                    clientid => ClientId
                },
            {ok, NClientInfo} = fix_mountpoint(Msg, ClientInfo),
            {ok, Channel#channel{clientinfo = NClientInfo}};
        _ ->
            ?SLOG(error, #{
                msg => "reject_REGISTER_request",
                reason => {wrong_paramters, Query}
            }),
            {error, "invalid queries", Channel}
    end.

set_log_meta(_Input, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

auth_connect(
    _Input,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    #{clientid := ClientId, username := Username} = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{
                clientinfo = NClientInfo,
                with_context = with_context(Ctx, ClientInfo)
            }};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                clientid => ClientId,
                username => Username,
                reason => Reason
            }),
            {error, Reason}
    end.

fix_mountpoint(_Packet, #{mountpoint := undefined} = ClientInfo) ->
    {ok, ClientInfo};
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

process_connect(
    Channel = #channel{
        ctx = Ctx,
        session = Session,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        with_context = WithContext
    },
    Msg,
    Result,
    Iter
) ->
    %% inherit the old session
    SessFun = fun(_, _) -> #{} end,
    case
        emqx_gateway_ctx:open_session(
            Ctx,
            true,
            ClientInfo,
            ConnInfo,
            SessFun,
            emqx_lwm2m_session
        )
    of
        {ok, _} ->
            Mountpoint = maps:get(mountpoint, ClientInfo, <<>>),
            NewResult0 = emqx_lwm2m_session:init(
                Msg,
                Mountpoint,
                WithContext,
                Session
            ),
            NewResult1 = NewResult0#{events => [{event, connected}]},
            iter(Iter, maps:merge(Result, NewResult1), Channel);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "falied_to_open_session",
                reason => Reason
            }),
            iter(Iter, reply({error, bad_request}, Msg, Result), Channel)
    end.

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

gets(_, undefined) ->
    undefined;
gets([H | T], Map) ->
    gets(T, maps:get(H, Map, undefined));
gets([], Val) ->
    Val.

%%--------------------------------------------------------------------
%% With Context

with_context(Ctx, ClientInfo) ->
    fun(Type, Topic) ->
        with_context(Type, Topic, Ctx, ClientInfo)
    end.

with_context(publish, [Topic, Msg], Ctx, ClientInfo) ->
    Action = publish_action(Msg),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, Topic) of
        allow ->
            _ = emqx_broker:publish(Msg),
            ok;
        _ ->
            ?SLOG(error, #{
                msg => "publish_denied",
                topic => Topic
            }),
            {error, deny}
    end;
with_context(subscribe, [Topic, Opts], Ctx, ClientInfo) ->
    #{
        clientid := ClientId,
        endpoint_name := EndpointName
    } = ClientInfo,
    Action = subscribe_action(Opts),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, Topic) of
        allow ->
            run_hooks(Ctx, 'session.subscribed', [ClientInfo, Topic, Opts]),
            ?SLOG(debug, #{
                msg => "subscribe_topic_succeed",
                topic => Topic,
                clientid => ClientId,
                endpoint_name => EndpointName
            }),
            emqx_broker:subscribe(Topic, ClientId, Opts),
            ok;
        _ ->
            ?SLOG(error, #{
                msg => "subscribe_denied",
                topic => Topic
            }),
            {error, deny}
    end;
with_context(metrics, Name, Ctx, _ClientInfo) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

publish_action(#message{qos = QoS, flags = Flags}) ->
    Retain = maps:get(retain, Flags, false),
    ?AUTHZ_PUBLISH(QoS, Retain).

subscribe_action(Opts) ->
    QoS = maps:get(qos, Opts, 0),
    ?AUTHZ_SUBSCRIBE(QoS).

%%--------------------------------------------------------------------
%% Call Chain
%%--------------------------------------------------------------------
call_session(
    Fun,
    Msg,
    #channel{
        session = Session,
        with_context = WithContext
    } = Channel
) ->
    iter(
        [
            session,
            fun process_session/4,
            proto,
            fun process_protocol/4,
            return,
            fun process_return/4,
            lifetime,
            fun process_lifetime/4,
            reply,
            fun process_reply/4,
            out,
            fun process_out/4,
            fun process_nothing/3
        ],
        emqx_lwm2m_session:Fun(Msg, WithContext, Session),
        Channel
    ).

process_session(Session, Result, Channel, Iter) ->
    iter(Iter, Result, Channel#channel{session = Session}).

process_protocol({request, Msg}, Result, Channel, Iter) ->
    #coap_message{method = Method} = Msg,
    handle_request_protocol(Method, Msg, Result, Channel, Iter);
process_protocol(
    Msg,
    Result,
    #channel{with_context = WithContext, session = Session} = Channel,
    Iter
) ->
    ProtoResult = emqx_lwm2m_session:handle_protocol_in(Msg, WithContext, Session),
    iter(Iter, maps:merge(Result, ProtoResult), Channel).

handle_request_protocol(
    post,
    #coap_message{options = Opts} = Msg,
    Result,
    Channel,
    Iter
) ->
    case Opts of
        #{uri_path := [?REG_PREFIX]} ->
            do_connect(Msg, Result, Channel, Iter);
        #{uri_path := Location} ->
            do_update(Location, Msg, Result, Channel, Iter);
        _ ->
            iter(Iter, reply({error, not_found}, Msg, Result), Channel)
    end;
handle_request_protocol(
    delete,
    #coap_message{options = Opts} = Msg,
    Result,
    Channel,
    Iter
) ->
    case Opts of
        #{uri_path := Location} ->
            case check_location(Location, Channel) of
                true ->
                    Reply = emqx_coap_message:piggyback({ok, deleted}, Msg),
                    {shutdown, close, Reply, Channel};
                _ ->
                    iter(Iter, reply({error, not_found}, Msg, Result), Channel)
            end;
        _ ->
            iter(Iter, reply({error, bad_request}, Msg, Result), Channel)
    end.

do_update(
    Location,
    Msg,
    Result,
    #channel{session = Session, with_context = WithContext} = Channel,
    Iter
) ->
    case check_location(Location, Channel) of
        true ->
            NewResult = emqx_lwm2m_session:update(Msg, WithContext, Session),
            iter(Iter, maps:merge(Result, NewResult), Channel);
        _ ->
            iter(Iter, reply({error, not_found}, Msg, Result), Channel)
    end.

process_return({Outs, Session}, Result, Channel, Iter) ->
    OldOuts = maps:get(out, Result, []),
    iter(
        Iter,
        Result#{out => Outs ++ OldOuts},
        Channel#channel{session = Session}
    ).

process_out(Outs, Result, Channel, _) ->
    Outs2 = lists:reverse(Outs),
    Outs3 =
        case maps:get(reply, Result, undefined) of
            undefined ->
                Outs2;
            Reply ->
                [Reply | Outs2]
        end,
    Events = maps:get(events, Result, []),
    {ok, [{outgoing, Outs3}] ++ Events, Channel}.

process_reply(Reply, Result, #channel{session = Session} = Channel, _) ->
    Session2 = emqx_lwm2m_session:set_reply(Reply, Session),
    Outs = maps:get(out, Result, []),
    Outs2 = lists:reverse(Outs),
    Events = maps:get(events, Result, []),
    {ok, [{outgoing, [Reply | Outs2]}] ++ Events, Channel#channel{session = Session2}}.

process_lifetime(_, Result, Channel, Iter) ->
    iter(Iter, Result, update_life_timer(Channel)).

process_nothing(_, _, Channel) ->
    {ok, Channel}.
