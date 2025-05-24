%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_channel).

-behaviour(emqx_gateway_channel).

-include("emqx_nats.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    init/2,
    handle_in/2,
    handle_frame_error/2,
    handle_out/3,
    handle_deliver/2,
    handle_timeout/3,
    terminate/2,
    set_conn_state/2
]).

-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% Stomp Connection Info
    conninfo :: emqx_types:conninfo(),
    %% Stomp Client Info
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: undefined | map(),
    %% ClientInfo override specs
    clientinfo_override :: map(),
    %% Channel State
    conn_state :: emqx_gateway_channel:conn_state() | anonymous,
    %% Subscriptions
    subscriptions :: [subscription()],
    %% Timer
    timers :: #{atom() => disable | undefined | reference()},
    %% Transaction
    transaction :: #{binary() => list()}
}).

-type channel() :: #channel{}.
-type replies() :: emqx_gateway_channel:replies().

-type subscription() :: #{
    sid => binary(),
    mounted_topic => binary(),
    subject => binary(),
    max_msgs => non_neg_integer() | infinity,
    sub_opts => map()
}.

-define(TIMER_TABLE, #{
    keepalive_send_timer => keepalive_send,
    keepalive_recv_timer => keepalive_recv,

    clean_trans_timer => clean_trans,
    connection_expire_timer => connection_expire
}).

-define(KEEPALIVE_SEND_INTERVAL, 30000).

-define(KEEPALIVE_RECV_INTERVAL, 5000).

-define(DEFAULT_OVERRIDE, #{
    username => <<"${Packet.user}">>,
    password => <<"${Packet.pass}">>
}).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).
-define(RAND_CLIENTID_BYETS, 16).
-define(ALLOW_PUB_SUB(S), (S =:= connected orelse S =:= anonymous)).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

%% @doc Init protocol
init(
    ConnInfo = #{
        peername := {PeerHost, _} = PeerName,
        sockname := {_, SockPort}
    },
    Option
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Option, undefined),
    ListenerId =
        case maps:get(listener, Option, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    EnableAuthn = maps:get(enable_authn, Option, true),
    ClientInfo = setting_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => nats,
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            clientid => emqx_utils:rand_id(?RAND_CLIENTID_BYETS),
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            enable_authn => EnableAuthn,
            mountpoint => Mountpoint
        }
    ),

    Ctx = maps:get(ctx, Option),
    Override = maps:merge(
        ?DEFAULT_OVERRIDE,
        maps:get(clientinfo_override, Option, #{})
    ),
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        clientinfo_override = Override,
        timers = #{},
        transaction = #{},
        conn_state = idle,
        subscriptions = []
    },
    Channel1 = init_conn_state(Channel),
    trigger_post_init(Channel1).

trigger_post_init(Channel) ->
    case Channel#channel.conninfo of
        #{conn_mod := emqx_gateway_conn} ->
            self() ! {timeout, undefined, post_init},
            Channel;
        _ ->
            Channel
    end.

init_conn_state(Channel = #channel{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    case is_auth_required(ClientInfo) of
        false ->
            NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
            ClientId = maps:get(clientid, ClientInfo),
            emqx_logger:set_metadata_clientid(ClientId),
            Channel#channel{conninfo = NConnInfo, conn_state = anonymous};
        true ->
            Channel
    end.

info_frame(#channel{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    {SockHost, SockPort} = maps:get(sockname, ConnInfo),
    {ok, Vsn} = application:get_key(emqx_gateway_nats, vsn),
    {TlsRequired, TlsVerify} = tls_required_and_verify(maps:get(listener, ClientInfo)),
    MsgContent = #{
        server_id => emqx_conf:get([gateway, nats, server_id]),
        server_name => emqx_conf:get([gateway, nats, server_name]),
        version => list_to_binary(Vsn),
        host => list_to_binary(inet:ntoa(SockHost)),
        port => SockPort,
        max_payload => emqx_conf:get([gateway, nats, protocol, max_payload_size]),
        proto => 0,
        headers => true,
        auth_required => is_auth_required(ClientInfo),
        tls_handshake_first => false,
        tls_required => TlsRequired,
        tls_verify => TlsVerify,
        jetstream => false
    },
    #nats_frame{operation = ?OP_INFO, message = MsgContent}.

is_auth_required(#{enable_authn := false}) ->
    false;
is_auth_required(#{enable_authn := true}) ->
    case emqx_conf:get([gateway, nats, authentication], undefined) of
        undefined ->
            false;
        _ ->
            true
    end.

tls_required_and_verify(ListenerId) ->
    F = fun(T, N) ->
        emqx_conf:get([gateway, nats, listeners, T, N, ssl_options, verify], verify_none) =:=
            verify_peer
    end,
    case emqx_gateway_utils:parse_listener_id(ListenerId) of
        {_, <<"ssl">>, Name} ->
            {true, F("ssl", Name)};
        {_, <<"wss">>, Name} ->
            {true, F("wss", Name)};
        _ ->
            {false, false}
    end.

setting_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
setting_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, _) ->
    #{};
info(will_msg, _) ->
    undefined;
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{subscriptions = Subs}) ->
    [{subscriptions_cnt, length(Subs)}].

set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

check_no_responders(
    #nats_frame{operation = ?OP_CONNECT, message = ConnParams}, _Channel
) ->
    NoResponders = maps:get(<<"no_responders">>, ConnParams, false),
    Headers = maps:get(<<"headers">>, ConnParams, false),
    case {NoResponders, Headers} of
        {false, _} ->
            ok;
        {true, false} ->
            {error, no_responders_required_headers_support};
        {true, true} ->
            ok
    end.

enrich_conninfo(
    #nats_frame{operation = ?OP_CONNECT, message = ConnParams},
    Channel = #channel{conninfo = ConnInfo}
) ->
    NConnInfo = ConnInfo#{
        proto_name => <<"NATS">>,
        proto_ver => <<"1">>,
        clean_start => true,
        keepalive => emqx_conf:get(
            [gateway, nats, default_heartbeat_interval], ?KEEPALIVE_SEND_INTERVAL
        ),
        expiry_interval => 0,
        conn_props => #{},
        receive_maximum => 0,
        no_responders => maps:get(no_responders, ConnParams, 0),
        conn_params => ConnParams
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

run_conn_hooks(
    Packet,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    %% XXX: Assign headers of Packet to ConnProps
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Packet, Channel}
    end.

enrich_clientinfo(
    Frame = #nats_frame{message = ConnParams},
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo0,
        clientinfo_override = Override
    }
) ->
    ClientInfo1 = write_clientinfo(
        feedvar(Override, ConnParams, ConnInfo, ClientInfo0),
        ClientInfo0
    ),
    {ok, _, ClientInfo2} = emqx_utils:pipeline(
        [
            %% FIXME: CALL After authentication successfully
            fun fix_mountpoint/2
        ],
        ConnParams,
        ClientInfo1
    ),
    {ok, Frame, Channel#channel{clientinfo = ClientInfo2}}.

assign_clientid_to_conninfo(
    Packet,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    ClientId = maps:get(clientid, ClientInfo),
    NConnInfo = maps:put(clientid, ClientId, ConnInfo),
    {ok, Packet, Channel#channel{conninfo = NConnInfo}}.

feedvar(Override, ConnParams, ConnInfo, ClientInfo) ->
    Envs = #{
        'ConnInfo' => ConnInfo,
        'ClientInfo' => ClientInfo,
        'Packet' => ConnParams
    },
    maps:map(
        fun(_K, V) ->
            Tokens = emqx_placeholder:preproc_tmpl(V),
            emqx_placeholder:proc_tmpl(Tokens, Envs)
        end,
        Override
    ).

write_clientinfo(Override, ClientInfo) ->
    Override1 = maps:with([username, password, clientid], Override),
    maps:merge(ClientInfo, Override1).

fix_mountpoint(_Packet, #{mountpoint := undefined}) ->
    ok;
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the variable replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

set_log_meta(_Packet, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

auth_connect(
    _Packet,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?SLOG(warning, #{
                tag => ?TAG,
                msg => "client_login_failed",
                clientid => ClientId,
                username => Username,
                reason => Reason
            }),
            {error, Reason}
    end.

ensure_connected(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
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
            ensure_timer(connection_expire_timer, Interval, Channel)
    end.

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
            handle_out(connected, [], Channel#channel{session = Session});
        {error, Reason} ->
            ?SLOG(error, #{
                tag => ?TAG,
                msg => "failed_to_open_session",
                reason => Reason
            }),
            ErrMsg = io_lib:format("Failed to open session: ~ts", [Reason]),
            handle_out(connerr, {failed_to_open_session, ErrMsg}, Channel)
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(nats_frame() | {frame_error, any()}, channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

handle_in(
    Packet = ?PACKET(?OP_CONNECT),
    Channel = #channel{conn_state = connected}
) ->
    %% Update conn_params if received double connect packet
    case check_no_responders(Packet, Channel) of
        {error, Reason} ->
            ErrMsg = io_lib:format("Failed to check no responders: ~ts", [Reason]),
            handle_out(error, ErrMsg, Channel);
        ok ->
            {ok, Channel1} = enrich_conninfo(Packet, Channel),
            handle_out(ok, [{event, updated}], Channel1)
    end;
handle_in(
    Packet = ?PACKET(?OP_CONNECT),
    Channel = #channel{conn_state = ConnState}
) when ConnState =:= anonymous; ConnState =:= idle ->
    case
        emqx_utils:pipeline(
            [
                fun check_no_responders/2,
                fun enrich_conninfo/2,
                fun enrich_clientinfo/2,
                fun assign_clientid_to_conninfo/2,
                fun run_conn_hooks/2,
                fun set_log_meta/2,
                fun auth_connect/2
            ],
            Packet,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, _NPacket, NChannel} ->
            process_connect(ensure_connected(NChannel));
        {error, ReasonCode, NChannel} ->
            ErrMsg = io_lib:format("Login Failed: ~ts", [ReasonCode]),
            handle_out(connerr, {ReasonCode, ErrMsg}, NChannel)
    end;
handle_in(
    Frame = ?PACKET(Op),
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo,
        conn_state = ConnState
    }
) when (Op =:= ?OP_PUB orelse Op =:= ?OP_HPUB) andalso ?ALLOW_PUB_SUB(ConnState) ->
    Subject = emqx_nats_frame:subject(Frame),
    Topic = emqx_nats_topic:nats_to_mqtt(Subject),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, ?AUTHZ_PUBLISH, Topic) of
        deny ->
            handle_out(error, err_msg_publish_denied(Subject), Channel);
        allow ->
            process_pub_frame(Frame, Channel)
    end;
handle_in(
    Frame = ?PACKET(?OP_SUB),
    Channel = #channel{
        ctx = Ctx,
        subscriptions = Subs,
        clientinfo = ClientInfo,
        conn_state = ConnState
    }
) when ?ALLOW_PUB_SUB(ConnState) ->
    SId = emqx_nats_frame:sid(Frame),
    Subject = emqx_nats_frame:subject(Frame),
    Topic = emqx_nats_topic:nats_to_mqtt(Subject),
    Topic1 =
        case emqx_nats_frame:queue_group(Frame) of
            undefined -> Topic;
            QGroup -> <<"$share/", QGroup/binary, "/", Topic/binary>>
        end,
    case
        emqx_utils:pipeline(
            [
                fun parse_topic_filter/2,
                fun check_subscribed_status/2,
                fun check_sub_acl/2
            ],
            {SId, Topic1},
            Channel
        )
    of
        {ok, {_, TopicFilter}, NChannel} ->
            TopicFilters = [TopicFilter],
            NTopicFilters = run_hooks(
                Ctx,
                'client.subscribe',
                [ClientInfo, #{}],
                TopicFilters
            ),
            case do_subscribe(NTopicFilters, NChannel) of
                [] ->
                    ErrMsg = io_lib:format(
                        "The client.subscribe hook blocked the ~s subscription request",
                        [TopicFilter]
                    ),
                    handle_out(error, ErrMsg, NChannel);
                [{MountedTopic, SubOpts} | _] ->
                    Subscription = #{
                        sid => SId,
                        mounted_topic => MountedTopic,
                        subject => Subject,
                        max_msgs => infinity,
                        sub_opts => SubOpts
                    },
                    NSubs = [Subscription | Subs],
                    NChannel1 = NChannel#channel{subscriptions = NSubs},
                    ?SLOG(info, #{
                        tag => ?TAG,
                        msg => "client_subscribe_success",
                        subject => Subject,
                        sid => SId,
                        topic => MountedTopic
                    }),
                    handle_out(ok, [{event, updated}], NChannel1)
            end;
        {error, {subscription_id_inused, {InusedSId, InusedSubject}}, NChannel} ->
            ErrMsg = io_lib:format("SId already in used by pair: ~s, ~s", [InusedSId, InusedSubject]),
            handle_out(error, ErrMsg, NChannel);
        {error, {topic_already_subscribed, {InusedSId, InusedSubject}}, NChannel} ->
            ErrMsg = io_lib:format("Subject already in subscribed by pair: ~s, ~s", [
                InusedSId, InusedSubject
            ]),
            handle_out(error, ErrMsg, NChannel);
        {error, acl_denied, NChannel} ->
            handle_out(error, err_msg_subscribe_denied(Subject), NChannel)
    end;
handle_in(
    Frame = ?PACKET(?OP_UNSUB),
    Channel = #channel{
        subscriptions = Subs,
        conn_state = ConnState
    }
) when ?ALLOW_PUB_SUB(ConnState) ->
    SId = emqx_nats_frame:sid(Frame),
    MaxMsgs = emqx_nats_frame:max_msgs(Frame),
    case find_sub_by_sid(SId, Subs) of
        #{} when MaxMsgs =:= 0 ->
            handle_out(ok, [{event, updated}], do_unsubscribe(SId, Channel));
        #{} when MaxMsgs > 0 ->
            NSubs = update_sub_max_msgs(SId, MaxMsgs, Subs),
            handle_out(ok, [], Channel#channel{subscriptions = NSubs});
        false ->
            ?SLOG(info, #{
                tag => ?TAG,
                msg => "ignore_unsubscribe_for_unknown_sid",
                sid => SId
            }),
            handle_out(ok, [], Channel)
    end;
handle_in(
    ?PACKET(Op),
    Channel
) when Op =:= ?OP_PUB orelse Op =:= ?OP_HPUB; Op =:= ?OP_SUB orelse Op =:= ?OP_UNSUB ->
    handle_out(error, <<"Must be connected to publish or subscribe">>, Channel);
handle_in(Frame = ?PACKET(?OP_OK), Channel) ->
    ?SLOG(info, #{
        tag => ?TAG,
        msg => "ignore_all_ok_frames",
        function => "handle_in/2",
        frame => Frame
    }),
    {ok, Channel};
handle_in(?PACKET(?OP_PING), Channel) ->
    handle_out(pong, [], Channel);
handle_in(?PACKET(?OP_PONG), Channel) ->
    Channel1 = ensure_timer(
        keepalive_send_timer,
        cancel_timer(keepalive_recv_timer, Channel)
    ),
    {ok, Channel1};
handle_in(Msg, Channel) ->
    ?SLOG(error, #{
        tag => ?TAG,
        msg => "unexpected_msg",
        function => "handle_in/2",
        message => Msg
    }),
    {ok, Channel}.

handle_frame_error(Reason, Channel = #channel{conn_state = idle}) ->
    shutdown(to_atom_shutdown_reason(Reason), Channel);
handle_frame_error(Reason, Channel = #channel{conn_state = _ConnState}) ->
    ErrMsg = io_lib:format("Frame error: ~0p", [Reason]),
    Frame = error_frame(ErrMsg),
    shutdown(to_atom_shutdown_reason(Reason), Frame, Channel).

to_atom_shutdown_reason(R) when is_atom(R) ->
    R;
to_atom_shutdown_reason({R, _}) when is_atom(R) ->
    R.

%%--------------------------------------------------------------------
%% Subs

parse_topic_filter({SId, Topic}, Channel) ->
    {ParsedTopic, SubOpts} = emqx_topic:parse(Topic),
    NSubOpts = SubOpts#{sub_props => #{sid => SId}},
    {ok, {SId, {ParsedTopic, NSubOpts}}, Channel}.

check_subscribed_status(
    {SId, {ParsedTopic, _SubOpts}},
    #channel{
        subscriptions = Subs,
        clientinfo = #{mountpoint := Mountpoint}
    }
) ->
    MountedTopic = emqx_mountpoint:mount(Mountpoint, ParsedTopic),
    case find_sub_by_sid(SId, Subs) of
        #{subject := Subject} ->
            {error, {subscription_id_inused, {SId, Subject}}};
        false ->
            case find_sub_by_topic(MountedTopic, Subs) of
                #{sid := OtherSId, subject := Subject} ->
                    {error, {topic_already_subscribed, {OtherSId, Subject}}};
                false ->
                    ok
            end
    end.

check_sub_acl(
    {_SId, {ParsedTopic, _SubOpts}},
    #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    %% QoS is not supported in stomp
    Action = ?AUTHZ_SUBSCRIBE,
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, ParsedTopic) of
        deny -> {error, acl_denied};
        allow -> ok
    end.

do_subscribe(TopicFilters, Channel) ->
    do_subscribe(TopicFilters, Channel, []).

do_subscribe([], _Channel, Acc) ->
    lists:reverse(Acc);
do_subscribe(
    [{ParsedTopic, SubOpts0} | More],
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{
                    clientid := ClientId,
                    mountpoint := Mountpoint
                }
    },
    Acc
) ->
    SubOpts = maps:merge(emqx_gateway_utils:default_subopts(), SubOpts0),
    MountedTopic = emqx_mountpoint:mount(Mountpoint, ParsedTopic),
    _ = emqx_broker:subscribe(MountedTopic, ClientId, SubOpts),
    run_hooks(Ctx, 'session.subscribed', [ClientInfo, MountedTopic, SubOpts]),
    do_subscribe(More, Channel, [{MountedTopic, SubOpts} | Acc]).

do_unsubscribe(
    SId,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo,
        subscriptions = Subs
    }
) ->
    Mountpoint = maps:get(mountpoint, ClientInfo),
    case find_sub_by_sid(SId, Subs) of
        #{mounted_topic := MountedTopic, subject := Subject} ->
            Topic = emqx_mountpoint:unmount(Mountpoint, MountedTopic),
            _ = run_hooks(
                Ctx,
                'client.unsubscribe',
                [ClientInfo, #{}],
                [{Topic, #{}}]
            ),
            ok = emqx_broker:unsubscribe(MountedTopic),
            _ = run_hooks(
                Ctx,
                'session.unsubscribed',
                [ClientInfo, MountedTopic, #{}]
            ),
            ?SLOG(info, #{
                tag => ?TAG,
                msg => "client_unsubscribe_success",
                subject => Subject,
                sid => SId,
                topic => MountedTopic
            }),
            Channel#channel{subscriptions = remove_sub_by_sid(SId, Subs)};
        false ->
            Channel
    end.
%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec handle_out(atom(), term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

handle_out(connerr, {Reason, ErrMsg}, Channel) ->
    shutdown(Reason, error_frame(ErrMsg), Channel);
handle_out(error, ErrMsg, Channel) ->
    {ok, {outgoing, error_frame(ErrMsg)}, Channel};
handle_out(
    connected,
    Replies,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    _ = run_hooks(Ctx, 'client.connack', [ConnInfo, connection_accepted, #{}]),
    Replies1 = [
        {event, connected} | Replies
    ],
    handle_out(ok, Replies1, ensure_timer(keepalive_send_timer, Channel));
handle_out(pong, _, Channel) ->
    {ok, {outgoing, #nats_frame{operation = ?OP_PONG}}, Channel};
handle_out(ping, _, Channel) ->
    {ok, {outgoing, #nats_frame{operation = ?OP_PING}}, Channel};
handle_out(ok, Replies, Channel) ->
    case is_verbose_mode(Channel) of
        true ->
            {ok, [{outgoing, #nats_frame{operation = ?OP_OK}} | Replies], Channel};
        false ->
            {ok, Replies, Channel}
    end.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), From :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), replies(), channel()}.
handle_call(subscriptions, _From, Channel = #channel{subscriptions = Subs}) ->
    %% Reply :: [{emqx_types:topic(), emqx_types:subopts()}]
    NSubs = lists:map(
        fun(#{mounted_topic := Topic, sub_opts := SubOpts}) ->
            {Topic, SubOpts}
        end,
        Subs
    ),
    reply({ok, NSubs}, Channel);
handle_call(kick, _From, Channel) ->
    NChannel = ensure_disconnected(kicked, Channel),
    Frame = error_frame(<<"Kicked out">>),
    shutdown_and_reply(kicked, ok, Frame, NChannel);
handle_call(discard, _From, Channel) ->
    Frame = error_frame(<<"Discarded">>),
    shutdown_and_reply(discarded, ok, Frame, Channel);
handle_call(Req, _From, Channel) ->
    ?SLOG(error, #{
        tag => ?TAG,
        msg => "unexpected_call",
        call => Req
    }),
    reply(ignored, Channel).

-spec handle_cast(Req :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_cast(Req, Channel) ->
    ?SLOG(error, #{
        tag => ?TAG,
        msg => "unexpected_cast",
        cast => Req
    }),
    {ok, Channel}.

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.

handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = idle}
) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = connecting}
) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{
        conn_state = ConnState,
        clientinfo = _ClientInfo
    }
) when ?ALLOW_PUB_SUB(ConnState) ->
    NChannel = ensure_disconnected(Reason, Channel),
    shutdown(Reason, NChannel);
handle_info(
    {sock_closed, _Reason},
    Channel = #channel{conn_state = disconnected}
) ->
    %% This can happen as a race:
    %% EMQX closes socket and marks 'disconnected' but 'tcp_closed' or 'ssl_closed'
    %% is already in process mailbox
    {ok, Channel};
handle_info(clean_authz_cache, Channel) ->
    ok = emqx_authz_cache:empty_authz_cache(),
    {ok, Channel};
handle_info(after_init, Channel) ->
    handle_after_init(Channel);
handle_info(Info, Channel) ->
    ?SLOG(error, #{
        tag => ?TAG,
        msg => "unexpected_info",
        info => Info
    }),
    {ok, Channel}.

handle_after_init(Channel) ->
    Replies = [{outgoing, info_frame(Channel)}],
    {ok, Replies, Channel}.

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(
    Reason,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        conn_state = ConnState
    }
) ->
    case ConnState of
        connected ->
            NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
            ok = run_hooks(
                Ctx,
                'client.disconnected',
                [ClientInfo, Reason, NConnInfo]
            ),
            Channel#channel{conninfo = NConnInfo, conn_state = disconnected};
        _ ->
            Channel#channel{conn_state = disconnected}
    end.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}.

handle_deliver(
    Delivers,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo = #{mountpoint := Mountpoint},
        subscriptions = Subs
    }
) ->
    {Frames0, NSubs} = lists:foldl(
        fun({deliver, SubTopic, Message}, {FrameAcc, SubsAcc}) ->
            ReplyTo = emqx_message:get_header(reply_to, Message),
            case find_sub_by_topic(SubTopic, SubsAcc) of
                #{sid := SId, max_msgs := MaxMsgs} when MaxMsgs > 0 ->
                    Message1 = emqx_mountpoint:unmount(Mountpoint, Message),
                    metrics_inc('messages.delivered', Channel),
                    NMessage = run_hooks_without_metrics(
                        Ctx,
                        'message.delivered',
                        [ClientInfo],
                        Message1
                    ),
                    MsgContent = #{
                        subject => emqx_nats_topic:mqtt_to_nats(emqx_message:topic(NMessage)),
                        sid => SId,
                        reply_to => ReplyTo,
                        payload => emqx_message:payload(NMessage)
                    },
                    Frame = #nats_frame{
                        operation = ?OP_MSG,
                        message = MsgContent
                    },
                    {[Frame | FrameAcc], reduce_sub_max_msgs(SId, SubsAcc)};
                #{max_msgs := 0} ->
                    metrics_inc('delivery.dropped', Channel),
                    metrics_inc('delivery.dropped.max_msgs', Channel),
                    {FrameAcc, SubsAcc};
                false ->
                    ?SLOG(error, #{
                        tag => ?TAG,
                        msg => "dropped_message_due_to_subscription_not_found",
                        message => Message,
                        matched_subscription_topic => SubTopic,
                        message_topic => emqx_message:topic(Message)
                    }),
                    metrics_inc('delivery.dropped', Channel),
                    metrics_inc('delivery.dropped.no_subid', Channel),
                    {FrameAcc, SubsAcc}
            end
        end,
        {[], Subs},
        Delivers
    ),
    %% Unsubscribe from subscriptions that have reached max_msgs
    Channel1 = Channel#channel{subscriptions = NSubs},
    Channel2 = lists:foldl(
        fun
            (#{max_msgs := 0, sid := SId}, Acc) ->
                do_unsubscribe(SId, Acc);
            (_, Acc) ->
                Acc
        end,
        Channel1,
        NSubs
    ),

    {ok, [{outgoing, lists:reverse(Frames0)}, {event, updated}], Channel2}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.

handle_timeout(_, post_init, Channel) ->
    handle_after_init(Channel);
handle_timeout(
    _TRef,
    {keepalive_send, _NewVal},
    Channel
) ->
    Channel1 = ensure_timer(
        keepalive_recv_timer,
        clean_timer(keepalive_send_timer, Channel)
    ),
    handle_out(ping, [], Channel1);
handle_timeout(_TRef, keepalive_recv, Channel) ->
    ErrMsg = <<"Keepalive recv timeout">>,
    shutdown(keepalive_recv_timeout, error_frame(ErrMsg), Channel);
handle_timeout(_TRef, connection_expire, Channel) ->
    ErrMsg = <<"Connection expired">>,
    shutdown(expired, error_frame(ErrMsg), Channel).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(Reason, #channel{
    ctx = Ctx,
    session = Session,
    clientinfo = ClientInfo
}) ->
    run_hooks(Ctx, 'session.terminated', [ClientInfo, Reason, Session]).

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(Reason, AckFrame, Channel) ->
    {shutdown, Reason, AckFrame, Channel}.

shutdown_and_reply(Reason, Reply, OutPkt, Channel) ->
    {shutdown, Reason, Reply, OutPkt, Channel}.

err_msg_publish_denied(Subject) ->
    iolist_to_binary(io_lib:format("Permissions Violation for Publish to ~s", [Subject])).

err_msg_subscribe_denied(Subject) ->
    iolist_to_binary(io_lib:format("Permissions Violation for Subscription to ~s", [Subject])).

error_frame(Msg) ->
    Msg1 = iolist_to_binary(Msg),
    #nats_frame{operation = ?OP_ERR, message = Msg1}.

frame2message(
    Frame,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = #{
            protocol := Protocol,
            clientid := ClientId,
            username := Username,
            peerhost := PeerHost,
            mountpoint := Mountpoint
        }
    }
) ->
    ProtoVer = maps:get(proto_ver, ConnInfo, <<"1">>),
    Subject = emqx_nats_frame:subject(Frame),
    Topic = emqx_nats_topic:nats_to_mqtt(Subject),
    Payload = emqx_nats_frame:payload(Frame),
    Headers = emqx_nats_frame:headers(Frame),
    ReplyTo = emqx_nats_frame:reply_to(Frame),
    QoS =
        case is_verbose_mode(Channel) of
            true -> 1;
            false -> 0
        end,
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    %% Pass-through of custom headers on the sending side
    Headers0 = #{
        proto_ver => ProtoVer,
        protocol => Protocol,
        username => Username,
        peerhost => PeerHost,
        nats_headers => Headers,
        reply_to => ReplyTo
    },
    Headers1 =
        case ReplyTo of
            undefined ->
                Headers0;
            _ ->
                Headers0#{reply_to => ReplyTo}
        end,
    NMsg = emqx_message:set_headers(Headers1, Msg),
    emqx_mountpoint:mount(Mountpoint, NMsg).

process_pub_frame(Frame, Channel) ->
    Msg = frame2message(Frame, Channel),
    ReplyToSubject = emqx_nats_frame:reply_to(Frame),
    PubResult = emqx_broker:publish(Msg),
    Replies = no_responders_fastfails(PubResult, ReplyToSubject, Channel),
    handle_out(ok, Replies, Channel).

no_responders_fastfails([], ReplyToSubject, Channel = #channel{conninfo = ConnInfo}) when
    is_binary(ReplyToSubject)
->
    ConnParams = maps:get(conn_params, ConnInfo, #{}),
    NoResponders = maps:get(<<"no_responders">>, ConnParams, false),
    Sub = match_subs_by_subject(ReplyToSubject, Channel),
    case {NoResponders, Sub} of
        {true, #{sid := SId}} ->
            Hmsg = #nats_frame{
                operation = ?OP_HMSG,
                message = #{
                    sid => SId,
                    subject => ReplyToSubject,
                    headers => #{
                        <<"code">> => 503
                    },
                    payload => <<>>
                }
            },
            [{outgoing, Hmsg}];
        {_, _} ->
            []
    end;
no_responders_fastfails(_, _, _) ->
    [].

%%--------------------------------------------------------------------
%% Timer

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso is_integer(Time) andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_utils:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

cancel_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    case TRef == undefined of
        true ->
            Channel;
        false ->
            emqx_utils:cancel_timer(TRef),
            Channel#channel{timers = maps:remove(Name, Timers)}
    end.

interval(keepalive_send_timer, #channel{conninfo = ConnInfo}) ->
    maps:get(keepalive, ConnInfo, ?KEEPALIVE_SEND_INTERVAL);
interval(keepalive_recv_timer, _) ->
    emqx_conf:get([gateway, nats, heartbeat_wait_timeout], ?KEEPALIVE_RECV_INTERVAL).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

run_hooks_without_metrics(_Ctx, Name, Args, Acc) ->
    emqx_hooks:run_fold(Name, Args, Acc).

metrics_inc(Name, #channel{ctx = Ctx}) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

is_verbose_mode(_Channel = #channel{conninfo = #{conn_params := ConnParams}}) ->
    maps:get(<<"verbose">>, ConnParams, true);
is_verbose_mode(_) ->
    true.

find_sub_by_topic(_Topic, []) ->
    false;
find_sub_by_topic(Topic, [E = #{mounted_topic := Topic} | _]) ->
    E;
find_sub_by_topic(Topic, [E = #{mounted_topic := {share, _Group, Topic}} | _]) ->
    E;
find_sub_by_topic(Topic, [_ | Rest]) ->
    find_sub_by_topic(Topic, Rest).

find_sub_by_sid(_SId, []) ->
    false;
find_sub_by_sid(SId, [E = #{sid := SId} | _]) ->
    E;
find_sub_by_sid(SId, [_ | Rest]) ->
    find_sub_by_sid(SId, Rest).

match_subs_by_subject(
    Subject,
    #channel{
        subscriptions = Subs,
        clientinfo = #{mountpoint := Mountpoint}
    }
) ->
    Topic0 = emqx_nats_topic:nats_to_mqtt(Subject),
    Topic1 = emqx_mountpoint:mount(Mountpoint, Topic0),
    match_sub_by_topic(Topic1, Subs).

match_sub_by_topic(_, []) ->
    false;
match_sub_by_topic(Topic, [E = #{mounted_topic := TopicFilter} | Rest]) ->
    case emqx_topic:match(Topic, TopicFilter) of
        true ->
            E;
        false ->
            match_sub_by_topic(Topic, Rest)
    end.

remove_sub_by_sid(SId, Subs) ->
    lists:filter(fun(#{sid := Id}) -> SId =/= Id end, Subs).

update_sub_max_msgs(SId, MaxMsgs, Subs) ->
    lists:map(
        fun
            (#{sid := Id} = Sub) when SId =:= Id ->
                Sub#{max_msgs => MaxMsgs};
            (Sub) ->
                Sub
        end,
        Subs
    ).

reduce_sub_max_msgs(SId, Subs) ->
    lists:map(
        fun
            (#{sid := Id, max_msgs := MaxMsgs} = Sub) when SId =:= Id andalso MaxMsgs > 0 ->
                Sub#{max_msgs := checked_sub_max_msgs(MaxMsgs)};
            (Sub) ->
                Sub
        end,
        Subs
    ).

checked_sub_max_msgs(Max) ->
    case Max of
        infinity -> infinity;
        _ -> Max - 1
    end.
