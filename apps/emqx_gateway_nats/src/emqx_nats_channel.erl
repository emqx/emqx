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
    conn_state :: emqx_gateway_channel:conn_state(),
    %% Subscriptions
    subscriptions = [],
    %% Timer
    timers :: #{atom() => disable | undefined | reference()},
    %% Transaction
    transaction :: #{binary() => list()}
}).

-type channel() :: #channel{}.
-type replies() :: emqx_gateway_channel:replies().

-define(TIMER_TABLE, #{
    keepalive_send_timer => keepalive_send,
    keepalive_recv_timer => keepalive_recv,

    clean_trans_timer => clean_trans,
    connection_expire_timer => connection_expire
}).

-define(KEEPALIVE_SEND_INTERVAL, 25000).

-define(KEEPALIVE_RECV_INTERVAL, 15000).

-define(TRANS_TIMEOUT, 60000).

-define(DEFAULT_OVERRIDE,
    %% Generate clientid by default
    #{
        clientid => <<"">>,
        username => <<"${Packet.user}">>,
        password => <<"${Packet.pass}">>
    }
).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).
-define(RAND_CLIENTID_BYETS, 16).

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
            clientid => undefined,
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
        conn_state = idle
    },
    _ = async_delivery_info_frame(Channel),
    Channel.

async_delivery_info_frame(_Channel) ->
    MsgContent = #{
        server_id => <<"example_server_id">>,
        server_name => <<"example_server_name">>,
        version => <<"0.1.0">>,
        host => <<"0.0.0.0">>,
        port => 20020,
        max_payload => ?DEFAULT_MAX_PAYLOAD,
        proto => 0,
        headers => false,
        auth_required => false,
        tls_required => false,
        jetstream => false
    },
    Frame = #nats_frame{operation = ?OP_INFO, message = MsgContent},
    self() ! {outgoing, Frame},
    ok.

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

enrich_conninfo(
    #nats_frame{operation = ?OP_CONNECT, message = ConnParams},
    Channel = #channel{conninfo = ConnInfo}
) ->
    NConnInfo = ConnInfo#{
        proto_name => <<"NATS">>,
        proto_ver => <<"1">>,
        clean_start => true,
        keepalive => ?KEEPALIVE_SEND_INTERVAL,
        expiry_interval => 0,
        conn_props => #{},
        receive_maximum => 0,
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
            fun maybe_assign_clientid/2,
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

maybe_assign_clientid(_Packet, ClientInfo = #{clientid := ClientId}) when
    ClientId == undefined;
    ClientId == <<>>
->
    {ok, ClientInfo#{clientid => emqx_utils:rand_id(?RAND_CLIENTID_BYETS)}};
maybe_assign_clientid(_Packet, ClientInfo) ->
    {ok, ClientInfo}.

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
    ?PACKET(?OP_CONNECT),
    Channel = #channel{conn_state = connected}
) ->
    {error, unexpected_connect, Channel};
handle_in(Packet = ?PACKET(?OP_CONNECT), Channel) ->
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun enrich_clientinfo/2,
                fun assign_clientid_to_conninfo/2,
                fun run_conn_hooks/2,
                fun set_log_meta/2,
                %% TODO: How to implement the banned in the gateway instance?
                %, fun check_banned/2
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
    Frame = ?PACKET(?OP_PUB),
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    Subject = emqx_nats_frame:subject(Frame),
    Topic = emqx_nats_topic:nats_to_mqtt(Subject),
    %% FIXME: replay to the sender
    %ReplyTo = emqx_nats_frame:reply_to(Frame),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, ?AUTHZ_PUBLISH, Topic) of
        deny ->
            handle_out(error, err_frame_publish_denied(Subject), Channel);
        allow ->
            process_pub_frame(Frame, Channel)
    end;
handle_in(
    Frame = ?PACKET(?OP_SUB),
    Channel = #channel{
        ctx = Ctx,
        subscriptions = Subs,
        clientinfo = ClientInfo
    }
) ->
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
                    NSubs = [{SId, MountedTopic, Subject, SubOpts} | Subs],
                    NChannel1 = NChannel#channel{subscriptions = NSubs},
                    ?SLOG(info, #{
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
            handle_out(error, err_frame_subscribe_denied(Subject), NChannel)
    end;
handle_in(
    Frame = ?PACKET(?OP_UNSUB),
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint},
        subscriptions = Subs
    }
) ->
    SId = emqx_nats_frame:sid(Frame),
    case lists:keyfind(SId, 1, Subs) of
        {SId, MountedTopic, Subject, _SubOpts} ->
            Topic = emqx_mountpoint:unmount(Mountpoint, MountedTopic),
            %% XXX: eval the return topics?
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
                msg => "client_unsubscribe_success",
                subject => Subject,
                sid => SId,
                topic => MountedTopic
            }),
            Channel1 = Channel#channel{subscriptions = lists:keydelete(SId, 1, Subs)},
            handle_out(ok, [{event, updated}], Channel1);
        false ->
            ?SLOG(info, #{
                msg => "ignore_unsubscribe_for_unknown_sid",
                sid => SId
            }),
            handle_out(ok, [], Channel)
    end;
%% FIXME: How to ack a frame ???
handle_in(Frame = ?PACKET(?OP_OK), Channel) ->
    ?SLOG(warning, #{
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
    case lists:keyfind(SId, 1, Subs) of
        {SId, _MountedTopic, Subject, _} ->
            {error, {subscription_id_inused, {SId, Subject}}};
        false ->
            case lists:keyfind(MountedTopic, 2, Subs) of
                {_OtherSId, MountedTopic, Subject, _} ->
                    {error, {topic_already_subscribed, {SId, Subject}}};
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
        fun({_SubId, Topic, _Subject, SubOpts}) ->
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
        msg => "unexpected_call",
        call => Req
    }),
    reply(ignored, Channel).

-spec handle_cast(Req :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_cast(Req, Channel) ->
    ?SLOG(error, #{
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
        conn_state = connected,
        clientinfo = _ClientInfo
    }
) ->
    %% XXX: Flapping detect ???
    %% How to get the flapping detect policy ???
    %emqx_zone:enable_flapping_detect(Zone)
    %    andalso emqx_flapping:detect(ClientInfo),
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
handle_info(Info, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_info",
        info => Info
    }),
    {ok, Channel}.

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
    Frames0 = lists:foldl(
        fun({_, _, Message}, Acc) ->
            Topic = emqx_message:topic(Message),
            case find_sub_by_topic(Topic, Subs) of
                {SId, _Topic, Subject, _SubOpts} ->
                    Message1 = emqx_mountpoint:unmount(Mountpoint, Message),
                    metrics_inc('messages.delivered', Channel),
                    NMessage = run_hooks_without_metrics(
                        Ctx,
                        'message.delivered',
                        [ClientInfo],
                        Message1
                    ),
                    MsgContent = #{
                        subject => Subject,
                        sid => SId,
                        payload => emqx_message:payload(NMessage)
                    },
                    Frame = #nats_frame{
                        operation = ?OP_MSG,
                        message = MsgContent
                    },
                    [Frame | Acc];
                false ->
                    ?SLOG(error, #{
                        msg => "dropped_message_due_to_subscription_not_found",
                        message => Message,
                        topic => emqx_message:topic(Message)
                    }),
                    metrics_inc('delivery.dropped', Channel),
                    metrics_inc('delivery.dropped.no_subid', Channel),
                    Acc
            end
        end,
        [],
        Delivers
    ),
    {ok, [{outgoing, lists:reverse(Frames0)}], Channel}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.

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

err_frame_publish_denied(Subject) ->
    Msg = io_lib:format("Permissions Violation for Publish to ~s", [Subject]),
    error_frame(Msg).

err_frame_subscribe_denied(Subject) ->
    Msg = io_lib:format("Permissions Violation for Subscription to ~s", [Subject]),
    error_frame(Msg).

error_frame(Msg) ->
    Msg1 = iolist_to_binary(Msg),
    #nats_frame{operation = ?OP_ERR, message = Msg1}.

frame2message(
    Frame,
    Channel = #channel{
        conninfo = #{proto_ver := ProtoVer},
        clientinfo = #{
            protocol := Protocol,
            clientid := ClientId,
            username := Username,
            peerhost := PeerHost,
            mountpoint := Mountpoint
        }
    }
) ->
    Subject = emqx_nats_frame:subject(Frame),
    Topic = emqx_nats_topic:nats_to_mqtt(Subject),
    Payload = emqx_nats_frame:payload(Frame),
    Headers = emqx_nats_frame:headers(Frame),
    QoS =
        case is_verbose_mode(Channel) of
            true -> 1;
            false -> 0
        end,
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    %% Pass-through of custom headers on the sending side
    NMsg = emqx_message:set_headers(
        #{
            proto_ver => ProtoVer,
            protocol => Protocol,
            username => Username,
            peerhost => PeerHost,
            nats_headers => Headers
        },
        Msg
    ),
    emqx_mountpoint:mount(Mountpoint, NMsg).

process_pub_frame(Frame, Channel) ->
    Msg = frame2message(Frame, Channel),
    _ = emqx_broker:publish(Msg),
    handle_out(ok, [], Channel).

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

interval(keepalive_send_timer, _) ->
    ?KEEPALIVE_SEND_INTERVAL;
interval(keepalive_recv_timer, _) ->
    ?KEEPALIVE_RECV_INTERVAL.

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
    maps:get(<<"verbose">>, ConnParams, false).

find_sub_by_topic(_Topic, []) ->
    false;
find_sub_by_topic(Topic, [E = {_, Topic, _, _} | _]) ->
    E;
find_sub_by_topic(Topic, [E = {_, {share, _Group, Topic}, _, _} | _]) ->
    E;
find_sub_by_topic(Topic, [{_, _, _, _} | Rest]) ->
    find_sub_by_topic(Topic, Rest).
