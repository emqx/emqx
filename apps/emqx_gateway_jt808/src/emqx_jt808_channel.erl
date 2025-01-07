%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_channel).
-behaviour(emqx_gateway_channel).

-include("emqx_jt808.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% behaviour callbacks
-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    init/2,
    handle_in/2,
    handle_frame_error/2,
    handle_deliver/2,
    handle_timeout/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export([
    terminate/2
]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% ConnInfo
    conninfo :: emqx_types:conninfo(),
    %% ClientInfo
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: undefined | map(),
    %% Conn State
    conn_state :: conn_state(),
    %% Timers
    timers :: #{atom() => undefined | disabled | reference()},
    %% AuthCode
    authcode :: undefined | anonymous | binary(),
    %% Keepalive
    keepalive :: option(emqx_keepalive:keepalive()),
    %% Msg SN
    msg_sn,
    %% Down Topic
    dn_topic,
    %% Up Topic
    up_topic,
    %% Auth
    auth :: emqx_jt808_auth:auth(),
    %% Inflight
    inflight :: emqx_inflight:inflight(),
    mqueue :: queue:queue(),
    max_mqueue_len,
    rsa_key,
    retx_interval,
    retx_max_times
}).

-type conn_state() :: idle | connecting | connected | disconnected.

-type channel() :: #channel{}.

-type reply() ::
    {outgoing, emqx_types:packet()}
    | {outgoing, [emqx_types:packet()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

-define(TIMER_TABLE, #{
    alive_timer => keepalive,
    retry_timer => retry_delivery
}).

-define(INFO_KEYS, [ctx, conninfo, zone, clientid, clientinfo, session, conn_state, authcode]).

-define(DN_TOPIC_SUBOPTS, #{rap => 0, nl => 0, qos => 0, rh => 0}).

-define(RETX_INTERVAL, 8000).
-define(RETX_MAX_TIME, 5).

-define(DEFAULT_KEEPALIVE, 300).

-define(MSG(MsgId), #{<<"header">> := #{<<"msg_id">> := MsgId}}).

-dialyzer({nowarn_function, init/2}).

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
info(ctx, #channel{ctx = Ctx}) ->
    Ctx;
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(zone, #channel{clientinfo = #{zone := Zone}}) ->
    Zone;
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    Session;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(authcode, #channel{authcode = AuthCode}) ->
    AuthCode.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{inflight = Inflight, mqueue = Queue}) ->
    %% XXX: A fake stats for managed by emqx_management
    [
        {subscriptions_cnt, 1},
        {subscriptions_max, 1},
        {inflight_cnt, emqx_inflight:size(Inflight)},
        {inflight_max, emqx_inflight:max_size(Inflight)},
        {mqueue_len, queue:len(Queue)},
        {mqueue_max, 0},
        {mqueue_dropped, 0},
        {next_pkt_id, 0},
        {awaiting_rel_cnt, 0},
        {awaiting_rel_max, 0}
    ].

%%--------------------------------------------------------------------
%% Init the Channel
%%--------------------------------------------------------------------

-spec init(emqx_types:conninfo(), map()) -> channel().
init(
    ConnInfo = #{
        peername := {PeerHost, _Port} = PeerName,
        sockname := {_Host, SockPort}
    },
    Options = #{
        ctx := Ctx,
        message_queue_len := MessageQueueLen,
        proto := #{auth := Auth} = ProtoConf
    }
) ->
    % TODO: init rsa_key from user input
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Options, ?DEFAULT_MOUNTPOINT),
    ListenerId =
        case maps:get(listener, Options, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    ClientInfo = setting_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => jt808,
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            clientid => undefined,
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            mountpoint => Mountpoint
        }
    ),

    #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        session = undefined,
        conn_state = idle,
        timers = #{},
        authcode = undefined,
        keepalive = undefined,
        msg_sn = 0,
        % TODO: init rsa_key from user input
        dn_topic = maps:get(dn_topic, ProtoConf, ?DEFAULT_DN_TOPIC),
        up_topic = maps:get(up_topic, ProtoConf, ?DEFAULT_UP_TOPIC),
        auth = emqx_jt808_auth:init(Auth),
        inflight = emqx_inflight:new(128),
        mqueue = queue:new(),
        max_mqueue_len = MessageQueueLen,
        rsa_key = [0, <<0:1024>>],
        retx_interval = maps:get(retry_interval, Options, ?RETX_INTERVAL),
        retx_max_times = maps:get(max_retry_times, Options, ?RETX_MAX_TIME)
    }.

setting_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
setting_peercert_infos(Peercert, ClientInfo) ->
    DN = esockd_peercert:subject(Peercert),
    CN = esockd_peercert:common_name(Peercert),
    ClientInfo#{dn => DN, cn => CN}.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(emqx_jt808_frame:frame() | {frame_error, any()}, channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_in(Frame = ?MSG(MType), Channel = #channel{conn_state = ConnState}) when
    ConnState /= connected, MType =:= ?MC_REGISTER;
    ConnState /= connected, MType =:= ?MC_AUTH
->
    ?SLOG(debug, #{msg => "recv_frame", frame => Frame}),
    do_handle_in(Frame, Channel#channel{conn_state = connecting});
handle_in(Frame, Channel = #channel{conn_state = connected}) ->
    ?SLOG(debug, #{msg => "recv_frame", frame => Frame}),
    do_handle_in(Frame, Channel);
handle_in(Frame = ?MSG(MType), Channel) when
    MType =:= ?MC_DEREGISTER
->
    ?SLOG(debug, #{msg => "recv_frame", frame => Frame, info => "jt808_client_deregister"}),
    do_handle_in(Frame, Channel#channel{conn_state = disconnected});
handle_in(Frame, Channel) ->
    ?SLOG(error, #{msg => "unexpected_jt808_frame", frame => Frame}),
    {shutdown, unexpected_frame, Channel}.

handle_frame_error(Reason, Channel) ->
    {shutdown, Reason, Channel}.

%% @private
do_handle_in(Frame = ?MSG(?MC_GENERAL_RESPONSE), Channel = #channel{inflight = Inflight}) ->
    #{<<"body">> := #{<<"seq">> := Seq, <<"id">> := Id}} = Frame,
    NewInflight = ack_msg(?MC_GENERAL_RESPONSE, {Id, Seq}, Inflight),
    {ok, Channel#channel{inflight = NewInflight}};
do_handle_in(Frame = ?MSG(?MC_REGISTER), Channel0) ->
    #{<<"header">> := #{<<"msg_sn">> := MsgSn}} = Frame,
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2
            ],
            Frame,
            Channel0
        )
    of
        {ok, _NFrame, Channel} ->
            case register_(Frame, Channel) of
                {ok, NChannel} ->
                    handle_out({?MS_REGISTER_ACK, 0}, MsgSn, NChannel);
                {error, ResCode} ->
                    handle_out({?MS_REGISTER_ACK, ResCode}, MsgSn, Channel)
            end
    end;
do_handle_in(Frame = ?MSG(?MC_AUTH), Channel0) ->
    #{<<"header">> := #{<<"msg_sn">> := MsgSn}} = Frame,
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2
            ],
            Frame,
            Channel0
        )
    of
        {ok, _NFrame, Channel} ->
            case authenticate(Frame, Channel) of
                true ->
                    NChannel = process_connect(Frame, ensure_connected(Channel)),
                    authack({0, MsgSn, NChannel});
                false ->
                    authack({1, MsgSn, Channel})
            end
    end;
do_handle_in(Frame = ?MSG(?MC_HEARTBEAT), Channel) ->
    handle_out({?MS_GENERAL_RESPONSE, 0, ?MC_HEARTBEAT}, msgsn(Frame), Channel);
do_handle_in(?MSG(?MC_RSA_KEY), Channel = #channel{rsa_key = [E, N]}) ->
    Response = #{
        <<"header">> => build_frame_header(?MS_RSA_KEY, Channel),
        <<"body">> => #{<<"e">> => E, <<"n">> => N}
    },
    % TODO: how to use client's RSA key?
    {ok, [{outgoing, Response}], state_inc_sn(Channel)};
do_handle_in(?MSG(?MC_MULTIMEDIA_DATA_REPORT), Channel = #channel{rsa_key = [_E, _N]}) ->
    Response = #{
        <<"header">> => build_frame_header(?MS_MULTIMEDIA_DATA_ACK, Channel),
        <<"body">> => #{}
    },
    % TODO: how to fill ?
    {ok, [{outgoing, Response}], state_inc_sn(Channel)};
do_handle_in(
    Frame = ?MSG(?MC_DRIVER_ID_REPORT),
    Channel = #channel{
        up_topic = Topic,
        inflight = Inflight
    }
) ->
    {MsgId, MsgSn} = msgidsn(Frame),
    _ = do_publish(Topic, Frame),
    case is_driver_id_req_exist(Channel) of
        % this is an device passive command
        false ->
            handle_out({?MS_GENERAL_RESPONSE, 0, MsgId}, MsgSn, Channel);
        % this is a response to MS_REQ_DRIVER_ID(0x8702)
        true ->
            {ok, Channel#channel{inflight = ack_msg(?MC_DRIVER_ID_REPORT, none, Inflight)}}
    end;
do_handle_in(?MSG(?MC_DEREGISTER), Channel) ->
    {shutdown, normal, Channel};
do_handle_in(Frame = #{}, Channel = #channel{up_topic = Topic, inflight = Inflight}) ->
    {MsgId, MsgSn} = msgidsn(Frame),
    _ = do_publish(Topic, Frame),
    case is_general_response_needed(MsgId) of
        % these frames device passive request
        true ->
            handle_out({?MS_GENERAL_RESPONSE, 0, MsgId}, MsgSn, Channel);
        % these frames are response to server's request
        false ->
            {ok, Channel#channel{inflight = ack_msg(MsgId, seq(Frame), Inflight)}}
    end;
do_handle_in(Frame, Channel) ->
    ?SLOG(error, #{msg => "ignore_unknown_frame", frame => Frame}),
    {ok, Channel}.

do_publish(Topic, Frame) ->
    ?SLOG(debug, #{msg => "publish_msg", to_topic => Topic, farme => Frame}),
    emqx:publish(emqx_message:make(jt808, ?QOS_1, Topic, emqx_utils_json:encode(Frame))).

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------
-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}.

handle_deliver(
    Messages0,
    Channel = #channel{
        clientinfo = #{mountpoint := Mountpoint},
        mqueue = Queue,
        max_mqueue_len = MaxQueueLen
    }
) ->
    Messages = lists:map(
        fun({deliver, _, M}) ->
            emqx_mountpoint:unmount(Mountpoint, M)
        end,
        Messages0
    ),
    case MaxQueueLen - queue:len(Queue) of
        N when N =< 0 ->
            discard_downlink_messages(Messages, Channel),
            {ok, Channel};
        N ->
            {NMessages, Dropped} = split_by_pos(Messages, N),
            log(debug, #{msg => "enqueue_messages", messages => NMessages}, Channel),
            metrics_inc('messages.delivered', Channel, erlang:length(NMessages)),
            discard_downlink_messages(Dropped, Channel),
            Frames = msgs2frame(NMessages, Channel),
            NQueue = lists:foldl(fun(F, Q) -> queue:in(F, Q) end, Queue, Frames),
            {Outgoings, NChannel} = dispatch_frame(Channel#channel{mqueue = NQueue}),
            {ok, [{outgoing, Outgoings}], NChannel}
    end.

split_by_pos(L, Pos) ->
    split_by_pos(L, Pos, []).

split_by_pos([], _, A1) ->
    {lists:reverse(A1), []};
split_by_pos(L, 0, A1) ->
    {lists:reverse(A1), L};
split_by_pos([E | L], N, A1) ->
    split_by_pos(L, N - 1, [E | A1]).

msgs2frame(Messages, Channel) ->
    lists:filtermap(
        fun(#message{payload = Payload}) ->
            case emqx_utils_json:safe_decode(Payload, [return_maps]) of
                {ok, Map = #{<<"header">> := #{<<"msg_id">> := MsgId}}} ->
                    NewHeader = build_frame_header(MsgId, Channel),
                    Frame = maps:put(<<"header">>, NewHeader, Map),
                    {true, Frame};
                {ok, _} ->
                    tp(
                        error,
                        invalid_dl_message,
                        #{reasons => "missing_msg_id", payload => Payload},
                        Channel
                    ),
                    false;
                {error, _Reason} ->
                    tp(
                        error,
                        invalid_dl_message,
                        #{reason => "invalid_json", payload => Payload},
                        Channel
                    ),
                    false
            end
        end,
        Messages
    ).

authack(
    {Code, MsgSn,
        Channel = #channel{
            conninfo = ConnInfo,
            clientinfo = ClientInfo
        }}
) ->
    Code == 0 andalso emqx_hooks:run('client.connected', [ClientInfo, ConnInfo]),
    handle_out({?MS_GENERAL_RESPONSE, Code, ?MC_AUTH}, MsgSn, Channel).

handle_out({?MS_GENERAL_RESPONSE, Result, InMsgId}, MsgSn, Channel) ->
    Frame = #{
        <<"header">> => build_frame_header(?MS_GENERAL_RESPONSE, Channel),
        <<"body">> => #{<<"seq">> => MsgSn, <<"result">> => Result, <<"id">> => InMsgId}
    },
    {ok, [{outgoing, Frame}], state_inc_sn(Channel)};
handle_out({?MS_REGISTER_ACK, 0}, MsgSn, Channel = #channel{authcode = Authcode0}) ->
    Authcode =
        case Authcode0 == anonymous of
            true -> <<>>;
            false -> Authcode0
        end,
    Frame = #{
        <<"header">> => build_frame_header(?MS_REGISTER_ACK, Channel),
        <<"body">> => #{<<"seq">> => MsgSn, <<"result">> => 0, <<"auth_code">> => Authcode}
    },
    {ok, [{outgoing, Frame}], state_inc_sn(Channel)};
handle_out({?MS_REGISTER_ACK, ResCode}, MsgSn, Channel) ->
    Frame = #{
        <<"header">> => build_frame_header(?MS_REGISTER_ACK, Channel),
        <<"body">> => #{<<"seq">> => MsgSn, <<"result">> => ResCode}
    },
    {ok, [{outgoing, Frame}], state_inc_sn(Channel)}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), From :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), emqx_jt808_frame:frame(), channel()}.

handle_call(kick, _From, Channel) ->
    Channel1 = ensure_disconnected(kicked, Channel),
    disconnect_and_shutdown(kicked, ok, Channel1);
handle_call(discard, _From, Channel) ->
    disconnect_and_shutdown(discarded, ok, Channel);
handle_call(subscriptions, _From, Channel = #channel{dn_topic = DnTopic}) ->
    reply({ok, [{DnTopic, ?DN_TOPIC_SUBOPTS}]}, Channel);
handle_call(Req, _From, Channel) ->
    log(error, #{msg => "unexpected_call", call => Req}, Channel),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle cast
%%--------------------------------------------------------------------

-spec handle_cast(Req :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_cast(_Req, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.

handle_info({sock_closed, Reason}, Channel = #channel{conn_state = idle}) ->
    shutdown(Reason, Channel);
handle_info({sock_closed, Reason}, Channel = #channel{conn_state = connecting}) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel =
        #channel{
            conn_state = connected
        }
) ->
    NChannel = ensure_disconnected(Reason, Channel),
    shutdown(Reason, NChannel);
handle_info({sock_closed, Reason}, Channel = #channel{conn_state = disconnected}) ->
    log(error, #{msg => "unexpected_sock_closed", reason => Reason}, Channel),
    {ok, Channel};
handle_info({keepalive, start, Interval}, Channel) ->
    NChannel = Channel#channel{keepalive = emqx_keepalive:init(Interval)},
    {ok, ensure_timer(alive_timer, NChannel)};
handle_info(Info, Channel) ->
    log(error, #{msg => "unexpected_info", info => Info}, Channel),
    {ok, Channel}.

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
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            shutdown(keepalive_timeout, Channel)
    end;
handle_timeout(
    _TRef, retry_delivery, Channel = #channel{inflight = Inflight, retx_interval = RetxInterval}
) ->
    case emqx_inflight:is_empty(Inflight) of
        true ->
            {ok, clean_timer(retry_timer, Channel)};
        false ->
            Frames = lists:sort(sortfun(), emqx_inflight:to_list(Inflight)),
            {Outgoings, NInflight} = retry_delivery(
                Frames, erlang:system_time(millisecond), RetxInterval, Inflight, []
            ),
            {Outgoings2, NChannel} = dispatch_frame(Channel#channel{inflight = NInflight}),
            {ok, [{outgoing, Outgoings ++ Outgoings2}], reset_timer(retry_timer, NChannel)}
    end.

sortfun() ->
    fun({_, {_, _, Ts1}}, {_, {_, _, Ts2}}) -> Ts1 < Ts2 end.

retry_delivery([], _Now, _Interval, Inflight, Acc) ->
    {lists:reverse(Acc), Inflight};
retry_delivery([{Key, {_Frame, 0, _}} | Frames], Now, Interval, Inflight, Acc) ->
    %% todo    log(error, "has arrived max re-send times, drop ~p", [Frame]),
    NInflight = emqx_inflight:delete(Key, Inflight),
    retry_delivery(Frames, Now, Interval, NInflight, Acc);
retry_delivery([{Key, {Frame, RetxCount, Ts}} | Frames], Now, Interval, Inflight, Acc) ->
    Diff = Now - Ts,
    case Diff >= Interval of
        true ->
            NInflight = emqx_inflight:update(Key, {Frame, RetxCount - 1, Now}, Inflight),
            retry_delivery(Frames, Now, Interval, NInflight, [Frame | Acc]);
        _ ->
            retry_delivery(Frames, Now, Interval, Inflight, Acc)
    end.

dispatch_frame(
    Channel = #channel{
        msg_sn = TxMsgSn,
        mqueue = Queue,
        inflight = Inflight,
        retx_max_times = RetxMax
    }
) ->
    case emqx_inflight:is_full(Inflight) orelse queue:is_empty(Queue) of
        true ->
            {[], Channel};
        false ->
            {{value, Frame}, NewQueue} = queue:out(Queue),

            log(debug, #{msg => "delivery", frame => Frame}, Channel),

            NewInflight = emqx_inflight:insert(
                set_msg_ack(msgid(Frame), TxMsgSn),
                {Frame, RetxMax, erlang:system_time(millisecond)},
                Inflight
            ),
            NChannel = Channel#channel{mqueue = NewQueue, inflight = NewInflight},
            {[Frame], ensure_timer(retry_timer, NChannel)}
    end.

%%--------------------------------------------------------------------
%% Ensure timers
%%--------------------------------------------------------------------

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    log(debug, #{msg => "start_timer", name => Name, time => Time}, Channel),
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_utils:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(alive_timer, #channel{keepalive = KeepAlive}) ->
    emqx_keepalive:info(check_interval, KeepAlive);
interval(retry_timer, #channel{retx_interval = RetxIntv}) ->
    RetxIntv.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(_Reason, #channel{clientinfo = #{clientid := undefined}}) ->
    ok;
terminate(_Reason, #channel{conn_state = disconnected}) ->
    ok;
terminate(Reason, #channel{clientinfo = ClientInfo, conninfo = ConnInfo}) ->
    ?SLOG(info, #{msg => "connection_shutdown", reason => Reason}),
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = emqx_hooks:run('client.disconnected', [ClientInfo, Reason, NConnInfo]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

maybe_fix_mountpoint(ClientInfo = #{mountpoint := undefined}) ->
    ClientInfo;
maybe_fix_mountpoint(ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the variable replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    ClientInfo#{mountpoint := Mountpoint1}.

process_connect(
    _Frame,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo = #{clientid := ClientId}
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
            %% Auto subscribe downlink topics
            ok = autosubcribe(NChannel),
            _ = start_keepalive(?DEFAULT_KEEPALIVE, NChannel),
            _ = run_hooks(Ctx, 'client.connack', [ConnInfo, connection_accepted, #{}]),
            _ = emqx_gateway_ctx:insert_channel_info(
                Ctx, ClientId, info(NChannel), stats(NChannel)
            ),
            NChannel;
        {error, Reason} ->
            log(
                error,
                #{
                    msg => "failed_to_open_session",
                    reason => Reason
                },
                Channel
            ),
            shutdown(Reason, Channel)
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
    prepare_adapter_topic(Channel#channel{conninfo = NConnInfo, conn_state = connected}).

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

ack_msg(MsgId, KeyParam, Inflight) ->
    Key = get_msg_ack(MsgId, KeyParam),
    case emqx_inflight:contain(Key, Inflight) of
        true -> emqx_inflight:delete(Key, Inflight);
        false -> Inflight
    end.

set_msg_ack(?MS_SET_CLIENT_PARAM, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_CLIENT_PARAM, MsgSn}};
set_msg_ack(?MS_QUERY_CLIENT_ALL_PARAM, MsgSn) ->
    {?MC_QUERY_PARAM_ACK, MsgSn};
set_msg_ack(?MS_QUERY_CLIENT_PARAM, MsgSn) ->
    {?MC_QUERY_PARAM_ACK, MsgSn};
set_msg_ack(?MS_CLIENT_CONTROL, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_CLIENT_CONTROL, MsgSn}};
set_msg_ack(?MS_QUERY_CLIENT_ATTRIB, _MsgSn) ->
    {?MC_QUERY_ATTRIB_ACK, none};
set_msg_ack(?MS_OTA, _MsgSn) ->
    {?MC_OTA_ACK, none};
set_msg_ack(?MS_QUERY_LOCATION, MsgSn) ->
    {?MC_QUERY_LOCATION_ACK, MsgSn};
set_msg_ack(?MS_TRACE_LOCATION, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_TRACE_LOCATION, MsgSn}};
set_msg_ack(?MS_CONFIRM_ALARM, _MsgSn) ->
    % TODO: how to ack this message?
    {};
set_msg_ack(?MS_SEND_TEXT, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SEND_TEXT, MsgSn}};
set_msg_ack(?MS_SET_EVENT, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_EVENT, MsgSn}};
set_msg_ack(?MS_SEND_QUESTION, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SEND_QUESTION, MsgSn}};
set_msg_ack(?MS_SET_MENU, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_MENU, MsgSn}};
set_msg_ack(?MS_INFO_CONTENT, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_INFO_CONTENT, MsgSn}};
set_msg_ack(?MS_PHONE_CALLBACK, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_PHONE_CALLBACK, MsgSn}};
set_msg_ack(?MS_SET_PHONE_NUMBER, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_PHONE_NUMBER, MsgSn}};
set_msg_ack(?MS_VEHICLE_CONTROL, MsgSn) ->
    {?MC_VEHICLE_CTRL_ACK, MsgSn};
set_msg_ack(?MS_SET_CIRCLE_AREA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_CIRCLE_AREA, MsgSn}};
set_msg_ack(?MS_DEL_CIRCLE_AREA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_DEL_CIRCLE_AREA, MsgSn}};
set_msg_ack(?MS_SET_RECT_AREA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_RECT_AREA, MsgSn}};
set_msg_ack(?MS_DEL_RECT_AREA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_DEL_RECT_AREA, MsgSn}};
set_msg_ack(?MS_SET_POLY_AREA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_POLY_AREA, MsgSn}};
set_msg_ack(?MS_DEL_POLY_AREA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_DEL_POLY_AREA, MsgSn}};
set_msg_ack(?MS_SET_PATH, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SET_PATH, MsgSn}};
set_msg_ack(?MS_DEL_PATH, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_DEL_PATH, MsgSn}};
set_msg_ack(?MS_DRIVE_RECORD_CAPTURE, MsgSn) ->
    {?MC_DRIVE_RECORD_REPORT, MsgSn};
set_msg_ack(?MS_DRIVE_REC_PARAM_SEND, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_DRIVE_REC_PARAM_SEND, MsgSn}};
set_msg_ack(?MS_REQ_DRIVER_ID, _MsgSn) ->
    {?MC_DRIVER_ID_REPORT, none};
set_msg_ack(?MS_CAMERA_SHOT, MsgSn) ->
    % TODO: spec has two conflicted statement about this ack
    %       section 7.9.3 requires general ack
    %       section 8.55 requires 0x0805
    {?MC_CAMERA_SHOT_ACK, MsgSn};
set_msg_ack(?MS_MM_DATA_SEARCH, MsgSn) ->
    {?MC_MM_DATA_SEARCH_ACK, MsgSn};
set_msg_ack(?MS_MM_DATA_UPLOAD, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_MM_DATA_UPLOAD, MsgSn}};
set_msg_ack(?MS_VOICE_RECORD, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_VOICE_RECORD, MsgSn}};
set_msg_ack(?MS_SINGLE_MM_DATA_CTRL, MsgSn) ->
    % TODO: right?
    {?MC_MM_DATA_SEARCH_ACK, MsgSn};
set_msg_ack(?MS_SEND_TRANSPARENT_DATA, MsgSn) ->
    {?MC_GENERAL_RESPONSE, {?MS_SEND_TRANSPARENT_DATA, MsgSn}};
set_msg_ack(MsgId, Param) ->
    error({invalid_message_type, MsgId, Param}).

get_msg_ack(?MC_GENERAL_RESPONSE, {MsgId, MsgSn}) ->
    {?MC_GENERAL_RESPONSE, {MsgId, MsgSn}};
get_msg_ack(?MC_QUERY_PARAM_ACK, MsgSn) ->
    {?MC_QUERY_PARAM_ACK, MsgSn};
get_msg_ack(?MC_QUERY_ATTRIB_ACK, _MsgSn) ->
    {?MC_QUERY_ATTRIB_ACK, none};
get_msg_ack(?MC_OTA_ACK, _MsgSn) ->
    {?MC_OTA_ACK, none};
get_msg_ack(?MC_QUERY_LOCATION_ACK, MsgSn) ->
    {?MC_QUERY_LOCATION_ACK, MsgSn};
get_msg_ack(?MC_QUESTION_ACK, MsgSn) ->
    {?MC_QUESTION_ACK, MsgSn};
get_msg_ack(?MC_VEHICLE_CTRL_ACK, MsgSn) ->
    {?MC_VEHICLE_CTRL_ACK, MsgSn};
get_msg_ack(?MC_DRIVE_RECORD_REPORT, MsgSn) ->
    {?MC_DRIVE_RECORD_REPORT, MsgSn};
get_msg_ack(?MC_CAMERA_SHOT_ACK, MsgSn) ->
    {?MC_CAMERA_SHOT_ACK, MsgSn};
get_msg_ack(?MC_MM_DATA_SEARCH_ACK, MsgSn) ->
    {?MC_MM_DATA_SEARCH_ACK, MsgSn};
get_msg_ack(?MC_DRIVER_ID_REPORT, _MsgSn) ->
    {?MC_DRIVER_ID_REPORT, none};
get_msg_ack(MsgId, MsgSn) ->
    error({invalid_message_type, MsgId, MsgSn}).

build_frame_header(MsgId, #channel{clientinfo = #{phone := Phone}, msg_sn = TxMsgSn}) ->
    build_frame_header(MsgId, 0, Phone, TxMsgSn).

build_frame_header(MsgId, Encrypt, Phone, TxMsgSn) ->
    #{
        <<"msg_id">> => MsgId,
        <<"encrypt">> => Encrypt,
        <<"phone">> => Phone,
        <<"msg_sn">> => TxMsgSn
    }.

seq(#{<<"body">> := #{<<"seq">> := MsgSn}}) -> MsgSn;
seq(#{}) -> 0.

msgsn(#{<<"header">> := #{<<"msg_sn">> := MsgSn}}) -> MsgSn.

msgid(#{<<"header">> := #{<<"msg_id">> := MsgId}}) -> MsgId.

msgidsn(#{
    <<"header">> := #{
        <<"msg_id">> := MsgId,
        <<"msg_sn">> := MsgSn
    }
}) ->
    {MsgId, MsgSn}.

state_inc_sn(Channel = #channel{msg_sn = Sn}) ->
    Channel#channel{msg_sn = next_msg_sn(Sn)}.

next_msg_sn(16#FFFF) -> 0;
next_msg_sn(Sn) -> Sn + 1.

is_general_response_needed(?MC_EVENT_REPORT) -> true;
is_general_response_needed(?MC_LOCATION_REPORT) -> true;
is_general_response_needed(?MC_INFO_REQ_CANCEL) -> true;
is_general_response_needed(?MC_WAYBILL_REPORT) -> true;
is_general_response_needed(?MC_BULK_LOCATION_REPORT) -> true;
is_general_response_needed(?MC_CAN_BUS_REPORT) -> true;
is_general_response_needed(?MC_MULTIMEDIA_EVENT_REPORT) -> true;
is_general_response_needed(?MC_SEND_TRANSPARENT_DATA) -> true;
is_general_response_needed(?MC_SEND_ZIP_DATA) -> true;
is_general_response_needed(_) -> false.

is_driver_id_req_exist(#channel{inflight = Inflight}) ->
    % if there is a MS_REQ_DRIVER_ID (0x8702) command in re-tx queue
    Key = get_msg_ack(?MC_DRIVER_ID_REPORT, none),
    emqx_inflight:contain(Key, Inflight).

register_(Frame, Channel0) ->
    case emqx_jt808_auth:register(Frame, Channel0#channel.auth) of
        {ok, Authcode} ->
            {ok, Channel0#channel{authcode = Authcode}};
        {error, Reason} ->
            ?SLOG(error, #{msg => "register_failed", reason => Reason}),
            ResCode =
                case is_integer(Reason) of
                    true -> Reason;
                    false -> 1
                end,
            {error, ResCode}
    end.

authenticate(_AuthFrame, #channel{authcode = anonymous}) ->
    true;
authenticate(AuthFrame, #channel{authcode = undefined, auth = Auth}) ->
    %% Try request authentication server
    case emqx_jt808_auth:authenticate(AuthFrame, Auth) of
        {ok, #{auth_result := IsAuth}} ->
            IsAuth;
        {error, Reason} ->
            ?SLOG(error, #{msg => "request_auth_server_failed", reason => Reason}),
            false
    end;
authenticate(
    #{<<"body">> := #{<<"code">> := InCode}},
    #channel{authcode = Authcode}
) ->
    InCode == Authcode.

enrich_conninfo(
    #{<<"header">> := #{<<"phone">> := Phone}},
    Channel = #channel{conninfo = ConnInfo}
) ->
    NConnInfo = ConnInfo#{
        proto_name => <<"jt808">>,
        proto_ver => <<"2013">>,
        clean_start => true,
        clientid => Phone,
        username => undefined,
        conn_props => #{},
        connected => true,
        connected_at => erlang:system_time(millisecond),
        keepalive => ?DEFAULT_KEEPALIVE,
        receive_maximum => 0,
        expiry_interval => 0
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

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

%% Register
enrich_clientinfo(
    #{
        <<"header">> := #{<<"phone">> := Phone},
        <<"body">> := #{
            <<"manufacturer">> := Manu,
            <<"dev_id">> := DevId
        }
    },
    Channel = #channel{clientinfo = ClientInfo}
) ->
    NClientInfo = maybe_fix_mountpoint(ClientInfo#{
        phone => Phone,
        clientid => Phone,
        manufacturer => Manu,
        terminal_id => DevId
    }),
    {ok, Channel#channel{clientinfo = NClientInfo}};
%% Auth
enrich_clientinfo(
    #{<<"header">> := #{<<"phone">> := Phone}},
    Channel = #channel{clientinfo = ClientInfo}
) ->
    NClientInfo = ClientInfo#{
        phone => Phone,
        clientid => Phone
    },
    {ok, Channel#channel{clientinfo = NClientInfo}}.

set_log_meta(_Packet, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

prepare_adapter_topic(Channel = #channel{up_topic = UpTopic, dn_topic = DnTopic}) ->
    Channel#channel{
        up_topic = replvar(UpTopic, Channel),
        dn_topic = replvar(DnTopic, Channel)
    }.

replvar(undefined, _Channel) ->
    undefined;
replvar(Topic, #channel{clientinfo = #{clientid := ClientId, phone := Phone}}) when
    is_binary(Topic)
->
    do_replvar(Topic, #{clientid => ClientId, phone => Phone}).

do_replvar(Topic, Vars) ->
    ClientID = maps:get(clientid, Vars, undefined),
    Phone = maps:get(phone, Vars, undefined),
    List = [
        {?PH_CLIENTID, ClientID},
        {?PH_PHONE, Phone}
    ],
    lists:foldl(fun feed_var/2, Topic, List).

feed_var({_PH, undefined}, Topic) ->
    Topic;
feed_var({PH, Value}, Topic) ->
    emqx_topic:feed_var(PH, Value, Topic).

autosubcribe(#channel{dn_topic = Topic}) when
    Topic == undefined;
    Topic == ""
->
    ok;
autosubcribe(#channel{
    clientinfo =
        ClientInfo =
            #{clientid := ClientId},
    dn_topic = Topic
}) ->
    _ = emqx_broker:subscribe(Topic, ClientId, ?DN_TOPIC_SUBOPTS),
    ok = emqx_hooks:run('session.subscribed', [
        ClientInfo, Topic, ?DN_TOPIC_SUBOPTS#{is_new => true}
    ]).

start_keepalive(Secs, _Channel) when Secs > 0 ->
    self() ! {keepalive, start, round(Secs) * 1000}.

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

discard_downlink_messages([], _Channel) ->
    ok;
discard_downlink_messages(Messages, Channel) ->
    log(
        error,
        #{
            msg => "discard_new_downlink_messages",
            reason =>
                "Discard new downlink messages due to that too"
                " many messages are waiting their ACKs.",
            messages => Messages
        },
        Channel
    ),
    metrics_inc('delivery.dropped', Channel, erlang:length(Messages)).

metrics_inc(Name, #channel{ctx = Ctx}, Oct) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name, Oct).

log(Level, Meta, #channel{clientinfo = #{clientid := ClientId, username := Username}} = _Channel) ->
    ?SLOG(Level, Meta#{clientid => ClientId, username => Username}).

tp(
    Level,
    Key,
    Meta,
    #channel{clientinfo = #{clientid := ClientId, username := Username}} = _Channel
) ->
    ?tp(Level, Key, Meta#{clientid => ClientId, username => Username}).

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

disconnect_and_shutdown(Reason, Reply, Channel) ->
    shutdown(Reason, Reply, Channel).
