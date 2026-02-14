%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifdef(TEST).
-export([
    try_insert_inflight/7,
    ack_msg/4,
    set_msg_ack/2,
    get_msg_ack/2,
    custom_msg_ack_key/2,
    custom_get_msg_ack/2,
    make_test_channel/0,
    normalize_queue_item/1
]).
-endif.

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

-define(IGNORE_UNSUPPORTED_FRAMES, true).

%% Downlink message sequence number types
-define(SN_AUTO, auto).
-define(SN_CUSTOM, custom).

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
    IgnoreUnsupportedFrames = maps:get(
        ignore_unsupported_frames, ProtoConf, ?IGNORE_UNSUPPORTED_FRAMES
    ),
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
            protocol => emqx_gateway_utils:protocol(jt808),
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            clientid => undefined,
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            mountpoint => Mountpoint,
            ignore_unsupported_frames => IgnoreUnsupportedFrames
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
handle_in(Frame, Channel = #channel{clientinfo = ClientInfo}) ->
    case maps:get(ignore_unsupported_frames, ClientInfo, ?IGNORE_UNSUPPORTED_FRAMES) of
        true ->
            ?SLOG(warning, #{msg => "ignore_unsupported_frames", frame => Frame}),
            {ok, Channel};
        false ->
            ?SLOG(error, #{msg => "unexpected_jt808_frame", frame => Frame}),
            {shutdown, unexpected_frame, Channel}
    end.

handle_frame_error(Reason, Channel = #channel{clientinfo = ClientInfo}) ->
    case maps:get(ignore_unsupported_frames, ClientInfo, ?IGNORE_UNSUPPORTED_FRAMES) of
        true ->
            ?SLOG(warning, #{msg => "ignore_frame_error", reason => Reason}),
            {ok, Channel};
        false ->
            ?SLOG(error, #{msg => "disconnect_client", reason => frame_error}),
            {shutdown, frame_error, Channel}
    end.

%% @private
do_handle_in(Frame = ?MSG(?MC_GENERAL_RESPONSE), Channel = #channel{inflight = Inflight}) ->
    #{<<"body">> := #{<<"seq">> := Seq, <<"id">> := Id}} = Frame,
    NewInflight = ack_msg(?MC_GENERAL_RESPONSE, {Id, Seq}, Inflight, Channel),
    dispatch_and_reply(Channel#channel{inflight = NewInflight});
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
do_handle_in(?MSG(?MC_QUERY_SERVER_TIME), Channel) ->
    %% 2019: Client requests server time, respond with 0x8004 (Server Time ACK)
    %% Time format: BCD[6] UTC time as YYMMDDHHMMSS
    Response = #{
        <<"header">> => build_frame_header(?MS_SERVER_TIME_ACK, Channel),
        <<"body">> => #{<<"time">> => current_utc_time()}
    },
    {ok, [{outgoing, Response}], state_inc_sn(Channel)};
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
            NewInflight = ack_msg(?MC_DRIVER_ID_REPORT, none, Inflight, Channel),
            dispatch_and_reply(Channel#channel{inflight = NewInflight})
    end;
do_handle_in(?MSG(?MC_DEREGISTER), Channel) ->
    {shutdown, normal, Channel};
do_handle_in(
    Frame = #{},
    Channel = #channel{up_topic = Topic, inflight = Inflight}
) ->
    IsUnknownMessage = emqx_utils_maps:deep_get([<<"body">>, <<"unknown_id">>], Frame, false),
    case IsUnknownMessage of
        %% Normal message, handle it
        false ->
            _ = do_publish(Topic, Frame),
            {MsgId, MsgSn} = msgidsn(Frame),
            case is_general_response_needed(MsgId) of
                % these frames device passive request
                true ->
                    handle_out({?MS_GENERAL_RESPONSE, 0, MsgId}, MsgSn, Channel);
                % these frames are response to server's request
                false ->
                    NewInflight = ack_msg(MsgId, seq(Frame), Inflight, Channel),
                    dispatch_and_reply(Channel#channel{inflight = NewInflight})
            end;
        true ->
            _ = do_publish(Topic, Frame),
            {ok, Channel}
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
            {Frames, NChannel1} = msgs2frame(NMessages, Channel),
            NQueue = lists:foldl(fun(F, Q) -> queue:in(F, Q) end, Queue, Frames),
            {Outgoings, NChannel} = dispatch_frame(NChannel1#channel{mqueue = NQueue}),
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
    {Frames, NChannel} = lists:foldl(
        fun(#message{payload = Payload}, {AccFrames, AccChannel}) ->
            case emqx_utils_json:safe_decode(Payload) of
                {ok, PayloadJson = #{<<"header">> := Header = #{<<"msg_id">> := MsgId}}} ->
                    {NewHeader, SnType, NAccChannel} = build_downlink_header(
                        MsgId, Header, AccChannel
                    ),
                    Frame = PayloadJson#{<<"header">> => NewHeader},
                    {[{Frame, SnType} | AccFrames], NAccChannel};
                {ok, _} ->
                    tp(
                        error,
                        invalid_dl_message,
                        #{reasons => "missing_msg_id", payload => Payload},
                        AccChannel
                    ),
                    {AccFrames, AccChannel};
                {error, _Reason} ->
                    tp(
                        error,
                        invalid_dl_message,
                        #{reason => "invalid_json", payload => Payload},
                        AccChannel
                    ),
                    {AccFrames, AccChannel}
            end
        end,
        {[], Channel},
        Messages
    ),
    {lists:reverse(Frames), NChannel}.

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
handle_out({?MS_REGISTER_ACK, 0}, MsgSn, Channel = #channel{authcode = AuthCode0}) ->
    AuthCode =
        case AuthCode0 == anonymous of
            true -> <<"anonymous">>;
            false -> AuthCode0
        end,
    Frame = #{
        <<"header">> => build_frame_header(?MS_REGISTER_ACK, Channel),
        <<"body">> => #{<<"seq">> => MsgSn, <<"result">> => 0, <<"auth_code">> => AuthCode}
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
        mqueue = Queue,
        inflight = Inflight,
        retx_max_times = RetxMax
    }
) ->
    case emqx_inflight:is_full(Inflight) orelse queue:is_empty(Queue) of
        true ->
            {[], Channel};
        false ->
            {{value, Item}, NewQueue} = queue:out(Queue),
            %% Handle both old format (Frame) and new format ({Frame, SnType})
            %% for hot upgrade compatibility
            {Frame, SnType} = normalize_queue_item(Item),

            log(debug, #{msg => "delivery", frame => Frame, sn_type => SnType}, Channel),

            MsgId = msgid(Frame),
            MsgSn = msgsn(Frame),
            case try_insert_inflight(MsgId, MsgSn, SnType, Frame, RetxMax, Inflight, Channel) of
                {ok, NewInflight} ->
                    NChannel = Channel#channel{mqueue = NewQueue, inflight = NewInflight},
                    {[Frame], ensure_timer(retry_timer, NChannel)};
                {ok_duplicate, NewInflight} ->
                    %% Duplicate detected but still deliver (as a gateway should ensure delivery)
                    %% The message is not added to inflight since it's already being tracked
                    NChannel = Channel#channel{mqueue = NewQueue, inflight = NewInflight},
                    {[Frame], ensure_timer(retry_timer, NChannel)}
            end
    end.

%% @doc Normalize queue item for hot upgrade compatibility.
%% Old format: Frame (map)
%% New format: {Frame, SnType}
normalize_queue_item({Frame, SnType}) when is_atom(SnType) ->
    {Frame, SnType};
normalize_queue_item(Frame) when is_map(Frame) ->
    %% Legacy format - treat as auto msg_sn
    {Frame, ?SN_AUTO}.

dispatch_and_reply(Channel) ->
    case dispatch_frame(Channel) of
        {[], NChannel} -> {ok, NChannel};
        {Outgoings, NChannel} -> {ok, [{outgoing, Outgoings}], NChannel}
    end.

%% @doc Try to insert a frame into inflight, handling race conditions
%% for custom msg_sn messages.
%%
%% For auto msg_sn: use key {AckMsgId, {MsgId, MsgSn}} or {AckMsgId, MsgSn}
%% For custom msg_sn: use key {custom, AckMsgId, {MsgId, MsgSn}} or {custom, AckMsgId, MsgSn}
%%
%% Race condition handling:
%% - If custom key already exists: duplicate custom msg, discard
%% - If auto key exists when inserting custom: record ?SN_AUTO in process dict
%% - If no key exists for custom: record ?SN_CUSTOM in process dict
try_insert_inflight(MsgId, MsgSn, ?SN_AUTO, Frame, RetxMax, Inflight, _Channel) ->
    %% Auto msg_sn: standard insertion
    Key = set_msg_ack(MsgId, MsgSn),
    NewInflight = emqx_inflight:insert(
        Key,
        {Frame, RetxMax, erlang:system_time(millisecond)},
        Inflight
    ),
    {ok, NewInflight};
try_insert_inflight(MsgId, MsgSn, ?SN_CUSTOM, Frame, RetxMax, Inflight, Channel) ->
    %% Custom msg_sn: check for conflicts
    AutoKey = set_msg_ack(MsgId, MsgSn),
    CustomKey = custom_msg_ack_key(MsgId, MsgSn),
    %% Use AutoKey in OrderKey to distinguish different message types with same MsgSn
    OrderKey = {msg_sn_order, AutoKey},
    case emqx_inflight:contain(CustomKey, Inflight) of
        true ->
            %% Duplicate custom msg_sn detected, but still deliver
            %% (as a gateway should ensure message delivery as much as possible)
            log(
                warning,
                #{msg => "duplicate_custom_msg_sn_delivered", msg_id => MsgId, msg_sn => MsgSn},
                Channel
            ),
            {ok_duplicate, Inflight};
        false ->
            case emqx_inflight:contain(AutoKey, Inflight) of
                true ->
                    %% Auto msg sent first, record ordering
                    log(
                        debug,
                        #{msg => "custom_msg_sn_after_auto", msg_id => MsgId, msg_sn => MsgSn},
                        Channel
                    ),
                    put(OrderKey, ?SN_AUTO),
                    NewInflight = emqx_inflight:insert(
                        CustomKey,
                        {Frame, RetxMax, erlang:system_time(millisecond)},
                        Inflight
                    ),
                    {ok, NewInflight};
                false ->
                    %% Custom sent first (or alone), record ordering
                    put(OrderKey, ?SN_CUSTOM),
                    NewInflight = emqx_inflight:insert(
                        CustomKey,
                        {Frame, RetxMax, erlang:system_time(millisecond)},
                        Inflight
                    ),
                    {ok, NewInflight}
            end
    end.

%% Build custom msg ack key
custom_msg_ack_key(MsgId, MsgSn) ->
    case set_msg_ack(MsgId, MsgSn) of
        {AckMsgId, AckParam} -> {custom, AckMsgId, AckParam};
        Other -> {custom, Other}
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

interval(alive_timer, #channel{keepalive = Keepalive}) ->
    emqx_keepalive:info(check_interval, Keepalive);
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

%% @doc Handle ACK message, considering both auto and custom msg_sn keys.
%% When there's a race condition (both auto and custom keys exist for same MsgSn),
%% use process dictionary to determine which message was sent first and delete that one.
ack_msg(AckMsgId, KeyParam, Inflight, Channel) ->
    AutoKey = get_msg_ack(AckMsgId, KeyParam),
    CustomKey = custom_get_msg_ack(AckMsgId, KeyParam),
    %% Use AutoKey in OrderKey to match the key used in try_insert_inflight
    OrderKey = {msg_sn_order, AutoKey},
    HasAuto = emqx_inflight:contain(AutoKey, Inflight),
    HasCustom = emqx_inflight:contain(CustomKey, Inflight),
    case {HasAuto, HasCustom} of
        {true, true} ->
            %% Both exist - race condition, check ordering
            case erase(OrderKey) of
                ?SN_CUSTOM ->
                    %% Custom was sent first, delete it
                    emqx_inflight:delete(CustomKey, Inflight);
                ?SN_AUTO ->
                    %% Auto was sent first, delete it
                    emqx_inflight:delete(AutoKey, Inflight);
                undefined ->
                    %% No ordering info, delete auto first (default)
                    emqx_inflight:delete(AutoKey, Inflight)
            end;
        {true, false} ->
            %% Only auto exists
            _ = erase(OrderKey),
            emqx_inflight:delete(AutoKey, Inflight);
        {false, true} ->
            %% Only custom exists
            _ = erase(OrderKey),
            emqx_inflight:delete(CustomKey, Inflight);
        {false, false} ->
            %% Neither exists - ACK for message not in inflight
            %% This can happen when duplicate messages are delivered and client sends multiple ACKs
            _ = erase(OrderKey),
            log(
                warning,
                #{
                    msg => "ack_for_unknown_msg",
                    ack_msg_id => AckMsgId,
                    key_param => KeyParam,
                    auto_key => AutoKey,
                    custom_key => CustomKey
                },
                Channel
            ),
            Inflight
    end.

%% Build custom get_msg_ack key
custom_get_msg_ack(MsgId, KeyParam) ->
    {AckMsgId, AckParam} = get_msg_ack(MsgId, KeyParam),
    {custom, AckMsgId, AckParam}.

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
set_msg_ack(?MS_QUERY_AREA_ROUTE, _MsgSn) ->
    {?MC_QUERY_AREA_ROUTE_ACK, none};
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
get_msg_ack(?MC_QUERY_AREA_ROUTE_ACK, _MsgSn) ->
    {?MC_QUERY_AREA_ROUTE_ACK, none};
get_msg_ack(MsgId, MsgSn) ->
    error({invalid_message_type, MsgId, MsgSn}).

build_frame_header(MsgId, #channel{clientinfo = ClientInfo, msg_sn = TxMsgSn}) ->
    #{phone := Phone} = ClientInfo,
    ProtoVer = maps:get(proto_ver, ClientInfo, ?PROTO_VER_2013),
    build_frame_header(MsgId, 0, Phone, TxMsgSn, ProtoVer).

build_frame_header_with_sn(MsgId, MsgSn, #channel{clientinfo = ClientInfo}) ->
    #{phone := Phone} = ClientInfo,
    ProtoVer = maps:get(proto_ver, ClientInfo, ?PROTO_VER_2013),
    build_frame_header(MsgId, 0, Phone, MsgSn, ProtoVer).

build_downlink_header(MsgId, PayloadHeader, Channel) ->
    case maps:get(<<"msg_sn">>, PayloadHeader, undefined) of
        undefined ->
            log(debug, #{msg => "downlink_use_channel_msg_sn", msg_id => MsgId}, Channel),
            {build_frame_header(MsgId, Channel), ?SN_AUTO, state_inc_sn(Channel)};
        PayloadMsgSn ->
            log(
                info,
                #{msg => "downlink_use_payload_msg_sn", msg_id => MsgId, msg_sn => PayloadMsgSn},
                Channel
            ),
            {build_frame_header_with_sn(MsgId, PayloadMsgSn, Channel), ?SN_CUSTOM, Channel}
    end.

build_frame_header(MsgId, Encrypt, Phone, TxMsgSn, ProtoVer) ->
    Header = #{
        <<"msg_id">> => MsgId,
        <<"encrypt">> => Encrypt,
        <<"phone">> => Phone,
        <<"msg_sn">> => TxMsgSn
    },
    case ProtoVer >= ?PROTO_VER_2019 of
        true -> Header#{<<"proto_ver">> => ProtoVer};
        false -> Header
    end.

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

%% @doc Returns current UTC time as binary string in YYMMDDHHMMSS format
%% Used for 0x8004 (Server Time ACK) response
current_utc_time() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:universal_time(),
    YY = Year rem 100,
    iolist_to_binary(
        io_lib:format(
            "~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
            [YY, Month, Day, Hour, Min, Sec]
        )
    ).

is_general_response_needed(?MC_EVENT_REPORT) -> true;
is_general_response_needed(?MC_LOCATION_REPORT) -> true;
is_general_response_needed(?MC_INFO_REQ_CANCEL) -> true;
is_general_response_needed(?MC_WAYBILL_REPORT) -> true;
is_general_response_needed(?MC_BULK_LOCATION_REPORT) -> true;
is_general_response_needed(?MC_CAN_BUS_REPORT) -> true;
is_general_response_needed(?MC_MULTIMEDIA_EVENT_REPORT) -> true;
is_general_response_needed(?MC_SEND_TRANSPARENT_DATA) -> true;
is_general_response_needed(?MC_SEND_ZIP_DATA) -> true;
is_general_response_needed(?MC_REQUEST_FRAGMENT) -> true;
is_general_response_needed(_) -> false.

is_driver_id_req_exist(#channel{inflight = Inflight}) ->
    % if there is a MS_REQ_DRIVER_ID (0x8702) command in re-tx queue
    Key = get_msg_ack(?MC_DRIVER_ID_REPORT, none),
    emqx_inflight:contain(Key, Inflight).

register_(Frame, Channel0) ->
    case emqx_jt808_auth:register(Frame, Channel0#channel.auth) of
        {ok, AuthCode} ->
            {ok, Channel0#channel{authcode = AuthCode}};
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
    #channel{authcode = AuthCode}
) ->
    InCode == AuthCode.

enrich_conninfo(
    #{<<"header">> := Header = #{<<"phone">> := Phone}},
    Channel = #channel{conninfo = ConnInfo}
) ->
    ProtoVerInt = maps:get(<<"proto_ver">>, Header, ?PROTO_VER_2013),
    ProtoVerStr = proto_ver_to_string(ProtoVerInt),
    NConnInfo = ConnInfo#{
        proto_name => <<"jt808">>,
        proto_ver => ProtoVerStr,
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

proto_ver_to_string(Ver) when Ver >= ?PROTO_VER_2019 -> <<"2019">>;
proto_ver_to_string(_) -> <<"2013">>.

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
        <<"header">> := Header = #{<<"phone">> := Phone},
        <<"body">> := #{
            <<"manufacturer">> := Manu,
            <<"dev_id">> := DevId
        }
    },
    Channel = #channel{clientinfo = ClientInfo}
) ->
    ProtoVer = maps:get(<<"proto_ver">>, Header, ?PROTO_VER_2013),
    NClientInfo = maybe_fix_mountpoint(ClientInfo#{
        phone => Phone,
        clientid => Phone,
        manufacturer => Manu,
        terminal_id => DevId,
        proto_ver => ProtoVer
    }),
    {ok, Channel#channel{clientinfo = NClientInfo}};
%% Auth
enrich_clientinfo(
    #{<<"header">> := Header = #{<<"phone">> := Phone}},
    Channel = #channel{clientinfo = ClientInfo}
) ->
    ProtoVer = maps:get(<<"proto_ver">>, Header, ?PROTO_VER_2013),
    NClientInfo = ClientInfo#{
        phone => Phone,
        clientid => Phone,
        proto_ver => ProtoVer
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
    ClientId = maps:get(clientid, Vars, undefined),
    Phone = maps:get(phone, Vars, undefined),
    List = [
        {?PH_CLIENTID, ClientId},
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

-ifdef(TEST).
%% @doc Create a minimal channel record for unit testing
make_test_channel() ->
    #channel{
        ctx = undefined,
        conninfo = #{},
        clientinfo = #{
            clientid => <<"test_client">>,
            username => undefined
        },
        session = undefined,
        conn_state = connected,
        timers = #{},
        authcode = undefined,
        keepalive = undefined,
        msg_sn = 0,
        dn_topic = undefined,
        up_topic = undefined,
        auth = undefined,
        inflight = emqx_inflight:new(128),
        mqueue = queue:new(),
        max_mqueue_len = 100,
        rsa_key = undefined,
        retx_interval = 8000,
        retx_max_times = 5
    }.
-endif.
