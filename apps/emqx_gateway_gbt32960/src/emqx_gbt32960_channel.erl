%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gbt32960_channel).
-behaviour(emqx_gateway_channel).

-include("emqx_gbt32960.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

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
    %% ConnInfo
    conninfo :: emqx_types:conninfo(),
    %% ClientInfo
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: undefined | map(),
    %% Keepalive
    keepalive :: option(emqx_keepalive:keepalive()),
    %% Conn State
    conn_state :: conn_state(),
    %% Timers
    timers :: #{atom() => undefined | disabled | reference()},
    %% Inflight
    inflight :: emqx_inflight:inflight(),
    %% Message Queue
    mqueue :: queue:queue(),
    %% Subscriptions
    subscriptions :: map(),
    retx_interval,
    retx_max_times,
    max_mqueue_len
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
    retry_timer => retry_delivery,
    connection_expire_timer => connection_expire
}).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

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
info(session, _) ->
    #{};
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(keepalive, #channel{keepalive = undefined}) ->
    undefined;
info(keepalive, #channel{keepalive = Keepalive}) ->
    emqx_keepalive:info(Keepalive);
info(will_msg, _) ->
    undefined.

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

set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

%%--------------------------------------------------------------------
%% Init the Channel
%%--------------------------------------------------------------------

init(
    ConnInfo = #{
        peername := {PeerHost, _Port} = PeerName,
        sockname := {_Host, SockPort}
    },
    Options
) ->
    % TODO: init rsa_key from user input
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Options, ?DEFAULT_MOUNTPOINT),
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
            protocol => gbt32960,
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

    Ctx = maps:get(ctx, Options),

    #{
        retry_interval := RetxInterv,
        max_retry_times := RetxMaxTime,
        message_queue_len := MessageQueueLen
    } = Options,

    #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        inflight = emqx_inflight:new(1),
        mqueue = queue:new(),
        subscriptions = #{},
        timers = #{},
        conn_state = idle,
        retx_interval = RetxInterv,
        retx_max_times = RetxMaxTime,
        max_mqueue_len = MessageQueueLen
    }.

setting_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
setting_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------
-spec handle_in(frame() | {frame_error, any()}, channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

handle_in(
    Frame = ?CMD(?CMD_VIHECLE_LOGIN),
    Channel
) ->
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2,
                %% TODO: How to implement the banned in the gateway instance?
                %, fun check_banned/2
                fun auth_connect/2
            ],
            Frame,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, _NPacket, NChannel} ->
            process_connect(Frame, ensure_connected(NChannel));
        {error, ReasonCode, NChannel} ->
            log(warning, #{msg => "login_failed", reason => ReasonCode}, NChannel),
            shutdown(ReasonCode, NChannel)
    end;
handle_in(_Frame, Channel = #channel{conn_state = ConnState}) when
    ConnState =/= connected
->
    shutdown(protocol_error, Channel);
handle_in(Frame = ?CMD(?CMD_INFO_REPORT), Channel) ->
    _ = upstreaming(Frame, Channel),
    {ok, Channel};
handle_in(Frame = ?CMD(?CMD_INFO_RE_REPORT), Channel) ->
    _ = upstreaming(Frame, Channel),
    {ok, Channel};
handle_in(Frame = ?CMD(?CMD_VIHECLE_LOGOUT), Channel) ->
    %% XXX: unsubscribe gbt32960/dnstream/${vin}?
    _ = upstreaming(Frame, Channel),
    {ok, Channel};
handle_in(Frame = ?CMD(?CMD_PLATFORM_LOGIN), Channel) ->
    #{
        <<"Username">> := _Username,
        <<"Password">> := _Password
    } = Frame#frame.data,
    %% TODO:
    _ = upstreaming(Frame, Channel),
    {ok, Channel};
handle_in(Frame = ?CMD(?CMD_PLATFORM_LOGOUT), Channel) ->
    %% TODO:
    _ = upstreaming(Frame, Channel),
    {ok, Channel};
handle_in(Frame = ?CMD(?CMD_HEARTBEAT), Channel) ->
    handle_out({?ACK_SUCCESS, Frame}, Channel);
handle_in(Frame = ?CMD(?CMD_SCHOOL_TIME), Channel) ->
    %% TODO: How verify this request
    handle_out({?ACK_SUCCESS, Frame}, Channel);
handle_in(Frame = #frame{cmd = Cmd}, Channel = #channel{inflight = Inflight}) ->
    {Outgoings, NChannel} = dispatch_frame(Channel#channel{inflight = ack_frame(Cmd, Inflight)}),
    _ = upstreaming(Frame, NChannel),
    {ok, [{outgoing, Outgoings}], NChannel};
handle_in(Frame, Channel) ->
    log(warning, #{msg => "unexpected_gbt32960_frame", frame => Frame}, Channel),
    {ok, Channel}.

handle_frame_error(Reason, Channel) ->
    shutdown(Reason, Channel).

%%--------------------------------------------------------------------
%% Handle out
%%--------------------------------------------------------------------

handle_out({AckCode, Frame}, Channel) when
    ?IS_ACK_CODE(AckCode)
->
    {ok, [{outgoing, ack(AckCode, Frame)}], Channel}.

handle_out({AckCode, Frame}, Outgoings, Channel) when ?IS_ACK_CODE(AckCode) ->
    {ok, [{outgoing, ack(AckCode, Frame)} | Outgoings], Channel}.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------
-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}.

handle_deliver(
    Messages0,
    Channel = #channel{
        clientinfo = #{clientid := ClientId, mountpoint := Mountpoint},
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
            Frames = msgs2frame(NMessages, ClientId, Channel),
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

msgs2frame(Messages, Vin, Channel) ->
    lists:filtermap(
        fun(#message{payload = Payload}) ->
            case emqx_utils_json:safe_decode(Payload, [return_maps]) of
                {ok, Maps} ->
                    case msg2frame(Maps, Vin) of
                        {error, Reason} ->
                            log(
                                debug,
                                #{
                                    msg => "convert_message_to_frame_error",
                                    reason => Reason,
                                    data => Maps
                                },
                                Channel
                            ),
                            false;
                        Frame ->
                            {true, Frame}
                    end;
                {error, Reason} ->
                    log(error, #{msg => "json_decode_error", reason => Reason}, Channel),
                    false
            end
        end,
        Messages
    ).

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), From :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), frame(), channel()}.

handle_call(kick, _From, Channel) ->
    Channel1 = ensure_disconnected(kicked, Channel),
    disconnect_and_shutdown(kicked, ok, Channel1);
handle_call(discard, _From, Channel) ->
    disconnect_and_shutdown(discarded, ok, Channel);
handle_call({subscribe, _Topic, _SubOpts}, _From, Channel) ->
    reply({error, not_support}, Channel);
handle_call({unsubscribe, _Topic}, _From, Channel) ->
    reply({error, not_found}, Channel);
handle_call(subscriptions, _From, Channel = #channel{subscriptions = Subscriptions}) ->
    reply({ok, maps:to_list(Subscriptions)}, Channel);
handle_call(Req, _From, Channel) ->
    log(error, #{msg => "unexpected_call", call => Req}, Channel),
    reply({error, unexpected_call}, Channel).

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
    _TRef,
    retry_delivery,
    Channel = #channel{inflight = Inflight, retx_interval = RetxInterv}
) ->
    case emqx_inflight:is_empty(Inflight) of
        true ->
            {ok, clean_timer(retry_timer, Channel)};
        false ->
            Frames = emqx_inflight:to_list(Inflight),
            {Outgoings, NInflight} = retry_delivery(
                Frames, erlang:system_time(millisecond), RetxInterv, Inflight, []
            ),
            {Outgoings2, NChannel} = dispatch_frame(Channel#channel{inflight = NInflight}),
            {ok, [{outgoing, Outgoings ++ Outgoings2}], reset_timer(retry_timer, NChannel)}
    end;
handle_timeout(
    _TRef,
    connection_expire,
    Channel
) ->
    NChannel = clean_timer(connection_expire_timer, Channel),
    {ok, [{event, disconnected}, {close, expired}], NChannel};
handle_timeout(_TRef, Msg, Channel) ->
    log(error, #{msg => "unexpected_timeout", content => Msg}, Channel),
    {ok, Channel}.

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

terminate(Reason, #channel{
    ctx = Ctx,
    session = Session,
    clientinfo = ClientInfo
}) ->
    run_hooks(Ctx, 'session.terminated', [ClientInfo, Reason, Session]).

%%--------------------------------------------------------------------
%% Ensure connected

enrich_clientinfo(
    Packet,
    Channel = #channel{
        clientinfo = ClientInfo
    }
) ->
    {ok, NPacket, NClientInfo} = emqx_utils:pipeline(
        [
            fun maybe_assign_clientid/2,
            %% FIXME: CALL After authentication successfully
            fun fix_mountpoint/2
        ],
        Packet,
        ClientInfo
    ),
    {ok, NPacket, Channel#channel{clientinfo = NClientInfo}}.

enrich_conninfo(
    _Packet,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    #{clientid := ClientId, username := Username} = ClientInfo,
    NConnInfo = ConnInfo#{
        proto_name => <<"GBT32960">>,
        proto_ver => <<"">>,
        clean_start => true,
        keepalive => 0,
        expiry_interval => 0,
        conn_props => #{},
        receive_maximum => 0,
        clientid => ClientId,
        username => Username
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

run_conn_hooks(
    Packet,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Packet, Channel}
    end.

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
    Frame,
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
            NChannel0 = Channel#channel{session = Session},
            NChannel = subscribe_downlink(?DEFAULT_DOWNLINK_TOPIC, NChannel0),
            _ = upstreaming(Frame, NChannel),
            %% XXX: connection_accepted is not defined by stomp protocol
            _ = run_hooks(Ctx, 'client.connack', [ConnInfo, connection_accepted, #{}]),
            handle_out({?ACK_SUCCESS, Frame}, [{event, connected}], NChannel);
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

maybe_assign_clientid(#frame{vin = Vin}, ClientInfo) ->
    {ok, ClientInfo#{clientid => Vin, username => Vin}}.

fix_mountpoint(_Packet, #{mountpoint := undefined}) ->
    ok;
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the variable replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

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
%% Helper functions
%%--------------------------------------------------------------------

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

disconnect_and_shutdown(Reason, Reply, Channel) ->
    shutdown(Reason, Reply, Channel).

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

upstreaming(
    Frame, Channel = #channel{clientinfo = #{mountpoint := Mountpoint, clientid := ClientId}}
) ->
    {Topic, Payload} = transform(Frame, Mountpoint),
    log(debug, #{msg => "upstreaming_to_topic", topic => Topic, payload => Payload}, Channel),
    emqx:publish(emqx_message:make(ClientId, ?QOS_1, Topic, Payload)).

transform(Frame = ?CMD(Cmd), Mountpoint) ->
    Suffix =
        case Cmd of
            ?CMD_VIHECLE_LOGIN -> <<"upstream/vlogin">>;
            ?CMD_INFO_REPORT -> <<"upstream/info">>;
            ?CMD_INFO_RE_REPORT -> <<"upstream/reinfo">>;
            ?CMD_VIHECLE_LOGOUT -> <<"upstream/vlogout">>;
            ?CMD_PLATFORM_LOGIN -> <<"upstream/plogin">>;
            ?CMD_PLATFORM_LOGOUT -> <<"upstream/plogout">>;
            %CMD_HEARTBEAT, CMD_SCHOOL_TIME ...
            _ -> <<"upstream/transparent">>
        end,
    Topic = emqx_mountpoint:mount(Mountpoint, Suffix),
    Payload = to_json(Frame),
    {Topic, Payload};
transform(Frame = #frame{ack = Ack}, Mountpoint) when
    ?IS_ACK_CODE(Ack)
->
    Topic = emqx_mountpoint:mount(Mountpoint, <<"upstream/response">>),
    Payload = to_json(Frame),
    {Topic, Payload}.

to_json(#frame{cmd = Cmd, vin = Vin, encrypt = Encrypt, data = Data}) ->
    emqx_utils_json:encode(#{'Cmd' => Cmd, 'Vin' => Vin, 'Encrypt' => Encrypt, 'Data' => Data}).

ack(Code, Frame = #frame{data = Data, ack = ?ACK_IS_CMD}) ->
    % PROTO: Update time & ack feilds only
    Frame#frame{ack = Code, data = Data#{<<"Time">> => gentime()}}.

ack_frame(Key, Inflight) ->
    case emqx_inflight:contain(Key, Inflight) of
        true -> emqx_inflight:delete(Key, Inflight);
        false -> Inflight
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
            {{value, Frame}, NewQueue} = queue:out(Queue),

            log(debug, #{msg => "delivery", frame => Frame}, Channel),

            NewInflight = emqx_inflight:insert(
                Frame#frame.cmd, {Frame, RetxMax, erlang:system_time(millisecond)}, Inflight
            ),
            NChannel = Channel#channel{mqueue = NewQueue, inflight = NewInflight},
            {[Frame], ensure_timer(retry_timer, NChannel)}
    end.

gentime() ->
    {Year, Mon, Day} = date(),
    {Hour, Min, Sec} = time(),
    Year1 = list_to_integer(string:substr(integer_to_list(Year), 3, 2)),
    #{
        <<"Year">> => Year1,
        <<"Month">> => Mon,
        <<"Day">> => Day,
        <<"Hour">> => Hour,
        <<"Minute">> => Min,
        <<"Second">> => Sec
    }.

%%--------------------------------------------------------------------
%% Message to frame
%%--------------------------------------------------------------------

msg2frame(#{<<"Action">> := <<"Query">>, <<"Total">> := Total, <<"Ids">> := Ids}, Vin) ->
    % Ids  = [<<"0x01">>, <<"0x02">>] --> [1, 2]
    Data = #{
        <<"Time">> => gentime(),
        <<"Total">> => Total,
        <<"Ids">> => lists:map(fun hexstring_to_byte/1, Ids)
    },
    #frame{
        cmd = ?CMD_PARAM_QUERY, ack = ?ACK_IS_CMD, vin = Vin, encrypt = ?ENCRYPT_NONE, data = Data
    };
msg2frame(#{<<"Action">> := <<"Setting">>, <<"Total">> := Total, <<"Params">> := Params}, Vin) ->
    % Params  = [#{<<"0x01">> := 5000}, #{<<"0x02">> := 400}]
    % Params1 = [#{1 := 5000}, #{2 := 400}]
    Params1 = lists:foldr(
        fun(M, Acc) ->
            [{K, V}] = maps:to_list(M),
            [#{hexstring_to_byte(K) => V} | Acc]
        end,
        [],
        Params
    ),
    Data = #{<<"Time">> => gentime(), <<"Total">> => Total, <<"Params">> => Params1},
    #frame{
        cmd = ?CMD_PARAM_SETTING, ack = ?ACK_IS_CMD, vin = Vin, encrypt = ?ENCRYPT_NONE, data = Data
    };
msg2frame(Data = #{<<"Action">> := <<"Control">>, <<"Command">> := Command}, Vin) ->
    Param = maps:get(<<"Param">>, Data, <<>>),
    Data1 = #{
        <<"Time">> => gentime(),
        <<"Command">> => hexstring_to_byte(Command),
        <<"Param">> => Param
    },
    #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = Vin,
        encrypt = ?ENCRYPT_NONE,
        data = Data1
    };
msg2frame(_Data, _Vin) ->
    {error, unsupproted}.

hexstring_to_byte(S) when is_binary(S) ->
    hexstring_to_byte(binary_to_list(S));
hexstring_to_byte("0x" ++ S) ->
    tune_byte(list_to_integer(S, 16));
hexstring_to_byte(S) ->
    tune_byte(list_to_integer(S)).

tune_byte(I) when I =< 16#FF -> I;
tune_byte(_) -> exit(invalid_byte).

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

log(Level, Meta, #channel{clientinfo = #{clientid := ClientId, username := Username}} = _Channel) ->
    ?SLOG(Level, Meta#{clientid => ClientId, username => Username}).

metrics_inc(Name, #channel{ctx = Ctx}, Oct) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name, Oct).

subscribe_downlink(
    Topic,
    #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{
                    clientid := ClientId,
                    mountpoint := Mountpoint
                },
        subscriptions = Subscriptions
    } = Channel
) ->
    {ParsedTopic, SubOpts0} = emqx_topic:parse(Topic),
    SubOpts = maps:merge(emqx_gateway_utils:default_subopts(), SubOpts0),
    MountedTopic = emqx_mountpoint:mount(Mountpoint, ParsedTopic),
    _ = emqx_broker:subscribe(MountedTopic, ClientId, SubOpts),
    run_hooks(Ctx, 'session.subscribed', [ClientInfo, MountedTopic, SubOpts]),
    Channel#channel{subscriptions = Subscriptions#{MountedTopic => SubOpts}}.
