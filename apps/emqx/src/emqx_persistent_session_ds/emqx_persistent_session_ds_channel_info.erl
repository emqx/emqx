%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_channel_info).

%% API:
-export([encode/1, enrich/2]).

-include("../../gen_src/ChannelInfo.hrl").
-include("pmap.hrl").

%% TODO: https://github.com/erlang/otp/issues/9841
-dialyzer({nowarn_function, [encode/1, enrich/2]}).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

encode(#{
    stats := Stats0,
    chan_info := #{conninfo := ConnInfo, clientinfo := ClientInfo},
    disconnected_at := DisconnectedAt,
    last_connected_to := Node
}) ->
    Stats = maps:from_list(Stats0),
    Stat = fun(K, Default) ->
        case Stats of
            #{K := Val} when is_integer(Val) -> Val;
            #{} -> Default
        end
    end,
    PeerName =
        case ConnInfo of
            #{peername := {IP, Port}} ->
                emqx_ds_msg_serializer:encode_ip_port(16, IP, Port);
            _ ->
                asn1_NOVALUE
        end,
    Rec = #'ChannelInfo'{
        recvOct = Stat(recv_oct, 0),
        recvCnt = Stat(recv_cnt, 0),
        recvPkt = Stat(recv_pkt, 0),
        recvMsg = Stat(recv_msg, 0),
        recvMsgQos0 = Stat('recv_msg.qos0', 0),
        recvMsgQos1 = Stat('recv_msg.qos1', 0),
        recvMsgQos2 = Stat('recv_msg.qos2', 0),
        recvMsgDropped = Stat('recv_msg.dropped', 0),
        recvMsgDroppedAwaitPubrelTimeout = Stat('recv_msg.dropped.await_pubrel_timeout', 0),

        sendOct = Stat(send_oct, 0),
        sendCnt = Stat(send_cnt, 0),
        sendPkt = Stat(send_pkt, 0),
        sendMsg = Stat(send_msg, 0),
        sendMsgQos0 = Stat('send_msg.qos0', 0),
        sendMsgQos1 = Stat('send_msg.qos1', 0),
        sendMsgQos2 = Stat('send_msg.qos2', 0),
        sendMsgDropped = Stat('send_msg.dropped', 0),
        sendMsgDroppedExpired = Stat('send_msg.dropped.expired', 0),
        sendMsgDroppedQueueFull = Stat('send_msg.dropped.queue_full', 0),
        sendMsgDroppedTooLarge = Stat('send_msg.dropped.too_large', 0),

        subscriptionsMax = Stat(subscriptions_max, asn1_NOVALUE),
        awaitingRelMax = Stat(awaiting_rel_max, asn1_NOVALUE),
        inflightMax = Stat(inflight_max, asn1_NOVALUE),

        connectedAt = maps:get(connected_at, ConnInfo, 0),
        disconnectedAt = DisconnectedAt,
        username = undefined_to_novalue(maps:get(username, ConnInfo, undefined)),
        mountpoint = undefined_to_novalue(maps:get(mountpoint, ClientInfo, undefined)),
        protocol = enc_protocol(ConnInfo, ClientInfo),
        peername = PeerName,
        listener = atom_to_binary(maps:get(listener, ClientInfo, undefined)),
        keepalive = maps:get(keepalive, ConnInfo, 0),
        node = atom_to_binary(Node),
        cleanStart = maps:get(clean_start, ConnInfo, true),
        isBridge = maps:get(is_bridge, ClientInfo, false)
    },
    'ChannelInfo':encode('ChannelInfo', Rec).

enrich(Bin, #{
    ?id := Id,
    ?subscriptions := Subs,
    ?streams := Streams,
    ?metadata := Meta,
    ?awaiting_rel := AwaitingRel
}) ->
    #'ChannelInfo'{
        recvOct = RecvOct,
        recvCnt = RecvCnt,
        recvPkt = RecvPkt,
        recvMsg = RecvMsg,
        recvMsgQos0 = RecvMsgQos0,
        recvMsgQos1 = RecvMsgQos1,
        recvMsgQos2 = RecvMsgQos2,
        recvMsgDropped = RecvMsgDropped,
        recvMsgDroppedAwaitPubrelTimeout = RecvMsgDroppedAwaitPubrelTimeout,

        sendOct = SendOct,
        sendCnt = SendCnt,
        sendPkt = SendPkt,
        sendMsg = SendMsg,
        sendMsgQos0 = SendMsgQos0,
        sendMsgQos1 = SendMsgQos1,
        sendMsgQos2 = SendMsgQos2,
        sendMsgDropped = SendMsgDropped,
        sendMsgDroppedExpired = SendMsgDroppedExpired,
        sendMsgDroppedQueueFull = SendMsgDroppedQueueFull,
        sendMsgDroppedTooLarge = SendMsgDroppedTooLarge,

        subscriptionsMax = SubscriptionsMax,
        awaitingRelMax = AwaitingRelMax,
        inflightMax = InflightMax,

        connectedAt = ConnectedAt,
        disconnectedAt = DisconnectedAt,
        username = Username,
        mountpoint = Mountpoint,
        protocol = Protocol,
        peername = PeerNameRaw,
        listener = Listener,
        keepalive = KeepAlive,
        node = NodeBin,
        cleanStart = CleanStart,
        isBridge = IsBridge
    } = 'ChannelInfo':decode('ChannelInfo', Bin),
    Stats = [
        {durable, true},
        {recv_oct, RecvOct},
        {recv_cnt, RecvCnt},
        {recv_pkt, RecvPkt},
        {recv_msg, RecvMsg},
        {'recv_msg.qos0', RecvMsgQos0},
        {'recv_msg.qos1', RecvMsgQos1},
        {'recv_msg.qos2', RecvMsgQos2},
        {'recv_msg.dropped', RecvMsgDropped},
        {'recv_msg.dropped.await_pubrel_timeout', RecvMsgDroppedAwaitPubrelTimeout},
        {send_oct, SendOct},
        {send_cnt, SendCnt},
        {send_pkt, SendPkt},
        {send_msg, SendMsg},
        {send_pend, 0},
        {awaitin_rel_cnt, maps:size(AwaitingRel)},
        {'send_msg.qos0', SendMsgQos0},
        {'send_msg.qos1', SendMsgQos1},
        {'send_msg.qos2', SendMsgQos2},
        {'send_msg.dropped', SendMsgDropped},
        {'send_msg.dropped.expired', SendMsgDroppedExpired},
        {'send_msg.dropped.queue_full', SendMsgDroppedQueueFull},
        {'send_msg.dropped.too_large', SendMsgDroppedTooLarge},
        {subscriptions_cnt, maps:size(Subs)},
        {subscriptions_max, cnt_maybe_inf(SubscriptionsMax)},
        {awaiting_rel_max, cnt_maybe_inf(AwaitingRelMax)},
        {inflight_cnt, 0},
        {inflight_max, cnt_maybe_inf(InflightMax)},
        {reductions, 0},
        {mailbox_len, 0},
        {memory, 0},
        {heap_size, 0},
        {total_heap_size, 0},
        {mqueue_dropped, 0},
        {mqueue_len, 0},
        {n_streams, maps:size(Streams)},
        %% TODO: this data is available
        {seqno_q1_comm, 0},
        {seqno_q1_dup, 0},
        {seqno_q1_next, 0},
        {seqno_q2_comm, 0},
        {seqno_q2_dup, 0},
        {seqno_q2_rec, 0},
        {seqno_q2_next, 0},
        {awaiting_rel_cnt, 0}
    ],
    {ProtoName, ProtoVer} = dec_protocol(Protocol),
    PeerName =
        case PeerNameRaw of
            asn1_NOVALUE -> undefined;
            _ -> emqx_ds_msg_serializer:decode_ip_port(16, PeerNameRaw)
        end,
    ExpiryInterval = maps:get(expiry_interval, Meta, 0),
    ConnInfo = #{
        clientid => Id,
        username => novalue_to_undefined(Username),
        keepalive => KeepAlive,
        connected_at => ConnectedAt,
        clean_start => CleanStart,
        proto_name => ProtoName,
        proto_ver => ProtoVer,
        peername => PeerName,
        expiry_interval => ExpiryInterval
    },
    ClientInfo = #{
        clientid => Id,
        username => novalue_to_undefined(Username),
        mountpoint => novalue_to_undefined(Mountpoint),
        is_bridge => IsBridge,
        protocol => ProtoName,
        peername => PeerName,
        listener => binary_to_atom(Listener)
    },
    Session = #{
        id => Id,
        is_persistent => true,
        created_at => maps:get(created_at, Meta, 0)
    },
    {
        {Id, undefined},
        #{
            session => Session,
            conninfo => ConnInfo,
            conn_state => disconnected,
            clientinfo => ClientInfo,
            node => erlang:binary_to_atom(NodeBin),
            disconnected_at => DisconnectedAt
        },
        Stats
    }.

%%================================================================================
%% Internal functions
%%================================================================================

cnt_maybe_inf(asn1_NOVALUE) ->
    infinity;
cnt_maybe_inf(N) when is_integer(N) ->
    N.

enc_protocol(#{proto_ver := Ver}, #{protocol := mqtt}) ->
    {mqtt, Ver};
enc_protocol(#{proto_ver := Ver}, #{protocol := Other}) ->
    ProtoBin =
        case Other of
            _ when is_atom(Other) -> atom_to_binary(Other);
            _ when is_binary(Other) -> Other
        end,
    {other, #'OtherProtocol'{ver = Ver, proto = ProtoBin}}.

dec_protocol({mqtt, Ver}) ->
    {<<"MQTT">>, Ver};
dec_protocol({other, #'OtherProtocol'{proto = Proto, ver = Ver}}) ->
    {Proto, Ver}.

undefined_to_novalue(undefined) ->
    asn1_NOVALUE;
undefined_to_novalue(Bin) when is_binary(Bin) ->
    Bin.

novalue_to_undefined(asn1_NOVALUE) ->
    undefined;
novalue_to_undefined(A) ->
    A.
