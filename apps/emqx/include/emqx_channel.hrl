%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

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

-define(INFO_KEYS, [
    conninfo,
    conn_state,
    clientinfo,
    session,
    will_msg
]).

-define(REPLY_OUTGOING(Packets), {outgoing, Packets}).
-define(REPLY_CONNACK(Packet), {connack, Packet}).
-define(REPLY_EVENT(StateOrEvent), {event, StateOrEvent}).
-define(REPLY_CLOSE(Reason), {close, Reason}).

-define(EXPIRE_INTERVAL_INFINITE, 4294967295000).
