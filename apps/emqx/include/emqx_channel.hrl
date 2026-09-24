%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% The session attributes cached in the `emqx_channel_info` table.
%%
%% The counters live in the stats element of the same table row, and `subscriptions` is
%% left out because building that map costs O(number of subscriptions) on every refresh.
%% `emqx_session:info/1` stays complete: it is the payload of the `session.created` and
%% `session.resumed` hooks.
-define(CHAN_INFO_SESSION_KEYS, [
    created_at,
    is_persistent,
    impl
]).

-define(REPLY_OUTGOING(Packets), {outgoing, Packets}).
-define(REPLY_CONNACK(Packet), {connack, Packet}).
-define(REPLY_EVENT(StateOrEvent), {event, StateOrEvent}).
-define(REPLY_CLOSE(Reason), {close, Reason}).

-define(EXPIRE_INTERVAL_INFINITE, 4294967295000).
