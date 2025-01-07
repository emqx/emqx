%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_METRICS_HRL).
-define(EMQX_METRICS_HRL, true).

%% Bytes sent and received
-define(BYTES_METRICS, [
    {counter, 'bytes.received', <<"Number of bytes received ">>},
    {counter, 'bytes.sent', <<"Number of bytes sent on this connection">>}
]).

%% Packets sent and received
-define(PACKET_METRICS, [
    {counter, 'packets.received', <<"Number of received packet">>},
    {counter, 'packets.sent', <<"Number of sent packet">>},
    {counter, 'packets.connect.received', <<"Number of received CONNECT packet">>},
    {counter, 'packets.connack.sent', <<"Number of sent CONNACK packet">>},
    {counter, 'packets.connack.error',
        <<"Number of received CONNECT packet with unsuccessful connections">>},
    {counter, 'packets.connack.auth_error',
        <<"Number of received CONNECT packet with failed Authentication">>},
    {counter, 'packets.publish.received', <<"Number of received PUBLISH packet">>},
    %% PUBLISH packets sent
    {counter, 'packets.publish.sent', <<"Number of sent PUBLISH packet">>},
    %% PUBLISH packet_id inuse
    {counter, 'packets.publish.inuse',
        <<"Number of received PUBLISH packet with occupied identifiers">>},
    %% PUBLISH failed for error
    {counter, 'packets.publish.error',
        <<"Number of received PUBLISH packet that cannot be published">>},
    %% PUBLISH failed for auth error
    {counter, 'packets.publish.auth_error',
        <<"Number of received PUBLISH packets with failed the Authorization check">>},
    %% PUBLISH(QoS2) packets dropped
    {counter, 'packets.publish.dropped',
        <<"Number of messages discarded due to the receiving limit">>},
    %% PUBACK packets received
    {counter, 'packets.puback.received', <<"Number of received PUBACK packet">>},
    %% PUBACK packets sent
    {counter, 'packets.puback.sent', <<"Number of sent PUBACK packet">>},
    %% PUBACK packet_id inuse
    {counter, 'packets.puback.inuse',
        <<"Number of received PUBACK packet with occupied identifiers">>},
    %% PUBACK packets missed
    {counter, 'packets.puback.missed', <<"Number of received packet with identifiers.">>},
    %% PUBREC packets received
    {counter, 'packets.pubrec.received', <<"Number of received PUBREC packet">>},
    %% PUBREC packets sent
    {counter, 'packets.pubrec.sent', <<"Number of sent PUBREC packet">>},
    %% PUBREC packet_id inuse
    {counter, 'packets.pubrec.inuse',
        <<"Number of received PUBREC packet with occupied identifiers">>},
    %% PUBREC packets missed
    {counter, 'packets.pubrec.missed',
        <<"Number of received PUBREC packet with unknown identifiers">>},
    %% PUBREL packets received
    {counter, 'packets.pubrel.received', <<"Number of received PUBREL packet">>},
    %% PUBREL packets sent
    {counter, 'packets.pubrel.sent', <<"Number of sent PUBREL packet">>},
    %% PUBREL packets missed
    {counter, 'packets.pubrel.missed',
        <<"Number of received PUBREC packet with unknown identifiers">>},
    %% PUBCOMP packets received
    {counter, 'packets.pubcomp.received', <<"Number of received PUBCOMP packet">>},
    %% PUBCOMP packets sent
    {counter, 'packets.pubcomp.sent', <<"Number of sent PUBCOMP packet">>},
    %% PUBCOMP packet_id inuse
    {counter, 'packets.pubcomp.inuse',
        <<"Number of received PUBCOMP packet with occupied identifiers">>},
    %% PUBCOMP packets missed
    {counter, 'packets.pubcomp.missed', <<"Number of missed PUBCOMP packet">>},
    %% SUBSCRIBE Packets received
    {counter, 'packets.subscribe.received', <<"Number of received SUBSCRIBE packet">>},
    %% SUBSCRIBE error
    {counter, 'packets.subscribe.error',
        <<"Number of received SUBSCRIBE packet with failed subscriptions">>},
    %% SUBSCRIBE failed for not auth
    {counter, 'packets.subscribe.auth_error',
        <<"Number of received SUBACK packet with failed Authorization check">>},
    %% SUBACK packets sent
    {counter, 'packets.suback.sent', <<"Number of sent SUBACK packet">>},
    %% UNSUBSCRIBE Packets received
    {counter, 'packets.unsubscribe.received', <<"Number of received UNSUBSCRIBE packet">>},
    %% UNSUBSCRIBE error
    {counter, 'packets.unsubscribe.error',
        <<"Number of received UNSUBSCRIBE packet with failed unsubscriptions">>},
    %% UNSUBACK Packets sent
    {counter, 'packets.unsuback.sent', <<"Number of sent UNSUBACK packet">>},
    %% PINGREQ packets received
    {counter, 'packets.pingreq.received', <<"Number of received PINGREQ packet">>},
    %% PINGRESP Packets sent
    {counter, 'packets.pingresp.sent', <<"Number of sent PUBRESP packet">>},
    %% DISCONNECT Packets received
    {counter, 'packets.disconnect.received', <<"Number of received DISCONNECT packet">>},
    %% DISCONNECT Packets sent
    {counter, 'packets.disconnect.sent', <<"Number of sent DISCONNECT packet">>},
    %% Auth Packets received
    {counter, 'packets.auth.received', <<"Number of received AUTH packet">>},
    %% Auth Packets sent
    {counter, 'packets.auth.sent', <<"Number of sent AUTH packet">>}
]).

%% Messages sent/received and pubsub
-define(MESSAGE_METRICS, [
    %% All Messages received
    {counter, 'messages.received', <<
        "Number of messages received from the client, equal to the sum of "
        "messages.qos0.received, messages.qos1.received and messages.qos2.received"
    >>},
    %% All Messages sent
    {counter, 'messages.sent', <<
        "Number of messages sent to the client, equal to the sum of "
        "messages.qos0.sent, messages.qos1.sent and messages.qos2.sent"
    >>},
    %% QoS0 Messages received
    {counter, 'messages.qos0.received', <<"Number of QoS 0 messages received from clients">>},
    %% QoS0 Messages sent
    {counter, 'messages.qos0.sent', <<"Number of QoS 0 messages sent to clients">>},
    %% QoS1 Messages received
    {counter, 'messages.qos1.received', <<"Number of QoS 1 messages received from clients">>},
    %% QoS1 Messages sent
    {counter, 'messages.qos1.sent', <<"Number of QoS 1 messages sent to clients">>},
    %% QoS2 Messages received
    {counter, 'messages.qos2.received', <<"Number of QoS 2 messages received from clients">>},
    %% QoS2 Messages sent
    {counter, 'messages.qos2.sent', <<"Number of QoS 2 messages sent to clients">>},
    %% PubSub Metrics

    %% Messages Publish
    {counter, 'messages.publish',
        <<"Number of messages published in addition to system messages">>},
    %% Messages dropped due to no subscribers
    {counter, 'messages.dropped',
        <<"Number of messages dropped before forwarding to the subscription process">>},
    %% Messages that failed validations
    {counter, 'messages.validation_failed', <<"Number of message validation failed">>},
    %% Messages that passed validations
    {counter, 'messages.validation_succeeded', <<"Number of message validation successful">>},
    %% % Messages that failed transformations
    {counter, 'messages.transformation_failed', <<"Number fo message transformation failed">>},
    %% % Messages that passed transformations
    {counter, 'messages.transformation_succeeded',
        <<"Number fo message transformation succeeded">>},
    %% QoS2 Messages expired
    {counter, 'messages.dropped.await_pubrel_timeout',
        <<"Number of messages dropped due to waiting PUBREL timeout">>},
    %% Messages dropped
    {counter, 'messages.dropped.no_subscribers',
        <<"Number of messages dropped due to no subscribers">>},
    %% Messages forward
    {counter, 'messages.forward', <<"Number of messages forwarded to other nodes">>},
    %% Messages delayed
    {counter, 'messages.delayed', <<"Number of delay-published messages">>},
    %% Messages delivered
    {counter, 'messages.delivered',
        <<"Number of messages forwarded to the subscription process internally">>},
    %% Messages acked
    {counter, 'messages.acked', <<"Number of received PUBACK and PUBREC packet">>},
    %% Messages persistently stored
    {counter, 'messages.persisted', <<"Number of message persisted">>}
]).

%% Delivery metrics
-define(DELIVERY_METRICS, [
    %% All Dropped during delivery
    {counter, 'delivery.dropped', <<"Total number of discarded messages when sending">>},
    %% Dropped due to no_local
    {counter, 'delivery.dropped.no_local', <<
        "Number of messages that were dropped due to the No Local subscription "
        "option when sending"
    >>},
    %% Dropped due to message too large
    {counter, 'delivery.dropped.too_large', <<
        "The number of messages that were dropped because the length exceeded "
        "the limit when sending"
    >>},
    %% Dropped qos0 message
    {counter, 'delivery.dropped.qos0_msg', <<
        "Number of messages with QoS 0 that were dropped because the message "
        "queue was full when sending"
    >>},
    %% Dropped due to queue full
    {counter, 'delivery.dropped.queue_full', <<
        "Number of messages with a non-zero QoS that were dropped because the "
        "message queue was full when sending"
    >>},
    %% Dropped due to expired
    {counter, 'delivery.dropped.expired',
        <<"Number of messages dropped due to message expiration on sending">>}
]).

%% Client Lifecircle metrics
-define(CLIENT_METRICS, [
    {counter, 'client.connect', <<"Number of client connections">>},
    {counter, 'client.connack', <<"Number of CONNACK packet sent">>},
    {counter, 'client.connected', <<"Number of successful client connected">>},
    {counter, 'client.authenticate', <<"Number of client Authentication">>},
    {counter, 'client.auth.anonymous', <<"Number of clients who log in anonymously">>},
    {counter, 'client.authorize', <<"Number of Authorization rule checks">>},
    {counter, 'client.subscribe', <<"Number of client subscriptions">>},
    {counter, 'client.unsubscribe', <<"Number of client unsubscriptions">>},
    {counter, 'client.disconnected', <<"Number of client disconnects">>}
]).

%% Session Lifecircle metrics
-define(SESSION_METRICS, [
    {counter, 'session.created', <<"Number of sessions created">>},
    {counter, 'session.resumed',
        <<"Number of sessions resumed because Clean Session or Clean Start is false">>},
    {counter, 'session.takenover',
        <<"Number of sessions takenover because Clean Session or Clean Start is false">>},
    {counter, 'session.discarded',
        <<"Number of sessions dropped because Clean Session or Clean Start is true">>},
    {counter, 'session.terminated', <<"Number of terminated sessions">>}
]).

%% Statistic metrics for ACL checking
-define(STASTS_ACL_METRICS, [
    {counter, 'authorization.allow', <<"Number of Authorization allow">>},
    {counter, 'authorization.deny', <<"Number of Authorization deny">>},
    {counter, 'authorization.cache_hit', <<"Number of Authorization hits the cache">>},
    {counter, 'authorization.cache_miss', <<"Number of Authorization cache missing">>}
]).

%% Statistic metrics for auth checking
-define(STASTS_AUTHN_METRICS, [
    {counter, 'authentication.success', <<"Number of successful client Authentication">>},
    {counter, 'authentication.success.anonymous',
        <<"Number of successful client Authentication due to anonymous">>},
    {counter, 'authentication.failure', <<"Number of failed client Authentication">>}
]).

%% Overload protection counters
-define(OLP_METRICS, [
    {counter, 'overload_protection.delay.ok', <<"Number of overload protection delayed">>},
    {counter, 'overload_protection.delay.timeout',
        <<"Number of overload protection delay timeout">>},
    {counter, 'overload_protection.hibernation', <<"Number of overload protection hibernation">>},
    {counter, 'overload_protection.gc', <<"Number of overload protection garbage collection">>},
    {counter, 'overload_protection.new_conn',
        <<"Number of overload protection close new incoming connection">>}
]).

-endif.
