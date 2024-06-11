%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    %% Total bytes received
    {counter, 'bytes.received'},
    %% Total bytes sent
    {counter, 'bytes.sent'}
]).

%% Packets sent and received
-define(PACKET_METRICS, [
    %% All Packets received
    {counter, 'packets.received'},
    %% All Packets sent
    {counter, 'packets.sent'},
    %% CONNECT Packets received
    {counter, 'packets.connect.received'},
    %% CONNACK Packets sent
    {counter, 'packets.connack.sent'},
    %% CONNACK error sent
    {counter, 'packets.connack.error'},
    %% CONNACK auth_error sent
    {counter, 'packets.connack.auth_error'},
    %% PUBLISH packets received
    {counter, 'packets.publish.received'},
    %% PUBLISH packets sent
    {counter, 'packets.publish.sent'},
    %% PUBLISH packet_id inuse
    {counter, 'packets.publish.inuse'},
    %% PUBLISH failed for error
    {counter, 'packets.publish.error'},
    %% PUBLISH failed for auth error
    {counter, 'packets.publish.auth_error'},
    %% PUBLISH(QoS2) packets dropped
    {counter, 'packets.publish.dropped'},
    %% PUBACK packets received
    {counter, 'packets.puback.received'},
    %% PUBACK packets sent
    {counter, 'packets.puback.sent'},
    %% PUBACK packet_id inuse
    {counter, 'packets.puback.inuse'},
    %% PUBACK packets missed
    {counter, 'packets.puback.missed'},
    %% PUBREC packets received
    {counter, 'packets.pubrec.received'},
    %% PUBREC packets sent
    {counter, 'packets.pubrec.sent'},
    %% PUBREC packet_id inuse
    {counter, 'packets.pubrec.inuse'},
    %% PUBREC packets missed
    {counter, 'packets.pubrec.missed'},
    %% PUBREL packets received
    {counter, 'packets.pubrel.received'},
    %% PUBREL packets sent
    {counter, 'packets.pubrel.sent'},
    %% PUBREL packets missed
    {counter, 'packets.pubrel.missed'},
    %% PUBCOMP packets received
    {counter, 'packets.pubcomp.received'},
    %% PUBCOMP packets sent
    {counter, 'packets.pubcomp.sent'},
    %% PUBCOMP packet_id inuse
    {counter, 'packets.pubcomp.inuse'},
    %% PUBCOMP packets missed
    {counter, 'packets.pubcomp.missed'},
    %% SUBSCRIBE Packets received
    {counter, 'packets.subscribe.received'},
    %% SUBSCRIBE error
    {counter, 'packets.subscribe.error'},
    %% SUBSCRIBE failed for not auth
    {counter, 'packets.subscribe.auth_error'},
    %% SUBACK packets sent
    {counter, 'packets.suback.sent'},
    %% UNSUBSCRIBE Packets received
    {counter, 'packets.unsubscribe.received'},
    %% UNSUBSCRIBE error
    {counter, 'packets.unsubscribe.error'},
    %% UNSUBACK Packets sent
    {counter, 'packets.unsuback.sent'},
    %% PINGREQ packets received
    {counter, 'packets.pingreq.received'},
    %% PINGRESP Packets sent
    {counter, 'packets.pingresp.sent'},
    %% DISCONNECT Packets received
    {counter, 'packets.disconnect.received'},
    %% DISCONNECT Packets sent
    {counter, 'packets.disconnect.sent'},
    %% Auth Packets received
    {counter, 'packets.auth.received'},
    %% Auth Packets sent
    {counter, 'packets.auth.sent'}
]).

%% Messages sent/received and pubsub
-define(MESSAGE_METRICS, [
    %% All Messages received
    {counter, 'messages.received'},
    %% All Messages sent
    {counter, 'messages.sent'},
    %% QoS0 Messages received
    {counter, 'messages.qos0.received'},
    %% QoS0 Messages sent
    {counter, 'messages.qos0.sent'},
    %% QoS1 Messages received
    {counter, 'messages.qos1.received'},
    %% QoS1 Messages sent
    {counter, 'messages.qos1.sent'},
    %% QoS2 Messages received
    {counter, 'messages.qos2.received'},
    %% QoS2 Messages sent
    {counter, 'messages.qos2.sent'},
    %% PubSub Metrics

    %% Messages Publish
    {counter, 'messages.publish'},
    %% Messages dropped due to no subscribers
    {counter, 'messages.dropped'},
    %% Messages that failed validations
    {counter, 'messages.validation_failed'},
    %% Messages that passed validations
    {counter, 'messages.validation_succeeded'},
    %% % Messages that failed transformations
    {counter, 'messages.transformation_failed'},
    %% % Messages that passed transformations
    {counter, 'messages.transformation_succeeded'},
    %% QoS2 Messages expired
    {counter, 'messages.dropped.await_pubrel_timeout'},
    %% Messages dropped
    {counter, 'messages.dropped.no_subscribers'},
    %% Messages forward
    {counter, 'messages.forward'},
    %% Messages delayed
    {counter, 'messages.delayed'},
    %% Messages delivered
    {counter, 'messages.delivered'},
    %% Messages acked
    {counter, 'messages.acked'},
    %% Messages persistently stored
    {counter, 'messages.persisted'}
]).

%% Delivery metrics
-define(DELIVERY_METRICS, [
    %% All Dropped during delivery
    {counter, 'delivery.dropped'},
    %% Dropped due to no_local
    {counter, 'delivery.dropped.no_local'},
    %% Dropped due to message too large
    {counter, 'delivery.dropped.too_large'},
    %% Dropped qos0 message
    {counter, 'delivery.dropped.qos0_msg'},
    %% Dropped due to queue full
    {counter, 'delivery.dropped.queue_full'},
    %% Dropped due to expired
    {counter, 'delivery.dropped.expired'}
]).

%% Client Lifecircle metrics
-define(CLIENT_METRICS, [
    {counter, 'client.connect'},
    {counter, 'client.connack'},
    {counter, 'client.connected'},
    {counter, 'client.authenticate'},
    {counter, 'client.auth.anonymous'},
    {counter, 'client.authorize'},
    {counter, 'client.subscribe'},
    {counter, 'client.unsubscribe'},
    {counter, 'client.disconnected'}
]).

%% Session Lifecircle metrics
-define(SESSION_METRICS, [
    {counter, 'session.created'},
    {counter, 'session.resumed'},
    %% Session taken over by another client (Connect with clean_session|clean_start=false)
    {counter, 'session.takenover'},
    %% Session taken over by another client (Connect with clean_session|clean_start=true)
    {counter, 'session.discarded'},
    {counter, 'session.terminated'}
]).

%% Statistic metrics for ACL checking
-define(STASTS_ACL_METRICS, [
    {counter, 'authorization.allow'},
    {counter, 'authorization.deny'},
    {counter, 'authorization.cache_hit'},
    {counter, 'authorization.cache_miss'}
]).

%% Statistic metrics for auth checking
-define(STASTS_AUTHN_METRICS, [
    {counter, 'authentication.success'},
    {counter, 'authentication.success.anonymous'},
    {counter, 'authentication.failure'}
]).

%% Overload protection counters
-define(OLP_METRICS, [
    {counter, 'overload_protection.delay.ok'},
    {counter, 'overload_protection.delay.timeout'},
    {counter, 'overload_protection.hibernation'},
    {counter, 'overload_protection.gc'},
    {counter, 'overload_protection.new_conn'}
]).

-endif.
