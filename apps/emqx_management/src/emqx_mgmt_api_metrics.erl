%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_metrics).

-behavior(minirest_api).

-export([api_spec/0]).

-export([list/2]).

api_spec() ->
    {[metrics_api()], [metrics_schema()]}.

metrics_schema() ->
    #{
        metrics => #{
            type => object,
            properties => #{
                'actions.failure' => #{
                    type => integer,
                    description => <<"Number of failure executions of the rule engine action">>},
                'actions.success' => #{
                    type => integer,
                    description => <<"Number of successful executions of the rule engine action">>},
                'bytes.received' => #{
                    type => integer,
                    description => <<"Number of bytes received by EMQ X Broker">>},
                'bytes.sent' => #{
                    type => integer,
                    description => <<"Number of bytes sent by EMQ X Broker on this connection">>},
                'client.authenticate' => #{
                    type => integer,
                    description => <<"Number of client authentications">>},
                'client.auth.anonymous' => #{
                    type => integer,
                    description => <<"Number of clients who log in anonymously">>},
                'client.connect' => #{
                    type => integer,
                    description => <<"Number of client connections">>},
                'client.connack' => #{
                    type => integer,
                    description => <<"Number of CONNACK packet sent">>},
                'client.connected' => #{
                    type => integer,
                    description => <<"Number of successful client connections">>},
                'client.disconnected' => #{
                    type => integer,
                    description => <<"Number of client disconnects">>},
                'client.check_acl' => #{
                    type => integer,
                    description => <<"Number of ACL rule checks">>},
                'client.subscribe' => #{
                    type => integer,
                    description => <<"Number of client subscriptions">>},
                'client.unsubscribe' => #{
                    type => integer,
                    description => <<"Number of client unsubscriptions">>},
                'delivery.dropped.too_large' => #{
                    type => integer,
                    description => <<"The number of messages that were dropped because the length exceeded the limit when sending">>},
                'delivery.dropped.queue_full' => #{
                    type => integer,
                    description => <<"Number of messages with a non-zero QoS that were dropped because the message queue was full when sending">>},
                'delivery.dropped.qos0_msg' => #{
                    type => integer,
                    description => <<"Number of messages with QoS 0 that were dropped because the message queue was full when sending">>},
                'delivery.dropped.expired' => #{
                    type => integer,
                    description => <<"Number of messages dropped due to message expiration on sending">>},
                'delivery.dropped.no_local' => #{
                    type => integer,
                    description => <<"Number of messages that were dropped due to the No Local subscription option when sending">>},
                'delivery.dropped' => #{
                    type => integer,
                    description => <<"Total number of discarded messages when sending">>},
                'messages.delayed' => #{
                    type => integer,
                    description => <<"Number of delay- published messages stored by EMQ X Broker">>},
                'messages.delivered' => #{
                    type => integer,
                    description => <<"Number of messages forwarded to the subscription process internally by EMQ X Broker">>},
                'messages.dropped' => #{
                    type => integer,
                    description => <<"Total number of messages dropped by EMQ X Broker before forwarding to the subscription process">>},
                'messages.dropped.expired' => #{
                    type => integer,
                    description => <<"Number of messages dropped due to message expiration when receiving">>},
                'messages.dropped.no_subscribers' => #{
                    type => integer,
                    description => <<"Number of messages dropped due to no subscribers">>},
                'messages.forward' => #{
                    type => integer,
                    description => <<"Number of messages forwarded to other nodes">>},
                'messages.publish' => #{
                    type => integer,
                    description => <<"Number of messages published in addition to system messages">>},
                'messages.qos0.received' => #{
                    type => integer,
                    description => <<"Number of QoS 0 messages received from clients">>},
                'messages.qos1.received' => #{
                    type => integer,
                    description => <<"Number of QoS 1 messages received from clients">>},
                'messages.qos2.received' => #{
                    type => integer,
                    description => <<"Number of QoS 2 messages received from clients">>},
                'messages.qos0.sent' => #{
                    type => integer,
                    description => <<"Number of QoS 0 messages sent to clients">>},
                'messages.qos1.sent' => #{
                    type => integer,
                    description => <<"Number of QoS 1 messages sent to clients">>},
                'messages.qos2.sent' => #{
                    type => integer,
                    description => <<"Number of QoS 2 messages sent to clients">>},
                'messages.received' => #{
                    type => integer,
                    description => <<"Number of messages received from the client, equal to the sum of messages.qos0.received，messages.qos1.received and messages.qos2.received">>},
                'messages.sent' => #{
                    type => integer,
                    description => <<"Number of messages sent to the client, equal to the sum of messages.qos0.sent，messages.qos1.sent and messages.qos2.sent">>},
                'messages.retained' => #{
                    type => integer,
                    description => <<"Number of retained messages stored by EMQ X Broker">>},
                'messages.acked' => #{
                    type => integer,
                    description => <<"Number of received PUBACK and PUBREC packet">>},
                'packets.received' => #{
                    type => integer,
                    description => <<"Number of received packet">>},
                'packets.sent' => #{
                    type => integer,
                    description => <<"Number of sent packet">>},
                'packets.connect.received' => #{
                    type => integer,
                    description => <<"Number of received CONNECT packet">>},
                'packets.connack.auth_error' => #{
                    type => integer,
                    description => <<"Number of received CONNECT packet with failed authentication">>},
                'packets.connack.error' => #{
                    type => integer,
                    description => <<"Number of received CONNECT packet with unsuccessful connections">>},
                'packets.connack.sent' => #{
                    type => integer,
                    description => <<"Number of sent CONNACK packet">>},
                'packets.publish.received' => #{
                    type => integer,
                    description => <<"Number of received PUBLISH packet">>},
                'packets.publish.sent' => #{
                    type => integer,
                    description => <<"Number of sent PUBLISH packet">>},
                'packets.publish.inuse' => #{
                    type => integer,
                    description => <<"Number of received PUBLISH packet with occupied identifiers">>},
                'packets.publish.auth_error' => #{
                    type => integer,
                    description => <<"Number of received PUBLISH packets with failed the ACL check">>},
                'packets.publish.error' => #{
                    type => integer,
                    description => <<"Number of received PUBLISH packet that cannot be published">>},
                'packets.publish.dropped' => #{
                    type => integer,
                    description => <<"Number of messages discarded due to the receiving limit">>},
                'packets.puback.received' => #{
                    type => integer,
                    description => <<"Number of received PUBACK packet">>},
                'packets.puback.sent' => #{
                    type => integer,
                    description => <<"Number of sent PUBACK packet">>},
                'packets.puback.inuse' => #{
                    type => integer,
                    description => <<"Number of received PUBACK packet with occupied identifiers">>},
                'packets.puback.missed' => #{
                    type => integer,
                    description => <<"Number of received packet with identifiers.">>},
                'packets.pubrec.received' => #{
                    type => integer,
                    description => <<"Number of received PUBREC packet">>},
                'packets.pubrec.sent' => #{
                    type => integer,
                    description => <<"Number of sent PUBREC packet">>},
                'packets.pubrec.inuse' => #{
                    type => integer,
                    description => <<"Number of received PUBREC packet with occupied identifiers">>},
                'packets.pubrec.missed' => #{
                    type => integer,
                    description => <<"Number of received PUBREC packet with unknown identifiers">>},
                'packets.pubrel.received' => #{
                    type => integer,
                    description => <<"Number of received PUBREL packet">>},
                'packets.pubrel.sent' => #{
                    type => integer,
                    description => <<"Number of sent PUBREL packet">>},
                'packets.pubrel.missed' => #{
                    type => integer,
                    description => <<"Number of received PUBREC packet with unknown identifiers">>},
                'packets.pubcomp.received' => #{
                    type => integer,
                    description => <<"Number of received PUBCOMP packet">>},
                'packets.pubcomp.sent' => #{
                    type => integer,
                    description => <<"Number of sent PUBCOMP packet">>},
                'packets.pubcomp.inuse' => #{
                    type => integer,
                    description => <<"Number of received PUBCOMP packet with occupied identifiers">>},
                'packets.pubcomp.missed' => #{
                    type => integer,
                    description => <<"Number of missed PUBCOMP packet">>},
                'packets.subscribe.received' => #{
                    type => integer,
                    description => <<"Number of received SUBSCRIBE packet">>},
                'packets.subscribe.error' => #{
                    type => integer,
                    description => <<"Number of received SUBSCRIBE packet with failed subscriptions">>},
                'packets.subscribe.auth_error' => #{
                    type => integer,
                    description => <<"Number of received SUBACK packet with failed ACL check">>},
                'packets.suback.sent' => #{
                    type => integer,
                    description => <<"Number of sent SUBACK packet">>},
                'packets.unsubscribe.received' => #{
                    type => integer,
                    description => <<"Number of received UNSUBSCRIBE packet">>},
                'packets.unsubscribe.error' => #{
                    type => integer,
                    description => <<"Number of received UNSUBSCRIBE packet with failed unsubscriptions">>},
                'packets.unsuback.sent' => #{
                    type => integer,
                    description => <<"Number of sent UNSUBACK packet">>},
                'packets.pingreq.received' => #{
                    type => integer,
                    description => <<"Number of received PINGREQ packet">>},
                'packets.pingresp.sent' => #{
                    type => integer,
                    description => <<"Number of sent PUBRESP packet">>},
                'packets.disconnect.received' => #{
                    type => integer,
                    description => <<"Number of received DISCONNECT packet">>},
                'packets.disconnect.sent' => #{
                    type => integer,
                    description => <<"Number of sent DISCONNECT packet">>},
                'packets.auth.received' => #{
                    type => integer,
                    description => <<"Number of received AUTH packet">>},
                'packets.auth.sent' => #{
                    type => integer,
                    description => <<"Number of sent AUTH packet">>},
                'rules.matched' => #{
                    type => integer,
                    description => <<"Number of rule matched">>},
                'session.created' => #{
                    type => integer,
                    description => <<"Number of sessions created">>},
                'session.discarded' => #{
                    type => integer,
                    description => <<"Number of sessions dropped because Clean Session or Clean Start is true">>},
                'session.resumed' => #{
                    type => integer,
                    description => <<"Number of sessions resumed because Clean Session or Clean Start is false">>},
                'session.takeovered' => #{
                    type => integer,
                    description => <<"Number of sessions takeovered because Clean Session or Clean Start is false">>},
                'session.terminated' => #{
                    type => integer,
                    description => <<"Number of terminated sessions">>}
            }
        }
    }.

metrics_api() ->
    Metadata = #{
        get => #{
            description => "EMQ X metrics",
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"List all metrics">>, <<"metrics">>)}}},
    {"/metrics", Metadata, list}.

%%%==============================================================================================
%% api apply
list(get, _) ->
    Response = emqx_json:encode(emqx_mgmt:get_metrics()),
    {200, Response}.
