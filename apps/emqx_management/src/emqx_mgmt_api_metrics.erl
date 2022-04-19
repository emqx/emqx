%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).

%% minirest/dashbaord_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    roots/0,
    fields/1
]).

%% http handlers
-export([metrics/2]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/metrics"].

%%--------------------------------------------------------------------
%% http handlers

metrics(get, #{query_string := Qs}) ->
    case maps:get(<<"aggregate">>, Qs, false) of
        true ->
            {200, emqx_mgmt:get_metrics()};
        false ->
            Data = [
                maps:from_list(
                    emqx_mgmt:get_metrics(Node) ++ [{node, Node}]
                )
             || Node <- mria_mnesia:running_nodes()
            ],
            {200, Data}
    end.

%%--------------------------------------------------------------------
%% swagger defines
%%--------------------------------------------------------------------

schema("/metrics") ->
    #{
        'operationId' => metrics,
        get =>
            #{
                description => <<"EMQX metrics">>,
                parameters =>
                    [
                        {aggregate,
                            mk(
                                boolean(),
                                #{
                                    in => query,
                                    required => false,
                                    desc => <<"Whether to aggregate all nodes Metrics">>
                                }
                            )}
                    ],
                responses =>
                    #{
                        200 => hoconsc:union(
                            [
                                ref(?MODULE, aggregated_metrics),
                                hoconsc:array(ref(?MODULE, node_metrics))
                            ]
                        )
                    }
            }
    }.

roots() ->
    [].

fields(aggregated_metrics) ->
    properties();
fields(node_metrics) ->
    [{node, mk(binary(), #{desc => <<"Node name">>})}] ++ properties().

properties() ->
    [
        m(
            'actions.failure',
            <<"Number of failure executions of the rule engine action">>
        ),
        m(
            'actions.success',
            <<"Number of successful executions of the rule engine action">>
        ),
        m(
            'bytes.received',
            <<"Number of bytes received ">>
        ),
        m(
            'bytes.sent',
            <<"Number of bytes sent on this connection">>
        ),
        m(
            'client.auth.anonymous',
            <<"Number of clients who log in anonymously">>
        ),
        m(
            'client.authenticate',
            <<"Number of client authentications">>
        ),
        m(
            'client.check_authz',
            <<"Number of Authorization rule checks">>
        ),
        m(
            'client.connack',
            <<"Number of CONNACK packet sent">>
        ),
        m(
            'client.connect',
            <<"Number of client connections">>
        ),
        m(
            'client.connected',
            <<"Number of successful client connections">>
        ),
        m(
            'client.disconnected',
            <<"Number of client disconnects">>
        ),
        m(
            'client.subscribe',
            <<"Number of client subscriptions">>
        ),
        m(
            'client.unsubscribe',
            <<"Number of client unsubscriptions">>
        ),
        m(
            'delivery.dropped',
            <<"Total number of discarded messages when sending">>
        ),
        m(
            'delivery.dropped.expired',
            <<"Number of messages dropped due to message expiration on sending">>
        ),
        m(
            'delivery.dropped.no_local',
            <<
                "Number of messages that were dropped due to the No Local subscription "
                "option when sending"
            >>
        ),
        m(
            'delivery.dropped.qos0_msg',
            <<
                "Number of messages with QoS 0 that were dropped because the message "
                "queue was full when sending"
            >>
        ),
        m(
            'delivery.dropped.queue_full',
            <<
                "Number of messages with a non-zero QoS that were dropped because the "
                "message queue was full when sending"
            >>
        ),
        m(
            'delivery.dropped.too_large',
            <<
                "The number of messages that were dropped because the length exceeded "
                "the limit when sending"
            >>
        ),
        m(
            'messages.acked',
            <<"Number of received PUBACK and PUBREC packet">>
        ),
        m(
            'messages.delayed',
            <<"Number of delay-published messages">>
        ),
        m(
            'messages.delivered',
            <<"Number of messages forwarded to the subscription process internally">>
        ),
        m(
            'messages.dropped',
            <<"Total number of messages dropped before forwarding to the subscription process">>
        ),
        m(
            'messages.dropped.await_pubrel_timeout',
            <<"Number of messages dropped due to waiting PUBREL timeout">>
        ),
        m(
            'messages.dropped.no_subscribers',
            <<"Number of messages dropped due to no subscribers">>
        ),
        m(
            'messages.forward',
            <<"Number of messages forwarded to other nodes">>
        ),
        m(
            'messages.publish',
            <<"Number of messages published in addition to system messages">>
        ),
        m(
            'messages.qos0.received',
            <<"Number of QoS 0 messages received from clients">>
        ),
        m(
            'messages.qos0.sent',
            <<"Number of QoS 0 messages sent to clients">>
        ),
        m(
            'messages.qos1.received',
            <<"Number of QoS 1 messages received from clients">>
        ),
        m(
            'messages.qos1.sent',
            <<"Number of QoS 1 messages sent to clients">>
        ),
        m(
            'messages.qos2.received',
            <<"Number of QoS 2 messages received from clients">>
        ),
        m(
            'messages.qos2.sent',
            <<"Number of QoS 2 messages sent to clients">>
        ),
        m(
            'messages.received',
            <<
                "Number of messages received from the client, equal to the sum of "
                "messages.qos0.received\fmessages.qos1.received and messages.qos2.received"
            >>
        ),
        m(
            'messages.retained',
            <<"Number of retained messages">>
        ),
        m(
            'messages.sent',
            <<
                "Number of messages sent to the client, equal to the sum of "
                "messages.qos0.sent\fmessages.qos1.sent and messages.qos2.sent"
            >>
        ),
        m(
            'packets.auth.received',
            <<"Number of received AUTH packet">>
        ),
        m(
            'packets.auth.sent',
            <<"Number of sent AUTH packet">>
        ),
        m(
            'packets.connack.auth_error',
            <<"Number of received CONNECT packet with failed authentication">>
        ),
        m(
            'packets.connack.error',
            <<"Number of received CONNECT packet with unsuccessful connections">>
        ),
        m(
            'packets.connack.sent',
            <<"Number of sent CONNACK packet">>
        ),
        m(
            'packets.connect.received',
            <<"Number of received CONNECT packet">>
        ),
        m(
            'packets.disconnect.received',
            <<"Number of received DISCONNECT packet">>
        ),
        m(
            'packets.disconnect.sent',
            <<"Number of sent DISCONNECT packet">>
        ),
        m(
            'packets.pingreq.received',
            <<"Number of received PINGREQ packet">>
        ),
        m(
            'packets.pingresp.sent',
            <<"Number of sent PUBRESP packet">>
        ),
        m(
            'packets.puback.inuse',
            <<"Number of received PUBACK packet with occupied identifiers">>
        ),
        m(
            'packets.puback.missed',
            <<"Number of received packet with identifiers.">>
        ),
        m(
            'packets.puback.received',
            <<"Number of received PUBACK packet">>
        ),
        m(
            'packets.puback.sent',
            <<"Number of sent PUBACK packet">>
        ),
        m(
            'packets.pubcomp.inuse',
            <<"Number of received PUBCOMP packet with occupied identifiers">>
        ),
        m(
            'packets.pubcomp.missed',
            <<"Number of missed PUBCOMP packet">>
        ),
        m(
            'packets.pubcomp.received',
            <<"Number of received PUBCOMP packet">>
        ),
        m(
            'packets.pubcomp.sent',
            <<"Number of sent PUBCOMP packet">>
        ),
        m(
            'packets.publish.auth_error',
            <<"Number of received PUBLISH packets with failed the Authorization check">>
        ),
        m(
            'packets.publish.dropped',
            <<"Number of messages discarded due to the receiving limit">>
        ),
        m(
            'packets.publish.error',
            <<"Number of received PUBLISH packet that cannot be published">>
        ),
        m(
            'packets.publish.inuse',
            <<"Number of received PUBLISH packet with occupied identifiers">>
        ),
        m(
            'packets.publish.received',
            <<"Number of received PUBLISH packet">>
        ),
        m(
            'packets.publish.sent',
            <<"Number of sent PUBLISH packet">>
        ),
        m(
            'packets.pubrec.inuse',
            <<"Number of received PUBREC packet with occupied identifiers">>
        ),
        m(
            'packets.pubrec.missed',
            <<"Number of received PUBREC packet with unknown identifiers">>
        ),
        m(
            'packets.pubrec.received',
            <<"Number of received PUBREC packet">>
        ),
        m(
            'packets.pubrec.sent',
            <<"Number of sent PUBREC packet">>
        ),
        m(
            'packets.pubrel.missed',
            <<"Number of received PUBREC packet with unknown identifiers">>
        ),
        m(
            'packets.pubrel.received',
            <<"Number of received PUBREL packet">>
        ),
        m(
            'packets.pubrel.sent',
            <<"Number of sent PUBREL packet">>
        ),
        m(
            'packets.received',
            <<"Number of received packet">>
        ),
        m(
            'packets.sent',
            <<"Number of sent packet">>
        ),
        m(
            'packets.suback.sent',
            <<"Number of sent SUBACK packet">>
        ),
        m(
            'packets.subscribe.auth_error',
            <<"Number of received SUBACK packet with failed Authorization check">>
        ),
        m(
            'packets.subscribe.error',
            <<"Number of received SUBSCRIBE packet with failed subscriptions">>
        ),
        m(
            'packets.subscribe.received',
            <<"Number of received SUBSCRIBE packet">>
        ),
        m(
            'packets.unsuback.sent',
            <<"Number of sent UNSUBACK packet">>
        ),
        m(
            'packets.unsubscribe.error',
            <<"Number of received UNSUBSCRIBE packet with failed unsubscriptions">>
        ),
        m(
            'packets.unsubscribe.received',
            <<"Number of received UNSUBSCRIBE packet">>
        ),
        m(
            'rules.matched',
            <<"Number of rule matched">>
        ),
        m(
            'session.created',
            <<"Number of sessions created">>
        ),
        m(
            'session.discarded',
            <<"Number of sessions dropped because Clean Session or Clean Start is true">>
        ),
        m(
            'session.resumed',
            <<"Number of sessions resumed because Clean Session or Clean Start is false">>
        ),
        m(
            'session.takenover',
            <<"Number of sessions takenover because Clean Session or Clean Start is false">>
        ),
        m(
            'session.terminated',
            <<"Number of terminated sessions">>
        )
    ].

m(K, Desc) ->
    {K, mk(non_neg_integer(), #{desc => Desc})}.
