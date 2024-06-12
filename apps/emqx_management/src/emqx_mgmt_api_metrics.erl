%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("hocon/include/hocon_types.hrl").

-import(hoconsc, [mk/2, ref/2]).

%% minirest/dashboard_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
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

namespace() -> undefined.

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
             || Node <- emqx:running_nodes()
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
                description => ?DESC(emqx_metrics),
                tags => [<<"Metrics">>],
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
    Metrics = lists:append([
        ?BYTES_METRICS,
        ?PACKET_METRICS,
        ?MESSAGE_METRICS,
        ?DELIVERY_METRICS,
        ?CLIENT_METRICS,
        ?SESSION_METRICS,
        ?STASTS_ACL_METRICS,
        ?STASTS_AUTHN_METRICS,
        ?OLP_METRICS
    ]),
    lists:reverse(
        lists:foldl(
            fun({_Type, MetricName}, Acc) ->
                [m(MetricName) | Acc]
            end,
            [],
            Metrics
        )
    ).

m('actions.failure' = K) ->
    m(K, <<"Number of failure executions of the rule engine action">>);
m('actions.success' = K) ->
    m(K, <<"Number of successful executions of the rule engine action">>);
m('bytes.received' = K) ->
    m(K, <<"Number of bytes received ">>);
m('bytes.sent' = K) ->
    m(K, <<"Number of bytes sent on this connection">>);
m('client.auth.anonymous' = K) ->
    m(K, <<"Number of clients who log in anonymously">>);
m('client.authenticate' = K) ->
    m(K, <<"Number of client authentications">>);
m('client.check_authz' = K) ->
    m(K, <<"Number of Authorization rule checks">>);
m('client.connack' = K) ->
    m(K, <<"Number of CONNACK packet sent">>);
m('client.connect' = K) ->
    m(K, <<"Number of client connections">>);
m('client.connected' = K) ->
    m(K, <<"Number of successful client connections">>);
m('client.disconnected' = K) ->
    m(K, <<"Number of client disconnects">>);
m('client.subscribe' = K) ->
    m(K, <<"Number of client subscriptions">>);
m('client.unsubscribe' = K) ->
    m(K, <<"Number of client unsubscriptions">>);
m('delivery.dropped' = K) ->
    m(K, <<"Total number of discarded messages when sending">>);
m('delivery.dropped.expired' = K) ->
    m(K, <<"Number of messages dropped due to message expiration on sending">>);
m('delivery.dropped.no_local' = K) ->
    m(K, <<
        "Number of messages that were dropped due to the No Local subscription "
        "option when sending"
    >>);
m('delivery.dropped.qos0_msg' = K) ->
    m(K, <<
        "Number of messages with QoS 0 that were dropped because the message "
        "queue was full when sending"
    >>);
m('delivery.dropped.queue_full' = K) ->
    m(K, <<
        "Number of messages with a non-zero QoS that were dropped because the "
        "message queue was full when sending"
    >>);
m('delivery.dropped.too_large' = K) ->
    m(K, <<
        "The number of messages that were dropped because the length exceeded "
        "the limit when sending"
    >>);
m('messages.acked' = K) ->
    m(K, <<"Number of received PUBACK and PUBREC packet">>);
m('messages.delayed' = K) ->
    m(K, <<"Number of delay-published messages">>);
m('messages.delivered' = K) ->
    m(K, <<"Number of messages forwarded to the subscription process internally">>);
m('messages.dropped' = K) ->
    m(K, <<"Total number of messages dropped before forwarding to the subscription process">>);
m('messages.dropped.await_pubrel_timeout' = K) ->
    m(K, <<"Number of messages dropped due to waiting PUBREL timeout">>);
m('messages.dropped.no_subscribers' = K) ->
    m(K, <<"Number of messages dropped due to no subscribers">>);
m('messages.forward' = K) ->
    m(K, <<"Number of messages forwarded to other nodes">>);
m('messages.publish' = K) ->
    m(K, <<"Number of messages published in addition to system messages">>);
m('messages.qos0.received' = K) ->
    m(K, <<"Number of QoS 0 messages received from clients">>);
m('messages.qos0.sent' = K) ->
    m(K, <<"Number of QoS 0 messages sent to clients">>);
m('messages.qos1.received' = K) ->
    m(K, <<"Number of QoS 1 messages received from clients">>);
m('messages.qos1.sent' = K) ->
    m(K, <<"Number of QoS 1 messages sent to clients">>);
m('messages.qos2.received' = K) ->
    m(K, <<"Number of QoS 2 messages received from clients">>);
m('messages.qos2.sent' = K) ->
    m(K, <<"Number of QoS 2 messages sent to clients">>);
m('messages.received' = K) ->
    m(K, <<
        "Number of messages received from the client, equal to the sum of "
        "messages.qos0.received\fmessages.qos1.received and messages.qos2.received"
    >>);
%% m(
%%     'messages.retained',
%%     <<"Number of retained messages">>
%% ),
m('messages.sent' = K) ->
    m(K, <<
        "Number of messages sent to the client, equal to the sum of "
        "messages.qos0.sent\fmessages.qos1.sent and messages.qos2.sent"
    >>);
m('packets.auth.received' = K) ->
    m(K, <<"Number of received AUTH packet">>);
m('packets.auth.sent' = K) ->
    m(K, <<"Number of sent AUTH packet">>);
m('packets.connack.auth_error' = K) ->
    m(K, <<"Number of received CONNECT packet with failed authentication">>);
m('packets.connack.error' = K) ->
    m(K, <<"Number of received CONNECT packet with unsuccessful connections">>);
m('packets.connack.sent' = K) ->
    m(K, <<"Number of sent CONNACK packet">>);
m('packets.connect.received' = K) ->
    m(K, <<"Number of received CONNECT packet">>);
m('packets.disconnect.received' = K) ->
    m(K, <<"Number of received DISCONNECT packet">>);
m('packets.disconnect.sent' = K) ->
    m(K, <<"Number of sent DISCONNECT packet">>);
m('packets.pingreq.received' = K) ->
    m(K, <<"Number of received PINGREQ packet">>);
m('packets.pingresp.sent' = K) ->
    m(K, <<"Number of sent PUBRESP packet">>);
m('packets.puback.inuse' = K) ->
    m(K, <<"Number of received PUBACK packet with occupied identifiers">>);
m('packets.puback.missed' = K) ->
    m(K, <<"Number of received packet with identifiers.">>);
m('packets.puback.received' = K) ->
    m(K, <<"Number of received PUBACK packet">>);
m('packets.puback.sent' = K) ->
    m(K, <<"Number of sent PUBACK packet">>);
m('packets.pubcomp.inuse' = K) ->
    m(K, <<"Number of received PUBCOMP packet with occupied identifiers">>);
m('packets.pubcomp.missed' = K) ->
    m(K, <<"Number of missed PUBCOMP packet">>);
m('packets.pubcomp.received' = K) ->
    m(K, <<"Number of received PUBCOMP packet">>);
m('packets.pubcomp.sent' = K) ->
    m(K, <<"Number of sent PUBCOMP packet">>);
m('packets.publish.auth_error' = K) ->
    m(K, <<"Number of received PUBLISH packets with failed the Authorization check">>);
m('packets.publish.dropped' = K) ->
    m(K, <<"Number of messages discarded due to the receiving limit">>);
m('packets.publish.error' = K) ->
    m(K, <<"Number of received PUBLISH packet that cannot be published">>);
m('packets.publish.inuse' = K) ->
    m(K, <<"Number of received PUBLISH packet with occupied identifiers">>);
m('packets.publish.received' = K) ->
    m(K, <<"Number of received PUBLISH packet">>);
m('packets.publish.sent' = K) ->
    m(K, <<"Number of sent PUBLISH packet">>);
m('packets.pubrec.inuse' = K) ->
    m(K, <<"Number of received PUBREC packet with occupied identifiers">>);
m('packets.pubrec.missed' = K) ->
    m(K, <<"Number of received PUBREC packet with unknown identifiers">>);
m('packets.pubrec.received' = K) ->
    m(K, <<"Number of received PUBREC packet">>);
m('packets.pubrec.sent' = K) ->
    m(K, <<"Number of sent PUBREC packet">>);
m('packets.pubrel.missed' = K) ->
    m(K, <<"Number of received PUBREC packet with unknown identifiers">>);
m('packets.pubrel.received' = K) ->
    m(K, <<"Number of received PUBREL packet">>);
m('packets.pubrel.sent' = K) ->
    m(K, <<"Number of sent PUBREL packet">>);
m('packets.received' = K) ->
    m(K, <<"Number of received packet">>);
m('packets.sent' = K) ->
    m(K, <<"Number of sent packet">>);
m('packets.suback.sent' = K) ->
    m(K, <<"Number of sent SUBACK packet">>);
m('packets.subscribe.auth_error' = K) ->
    m(K, <<"Number of received SUBACK packet with failed Authorization check">>);
m('packets.subscribe.error' = K) ->
    m(K, <<"Number of received SUBSCRIBE packet with failed subscriptions">>);
m('packets.subscribe.received' = K) ->
    m(K, <<"Number of received SUBSCRIBE packet">>);
m('packets.unsuback.sent' = K) ->
    m(K, <<"Number of sent UNSUBACK packet">>);
m('packets.unsubscribe.error' = K) ->
    m(K, <<"Number of received UNSUBSCRIBE packet with failed unsubscriptions">>);
m('packets.unsubscribe.received' = K) ->
    m(K, <<"Number of received UNSUBSCRIBE packet">>);
m('rules.matched' = K) ->
    m(K, <<"Number of rule matched">>);
m('session.created' = K) ->
    m(K, <<"Number of sessions created">>);
m('session.discarded' = K) ->
    m(K, <<"Number of sessions dropped because Clean Session or Clean Start is true">>);
m('session.resumed' = K) ->
    m(K, <<"Number of sessions resumed because Clean Session or Clean Start is false">>);
m('session.takenover' = K) ->
    m(K, <<"Number of sessions takenover because Clean Session or Clean Start is false">>);
m('session.terminated' = K) ->
    m(K, <<"Number of terminated sessions">>).

m(K, Desc) ->
    {K, mk(non_neg_integer(), #{desc => Desc})}.
