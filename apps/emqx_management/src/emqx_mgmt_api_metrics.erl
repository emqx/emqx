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

-export([api_spec/0]).

-export([list/2]).

api_spec() ->
    {[metrics_api()], [metrics_schema()]}.

metrics_schema() ->
    Metric = #{
        type => object,
        properties => emqx_mgmt_util:properties(properties())
    },
    Metrics = #{
        type => array,
        items => #{
            type => object,
            properties => emqx_mgmt_util:properties([{node, string} | properties()])
        }
    },
    MetricsInfo = #{
        oneOf => [ minirest:ref(metric)
                 , minirest:ref(metrics)
                 ]
    },
    #{metric => Metric, metrics => Metrics, metrics_info => MetricsInfo}.

properties() ->
    [
        {'actions.failure',                 integer, <<"Number of failure executions of the rule engine action">>},
        {'actions.success',                 integer, <<"Number of successful executions of the rule engine action">>},
        {'bytes.received',                  integer, <<"Number of bytes received by EMQ X Broker">>},
        {'bytes.sent',                      integer, <<"Number of bytes sent by EMQ X Broker on this connection">>},
        {'client.auth.anonymous',           integer, <<"Number of clients who log in anonymously">>},
        {'client.authenticate',             integer, <<"Number of client authentications">>},
        {'client.check_authz',              integer, <<"Number of Authorization rule checks">>},
        {'client.connack',                  integer, <<"Number of CONNACK packet sent">>},
        {'client.connect',                  integer, <<"Number of client connections">>},
        {'client.connected',                integer, <<"Number of successful client connections">>},
        {'client.disconnected',             integer, <<"Number of client disconnects">>},
        {'client.subscribe',                integer, <<"Number of client subscriptions">>},
        {'client.unsubscribe',              integer, <<"Number of client unsubscriptions">>},
        {'delivery.dropped',                integer, <<"Total number of discarded messages when sending">>},
        {'delivery.dropped.expired',        integer, <<"Number of messages dropped due to message expiration on sending">>},
        {'delivery.dropped.no_local',       integer, <<"Number of messages that were dropped due to the No Local subscription option when sending">>},
        {'delivery.dropped.qos0_msg',       integer, <<"Number of messages with QoS 0 that were dropped because the message queue was full when sending">>},
        {'delivery.dropped.queue_full',     integer, <<"Number of messages with a non-zero QoS that were dropped because the message queue was full when sending">>},
        {'delivery.dropped.too_large',      integer, <<"The number of messages that were dropped because the length exceeded the limit when sending">>},
        {'messages.acked',                  integer, <<"Number of received PUBACK and PUBREC packet">>},
        {'messages.delayed',                integer, <<"Number of delay- published messages stored by EMQ X Broker">>},
        {'messages.delivered',              integer, <<"Number of messages forwarded to the subscription process internally by EMQ X Broker">>},
        {'messages.dropped',                integer, <<"Total number of messages dropped by EMQ X Broker before forwarding to the subscription process">>},
        {'messages.dropped.expired',        integer, <<"Number of messages dropped due to message expiration when receiving">>},
        {'messages.dropped.no_subscribers', integer, <<"Number of messages dropped due to no subscribers">>},
        {'messages.forward',                integer, <<"Number of messages forwarded to other nodes">>},
        {'messages.publish',                integer, <<"Number of messages published in addition to system messages">>},
        {'messages.qos0.received',          integer, <<"Number of QoS 0 messages received from clients">>},
        {'messages.qos0.sent',              integer, <<"Number of QoS 0 messages sent to clients">>},
        {'messages.qos1.received',          integer, <<"Number of QoS 1 messages received from clients">>},
        {'messages.qos1.sent',              integer, <<"Number of QoS 1 messages sent to clients">>},
        {'messages.qos2.received',          integer, <<"Number of QoS 2 messages received from clients">>},
        {'messages.qos2.sent',              integer, <<"Number of QoS 2 messages sent to clients">>},
        {'messages.received',               integer, <<"Number of messages received from the client, equal to the sum of messages.qos0.received\fmessages.qos1.received and messages.qos2.received">>},
        {'messages.retained',               integer, <<"Number of retained messages stored by EMQ X Broker">>},
        {'messages.sent',                   integer, <<"Number of messages sent to the client, equal to the sum of messages.qos0.sent\fmessages.qos1.sent and messages.qos2.sent">>},
        {'packets.auth.received',           integer, <<"Number of received AUTH packet">>},
        {'packets.auth.sent',               integer, <<"Number of sent AUTH packet">>},
        {'packets.connack.auth_error',      integer, <<"Number of received CONNECT packet with failed authentication">>},
        {'packets.connack.error',           integer, <<"Number of received CONNECT packet with unsuccessful connections">>},
        {'packets.connack.sent',            integer, <<"Number of sent CONNACK packet">>},
        {'packets.connect.received',        integer, <<"Number of received CONNECT packet">>},
        {'packets.disconnect.received',     integer, <<"Number of received DISCONNECT packet">>},
        {'packets.disconnect.sent',         integer, <<"Number of sent DISCONNECT packet">>},
        {'packets.pingreq.received',        integer, <<"Number of received PINGREQ packet">>},
        {'packets.pingresp.sent',           integer, <<"Number of sent PUBRESP packet">>},
        {'packets.puback.inuse',            integer, <<"Number of received PUBACK packet with occupied identifiers">>},
        {'packets.puback.missed',           integer, <<"Number of received packet with identifiers.">>},
        {'packets.puback.received',         integer, <<"Number of received PUBACK packet">>},
        {'packets.puback.sent',             integer, <<"Number of sent PUBACK packet">>},
        {'packets.pubcomp.inuse',           integer, <<"Number of received PUBCOMP packet with occupied identifiers">>},
        {'packets.pubcomp.missed',          integer, <<"Number of missed PUBCOMP packet">>},
        {'packets.pubcomp.received',        integer, <<"Number of received PUBCOMP packet">>},
        {'packets.pubcomp.sent',            integer, <<"Number of sent PUBCOMP packet">>},
        {'packets.publish.auth_error',      integer, <<"Number of received PUBLISH packets with failed the Authorization check">>},
        {'packets.publish.dropped',         integer, <<"Number of messages discarded due to the receiving limit">>},
        {'packets.publish.error',           integer, <<"Number of received PUBLISH packet that cannot be published">>},
        {'packets.publish.inuse',           integer, <<"Number of received PUBLISH packet with occupied identifiers">>},
        {'packets.publish.received',        integer, <<"Number of received PUBLISH packet">>},
        {'packets.publish.sent',            integer, <<"Number of sent PUBLISH packet">>},
        {'packets.pubrec.inuse',            integer, <<"Number of received PUBREC packet with occupied identifiers">>},
        {'packets.pubrec.missed',           integer, <<"Number of received PUBREC packet with unknown identifiers">>},
        {'packets.pubrec.received',         integer, <<"Number of received PUBREC packet">>},
        {'packets.pubrec.sent',             integer, <<"Number of sent PUBREC packet">>},
        {'packets.pubrel.missed',           integer, <<"Number of received PUBREC packet with unknown identifiers">>},
        {'packets.pubrel.received',         integer, <<"Number of received PUBREL packet">>},
        {'packets.pubrel.sent',             integer, <<"Number of sent PUBREL packet">>},
        {'packets.received',                integer, <<"Number of received packet">>},
        {'packets.sent',                    integer, <<"Number of sent packet">>},
        {'packets.suback.sent',             integer, <<"Number of sent SUBACK packet">>},
        {'packets.subscribe.auth_error',    integer, <<"Number of received SUBACK packet with failed Authorization check">>},
        {'packets.subscribe.error',         integer, <<"Number of received SUBSCRIBE packet with failed subscriptions">>},
        {'packets.subscribe.received',      integer, <<"Number of received SUBSCRIBE packet">>},
        {'packets.unsuback.sent',           integer, <<"Number of sent UNSUBACK packet">>},
        {'packets.unsubscribe.error',       integer, <<"Number of received UNSUBSCRIBE packet with failed unsubscriptions">>},
        {'packets.unsubscribe.received',    integer, <<"Number of received UNSUBSCRIBE packet">>},
        {'rules.matched',                   integer, <<"Number of rule matched">>},
        {'session.created',                 integer, <<"Number of sessions created">>},
        {'session.discarded',               integer, <<"Number of sessions dropped because Clean Session or Clean Start is true">>},
        {'session.resumed',                 integer, <<"Number of sessions resumed because Clean Session or Clean Start is false">>},
        {'session.takenover',               integer, <<"Number of sessions takenover because Clean Session or Clean Start is false">>},
        {'session.terminated',              integer, <<"Number of terminated sessions">>}
    ].

metrics_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X metrics">>,
            parameters => [#{
                name => aggregate,
                in => query,
                schema => #{type => boolean}
            }],
            responses => #{
                <<"200">> => emqx_mgmt_util:schema(metrics_info, <<"List all metrics">>)
            }
        }
    },
    {"/metrics", Metadata, list}.

%%%==============================================================================================
%% api apply
list(get, #{query_string := Qs}) ->
    case maps:get(<<"aggregate">>, Qs, undefined) of
        <<"true">> ->
            {200, emqx_mgmt:get_metrics()};
        _ ->
            Data = [maps:from_list(emqx_mgmt:get_metrics(Node) ++ [{node, Node}]) ||
                        Node <- mria_mnesia:running_nodes()],
            {200, Data}
    end.
