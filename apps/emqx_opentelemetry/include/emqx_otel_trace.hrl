%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_OTEL_TRACE_HRL).
-define(EMQX_OTEL_TRACE_HRL, true).

%% ====================
%% Root Spans
-define(CLIENT_CONNECT_SPAN_NAME, 'client.connect').
-define(CLIENT_DISCONNECT_SPAN_NAME, 'client.disconnect').
-define(CLIENT_SUBSCRIBE_SPAN_NAME, 'client.subscribe').
-define(CLIENT_UNSUBSCRIBE_SPAN_NAME, 'client.unsubscribe').
-define(CLIENT_PUBLISH_SPAN_NAME, 'client.publish').

-define(BROKER_DISCONNECT_SPAN_NAME, 'broker.disconnect').
-define(BROKER_SUBSCRIBE_SPAN_NAME, 'broker.subscribe').
-define(BROKER_UNSUBSCRIBE_SPAN_NAME, 'broker.unsubscribe').

-define(CLIENT_AUTHN_SPAN_NAME, 'client.authn').
-define(CLIENT_AUTHZ_SPAN_NAME, 'client.authz').

-define(BROKER_PUBLISH_SPAN_NAME, 'broker.publish').
-define(BROKER_PUBACK_SPAN_NAME, 'broker.puback').
-define(BROKER_PUBREC_SPAN_NAME, 'broker.pubrec').
-define(BROKER_PUBREL_SPAN_NAME, 'broker.pubrel').
-define(BROKER_PUBCOMP_SPAN_NAME, 'broker.pubcomp').

-define(CLIENT_PUBACK_SPAN_NAME, 'client.puback').
-define(CLIENT_PUBREC_SPAN_NAME, 'client.pubrec').
-define(CLIENT_PUBREL_SPAN_NAME, 'client.pubrel').
-define(CLIENT_PUBCOMP_SPAN_NAME, 'client.pubcomp').

-define(MSG_ROUTE_SPAN_NAME, 'message.route').
-define(MSG_FORWARD_SPAN_NAME, 'message.forward').
-define(MSG_HANDLE_FORWARD_SPAN_NAME, 'message.handle_forward').

%% ====================
%% OTEL sample whitelist/blacklist Table
-define(EMQX_OTEL_SAMPLER, emqx_otel_sampler).
-define(EMQX_OTEL_SAMPLER_SHARD, emqx_otel_sampler_shard).

-define(EMQX_OTEL_SAMPLE_CLIENTID, 1).
-define(EMQX_OTEL_SAMPLE_TOPIC, 2).

-define(EMQX_OTEL_DEFAULT_CLUSTER_ID, <<"emqxcl">>).

-record(?EMQX_OTEL_SAMPLER, {
    type ::
        {?EMQX_OTEL_SAMPLE_CLIENTID, binary() | '_'}
        | {?EMQX_OTEL_SAMPLE_TOPIC, binary() | '_'},
    extra :: map() | '_'
}).

-endif.
