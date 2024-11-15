%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_OTEL_TRACE_HRL).
-define(EMQX_OTEL_TRACE_HRL, true).

%% ====================
%% Root Spans
-define(CLIENT_CONNECT_SPAN_NAME, 'client.connect').
-define(CLIENT_DISCONNECT_SPAN_NAME, 'client.disconnect').
-define(CLIENT_SUBSCRIBE_SPAN_NAME, 'client.subscribe').
-define(CLIENT_UNSUBSCRIBE_SPAN_NAME, 'client.unsubscribe').
-define(CLIENT_PUBLISH_SPAN_NAME, 'client.publish').

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

-define(EMQX_OTEL_SAMPLE_CLIENTID, 1).
-define(EMQX_OTEL_SAMPLE_TOPIC, 2).

-record(?EMQX_OTEL_SAMPLER, {
    type ::
        {?EMQX_OTEL_SAMPLE_CLIENTID, binary()}
        | {?EMQX_OTEL_SAMPLE_TOPIC, binary()},
    extra :: map()
}).

-endif.
