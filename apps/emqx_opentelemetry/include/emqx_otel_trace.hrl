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

-define(CLIENT_CONNECT_SPAN_NAME, <<"client.connect">>).
-define(CLIENT_DISCONNECT_SPAN_NAME, <<"client.disconnect">>).
-define(CLIENT_AUTHN_SPAN_NAME, <<"client.authn">>).
-define(CLIENT_AUTHZ_SPAN_NAME, <<"client.authz">>).
-define(CLIENT_SUBSCRIBE_SPAN_NAME, <<"client.subscribe">>).
-define(CLIENT_UNSUBSCRIBE_SPAN_NAME, <<"client.unsubscribe">>).

-define(EMQX_PUBACK_SPAN_NAME, <<"emqx.puback">>).
-define(EMQX_PUBREC_SPAN_NAME, <<"emqx.pubrec">>).
-define(EMQX_PUBREL_SPAN_NAME, <<"emqx.pubrel">>).
-define(EMQX_PUBCOMP_SPAN_NAME, <<"emqx.pubcomp">>).

-define(CLIENT_PUBLISH_SPAN_NAME, <<"client.publish">>).
-define(CLIENT_PUBACK_SPAN_NAME, <<"client.puback">>).
-define(CLIENT_PUBREC_SPAN_NAME, <<"client.pubrec">>).
-define(CLIENT_PUBREL_SPAN_NAME, <<"client.pubrel">>).
-define(CLIENT_PUBCOMP_SPAN_NAME, <<"client.pubcomp">>).

-define(MSG_ROUTE_SPAN_NAME, <<"message.route">>).
-define(MSG_DISPATCH_SPAN_NAME, <<"message.dispatch">>).
-define(MSG_FORWARD_SPAN_NAME, <<"message.forward">>).
-define(MSG_HANDLE_FORWARD_SPAN_NAME, <<"message.handle_forward">>).
-define(MSG_DELIVER_SPAN_NAME, <<"message.deliver">>).

-endif.
