%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> "gateway".

fields(coap) ->
    [
        {heartbeat,
            sc(
                emqx_schema:duration_s(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(coap_heartbeat)
                }
            )},
        {connection_required,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(coap_connection_required)
                }
            )},
        {notify_type,
            sc(
                hoconsc:enum([non, con, qos]),
                #{
                    default => qos,
                    desc => ?DESC(coap_notify_type)
                }
            )},
        {subscribe_qos,
            sc(
                hoconsc:enum([qos0, qos1, qos2, coap]),
                #{
                    default => coap,
                    desc => ?DESC(coap_subscribe_qos)
                }
            )},
        {publish_qos,
            sc(
                hoconsc:enum([qos0, qos1, qos2, coap]),
                #{
                    default => coap,
                    desc => ?DESC(coap_publish_qos)
                }
            )},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners,
            sc(
                ref(emqx_gateway_schema, udp_listeners),
                #{desc => ?DESC(udp_listeners)}
            )}
    ] ++ emqx_gateway_schema:gateway_common_options().

desc(coap) ->
    "The CoAP protocol gateway provides EMQX with the access capability of the CoAP protocol.\n"
    "It allows publishing, subscribing, and receiving messages to EMQX in accordance\n"
    "with a certain defined CoAP message format.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% helpers

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
