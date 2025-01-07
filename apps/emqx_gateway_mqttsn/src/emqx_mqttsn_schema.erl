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

-module(emqx_mqttsn_schema).

-include("emqx_mqttsn.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> "gateway".

fields(mqttsn) ->
    [
        {gateway_id,
            sc(
                integer(),
                #{
                    default => 1,
                    required => true,
                    desc => ?DESC(mqttsn_gateway_id)
                }
            )},
        {broadcast,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqttsn_broadcast)
                }
            )},
        {enable_qos3,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqttsn_enable_qos3)
                }
            )},
        {subs_resume,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqttsn_subs_resume)
                }
            )},
        {predefined,
            sc(
                hoconsc:array(ref(mqttsn_predefined)),
                #{
                    default => [],
                    required => {false, recursively},
                    desc => ?DESC(mqttsn_predefined)
                }
            )},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(emqx_gateway_schema, udp_listeners), #{desc => ?DESC(udp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(mqttsn_predefined) ->
    [
        {id,
            sc(range(1, ?SN_MAX_PREDEF_TOPIC_ID), #{
                required => true,
                desc => ?DESC(mqttsn_predefined_id)
            })},

        {topic,
            sc(binary(), #{
                required => true,
                desc => ?DESC(mqttsn_predefined_topic)
            })}
    ].

desc(mqttsn) ->
    "The MQTT-SN (MQTT for Sensor Networks) protocol gateway.";
desc(mqttsn_predefined) ->
    "The pre-defined topic name corresponding to the pre-defined topic\n"
    "ID of N.\n\n"
    "Note: the pre-defined topic ID of 0 is reserved.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
