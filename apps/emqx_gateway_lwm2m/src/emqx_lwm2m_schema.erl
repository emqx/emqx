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

-module(emqx_lwm2m_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-type duration() :: non_neg_integer().
-type duration_s() :: non_neg_integer().

-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).

-reflect_type([duration/0, duration_s/0]).

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> gateway.

fields(lwm2m) ->
    [
        {xml_dir,
            sc(
                binary(),
                #{
                    %% since this is not packaged with emqx, nor
                    %% present in the packages, we must let the user
                    %% specify it rather than creating a dynamic
                    %% default (especially difficult to handle when
                    %% generating docs).
                    example => <<"/etc/emqx/lwm2m_xml">>,
                    required => true,
                    desc => ?DESC(lwm2m_xml_dir)
                }
            )},
        {lifetime_min,
            sc(
                duration(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(lwm2m_lifetime_min)
                }
            )},
        {lifetime_max,
            sc(
                duration(),
                #{
                    default => <<"86400s">>,
                    desc => ?DESC(lwm2m_lifetime_max)
                }
            )},
        {qmode_time_window,
            sc(
                duration_s(),
                #{
                    default => <<"22s">>,
                    desc => ?DESC(lwm2m_qmode_time_window)
                }
            )},
        %% TODO: Support config resource path
        {auto_observe,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(lwm2m_auto_observe)
                }
            )},
        %% FIXME: not working now
        {update_msg_publish_condition,
            sc(
                hoconsc:enum([always, contains_object_list]),
                #{
                    default => contains_object_list,
                    desc => ?DESC(lwm2m_update_msg_publish_condition)
                }
            )},
        {translators,
            sc(
                ref(lwm2m_translators),
                #{
                    required => true,
                    desc => ?DESC(lwm2m_translators)
                }
            )},
        {mountpoint, emqx_gateway_schema:mountpoint("lwm2m/${endpoint_name}/")},
        {listeners, sc(ref(emqx_gateway_schema, udp_listeners), #{desc => ?DESC(udp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(lwm2m_translators) ->
    [
        {command,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_command),
                    required => true
                }
            )},
        {response,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_response),
                    required => true
                }
            )},
        {notify,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_notify),
                    required => true
                }
            )},
        {register,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_register),
                    required => true
                }
            )},
        {update,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_update),
                    required => true
                }
            )}
    ];
fields(translator) ->
    [
        {topic,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(translator_topic)
                }
            )},
        {qos,
            sc(
                emqx_schema:qos(),
                #{
                    default => 0,
                    desc => ?DESC(translator_qos)
                }
            )}
    ].

desc(lwm2m) ->
    "The LwM2M protocol gateway.";
desc(lwm2m_translators) ->
    "MQTT topics that correspond to LwM2M events.";
desc(translator) ->
    "MQTT topic that corresponds to a particular type of event.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% helpers

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
