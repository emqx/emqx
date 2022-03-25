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
-module(emqx_auto_subscribe_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-export([ namespace/0
        , roots/0
        , fields/1]).

namespace() -> "auto_subscribe".

roots() ->
    ["auto_subscribe"].

fields("auto_subscribe") ->
    [ {topics, hoconsc:array(hoconsc:ref(?MODULE, "topic"))}
    ];

fields("topic") ->
    [ {topic, sc(binary(), #{
        example => topic_example(),
        desc => "Topic name, placeholders is supported. For example: "
                ++ binary_to_list(topic_example())})}
    , {qos, sc(emqx_schema:qos(), #{
        default => 0,
        desc => "Quality of service. MQTT definition."})}
    , {rh, sc(range(0,2), #{
        default => 0,
        desc => "Retain handling. MQTT 5.0 definition."})}
    , {rap, sc(range(0, 1), #{
        default => 0,
        desc => "Retain as Published. MQTT 5.0 definition."})}
    , {nl, sc(range(0, 1), #{
        default => 0,
        desc => "Not local. MQTT 5.0 definition."})}
    ].

topic_example() ->
    <<"/clientid/", ?PH_S_CLIENTID,
      "/username/", ?PH_S_USERNAME,
      "/host/", ?PH_S_HOST,
      "/port/", ?PH_S_PORT>>.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).
