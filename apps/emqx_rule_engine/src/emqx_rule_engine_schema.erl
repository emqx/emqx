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

-module(emqx_rule_engine_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).

-export([ namespace/0
        , roots/0
        , fields/1
        , desc/1
        ]).

-export([ validate_sql/1
        ]).

namespace() -> rule_engine.

roots() -> ["rule_engine"].

fields("rule_engine") ->
    [ {ignore_sys_message, sc(boolean(), #{default => true, desc => ?DESC("rule_engine_ignore_sys_message")
    })}
    , {rules, sc(hoconsc:map("id", ref("rules")), #{desc => ?DESC("rule_engine_rules"), default => #{}})}
    ];

fields("rules") ->
    [ rule_name()
    , {"sql", sc(binary(),
        #{ desc => ?DESC("rules_sql")
         , example => "SELECT * FROM \"test/topic\" WHERE payload.x = 1"
         , required => true
         , validator => fun ?MODULE:validate_sql/1
         })}
    , {"outputs", sc(hoconsc:array(hoconsc:union(outputs())),
        #{ desc => ?DESC("rules_outputs")
        , default => []
        , example => [
            <<"http:my_http_bridge">>,
            #{function => republish, args => #{
                topic => <<"t/1">>, payload => <<"${payload}">>}},
            #{function => console}
          ]
        })}
    , {"enable", sc(boolean(), #{desc => ?DESC("rules_enable"), default => true})}
    , {"description", sc(binary(),
        #{ desc => ?DESC("rules_description")
         , example => "Some description"
         , default => <<>>
         })}
    ];

fields("builtin_output_republish") ->
    [ {function, sc(republish, #{desc => ?DESC("republish_function")})}
    , {args, sc(ref("republish_args"), #{default => #{}})}
    ];

fields("builtin_output_console") ->
    [ {function, sc(console, #{desc => ?DESC("console_function")})}
    %% we may support some args for the console output in the future
    %, {args, sc(map(), #{desc => "The arguments of the built-in 'console' output",
    %    default => #{}})}
    ];

fields("user_provided_function") ->
    [ {function, sc(binary(),
        #{ desc => ?DESC("user_provided_function_function")
        , example => "module:function"
        })}
    , {args, sc(map(),
        #{ desc => ?DESC("user_provided_function_args")
         , default => #{}
         })}
    ];

fields("republish_args") ->
    [ {topic, sc(binary(),
        #{ desc => ?DESC("republish_args_topic")
          , required => true
          , example => <<"a/1">>
          })}
    , {qos, sc(qos(),
        #{ desc => ?DESC("republish_args_qos")
         , default => <<"${qos}">>
         , example => <<"${qos}">>
         })}
    , {retain, sc(hoconsc:union([binary(), boolean()]),
        #{ desc => ?DESC("republish_args_retain")
        , default => <<"${retain}">>
        , example => <<"${retain}">>
        })}
    , {payload, sc(binary(),
        #{ desc => ?DESC("republish_args_payload")
         , default => <<"${payload}">>
         , example => <<"${payload}">>
         })}
    ].

desc("rule_engine") ->
    "Configuration for the EMQX Rule Engine.";
desc("rules") ->
    "Configuration for a rule.";
desc("builtin_output_republish") ->
    "Configuration for a built-in output.";
desc("builtin_output_console") ->
    "Configuration for a built-in output.";
desc("user_provided_function") ->
    "Configuration for a built-in output.";
desc("republish_args") ->
    "The arguments of the built-in 'republish' output.<br>"
    "One can use variables in the args.<br>\n"
    "The variables are selected by the rule. For example, if the rule SQL is defined as following:\n"
    "<code>\n"
    "    SELECT clientid, qos, payload FROM \"t/1\"\n"
    "</code>\n"
    "Then there are 3 variables available: <code>clientid</code>, <code>qos</code> and\n"
    "<code>payload</code>. And if we've set the args to:\n"
    "<code>\n"
    "    {\n"
    "        topic = \"t/${clientid}\"\n"
    "        qos = \"${qos}\"\n"
    "        payload = \"msg: ${payload}\"\n"
    "    }\n"
    "</code>\n"
    "When the rule is triggered by an MQTT message with payload = `hello`, qos = 1,\n"
    "clientid = `Steve`, the rule will republish a new MQTT message to topic `t/Steve`,\n"
    "payload = `msg: hello`, and `qos = 1`.";
desc(_) ->
    undefined.

rule_name() ->
    {"name", sc(binary(),
        #{ desc => ?DESC("rules_name")
         , default => ""
         , required => true
         , example => "foo"
         })}.

outputs() ->
    [ binary()
    , ref("builtin_output_republish")
    , ref("builtin_output_console")
    , ref("user_provided_function")
    ].

qos() ->
    hoconsc:union([emqx_schema:qos(), binary()]).

validate_sql(Sql) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, _Result} -> ok;
        {error, Reason} -> {error, Reason}
    end.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
