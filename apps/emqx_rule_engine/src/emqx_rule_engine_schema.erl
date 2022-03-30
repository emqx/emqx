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
    [ {ignore_sys_message, sc(boolean(), #{default => true, desc =>
"When set to 'true' (default), rule-engine will ignore messages published to $SYS topics."
    })}
    , {rules, sc(hoconsc:map("id", ref("rules")), #{desc => "The rules", default => #{}})}
    ];

fields("rules") ->
    [ rule_name()
    , {"sql", sc(binary(),
        #{ desc => """
SQL query to transform the messages.<br>
Example: <code>SELECT * FROM \"test/topic\" WHERE payload.x = 1</code><br>
"""
         , example => "SELECT * FROM \"test/topic\" WHERE payload.x = 1"
         , required => true
         , validator => fun ?MODULE:validate_sql/1
         })}
    , {"outputs", sc(hoconsc:array(hoconsc:union(outputs())),
        #{ desc => """
A list of outputs of the rule.<br>
An output can be a string that refers to the channel ID of an EMQX bridge, or an object
that refers to a function.<br>
There a some built-in functions like \"republish\" and \"console\", and we also support user
provided functions in the format: \"{module}:{function}\".<br>
The outputs in the list are executed sequentially.
This means that if one of the output is executing slowly, all the following outputs will not
be executed until it returns.<br>
If one of the output crashed, all other outputs come after it will still be executed, in the
original order.<br>
If there's any error when running an output, there will be an error message, and the 'failure'
counter of the function output or the bridge channel will increase.
"""
        , default => []
        , example => [
            <<"http:my_http_bridge">>,
            #{function => republish, args => #{
                topic => <<"t/1">>, payload => <<"${payload}">>}},
            #{function => console}
          ]
        })}
    , {"enable", sc(boolean(), #{desc => "Enable or disable the rule", default => true})}
    , {"description", sc(binary(),
        #{ desc => "The description of the rule"
         , example => "Some description"
         , default => <<>>
         })}
    ];

fields("builtin_output_republish") ->
    [ {function, sc(republish, #{desc => "Republish the message as a new MQTT message"})}
    , {args, sc(ref("republish_args"), #{default => #{}})}
    ];

fields("builtin_output_console") ->
    [ {function, sc(console, #{desc => "Print the outputs to the console"})}
    %% we may support some args for the console output in the future
    %, {args, sc(map(), #{desc => "The arguments of the built-in 'console' output",
    %    default => #{}})}
    ];

fields("user_provided_function") ->
    [ {function, sc(binary(),
        #{ desc => """
The user provided function. Should be in the format: '{module}:{function}'.<br>
Where {module} is the Erlang callback module and {function} is the Erlang function.
<br>
To write your own function, checkout the function <code>console</code> and
<code>republish</code> in the source file:
<code>apps/emqx_rule_engine/src/emqx_rule_outputs.erl</code> as an example.
"""
        , example => "module:function"
        })}
    , {args, sc(map(),
        #{ desc => """
The args will be passed as the 3rd argument to module:function/3,
checkout the function <code>console</code> and <code>republish</code> in the source file:
<code>apps/emqx_rule_engine/src/emqx_rule_outputs.erl</code> as an example.
"""
         , default => #{}
         })}
    ];

fields("republish_args") ->
    [ {topic, sc(binary(),
        #{ desc =>"""
The target topic of message to be re-published.<br>
Template with variables is allowed, see description of the 'republish_args'.
"""
          , required => true
          , example => <<"a/1">>
          })}
    , {qos, sc(qos(),
        #{ desc => """
The qos of the message to be re-published.
Template with variables is allowed, see description of the 'republish_args'.<br>
Defaults to ${qos}. If variable ${qos} is not found from the selected result of the rule,
0 is used.
"""
         , default => <<"${qos}">>
         , example => <<"${qos}">>
         })}
    , {retain, sc(hoconsc:union([binary(), boolean()]),
        #{ desc => """
The 'retain' flag of the message to be re-published.
Template with variables is allowed, see description of the 'republish_args'.<br>
Defaults to ${retain}. If variable ${retain} is not found from the selected result
of the rule, false is used.
"""
        , default => <<"${retain}">>
        , example => <<"${retain}">>
        })}
    , {payload, sc(binary(),
        #{ desc => """
The payload of the message to be re-published.
Template with variables is allowed, see description of the 'republish_args'.<br>.
Defaults to ${payload}. If variable ${payload} is not found from the selected result
of the rule, then the string \"undefined\" is used.
"""
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
        #{ desc => "The name of the rule"
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
