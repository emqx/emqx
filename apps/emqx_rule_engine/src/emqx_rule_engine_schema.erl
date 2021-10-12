%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , fields/1]).

-export([ validate_sql/1
        ]).

namespace() -> rule_engine.

roots() -> ["rule_engine"].

fields("rule_engine") ->
    [ {ignore_sys_message, sc(boolean(), #{default => true})}
    , {rules, sc(hoconsc:map("id", ref("rules")), #{desc => "The rules", default => #{}})}
    ];

fields("rules") ->
    [ {"sql", sc(binary(), #{desc => "The SQL of the rule", nullable => false,
                             validator => fun ?MODULE:validate_sql/1})}
    , {"outputs", sc(hoconsc:array(hoconsc:union(
                                   [ binary()
                                   , ref("builtin_output_republish")
                                   , ref("builtin_output_console")
                                   ])),
          #{desc => "The outputs of the rule. An output can be a string refers to the channel Id "
                    "of a emqx bridge, or a object refers to a built-in function.",
            default => []})}
    , {"enable", sc(boolean(), #{desc => "Enable or disable the rule", default => true})}
    , {"description", sc(binary(), #{desc => "The description of the rule", default => <<>>})}
    ];

fields("builtin_output_republish") ->
    [ {function, sc(republish, #{desc => "Republish the message as a new MQTT message"})}
    , {args, sc(ref("republish_args"), #{desc => "The arguments of the built-in 'republish' output",
        default => #{}})}
    ];

fields("builtin_output_console") ->
    [ {function, sc(console, #{desc => "Print the outputs to the console"})}
    %% we may support some args for the console output in the future
    %, {args, sc(map(), #{desc => "The arguments of the built-in 'console' output",
    %    default => #{}})}
    ];

fields("republish_args") ->
    [ {topic, sc(binary(),
        #{desc => "The target topic of the re-published message."
                  " Template with with variables is allowed.",
          nullable => false})}
    , {qos, sc(binary(),
        #{desc => "The qos of the re-published message."
                  " Template with with variables is allowed. Defaults to ${qos}.",
          default => <<"${qos}">> })}
    , {retain, sc(binary(),
        #{desc => "The retain of the re-published message."
                  " Template with with variables is allowed. Defaults to ${retain}.",
          default => <<"${retain}">> })}
    , {payload, sc(binary(),
        #{desc => "The payload of the re-published message."
                  " Template with with variables is allowed. Defaults to ${payload}.",
          default => <<"${payload}">>})}
    ].

validate_sql(Sql) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, _Result} -> ok;
        {error, Reason} -> {error, Reason}
    end.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
