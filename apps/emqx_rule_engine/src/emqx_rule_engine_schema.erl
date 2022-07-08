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

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([validate_sql/1]).

namespace() -> rule_engine.

roots() -> ["rule_engine"].

fields("rule_engine") ->
    [
        {ignore_sys_message,
            ?HOCON(boolean(), #{default => true, desc => ?DESC("rule_engine_ignore_sys_message")})},
        {rules,
            ?HOCON(hoconsc:map("id", ?R_REF("rules")), #{
                desc => ?DESC("rule_engine_rules"), default => #{}
            })},
        {jq_function_default_timeout,
            ?HOCON(
                emqx_schema:duration_ms(),
                #{
                    default => "10s",
                    desc => ?DESC("rule_engine_jq_function_default_timeout")
                }
            )}
    ];
fields("rules") ->
    [
        rule_name(),
        {"sql",
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC("rules_sql"),
                    example => "SELECT * FROM \"test/topic\" WHERE payload.x = 1",
                    required => true,
                    validator => fun ?MODULE:validate_sql/1
                }
            )},
        {"actions",
            ?HOCON(
                ?ARRAY(?UNION(actions())),
                #{
                    desc => ?DESC("rules_actions"),
                    default => [],
                    example => [
                        <<"webhook:my_webhook">>,
                        #{
                            function => republish,
                            args => #{
                                topic => <<"t/1">>, payload => <<"${payload}">>
                            }
                        },
                        #{function => console}
                    ]
                }
            )},
        {"enable", ?HOCON(boolean(), #{desc => ?DESC("rules_enable"), default => true})},
        {"description",
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC("rules_description"),
                    example => "Some description",
                    default => <<>>
                }
            )},
        {"metadata", ?HOCON(map(), #{desc => ?DESC("rules_metadata")})}
    ];
fields("builtin_action_republish") ->
    [
        {function, ?HOCON(republish, #{desc => ?DESC("republish_function")})},
        {args, ?HOCON(?R_REF("republish_args"), #{default => #{}})}
    ];
fields("builtin_action_console") ->
    [
        {function, ?HOCON(console, #{desc => ?DESC("console_function")})}
        %% we may support some args for the console action in the future
        %, {args, sc(map(), #{desc => "The arguments of the built-in 'console' action",
        %    default => #{}})}
    ];
fields("user_provided_function") ->
    [
        {function,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC("user_provided_function_function"),
                    required => true,
                    example => "module:function"
                }
            )},
        {args,
            ?HOCON(
                map(),
                #{
                    desc => ?DESC("user_provided_function_args"),
                    default => #{}
                }
            )}
    ];
fields("republish_args") ->
    [
        {topic,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC("republish_args_topic"),
                    required => true,
                    example => <<"a/1">>
                }
            )},
        {qos,
            ?HOCON(
                qos(),
                #{
                    desc => ?DESC("republish_args_qos"),
                    default => <<"${qos}">>,
                    example => <<"${qos}">>
                }
            )},
        {retain,
            ?HOCON(
                hoconsc:union([boolean(), binary()]),
                #{
                    desc => ?DESC("republish_args_retain"),
                    default => <<"${retain}">>,
                    example => <<"${retain}">>
                }
            )},
        {payload,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC("republish_args_payload"),
                    default => <<"${payload}">>,
                    example => <<"${payload}">>
                }
            )}
    ].

desc("rule_engine") ->
    ?DESC("desc_rule_engine");
desc("rules") ->
    ?DESC("desc_rules");
desc("builtin_action_republish") ->
    ?DESC("desc_builtin_action_republish");
desc("builtin_action_console") ->
    ?DESC("desc_builtin_action_console");
desc("user_provided_function") ->
    ?DESC("desc_user_provided_function");
desc("republish_args") ->
    ?DESC("desc_republish_args");
desc(_) ->
    undefined.

rule_name() ->
    {"name",
        ?HOCON(
            binary(),
            #{
                desc => ?DESC("rules_name"),
                default => <<"">>,
                required => false,
                example => "foo"
            }
        )}.

actions() ->
    [
        binary(),
        ?R_REF("builtin_action_republish"),
        ?R_REF("builtin_action_console"),
        ?R_REF("user_provided_function")
    ].

qos() ->
    ?UNION([emqx_schema:qos(), binary()]).

validate_sql(Sql) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, _Result} -> ok;
        {error, Reason} -> {error, Reason}
    end.
