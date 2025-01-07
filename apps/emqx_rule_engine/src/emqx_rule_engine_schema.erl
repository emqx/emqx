%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    tags/0,
    roots/0,
    fields/1,
    desc/1,
    post_config_update/5,
    rule_engine_settings/0
]).

-export([validate_sql/1]).

namespace() -> rule_engine.

tags() ->
    [<<"Rule Engine">>].

roots() ->
    [{"rule_engine", ?HOCON(?R_REF("rule_engine"), #{importance => ?IMPORTANCE_HIDDEN})}].

fields("rule_engine") ->
    rule_engine_settings() ++
        [
            {rules,
                ?HOCON(hoconsc:map("id", ?R_REF("rules")), #{
                    desc => ?DESC("rule_engine_rules"), default => #{}
                })}
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
                ?ARRAY(hoconsc:union(actions())),
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
        {function, ?HOCON(console, #{desc => ?DESC("console_function")})},
        %% we may support some args for the console action in the future

        %% "args" needs to be a reserved/ignored field in the schema
        %% to maintain compatibility with rule data that may contain
        %% it due to a validation bug in previous versions.

        %% The "args" field was not validated by the HOCON schema before 5.2.0,
        %% which allowed rules to be created with invalid "args" data.
        %% In 5.2.1 the validation was added,
        %% so existing rules saved with invalid "args" would now fail validation
        %% To maintain backward compatibility for existing rule data that may contain invalid "args",
        %% the field needs to be included in the schema even though it is not a valid field.
        {args,
            ?HOCON(map(), #{
                deprecated => true,
                importance => ?IMPORTANCE_HIDDEN,
                desc => "The arguments of the built-in 'console' action",
                default => #{}
            })}
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
                emqx_schema:template(),
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
                hoconsc:union([boolean(), emqx_schema:template()]),
                #{
                    desc => ?DESC("republish_args_retain"),
                    default => <<"${retain}">>,
                    example => <<"${retain}">>
                }
            )},
        {payload,
            ?HOCON(
                emqx_schema:template(),
                #{
                    desc => ?DESC("republish_args_payload"),
                    default => <<"${payload}">>,
                    example => <<"${payload}">>
                }
            )},
        {mqtt_properties,
            ?HOCON(
                ?R_REF("republish_mqtt_properties"),
                #{
                    desc => ?DESC("republish_args_mqtt_properties"),
                    default => #{}
                }
            )},
        {user_properties,
            ?HOCON(
                emqx_schema:template(),
                #{
                    desc => ?DESC("republish_args_user_properties"),
                    default => <<"${user_properties}">>,
                    example => <<"${pub_props.'User-Property'}">>
                }
            )},
        {direct_dispatch,
            ?HOCON(
                hoconsc:union([boolean(), emqx_schema:template()]),
                #{
                    desc => ?DESC("republish_args_direct_dispatch"),
                    default => false
                }
            )}
    ];
fields("republish_mqtt_properties") ->
    [
        {'Payload-Format-Indicator',
            ?HOCON(binary(), #{required => false, desc => ?DESC('Payload-Format-Indicator')})},
        {'Message-Expiry-Interval',
            ?HOCON(binary(), #{required => false, desc => ?DESC('Message-Expiry-Interval')})},
        {'Content-Type', ?HOCON(binary(), #{required => false, desc => ?DESC('Content-Type')})},
        {'Response-Topic', ?HOCON(binary(), #{required => false, desc => ?DESC('Response-Topic')})},
        {'Correlation-Data',
            ?HOCON(binary(), #{required => false, desc => ?DESC('Correlation-Data')})}
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
    fun
        (all_union_members) ->
            [
                binary(),
                ?R_REF("builtin_action_republish"),
                ?R_REF("builtin_action_console"),
                ?R_REF("user_provided_function")
            ];
        ({value, V}) ->
            case V of
                #{<<"function">> := <<"console">>} ->
                    [?R_REF("builtin_action_console")];
                #{<<"function">> := <<"republish">>} ->
                    [?R_REF("builtin_action_republish")];
                #{<<"function">> := <<_/binary>>} ->
                    [?R_REF("user_provided_function")];
                <<_/binary>> ->
                    [binary()];
                _ ->
                    throw(#{
                        field_name => actions,
                        reason => <<"unknown action type">>
                    })
            end
    end.

qos() ->
    hoconsc:union([emqx_schema:qos(), emqx_schema:template()]).

rule_engine_settings() ->
    [
        {ignore_sys_message,
            ?HOCON(boolean(), #{default => true, desc => ?DESC("rule_engine_ignore_sys_message")})},
        {jq_function_default_timeout,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"10s">>,
                    desc => ?DESC("rule_engine_jq_function_default_timeout")
                }
            )},
        {jq_implementation_module,
            ?HOCON(
                hoconsc:enum([jq_nif, jq_port]),
                #{
                    default => jq_nif,
                    mapping => "jq.jq_implementation_module",
                    desc => ?DESC("rule_engine_jq_implementation_module"),
                    deprecated => {since, "v5.0.22"},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ].

validate_sql(Sql) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, _Result} -> ok;
        {error, Reason} -> {error, Reason}
    end.

post_config_update(
    [rule_engine, jq_implementation_module],
    _Req,
    NewSysConf,
    _OldSysConf,
    _AppEnvs
) ->
    jq:set_implementation_module(NewSysConf),
    ok.
