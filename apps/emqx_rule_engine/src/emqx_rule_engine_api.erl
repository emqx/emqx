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

-module(emqx_rule_engine_api).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(minirest_api).

-import(hoconsc, [mk/2, ref/2, array/1]).

-export([printable_function_name/2]).

%% Swagger specs from hocon schema
-export([api_spec/0, paths/0, schema/1, namespace/0]).

%% API callbacks
-export(['/rule_events'/2, '/rule_test'/2, '/rules'/2, '/rules/:id'/2, '/rules/:id/reset_metrics'/2]).

-define(ERR_NO_RULE(ID), list_to_binary(io_lib:format("Rule ~ts Not Found", [(ID)]))).
-define(ERR_BADARGS(REASON), begin
    R0 = err_msg(REASON),
    <<"Bad Arguments: ", R0/binary>>
end).
-define(CHECK_PARAMS(PARAMS, TAG, EXPR),
    case emqx_rule_api_schema:check_params(PARAMS, TAG) of
        {ok, CheckedParams} ->
            EXPR;
        {error, REASON} ->
            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(REASON)}}
    end
).
-define(METRICS(
    MATCH,
    PASS,
    FAIL,
    FAIL_EX,
    FAIL_NORES,
    O_TOTAL,
    O_FAIL,
    O_FAIL_OOS,
    O_FAIL_UNKNOWN,
    O_SUCC,
    RATE,
    RATE_MAX,
    RATE_5
),
    #{
        'matched' => MATCH,
        'passed' => PASS,
        'failed' => FAIL,
        'failed.exception' => FAIL_EX,
        'failed.no_result' => FAIL_NORES,
        'actions.total' => O_TOTAL,
        'actions.failed' => O_FAIL,
        'actions.failed.out_of_service' => O_FAIL_OOS,
        'actions.failed.unknown' => O_FAIL_UNKNOWN,
        'actions.success' => O_SUCC,
        'matched.rate' => RATE,
        'matched.rate.max' => RATE_MAX,
        'matched.rate.last5m' => RATE_5
    }
).
-define(metrics(
    MATCH,
    PASS,
    FAIL,
    FAIL_EX,
    FAIL_NORES,
    O_TOTAL,
    O_FAIL,
    O_FAIL_OOS,
    O_FAIL_UNKNOWN,
    O_SUCC,
    RATE,
    RATE_MAX,
    RATE_5
),
    #{
        'matched' := MATCH,
        'passed' := PASS,
        'failed' := FAIL,
        'failed.exception' := FAIL_EX,
        'failed.no_result' := FAIL_NORES,
        'actions.total' := O_TOTAL,
        'actions.failed' := O_FAIL,
        'actions.failed.out_of_service' := O_FAIL_OOS,
        'actions.failed.unknown' := O_FAIL_UNKNOWN,
        'actions.success' := O_SUCC,
        'matched.rate' := RATE,
        'matched.rate.max' := RATE_MAX,
        'matched.rate.last5m' := RATE_5
    }
).

namespace() -> "rule".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() -> ["/rule_events", "/rule_test", "/rules", "/rules/:id", "/rules/:id/reset_metrics"].

error_schema(Code, Message) when is_atom(Code) ->
    emqx_dashboard_swagger:error_codes([Code], list_to_binary(Message)).

rule_creation_schema() ->
    ref(emqx_rule_api_schema, "rule_creation").

rule_test_schema() ->
    ref(emqx_rule_api_schema, "rule_test").

rule_info_schema() ->
    ref(emqx_rule_api_schema, "rule_info").

schema("/rules") ->
    #{
        'operationId' => '/rules',
        get => #{
            tags => [<<"rules">>],
            description => ?DESC("api1"),
            summary => <<"List Rules">>,
            responses => #{
                200 => mk(array(rule_info_schema()), #{desc => ?DESC("desc9")})
            }
        },
        post => #{
            tags => [<<"rules">>],
            description => ?DESC("api2"),
            summary => <<"Create a Rule">>,
            'requestBody' => rule_creation_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                201 => rule_info_schema()
            }
        }
    };
schema("/rule_events") ->
    #{
        'operationId' => '/rule_events',
        get => #{
            tags => [<<"rules">>],
            description => ?DESC("api3"),
            summary => <<"List Events">>,
            responses => #{
                200 => mk(ref(emqx_rule_api_schema, "rule_events"), #{})
            }
        }
    };
schema("/rules/:id") ->
    #{
        'operationId' => '/rules/:id',
        get => #{
            tags => [<<"rules">>],
            description => ?DESC("api4"),
            summary => <<"Get a Rule">>,
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Rule not found"),
                200 => rule_info_schema()
            }
        },
        put => #{
            tags => [<<"rules">>],
            description => ?DESC("api5"),
            summary => <<"Update a Rule">>,
            parameters => param_path_id(),
            'requestBody' => rule_creation_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                200 => rule_info_schema()
            }
        },
        delete => #{
            tags => [<<"rules">>],
            description => ?DESC("api6"),
            summary => <<"Delete a Rule">>,
            parameters => param_path_id(),
            responses => #{
                204 => <<"Delete rule successfully">>
            }
        }
    };
schema("/rules/:id/reset_metrics") ->
    #{
        'operationId' => '/rules/:id/reset_metrics',
        put => #{
            tags => [<<"rules">>],
            description => ?DESC("api7"),
            summary => <<"Reset a Rule Metrics">>,
            parameters => param_path_id(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "RPC Call Failed"),
                200 => <<"Reset Success">>
            }
        }
    };
schema("/rule_test") ->
    #{
        'operationId' => '/rule_test',
        post => #{
            tags => [<<"rules">>],
            description => ?DESC("api8"),
            summary => <<"Test a Rule">>,
            'requestBody' => rule_test_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                412 => error_schema('NOT_MATCH', "SQL Not Match"),
                200 => <<"Rule Test Pass">>
            }
        }
    }.

param_path_id() ->
    [{id, mk(binary(), #{in => path, example => <<"my_rule_id">>})}].

%%------------------------------------------------------------------------------
%% Rules API
%%------------------------------------------------------------------------------

'/rule_events'(get, _Params) ->
    {200, emqx_rule_events:event_info()}.

'/rules'(get, _Params) ->
    Records = emqx_rule_engine:get_rules_ordered_by_ts(),
    {200, format_rule_resp(Records)};
'/rules'(post, #{body := Params0}) ->
    case maps:get(<<"id">>, Params0, list_to_binary(emqx_misc:gen_id(8))) of
        <<>> ->
            {400, #{code => 'BAD_REQUEST', message => <<"empty rule id is not allowed">>}};
        Id ->
            Params = filter_out_request_body(add_metadata(Params0)),
            ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
            case emqx_rule_engine:get_rule(Id) of
                {ok, _Rule} ->
                    {400, #{code => 'BAD_REQUEST', message => <<"rule id already exists">>}};
                not_found ->
                    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
                        {ok, #{post_config_update := #{emqx_rule_engine := AllRules}}} ->
                            [Rule] = get_one_rule(AllRules, Id),
                            {201, format_rule_resp(Rule)};
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "create_rule_failed",
                                id => Id,
                                reason => Reason
                            }),
                            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
                    end
            end
    end.

'/rule_test'(post, #{body := Params}) ->
    ?CHECK_PARAMS(
        Params,
        rule_test,
        case emqx_rule_sqltester:test(CheckedParams) of
            {ok, Result} ->
                {200, Result};
            {error, {parse_error, Reason}} ->
                {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
            {error, nomatch} ->
                {412, #{code => 'NOT_MATCH', message => <<"SQL Not Match">>}};
            {error, Reason} ->
                {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
        end
    ).

'/rules/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, Rule} ->
            {200, format_rule_resp(Rule)};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end;
'/rules/:id'(put, #{bindings := #{id := Id}, body := Params0}) ->
    Params = filter_out_request_body(Params0),
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
        {ok, #{post_config_update := #{emqx_rule_engine := AllRules}}} ->
            [Rule] = get_one_rule(AllRules, Id),
            {200, format_rule_resp(Rule)};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "update_rule_failed",
                id => Id,
                reason => Reason
            }),
            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
    end;
'/rules/:id'(delete, #{bindings := #{id := Id}}) ->
    ConfPath = emqx_rule_engine:config_key_path() ++ [Id],
    case emqx_conf:remove(ConfPath, #{override_to => cluster}) of
        {ok, _} ->
            {204};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "delete_rule_failed",
                id => Id,
                reason => Reason
            }),
            {500, #{code => 'INTERNAL_ERROR', message => ?ERR_BADARGS(Reason)}}
    end.
'/rules/:id/reset_metrics'(put, #{bindings := #{id := RuleId}}) ->
    case emqx_rule_engine_proto_v1:reset_metrics(RuleId) of
        ok ->
            {200, <<"Reset Success">>};
        Failed ->
            {400, #{
                code => 'BAD_REQUEST',
                message => err_msg(Failed)
            }}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

err_msg(Msg) -> emqx_misc:readable_error_msg(Msg).

format_rule_resp(Rules) when is_list(Rules) ->
    [format_rule_resp(R) || R <- Rules];
format_rule_resp(#{
    id := Id,
    name := Name,
    created_at := CreatedAt,
    from := Topics,
    actions := Action,
    sql := SQL,
    enable := Enable,
    description := Descr
}) ->
    NodeMetrics = get_rule_metrics(Id),
    #{
        id => Id,
        name => Name,
        from => Topics,
        actions => format_action(Action),
        sql => SQL,
        metrics => aggregate_metrics(NodeMetrics),
        node_metrics => NodeMetrics,
        enable => Enable,
        created_at => format_datetime(CreatedAt, millisecond),
        description => Descr
    }.

format_datetime(Timestamp, Unit) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, Unit}])).

format_action(Actions) ->
    [do_format_action(Act) || Act <- Actions].

do_format_action(#{mod := Mod, func := Func, args := Args}) ->
    #{
        function => printable_function_name(Mod, Func),
        args => maps:remove(preprocessed_tmpl, Args)
    };
do_format_action(BridgeChannelId) when is_binary(BridgeChannelId) ->
    BridgeChannelId.

printable_function_name(emqx_rule_actions, Func) ->
    Func;
printable_function_name(Mod, Func) ->
    list_to_binary(lists:concat([Mod, ":", Func])).

get_rule_metrics(Id) ->
    Format = fun(
        Node,
        #{
            counters :=
                #{
                    'matched' := Matched,
                    'passed' := Passed,
                    'failed' := Failed,
                    'failed.exception' := FailedEx,
                    'failed.no_result' := FailedNoRes,
                    'actions.total' := OTotal,
                    'actions.failed' := OFailed,
                    'actions.failed.out_of_service' := OFailedOOS,
                    'actions.failed.unknown' := OFailedUnknown,
                    'actions.success' := OFailedSucc
                },
            rate :=
                #{
                    'matched' :=
                        #{current := Current, max := Max, last5m := Last5M}
                }
        }
    ) ->
        #{
            metrics => ?METRICS(
                Matched,
                Passed,
                Failed,
                FailedEx,
                FailedNoRes,
                OTotal,
                OFailed,
                OFailedOOS,
                OFailedUnknown,
                OFailedSucc,
                Current,
                Max,
                Last5M
            ),
            node => Node
        }
    end,
    [
        Format(Node, emqx_plugin_libs_proto_v1:get_metrics(Node, rule_metrics, Id))
     || Node <- mria_mnesia:running_nodes()
    ].

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?METRICS(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    lists:foldl(
        fun(
            #{
                metrics := ?metrics(
                    Match1,
                    Passed1,
                    Failed1,
                    FailedEx1,
                    FailedNoRes1,
                    OTotal1,
                    OFailed1,
                    OFailedOOS1,
                    OFailedUnknown1,
                    OFailedSucc1,
                    Rate1,
                    RateMax1,
                    Rate5m1
                )
            },
            ?metrics(
                Match0,
                Passed0,
                Failed0,
                FailedEx0,
                FailedNoRes0,
                OTotal0,
                OFailed0,
                OFailedOOS0,
                OFailedUnknown0,
                OFailedSucc0,
                Rate0,
                RateMax0,
                Rate5m0
            )
        ) ->
            ?METRICS(
                Match1 + Match0,
                Passed1 + Passed0,
                Failed1 + Failed0,
                FailedEx1 + FailedEx0,
                FailedNoRes1 + FailedNoRes0,
                OTotal1 + OTotal0,
                OFailed1 + OFailed0,
                OFailedOOS1 + OFailedOOS0,
                OFailedUnknown1 + OFailedUnknown0,
                OFailedSucc1 + OFailedSucc0,
                Rate1 + Rate0,
                RateMax1 + RateMax0,
                Rate5m1 + Rate5m0
            )
        end,
        InitMetrics,
        AllMetrics
    ).

get_one_rule(AllRules, Id) ->
    [R || R = #{id := Id0} <- AllRules, Id0 == Id].

add_metadata(Params) ->
    Params#{
        <<"metadata">> => #{
            <<"created_at">> => emqx_rule_engine:now_ms()
        }
    }.

filter_out_request_body(Conf) ->
    ExtraConfs = [
        <<"id">>,
        <<"status">>,
        <<"node_status">>,
        <<"node_metrics">>,
        <<"metrics">>,
        <<"node">>
    ],
    maps:without(ExtraConfs, Conf).
