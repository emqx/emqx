%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([
    '/rule_engine'/2,
    '/rule_events'/2,
    '/rule_test'/2,
    '/rules'/2,
    '/rules/:id'/2,
    '/rules/:id/test'/2,
    '/rules/:id/metrics'/2,
    '/rules/:id/metrics/reset'/2
]).

%% query callback
-export([qs2ms/2, run_fuzzy_match/2, format_rule_info_resp/1]).

-define(RPC_GET_METRICS_TIMEOUT, 5000).

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

-define(RULE_QS_SCHEMA, [
    {<<"enable">>, atom},
    {<<"from">>, binary},
    {<<"like_id">>, binary},
    {<<"like_from">>, binary},
    {<<"match_from">>, binary},
    {<<"like_description">>, binary}
]).

namespace() -> "rule".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/rule_engine",
        "/rule_events",
        "/rule_test",
        "/rules",
        "/rules/:id",
        "/rules/:id/test",
        "/rules/:id/metrics",
        "/rules/:id/metrics/reset"
    ].

error_schema(Code, Message) when is_atom(Code) ->
    emqx_dashboard_swagger:error_codes([Code], list_to_binary(Message)).

rule_engine_schema() ->
    ref(emqx_rule_api_schema, "rule_engine").

rule_creation_schema() ->
    ref(emqx_rule_api_schema, "rule_creation").

rule_test_schema() ->
    ref(emqx_rule_api_schema, "rule_test").

rule_apply_test_schema() ->
    ref(emqx_rule_api_schema, "rule_apply_test").

rule_info_schema() ->
    ref(emqx_rule_api_schema, "rule_info").

rule_metrics_schema() ->
    ref(emqx_rule_api_schema, "rule_metrics").

schema("/rules") ->
    #{
        'operationId' => '/rules',
        get => #{
            tags => [<<"rules">>],
            description => ?DESC("api1"),
            parameters => [
                {enable,
                    mk(boolean(), #{desc => ?DESC("api1_enable"), in => query, required => false})},
                {from, mk(binary(), #{desc => ?DESC("api1_from"), in => query, required => false})},
                {like_id,
                    mk(binary(), #{desc => ?DESC("api1_like_id"), in => query, required => false})},
                {like_from,
                    mk(binary(), #{desc => ?DESC("api1_like_from"), in => query, required => false})},
                {like_description,
                    mk(binary(), #{
                        desc => ?DESC("api1_like_description"), in => query, required => false
                    })},
                {match_from,
                    mk(binary(), #{desc => ?DESC("api1_match_from"), in => query, required => false})},
                ref(emqx_dashboard_swagger, page),
                ref(emqx_dashboard_swagger, limit)
            ],
            summary => <<"List rules">>,
            responses => #{
                200 =>
                    [
                        {data, mk(array(rule_info_schema()), #{desc => ?DESC("api1_resp")})},
                        {meta, mk(ref(emqx_dashboard_swagger, meta), #{})}
                    ],
                400 => error_schema('BAD_REQUEST', "Invalid Parameters")
            }
        },
        post => #{
            tags => [<<"rules">>],
            description => ?DESC("api2"),
            summary => <<"Create a rule">>,
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
            summary => <<"List rule events">>,
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
            summary => <<"Get rule">>,
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Rule not found"),
                200 => rule_info_schema()
            }
        },
        put => #{
            tags => [<<"rules">>],
            description => ?DESC("api5"),
            summary => <<"Update rule">>,
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
            summary => <<"Delete rule">>,
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Rule not found"),
                204 => <<"Delete rule successfully">>
            }
        }
    };
schema("/rules/:id/test") ->
    #{
        'operationId' => '/rules/:id/test',
        post => #{
            tags => [<<"rules">>],
            description => ?DESC("api11"),
            summary => <<"Apply a rule for testing">>,
            parameters => param_path_id(),
            'requestBody' => rule_apply_test_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                412 => error_schema('NOT_MATCH', "SQL Not Match"),
                404 => error_schema('RULE_NOT_FOUND', "The rule could not be found"),
                200 => <<"Rule Applied">>
            }
        }
    };
schema("/rules/:id/metrics") ->
    #{
        'operationId' => '/rules/:id/metrics',
        get => #{
            tags => [<<"rules">>],
            description => ?DESC("api4_1"),
            summary => <<"Get rule metrics">>,
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Rule not found"),
                200 => rule_metrics_schema()
            }
        }
    };
schema("/rules/:id/metrics/reset") ->
    #{
        'operationId' => '/rules/:id/metrics/reset',
        put => #{
            tags => [<<"rules">>],
            description => ?DESC("api7"),
            summary => <<"Reset rule metrics">>,
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Rule not found"),
                204 => <<"Reset Success">>
            }
        }
    };
schema("/rule_test") ->
    #{
        'operationId' => '/rule_test',
        post => #{
            tags => [<<"rules">>],
            description => ?DESC("api8"),
            summary => <<"Test a rule">>,
            'requestBody' => rule_test_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                412 => error_schema('NOT_MATCH', "SQL Not Match"),
                200 => <<"Rule Test Pass">>
            }
        }
    };
schema("/rule_engine") ->
    #{
        'operationId' => '/rule_engine',
        get => #{
            tags => [<<"rules">>],
            description => ?DESC("api9"),
            responses => #{
                200 => rule_engine_schema()
            }
        },
        put => #{
            tags => [<<"rules">>],
            description => ?DESC("api10"),
            'requestBody' => rule_engine_schema(),
            responses => #{
                200 => rule_engine_schema(),
                400 => error_schema('BAD_REQUEST', "Invalid request")
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

'/rules'(get, #{query_string := QueryString}) ->
    case
        emqx_mgmt_api:node_query(
            node(),
            ?RULE_TAB,
            QueryString,
            ?RULE_QS_SCHEMA,
            fun ?MODULE:qs2ms/2,
            fun ?MODULE:format_rule_info_resp/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => 'BAD_REQUEST', message => <<"page_limit_invalid">>}};
        {error, Node, Error} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, Error])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end;
'/rules'(post, #{body := Params0}) ->
    case maps:get(<<"id">>, Params0, list_to_binary(emqx_utils:gen_id(8))) of
        <<>> ->
            {400, #{code => 'BAD_REQUEST', message => <<"empty rule id is not allowed">>}};
        Id ->
            Params = filter_out_request_body(add_metadata(Params0)),
            case emqx_rule_engine:get_rule(Id) of
                {ok, _Rule} ->
                    {400, #{code => 'BAD_REQUEST', message => <<"rule id already exists">>}};
                not_found ->
                    ConfPath = ?RULE_PATH(Id),
                    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
                        {ok, #{post_config_update := #{emqx_rule_engine := Rule}}} ->
                            {201, format_rule_info_resp(Rule)};
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

'/rules/:id/test'(post, #{body := Params, bindings := #{id := RuleId}}) ->
    ?CHECK_PARAMS(
        Params,
        rule_apply_test,
        begin
            case emqx_rule_sqltester:apply_rule(RuleId, CheckedParams) of
                {ok, Result} ->
                    {200, emqx_logger_jsonfmt:best_effort_json_obj(Result)};
                {error, {parse_error, Reason}} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
                {error, nomatch} ->
                    {412, #{code => 'NOT_MATCH', message => <<"SQL Not Match">>}};
                {error, rule_not_found} ->
                    {404, #{code => 'RULE_NOT_FOUND', message => <<"The rule could not be found">>}};
                {error, Reason} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
            end
        end
    ).

'/rules/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, Rule} ->
            {200, format_rule_info_resp(Rule)};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end;
'/rules/:id'(put, #{bindings := #{id := Id}, body := Params0}) ->
    Params = filter_out_request_body(Params0),
    ConfPath = ?RULE_PATH(Id),
    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
        {ok, #{post_config_update := #{emqx_rule_engine := Rule}}} ->
            {200, format_rule_info_resp(Rule)};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "update_rule_failed",
                id => Id,
                reason => Reason
            }),
            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
    end;
'/rules/:id'(delete, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, _Rule} ->
            ConfPath = ?RULE_PATH(Id),
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
            end;
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end.

'/rules/:id/metrics'(get, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, _Rule} ->
            NodeMetrics = get_rule_metrics(Id),
            MetricsResp =
                #{
                    id => Id,
                    metrics => aggregate_metrics(NodeMetrics),
                    node_metrics => NodeMetrics
                },
            {200, MetricsResp};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end.

'/rules/:id/metrics/reset'(put, #{bindings := #{id := Id}}) ->
    case emqx_rule_engine:get_rule(Id) of
        {ok, _Rule} ->
            ok = emqx_rule_engine_proto_v1:reset_metrics(Id),
            {204};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end.

'/rule_engine'(get, _Params) ->
    {200, format_rule_engine_resp(emqx_conf:get([rule_engine]))};
'/rule_engine'(put, #{body := Params}) ->
    case rule_engine_update(Params) of
        {ok, Config} ->
            {200, format_rule_engine_resp(Config)};
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

err_msg({RuleError, {_E, Reason, _S}}) ->
    emqx_utils:readable_error_msg(encode_nested_error(RuleError, Reason));
err_msg({Reason, _Details}) ->
    emqx_utils:readable_error_msg(Reason);
err_msg(Msg) ->
    emqx_utils:readable_error_msg(Msg).

encode_nested_error(RuleError, Reason) when is_tuple(Reason) ->
    encode_nested_error(RuleError, element(1, Reason));
encode_nested_error(RuleError, Reason) ->
    case emqx_utils_json:safe_encode([{RuleError, Reason}]) of
        {ok, Json} ->
            Json;
        _ ->
            {RuleError, Reason}
    end.

format_rule_info_resp({Id, Rule}) ->
    format_rule_info_resp(Rule#{id => Id});
format_rule_info_resp(#{
    id := Id,
    name := Name,
    created_at := CreatedAt,
    from := Topics,
    actions := Action,
    sql := SQL,
    enable := Enable,
    description := Descr
}) ->
    #{
        id => Id,
        name => Name,
        from => Topics,
        actions => format_action(Action),
        sql => SQL,
        enable => Enable,
        created_at => format_datetime(CreatedAt, millisecond),
        description => Descr
    }.

format_rule_engine_resp(Config) ->
    maps:remove(rules, Config).

format_datetime(Timestamp, Unit) ->
    emqx_utils_calendar:epoch_to_rfc3339(Timestamp, Unit).

format_action(Actions) ->
    [do_format_action(Act) || Act <- Actions].

do_format_action({bridge, BridgeType, BridgeName, _ResId}) ->
    emqx_bridge_resource:bridge_id(BridgeType, BridgeName);
do_format_action({bridge_v2, BridgeType, BridgeName}) ->
    emqx_bridge_resource:bridge_id(BridgeType, BridgeName);
do_format_action(#{mod := Mod, func := Func, args := Args}) ->
    #{
        function => printable_function_name(Mod, Func),
        args => maps:remove(preprocessed_tmpl, Args)
    };
do_format_action(#{mod := Mod, func := Func}) ->
    #{
        function => printable_function_name(Mod, Func)
    }.

printable_function_name(emqx_rule_actions, Func) ->
    Func;
printable_function_name(Mod, Func) ->
    list_to_binary(lists:concat([Mod, ":", Func])).

get_rule_metrics(Id) ->
    Nodes = emqx:running_nodes(),
    Results = emqx_metrics_proto_v2:get_metrics(Nodes, rule_metrics, Id, ?RPC_GET_METRICS_TIMEOUT),
    NodeResults = lists:zip(Nodes, Results),
    NodeMetrics = [format_metrics(Node, Metrics) || {Node, {ok, Metrics}} <- NodeResults],
    NodeErrors = [Result || Result = {_Node, {NOk, _}} <- NodeResults, NOk =/= ok],
    NodeErrors == [] orelse
        ?SLOG(warning, #{
            msg => "rpc_get_rule_metrics_errors",
            errors => NodeErrors
        }),
    NodeMetrics.

format_metrics(Node, #{
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
}) ->
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
    };
format_metrics(Node, _Metrics) ->
    %% Empty metrics: can happen when a node joins another and a bridge is not yet
    %% replicated to it, so the counters map is empty.
    #{
        metrics => ?METRICS(
            _Matched = 0,
            _Passed = 0,
            _Failed = 0,
            _FailedEx = 0,
            _FailedNoRes = 0,
            _OTotal = 0,
            _OFailed = 0,
            _OFailedOOS = 0,
            _OFailedUnknown = 0,
            _OFailedSucc = 0,
            _Current = 0,
            _Max = 0,
            _Last5M = 0
        ),
        node => Node
    }.

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

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {Qs, Fuzzy}) ->
    case lists:keytake(from, 1, Qs) of
        false ->
            #{match_spec => generate_match_spec(Qs), fuzzy_fun => fuzzy_match_fun(Fuzzy)};
        {value, {from, '=:=', From}, Ls} ->
            #{
                match_spec => generate_match_spec(Ls),
                fuzzy_fun => fuzzy_match_fun([{from, '=:=', From} | Fuzzy])
            }
    end.

generate_match_spec(Qs) ->
    {MtchHead, Conds} = generate_match_spec(Qs, 2, {#{}, []}),
    [{{'_', MtchHead}, Conds, ['$_']}].

generate_match_spec([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};
generate_match_spec([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = list_to_atom([$$ | integer_to_list(N)]),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    generate_match_spec(Rest, N + 1, {NMtchHead, NConds}).

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds].

ms(enable, X) ->
    #{enable => X}.

fuzzy_match_fun([]) ->
    undefined;
fuzzy_match_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_match/2, [Fuzzy]}.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {Id, _}, [{id, like, Pattern} | Fuzzy]) ->
    binary:match(Id, Pattern) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_Id, #{description := Desc}}, [{description, like, Pattern} | Fuzzy]) ->
    binary:match(Desc, Pattern) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{from := Topics}}, [{from, '=:=', Pattern} | Fuzzy]) ->
    lists:member(Pattern, Topics) /= false andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_Id, #{from := Topics}}, [{from, match, Pattern} | Fuzzy]) ->
    lists:any(fun(For) -> emqx_topic:match(For, Pattern) end, Topics) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_Id, #{from := Topics}}, [{from, like, Pattern} | Fuzzy]) ->
    lists:any(fun(For) -> binary:match(For, Pattern) /= nomatch end, Topics) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E, [_ | Fuzzy]) ->
    run_fuzzy_match(E, Fuzzy).

rule_engine_update(Params) ->
    case emqx_rule_api_schema:check_params(Params, rule_engine) of
        {ok, _CheckedParams} ->
            {ok, #{config := Config}} = emqx_conf:update([rule_engine], Params, #{
                override_to => cluster
            }),
            {ok, Config};
        {error, Reason} ->
            {error, Reason}
    end.
