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

-module(emqx_rule_engine_api).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

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

%% Metrics map value
-define(METRICS_VAL(
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
    O_DISCARDED,
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
        'actions.discarded' => O_DISCARDED,
        'matched.rate' => RATE,
        'matched.rate.max' => RATE_MAX,
        'matched.rate.last5m' => RATE_5
    }
).

-define(METRICS_VAL_ZERO, ?METRICS_VAL(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)).

%% Metrics map match pattern
-define(METRICS_PAT(
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
    O_DISCARDED,
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
        'actions.discarded' := O_DISCARDED,
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
    {<<"action">>, binary},
    {<<"source">>, binary},
    {<<"like_description">>, binary}
]).

namespace() -> "rule".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => fun emqx_dashboard_swagger:validate_content_type_json/2
    }).

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
                {action,
                    mk(hoconsc:array(binary()), #{in => query, desc => ?DESC("api1_qs_action")})},
                {source,
                    mk(hoconsc:array(binary()), #{in => query, desc => ?DESC("api1_qs_source")})},
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
        Id when is_binary(Id) ->
            Params = filter_out_request_body(add_metadata(Params0)),
            case emqx_rule_engine:get_rule(Id) of
                {ok, _Rule} ->
                    ?BAD_REQUEST(<<"rule id already exists">>);
                not_found ->
                    ConfPath = ?RULE_PATH(Id),
                    case emqx_conf:update(ConfPath, Params, #{override_to => cluster}) of
                        {ok, #{post_config_update := #{emqx_rule_engine := Rule}}} ->
                            ?CREATED(format_rule_info_resp(Rule));
                        {error, Reason} ->
                            ?SLOG(
                                info,
                                #{
                                    msg => "create_rule_failed",
                                    rule_id => Id,
                                    reason => Reason
                                },
                                #{tag => ?TAG}
                            ),
                            ?BAD_REQUEST(?ERR_BADARGS(Reason))
                    end
            end;
        _BadId ->
            ?BAD_REQUEST(<<"rule id must be a string">>)
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
            ?SLOG(
                info,
                #{
                    msg => "update_rule_failed",
                    rule_id => Id,
                    reason => Reason
                },
                #{tag => ?TAG}
            ),
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
                    ?SLOG(
                        error,
                        #{
                            msg => "delete_rule_failed",
                            rule_id => Id,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
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
        ?SLOG(
            warning,
            #{
                msg => "rpc_get_rule_metrics_errors",
                rule_id => Id,
                errors => NodeErrors
            },
            #{tag => ?TAG}
        ),
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
            'actions.success' := OSucc,
            'actions.discarded' := ODiscard
        },
    rate :=
        #{
            'matched' :=
                #{current := Current, max := Max, last5m := Last5M}
        }
}) ->
    #{
        metrics => ?METRICS_VAL(
            Matched,
            Passed,
            Failed,
            FailedEx,
            FailedNoRes,
            OTotal,
            OFailed,
            OFailedOOS,
            OFailedUnknown,
            OSucc,
            ODiscard,
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
        metrics => ?METRICS_VAL_ZERO,
        node => Node
    }.

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?METRICS_VAL_ZERO,
    lists:foldl(fun do_aggregate_metrics/2, InitMetrics, AllMetrics).

do_aggregate_metrics(#{metrics := Mt1}, Mt0) when map_size(Mt1) =:= map_size(Mt0) ->
    ?METRICS_PAT(A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1, M1, N1) = Mt1,
    ?METRICS_PAT(A0, B0, C0, D0, E0, F0, G0, H0, I0, J0, K0, L0, M0, N0) = Mt0,
    ?METRICS_VAL(
        A0 + A1,
        B0 + B1,
        C0 + C1,
        D0 + D1,
        E0 + E1,
        F0 + F1,
        G0 + G1,
        H0 + H1,
        I0 + I1,
        J0 + J1,
        K0 + K1,
        L0 + L1,
        M0 + M1,
        N0 + N1
    );
do_aggregate_metrics(#{metrics := M1}, M0) ->
    %% this happens during rolling upgrade
    %% fallback to per-map-key iteration
    maps:fold(
        fun(Name, V1, Acc) ->
            case maps:get(Name, Acc, false) of
                false ->
                    %% this is an unknown metric name for this node
                    %% discard
                    Acc;
                V0 ->
                    Acc#{Name => V0 + V1}
            end
        end,
        M0,
        M1
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
qs2ms(_Tab, {Qs0, Fuzzy0}) ->
    {Qs, Fuzzy} = adapt_custom_filters(Qs0, Fuzzy0),
    case lists:keytake(from, 1, Qs) of
        false ->
            #{match_spec => generate_match_spec(Qs), fuzzy_fun => fuzzy_match_fun(Fuzzy)};
        {value, {from, '=:=', From}, Ls} ->
            #{
                match_spec => generate_match_spec(Ls),
                fuzzy_fun => fuzzy_match_fun([{from, '=:=', From} | Fuzzy])
            }
    end.

%% Some filters are run as fuzzy filters because they cannot be expressed as simple ETS
%% match specs.
-spec adapt_custom_filters(Qs, Fuzzy) -> {Qs, Fuzzy}.
adapt_custom_filters(Qs, Fuzzy) ->
    lists:foldl(
        fun
            ({action, '=:=', X}, {QsAcc, FuzzyAcc}) ->
                ActionIds = wrap(X),
                Parsed = lists:map(fun emqx_rule_actions:parse_action/1, ActionIds),
                {QsAcc, [{action, in, Parsed} | FuzzyAcc]};
            ({source, '=:=', X}, {QsAcc, FuzzyAcc}) ->
                SourceIds = wrap(X),
                Parsed = lists:flatmap(
                    fun(SourceId) ->
                        [
                            emqx_bridge_resource:bridge_hookpoint(SourceId),
                            emqx_bridge_v2:source_hookpoint(SourceId)
                        ]
                    end,
                    SourceIds
                ),
                {QsAcc, [{source, in, Parsed} | FuzzyAcc]};
            (Clause, {QsAcc, FuzzyAcc}) ->
                {[Clause | QsAcc], FuzzyAcc}
        end,
        {[], Fuzzy},
        Qs
    ).

wrap(Xs) when is_list(Xs) -> Xs;
wrap(X) -> [X].

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
run_fuzzy_match(E = {_Id, #{actions := Actions}}, [{action, in, ActionIds} | Fuzzy]) ->
    lists:any(fun(AId) -> lists:member(AId, Actions) end, ActionIds) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_Id, #{from := Froms}}, [{source, in, SourceIds} | Fuzzy]) ->
    lists:any(fun(SId) -> lists:member(SId, Froms) end, SourceIds) andalso
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
