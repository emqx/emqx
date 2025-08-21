%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_api).

-include("rule_engine.hrl").
-include("emqx_rule_engine_internal.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_config.hrl").

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
-export([run_fuzzy_match/2, format_rule_info_resp/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(RPC_GET_METRICS_TIMEOUT, 5_000).

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

-type maybe_namespace() :: ?global_ns | binary().

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

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
                404 => error_schema('NOT_FOUND', "Rule not found"),
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

'/rules'(get, #{query_string := QueryString} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case
        emqx_mgmt_api:node_query(
            node(),
            ?RULE_TAB,
            QueryString,
            ?RULE_QS_SCHEMA,
            mk_qs2ms_fn(Namespace),
            mk_format_fn(Namespace)
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
'/rules'(post, #{body := Params0} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case maps:get(<<"id">>, Params0, list_to_binary(emqx_utils:gen_id(8))) of
        <<>> ->
            {400, #{code => 'BAD_REQUEST', message => <<"empty rule id is not allowed">>}};
        Id when is_binary(Id) ->
            Params = filter_out_request_body(add_metadata(Params0)),
            case emqx_rule_engine:get_rule(Namespace, Id) of
                {ok, _Rule} ->
                    ?BAD_REQUEST(<<"rule id already exists">>);
                not_found ->
                    UpdateRes = emqx_rule_engine_config:create_or_update_rule(
                        Namespace,
                        Id,
                        Params
                    ),
                    case UpdateRes of
                        {ok, #{post_config_update := #{emqx_rule_engine_config := Rule}}} ->
                            FormatFn = mk_format_fn(Namespace),
                            ?CREATED(FormatFn(Rule));
                        {error, Reason} ->
                            ?SLOG(
                                info,
                                #{
                                    msg => "create_rule_failed",
                                    namespace => Namespace,
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

'/rules/:id/test'(post, #{body := Params, bindings := #{id := RuleId}} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    ?CHECK_PARAMS(
        Params,
        rule_apply_test,
        case emqx_rule_sqltester:apply_rule(Namespace, RuleId, CheckedParams) of
            {ok, Result} ->
                {200, emqx_utils_json:best_effort_json_obj(Result)};
            {error, {parse_error, Reason}} ->
                {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
            {error, nomatch} ->
                {412, #{code => 'NOT_MATCH', message => <<"SQL Not Match">>}};
            {error, rule_not_found} ->
                {404, #{code => 'RULE_NOT_FOUND', message => <<"The rule could not be found">>}};
            {error, Reason} ->
                {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
        end
    ).

'/rules/:id'(get, #{bindings := #{id := Id}} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case emqx_rule_engine:get_rule(Namespace, Id) of
        {ok, Rule} ->
            FormatFn = mk_format_fn(Namespace),
            {200, FormatFn(Rule)};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end;
'/rules/:id'(put, #{bindings := #{id := Id}, body := Params0} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    Params1 = filter_out_request_body(Params0),
    Params2 = ensure_last_modified_at(Params1),
    case emqx_rule_engine_config:get_raw_rule(Namespace, Id) of
        {ok, RawRule0} ->
            Params = maps:merge(RawRule0, Params2),
            UpdateRes = emqx_rule_engine_config:create_or_update_rule(
                Namespace,
                Id,
                Params
            ),
            case UpdateRes of
                {ok, #{post_config_update := #{emqx_rule_engine_config := Rule}}} ->
                    FormatFn = mk_format_fn(Namespace),
                    {200, FormatFn(Rule)};
                {error, Reason} ->
                    ?SLOG(
                        info,
                        #{
                            msg => "update_rule_failed",
                            namespace => Namespace,
                            rule_id => Id,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
                    {400, #{code => 'BAD_REQUEST', message => ?ERR_BADARGS(Reason)}}
            end;
        {error, not_found} ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end;
'/rules/:id'(delete, #{bindings := #{id := Id}} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case emqx_rule_engine:get_rule(Namespace, Id) of
        {ok, _Rule} ->
            UpdateRes = emqx_rule_engine_config:delete_rule(
                Namespace,
                Id
            ),
            case UpdateRes of
                {ok, _} ->
                    {204};
                {error, Reason} ->
                    ?SLOG(
                        error,
                        #{
                            msg => "delete_rule_failed",
                            namespace => Namespace,
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

'/rules/:id/metrics'(get, #{bindings := #{id := Id}} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case emqx_rule_engine:get_rule(Namespace, Id) of
        {ok, Rule} ->
            RuleResId = emqx_rule_engine:rule_resource_id(Rule),
            NodeMetrics = get_rule_metrics(RuleResId),
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

'/rules/:id/metrics/reset'(put, #{bindings := #{id := Id}} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case emqx_rule_engine:get_rule(Namespace, Id) of
        {ok, Rule} ->
            RuleResId = emqx_rule_engine:rule_resource_id(Rule),
            ok = emqx_rule_engine_proto_v1:reset_metrics(RuleResId),
            {204};
        not_found ->
            {404, #{code => 'NOT_FOUND', message => <<"Rule Id Not Found">>}}
    end.

'/rule_engine'(get, Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    {200, format_rule_engine_resp(get_config(Namespace, [rule_engine], #{}))};
'/rule_engine'(put, #{body := Params} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case rule_engine_update(Namespace, Params) of
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
    case emqx_utils_json:safe_encode(#{RuleError => Reason}) of
        {ok, Json} ->
            Json;
        _ ->
            {RuleError, Reason}
    end.

mk_format_fn(Namespace) ->
    SummaryIndex =
        maybe
            {ok, Summary} = emqx_bridge_v2_api:do_handle_summary(Namespace, actions),
            lists:foldl(
                fun(#{name := N, type := T, status := S}, Acc) ->
                    Acc#{{T, N} => S}
                end,
                #{},
                Summary
            )
        else
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "failed_to_fetch_action_summary_for_rules", reason => Reason
                }),
                #{}
        end,
    fun(Row) ->
        ?MODULE:format_rule_info_resp(Row, #{action_summary_index => SummaryIndex})
    end.

format_rule_info_resp({?KEY(_Namespace, Id), Rule}, Context) ->
    format_rule_info_resp(Rule#{id => Id}, Context);
format_rule_info_resp(
    #{
        id := Id,
        name := Name,
        created_at := CreatedAt,
        updated_at := LastModifiedAt,
        from := Topics,
        actions := Actions,
        sql := SQL,
        enable := Enable,
        description := Descr
    },
    Context
) ->
    #{
        id => Id,
        name => Name,
        from => Topics,
        actions => format_action(Actions),
        action_details => format_action_details(Actions, Context),
        sql => SQL,
        enable => Enable,
        created_at => format_datetime(CreatedAt, millisecond),
        last_modified_at => format_datetime(LastModifiedAt, millisecond),
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

format_action_details(RuleActions, Context) ->
    #{action_summary_index := SummaryIndex} = Context,
    Actions0 =
        lists:filtermap(
            fun
                ({bridge, Type, Name}) ->
                    {true, {bin(Type), bin(Name)}};
                ({bridge_v2, Type, Name}) ->
                    {true, {bin(Type), bin(Name)}};
                (_) ->
                    false
            end,
            RuleActions
        ),
    Actions = lists:sort(Actions0),
    lists:foldl(
        fun({T, N}, Acc) ->
            case SummaryIndex of
                #{{T, N} := S} ->
                    [#{type => T, name => N, status => S} | Acc];
                _ ->
                    [#{type => T, name => N, status => not_found} | Acc]
            end
        end,
        [],
        Actions
    ).

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
    NowMS = emqx_rule_engine:now_ms(),
    Params#{
        <<"metadata">> => #{
            <<"created_at">> => NowMS,
            <<"last_modified_at">> => NowMS
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

-spec mk_qs2ms_fn(maybe_namespace()) ->
    fun((atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter()).
mk_qs2ms_fn(Namespace) ->
    fun(_Tab, {Qs0, Fuzzy0}) ->
        {Qs1, Fuzzy} = adapt_custom_filters(Qs0, Fuzzy0),
        case lists:keytake(from, 1, Qs1) of
            false ->
                #{
                    match_spec => generate_match_spec(Namespace, Qs1),
                    fuzzy_fun => fuzzy_match_fun(Fuzzy)
                };
            {value, {from, '=:=', From}, Qs2} ->
                #{
                    match_spec => generate_match_spec(Namespace, Qs2),
                    fuzzy_fun => fuzzy_match_fun([{from, '=:=', From} | Fuzzy])
                }
        end
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

generate_match_spec(Namespace, Qs) ->
    {MatchHead, Conds} = generate_match_spec(Qs, 2, {#{}, []}),
    [{{?KEY(Namespace, '_'), MatchHead}, Conds, ['$_']}].

generate_match_spec([], _, {MatchHead, Conds}) ->
    {MatchHead, lists:reverse(Conds)};
generate_match_spec([Qs | Rest], N, {MatchHead, Conds}) ->
    Holder = list_to_atom([$$ | integer_to_list(N)]),
    NMatchHead = emqx_mgmt_util:merge_maps(MatchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    generate_match_spec(Rest, N + 1, {NMatchHead, NConds}).

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
run_fuzzy_match(E = {?KEY(_, Id), _}, [{id, like, Pattern} | Fuzzy]) ->
    binary:match(Id, Pattern) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{description := Desc}}, [{description, like, Pattern} | Fuzzy]) ->
    binary:match(Desc, Pattern) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{from := Topics}}, [{from, '=:=', Pattern} | Fuzzy]) ->
    lists:member(Pattern, Topics) /= false andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{from := Topics}}, [{from, match, Pattern} | Fuzzy]) ->
    lists:any(fun(For) -> emqx_topic:match(For, Pattern) end, Topics) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{from := Topics}}, [{from, like, Pattern} | Fuzzy]) ->
    lists:any(fun(For) -> binary:match(For, Pattern) /= nomatch end, Topics) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{actions := Actions}}, [{action, in, ActionIds} | Fuzzy]) ->
    lists:any(fun(AId) -> lists:member(AId, Actions) end, ActionIds) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, #{from := Froms}}, [{source, in, SourceIds} | Fuzzy]) ->
    lists:any(fun(SId) -> lists:member(SId, Froms) end, SourceIds) andalso
        run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E, [_ | Fuzzy]) ->
    run_fuzzy_match(E, Fuzzy).

rule_engine_update(Namespace, Params0) ->
    case emqx_rule_api_schema:check_params(Params0, rule_engine) of
        {ok, _CheckedParams} ->
            Rules = emqx_rule_engine_config:list_raw_rules(Namespace),
            Params = Params0#{<<"rules">> => Rules},
            {ok, #{config := Config}} = emqx_conf:update(
                [?ROOT_KEY],
                Params,
                with_namespace(#{override_to => cluster}, Namespace)
            ),
            {ok, Config};
        {error, Reason} ->
            {error, Reason}
    end.

ensure_last_modified_at(RawConfig) ->
    emqx_utils_maps:deep_merge(
        RawConfig,
        #{<<"metadata">> => #{<<"last_modified_at">> => emqx_rule_engine:now_ms()}}
    ).

bin(X) -> emqx_utils_conv:bin(X).

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.

get_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_namespaced_config(Namespace, KeyPath, Default);
get_config(?global_ns, KeyPath, Default) ->
    emqx:get_config(KeyPath, Default).
