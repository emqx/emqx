%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_runtime).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("emqx/include/emqx_external_trace.hrl").
-include_lib("emqx_resource/include/emqx_resource_errors.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    apply_rule/3,
    apply_rules/3,
    eval_action_reply_to/2
]).

%% Internal exports used by schema validation and message transformation.
-export([evaluate_select/3, clear_rule_payload/0]).

-import(
    emqx_rule_maps,
    [
        nested_get/2,
        range_gen/2,
        range_get/3
    ]
).

-compile({no_auto_import, [alias/2]}).

-type columns() :: map().
-type alias() :: atom().
-type collection() :: {alias(), [term()]}.
-type rule() :: emqx_rule_engine:rule().

-elvis([
    {elvis_style, invalid_dynamic_call, #{ignore => [emqx_rule_runtime]}},
    {elvis_style, used_ignored_variable, disable}
]).

-define(ephemeral_alias(TYPE, NAME),
    iolist_to_binary(io_lib:format("_v_~ts_~p_~p", [TYPE, NAME, erlang:system_time()]))
).

-define(IS_RES_DOWN(R), R == stopped; R == not_connected; R == not_found; R == unhealthy_target).

%%------------------------------------------------------------------------------
%% Apply rules
%%------------------------------------------------------------------------------
-spec apply_rules(
    list(#{
        rule => rule(),
        trigger := binary(),
        matched := binary()
    }),
    columns(),
    envs()
) -> ok.
apply_rules([], _Columns, _Envs) ->
    ?tp("rule_engine_applied_all_rules", #{}),
    ok;
apply_rules([#{rule := #{enable := false, id := RuleId}} | More], Columns, Envs) ->
    ?TRACE("RULE", "skip_apply_disabled_rule", #{rule_id => RuleId}),
    apply_rules(More, Columns, Envs);
apply_rules([RichedRule | More], Columns, Envs) ->
    apply_rule_discard_result(RichedRule, Columns, Envs),
    apply_rules(More, Columns, Envs).

apply_rule_discard_result(RichedRule, Columns, Envs) ->
    _ = apply_rule(RichedRule, Columns, Envs),
    ok.

apply_rule(
    #{rule := #{id := RuleId, namespace := Namespace}} = RichedRule,
    Columns,
    RawEnvs
) ->
    %% add metadata before the rule is applied
    %% but OTEL trace should not be included in metadata
    ?EXT_TRACE_APPLY_RULE(
        ?EXT_TRACE_ATTR(rule_attrs(RichedRule)),
        fun(Envs) ->
            do_apply_rule(
                RichedRule,
                add_metadata(Columns, #{rule_id => RuleId, namespace => Namespace}),
                add_metadata(Envs, maps:with([trigger, matched], RichedRule))
            )
        end,
        [RawEnvs]
    ).

do_apply_rule(
    #{
        rule := #{id := _, namespace := _} = Rule
    } = RichedRule,
    Columns,
    Envs
) ->
    RuleResId = emqx_rule_engine:rule_resource_id(Rule),
    PrevProcessMetadata = logger:get_process_metadata(),
    set_process_trace_metadata(RichedRule, Columns),
    trace_rule_sql(
        "rule_activated",
        #{
            input => Columns, environment => Envs
        },
        debug
    ),
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'matched'),
    clear_rule_payload(),
    try
        do_apply_rule2(Rule, Columns, Envs)
    catch
        %% ignore the errors if select or match failed
        _:Reason = {select_and_transform_error, Error} ->
            ?EXT_TRACE_SET_STATUS_ERROR(select_and_transform_error),
            ok = metrics_inc_exception(RuleResId),
            trace_rule_sql(
                "SELECT_clause_exception",
                #{
                    reason => Error
                },
                warning
            ),
            {error, Reason};
        _:Reason = {match_conditions_error, Error} ->
            ?EXT_TRACE_SET_STATUS_ERROR(match_conditions_error),
            ok = metrics_inc_exception(RuleResId),
            trace_rule_sql(
                "WHERE_clause_exception",
                #{
                    reason => Error
                },
                warning
            ),
            {error, Reason};
        _:Reason = {select_and_collect_error, Error} ->
            ?EXT_TRACE_SET_STATUS_ERROR(select_and_collect_error),
            ok = metrics_inc_exception(RuleResId),
            trace_rule_sql(
                "FOREACH_clause_exception",
                #{
                    reason => Error
                },
                warning
            ),
            {error, Reason};
        _:Reason = {match_incase_error, Error} ->
            ?EXT_TRACE_SET_STATUS_ERROR(match_incase_error),
            ok = metrics_inc_exception(RuleResId),
            trace_rule_sql(
                "INCASE_clause_exception",
                #{
                    reason => Error
                },
                warning
            ),
            {error, Reason};
        Class:Error:StkTrace ->
            ?EXT_TRACE_SET_STATUS_ERROR(emqx_utils:readable_error_msg(Error)),
            ok = metrics_inc_exception(RuleResId),
            trace_rule_sql(
                "apply_rule_failed",
                #{
                    exception => Class,
                    reason => Error,
                    stacktrace => StkTrace
                },
                error
            ),
            {error, {Error, StkTrace}}
    after
        reset_logger_process_metadata(PrevProcessMetadata)
    end.

set_process_trace_metadata(
    #{rule := #{id := RuleId, namespace := Namespace}} = RichedRule,
    Columns
) ->
    Metadata = maps:merge(
        maps:with([trigger, matched], RichedRule),
        maps:with([clientid], Columns)
    ),
    logger:update_process_metadata(Metadata#{
        rule_id => RuleId,
        namespace => Namespace,
        rule_trigger_ts => [rule_trigger_time(Columns)]
    }).

reset_logger_process_metadata(undefined = _PrevProcessMetadata) ->
    logger:unset_process_metadata();
reset_logger_process_metadata(PrevProcessMetadata) ->
    logger:set_process_metadata(PrevProcessMetadata).

rule_trigger_time(Columns) ->
    case Columns of
        #{timestamp := Timestamp} ->
            Timestamp;
        _ ->
            erlang:system_time(millisecond)
    end.

do_apply_rule2(
    #{
        id := _RuleId,
        is_foreach := true,
        fields := Fields,
        doeach := DoEach,
        incase := InCase,
        conditions := Conditions,
        actions := Actions
    } = Rule,
    Columns,
    Envs
) ->
    RuleResId = emqx_rule_engine:rule_resource_id(Rule),
    case evaluate_foreach(Fields, Columns, Conditions, InCase, DoEach) of
        {ok, ColumnsAndSelected, FinalCollection} ->
            case FinalCollection of
                [] ->
                    trace_rule_sql("SQL_yielded_no_result"),
                    ok = metrics_inc_no_result(RuleResId);
                _ ->
                    trace_rule_sql(
                        "SQL_yielded_result", #{result => FinalCollection}, debug
                    ),
                    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'passed')
            end,
            NewEnvs = emqx_utils_maps:deep_merge(ColumnsAndSelected, Envs),
            {ok, [handle_action_list(Rule, Actions, Coll, NewEnvs) || Coll <- FinalCollection]};
        false ->
            trace_rule_sql("SQL_yielded_no_result"),
            ok = metrics_inc_no_result(RuleResId),
            {error, nomatch}
    end;
do_apply_rule2(
    #{
        id := _RuleId,
        is_foreach := false,
        fields := Fields,
        conditions := Conditions,
        actions := Actions
    } = Rule,
    Columns,
    Envs
) ->
    RuleResId = emqx_rule_engine:rule_resource_id(Rule),
    case evaluate_select(Fields, Columns, Conditions) of
        {ok, Selected} ->
            trace_rule_sql("SQL_yielded_result", #{result => Selected}, debug),
            ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'passed'),
            {ok,
                handle_action_list(
                    Rule, Actions, Selected, emqx_utils_maps:deep_merge(Columns, Envs)
                )};
        false ->
            trace_rule_sql("SQL_yielded_no_result"),
            ok = metrics_inc_no_result(RuleResId),
            {error, nomatch}
    end.

evaluate_select(Fields, Columns, Conditions) ->
    Selected = ?RAISE(
        select_and_transform(Fields, Columns),
        {select_and_transform_error, {EXCLASS, EXCPTION, ST}}
    ),
    case
        ?RAISE(
            match_conditions(Conditions, maps:merge(Columns, Selected)),
            {match_conditions_error, {EXCLASS, EXCPTION, ST}}
        )
    of
        true ->
            {ok, Selected};
        false ->
            false
    end.

evaluate_foreach(Fields, Columns, Conditions, InCase, DoEach) ->
    {Selected, Collection} = ?RAISE(
        select_and_collect(Fields, Columns),
        {select_and_collect_error, {EXCLASS, EXCPTION, ST}}
    ),
    ColumnsAndSelected = maps:merge(Columns, Selected),
    case
        ?RAISE(
            match_conditions(Conditions, ColumnsAndSelected),
            {match_conditions_error, {EXCLASS, EXCPTION, ST}}
        )
    of
        true ->
            FinalCollection = filter_collection(ColumnsAndSelected, InCase, DoEach, Collection),
            {ok, ColumnsAndSelected, FinalCollection};
        false ->
            false
    end.

clear_rule_payload() ->
    erlang:erase(rule_payload).

%% SELECT Clause
select_and_transform(Fields, Columns) ->
    select_and_transform(Fields, Columns, #{}).

select_and_transform([], _Columns, Action) ->
    Action;
select_and_transform(['*' | More], Columns, Action) ->
    select_and_transform(More, Columns, maps:merge(Action, Columns));
select_and_transform([{as, Field, Alias} | More], Columns, Action) ->
    Val = eval(Field, [Action, Columns]),
    select_and_transform(
        More,
        Columns,
        nested_put(Alias, Val, Action)
    );
select_and_transform([Field | More], Columns, Action) ->
    Val = eval(Field, [Action, Columns]),
    Key = alias(Field, Columns),
    select_and_transform(
        More,
        Columns,
        nested_put(Key, Val, Action)
    ).

%% FOREACH Clause
-spec select_and_collect(list(), columns()) -> {columns(), collection()}.
select_and_collect(Fields, Columns) ->
    select_and_collect(Fields, Columns, {#{}, {'item', []}}).

select_and_collect([{as, Field, {_, A} = Alias}], Columns, {Action, _}) ->
    Val = eval(Field, [Action, Columns]),
    {nested_put(Alias, Val, Action), {A, ensure_list(Field, Val)}};
select_and_collect([{as, Field, Alias} | More], Columns, {Action, LastKV}) ->
    Val = eval(Field, [Action, Columns]),
    select_and_collect(
        More,
        nested_put(Alias, Val, Columns),
        {nested_put(Alias, Val, Action), LastKV}
    );
select_and_collect([Field], Columns, {Action, _}) ->
    Val = eval(Field, [Action, Columns]),
    Key = alias(Field, Columns),
    {nested_put(Key, Val, Action), {'item', ensure_list(Field, Val)}};
select_and_collect([Field | More], Columns, {Action, LastKV}) ->
    Val = eval(Field, [Action, Columns]),
    Key = alias(Field, Columns),
    select_and_collect(
        More,
        Columns,
        {nested_put(Key, Val, Action), LastKV}
    ).

%% Filter each item got from FOREACH
filter_collection(Columns, InCase, DoEach, {CollKey, CollVal}) ->
    lists:filtermap(
        fun(Item) ->
            ColumnsAndItem = maps:merge(Columns, #{CollKey => Item}),
            case
                ?RAISE(
                    match_conditions(InCase, ColumnsAndItem),
                    {match_incase_error, {EXCLASS, EXCPTION, ST}}
                )
            of
                true when DoEach == [] -> {true, ColumnsAndItem};
                true ->
                    {true,
                        ?RAISE(
                            select_and_transform(DoEach, ColumnsAndItem),
                            {doeach_error, {EXCLASS, EXCPTION, ST}}
                        )};
                false ->
                    false
            end
        end,
        CollVal
    ).

%% Conditional Clauses such as WHERE, WHEN.
match_conditions({'and', L, R}, Data) ->
    match_conditions(L, Data) andalso match_conditions(R, Data);
match_conditions({'or', L, R}, Data) ->
    match_conditions(L, Data) orelse match_conditions(R, Data);
match_conditions({'not', Var}, Data) ->
    case eval(Var, Data) of
        Bool when is_boolean(Bool) ->
            not Bool;
        _Other ->
            false
    end;
match_conditions({in, Var, {list, Vals}}, Data) ->
    lists:member(eval(Var, Data), [eval(V, Data) || V <- Vals]);
match_conditions({'fun', {_, Name}, Args}, Data) ->
    apply_func(Name, [eval(Arg, Data) || Arg <- Args], Data);
match_conditions({Op, L, R}, Data) when ?is_comp(Op) ->
    compare(Op, eval(L, Data), eval(R, Data));
match_conditions({const, true}, _Data) ->
    true;
match_conditions({const, false}, _Data) ->
    false;
match_conditions({}, _Data) ->
    true.

%% compare to an undefined variable
compare(Op, undefined, undefined) ->
    do_compare(Op, undefined, undefined);
compare(_Op, L, R) when L == undefined; R == undefined ->
    false;
%% comparing numbers against strings
compare(Op, L, R) when is_number(L), is_binary(R) ->
    do_compare(Op, L, number(R));
compare(Op, L, R) when is_binary(L), is_number(R) ->
    do_compare(Op, number(L), R);
compare(Op, L, R) when is_atom(L), is_binary(R) ->
    do_compare(Op, atom_to_binary(L, utf8), R);
compare(Op, L, R) when is_binary(L), is_atom(R) ->
    do_compare(Op, L, atom_to_binary(R, utf8));
compare(Op, L, R) ->
    do_compare(Op, L, R).

do_compare('=', L, R) -> L == R;
do_compare('>', L, R) -> L > R;
do_compare('<', L, R) -> L < R;
do_compare('<=', L, R) -> L =< R;
do_compare('>=', L, R) -> L >= R;
do_compare('<>', L, R) -> L /= R;
do_compare('!=', L, R) -> L /= R;
do_compare('=~', T, F) -> emqx_topic:match(T, F).

number(Bin) ->
    try
        binary_to_integer(Bin)
    catch
        error:badarg -> binary_to_float(Bin)
    end.

handle_action_list(Rule, Actions, Selected, Envs) ->
    [handle_action(Rule, Act, Selected, Envs) || Act <- Actions].

handle_action(#{id := RuleId} = Rule, Act, Selected, Envs) ->
    NEnvs = ?EXT_TRACE_HANDLE_ACTION_START(
        ?EXT_TRACE_ATTR((action_attrs(Act))#{'rule.id' => RuleId}),
        Envs
    ),
    do_handle_action(Rule, Act, Selected, NEnvs).

do_handle_action(Rule, ActId, Selected, Envs) ->
    RuleResId = emqx_rule_engine:rule_resource_id(Rule),
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.total'),
    try
        do_handle_action2(Rule, ActId, Selected, Envs)
    catch
        throw:{discard, Reason} ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.discarded'),
            trace_action(ActId, "discarded", #{cause => Reason}, debug);
        error:?EMQX_TRACE_STOP_ACTION_MATCH = Reason ->
            ?EMQX_TRACE_STOP_ACTION(Explanation) = Reason,
            trace_action(
                ActId,
                "action_stopped_after_template_rendering",
                #{reason => Explanation}
            ),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.unknown');
        throw:{failed, unhealthy_target} ->
            ?tp("rule_runtime_unhealthy_target", #{}),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.out_of_service'),
            trace_action(ActId, "action_failed", #{reason => unhealthy_target}, error);
        Err:Reason:ST ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
            ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.unknown'),
            trace_action(
                ActId,
                "action_failed",
                #{
                    exception => Err,
                    reason => Reason,
                    stacktrace => ST
                },
                error
            )
    end.

do_handle_action2(#{} = Rule, {bridge, BridgeType, BridgeName, ResId} = Action, Selected, _Envs) ->
    trace_action_bridge("BRIDGE", Action, "bridge_action", #{}, debug),
    {TraceCtx, IncCtx} = do_handle_action_get_trace_inc_metrics_context(Rule, Action),
    ReplyTo = {
        fun ?MODULE:eval_action_reply_to/2,
        [IncCtx],
        ?EXT_TRACE_WITH_ACTION_METADATA(_Envs, #{reply_dropped => true})
    },
    case
        emqx_bridge:send_message(BridgeType, BridgeName, ResId, Selected, #{
            reply_to => ReplyTo, trace_ctx => TraceCtx
        })
    of
        {error, Reason} when Reason == bridge_not_found; Reason == bridge_disabled ->
            throw({discard, Reason});
        Result ->
            Result
    end;
do_handle_action2(
    #{namespace := Namespace} = Rule,
    {bridge_v2, BridgeType, BridgeName} = Action,
    Selected,
    _Envs
) ->
    trace_action_bridge("BRIDGE", Action, "bridge_action", #{}, debug),
    {TraceCtx, IncCtx} = do_handle_action_get_trace_inc_metrics_context(Rule, Action),
    ReplyTo = {
        fun ?MODULE:eval_action_reply_to/2,
        [IncCtx],
        ?EXT_TRACE_WITH_ACTION_METADATA(_Envs, #{reply_dropped => true})
    },
    case
        emqx_bridge_v2:send_message(
            Namespace,
            BridgeType,
            BridgeName,
            Selected,
            #{reply_to => ReplyTo, trace_ctx => TraceCtx}
        )
    of
        {error, Reason} when Reason == bridge_not_found; Reason == bridge_disabled ->
            throw({discard, Reason});
        {error, {resource_error, #{reason := unhealthy_target}}} ->
            throw({failed, unhealthy_target});
        Result ->
            Result
    end;
do_handle_action2(#{} = Rule, #{mod := Mod, func := Func} = Action, Selected, Envs) ->
    trace_action(Action, "call_action_function"),
    %% the function can also throw 'out_of_service'
    Args = maps:get(args, Action, []),
    PrevProcessMetadata =
        case logger:get_process_metadata() of
            undefined -> #{};
            D -> D
        end,
    Result =
        try
            logger:update_process_metadata(#{action_id => Action}),
            Mod:Func(Selected, Envs, Args)
        after
            logger:set_process_metadata(PrevProcessMetadata)
        end,
    {_, IncCtx} = do_handle_action_get_trace_inc_metrics_context(Rule, Action),
    eval_action_reply_to(
        IncCtx, ?EXT_TRACE_WITH_ACTION_METADATA(Envs, #{result => Result})
    ),
    Result.

do_handle_action_get_trace_inc_metrics_context(Rule, Action) ->
    #{id := RuleId, namespace := Namespace} = Rule,
    RuleResId = emqx_rule_engine:rule_resource_id(Rule),
    case {emqx_trace:list(), logger:get_process_metadata()} of
        {[], #{stop_action_after_render := true}} ->
            %% Even if there is no trace we still need to pass
            %% stop_action_after_render in the trace meta data so that the
            %% action will be stopped.
            {
                #{
                    stop_action_after_render => true
                },
                #{
                    rule_id => RuleId,
                    rule_res_id => RuleResId,
                    namespace => Namespace,
                    action_id => Action
                }
            };
        {[], _} ->
            %% As a performance/memory optimization, we don't create any trace
            %% context if there are no trace patterns.
            {#{}, #{
                rule_id => RuleId,
                namespace => Namespace,
                rule_res_id => RuleResId,
                action_id => Action
            }};
        {_List, TraceMeta} ->
            Ctx0 = do_handle_action_get_trace_inc_metrics_context_unconditionally(
                Action, TraceMeta
            ),
            Ctx = Ctx0#{rule_id => RuleId, rule_res_id => RuleResId, namespace => Namespace},
            {maps:remove(action_id, Ctx), Ctx}
    end.

do_handle_action_get_trace_inc_metrics_context_unconditionally(Action, TraceMeta) ->
    StopAfterRenderMap =
        case maps:get(stop_action_after_render, TraceMeta, false) of
            false ->
                #{};
            true ->
                #{stop_action_after_render => true}
        end,
    case TraceMeta of
        #{
            rule_id := RuleId,
            clientid := ClientID,
            rule_trigger_ts := Timestamp
        } ->
            maps:merge(
                #{
                    rule_id => RuleId,
                    clientid => ClientID,
                    action_id => Action,
                    rule_trigger_ts => Timestamp
                },
                StopAfterRenderMap
            );
        #{
            rule_id := RuleId,
            rule_trigger_ts := Timestamp
        } ->
            maps:merge(
                #{
                    rule_id => RuleId,
                    action_id => Action,
                    rule_trigger_ts => Timestamp
                },
                StopAfterRenderMap
            )
    end.

action_info({bridge, BridgeType, BridgeName, _ResId}) ->
    #{type => BridgeType, name => BridgeName};
action_info({bridge_v2, BridgeType, BridgeName}) ->
    #{type => BridgeType, name => BridgeName};
action_info(FuncInfoMap) ->
    FuncInfoMap.

eval({Op, _} = Exp, Context) when is_list(Context) andalso (Op == path orelse Op == var) ->
    case Context of
        [Columns] ->
            eval(Exp, Columns);
        [Columns | Rest] ->
            case eval(Exp, Columns) of
                undefined -> eval(Exp, Rest);
                Val -> Val
            end
    end;
eval({path, [{key, <<"payload">>} | Path]}, #{payload := Payload}) ->
    nested_get({path, Path}, maybe_decode_payload(Payload));
eval({path, [{key, <<"payload">>} | Path]}, #{<<"payload">> := Payload}) ->
    nested_get({path, Path}, maybe_decode_payload(Payload));
eval({path, _} = Path, Columns) ->
    nested_get(Path, Columns);
eval({range, {Begin, End}}, _Columns) ->
    range_gen(Begin, End);
eval({get_range, {Begin, End}, Data}, Columns) ->
    range_get(Begin, End, eval(Data, Columns));
eval({var, _} = Var, Columns) ->
    nested_get(Var, Columns);
eval({const, Val}, _Columns) ->
    Val;
%% unary add
eval({'+', L}, Columns) ->
    eval(L, Columns);
%% unary subtract
eval({'-', L}, Columns) ->
    -(eval(L, Columns));
eval({Op, L, R}, Columns) when ?is_arith(Op) ->
    apply_func(Op, [eval(L, Columns), eval(R, Columns)], Columns);
eval({Op, L, R}, Columns) when ?is_comp(Op) ->
    compare(Op, eval(L, Columns), eval(R, Columns));
eval({list, List}, Columns) ->
    [eval(L, Columns) || L <- List];
eval({'case', <<>>, CaseClauses, ElseClauses}, Columns) ->
    eval_case_clauses(CaseClauses, ElseClauses, Columns);
eval({'case', CaseOn, CaseClauses, ElseClauses}, Columns) ->
    eval_switch_clauses(CaseOn, CaseClauses, ElseClauses, Columns);
eval({'fun', {_, Name}, Args}, Columns) ->
    apply_func(Name, [eval(Arg, Columns) || Arg <- Args], Columns).

%% the payload maybe is JSON data, decode it to a `map` first for nested put
ensure_decoded_payload({path, [{key, payload} | _]}, #{payload := Payload} = Columns) ->
    Columns#{payload => maybe_decode_payload(Payload)};
ensure_decoded_payload(
    {path, [{key, <<"payload">>} | _]}, #{<<"payload">> := Payload} = Columns
) ->
    Columns#{<<"payload">> => maybe_decode_payload(Payload)};
ensure_decoded_payload(_, Columns) ->
    Columns.

alias({var, Var}, _Columns) ->
    {var, Var};
alias({const, Val}, _Columns) when is_binary(Val) ->
    {var, Val};
alias({list, L}, _Columns) ->
    {var, ?ephemeral_alias(list, length(L))};
alias({range, R}, _Columns) ->
    {var, ?ephemeral_alias(range, R)};
alias({get_range, _, {var, Key}}, _Columns) ->
    {var, Key};
alias({get_range, _, {path, _Path} = Path}, Columns) ->
    handle_path_alias(Path, Columns);
alias({path, _Path} = Path, Columns) ->
    handle_path_alias(Path, Columns);
alias({const, Val}, _Columns) ->
    {var, ?ephemeral_alias(const, Val)};
alias({Op, _L, _R}, _Columns) when ?is_arith(Op); ?is_comp(Op) ->
    {var, ?ephemeral_alias(op, Op)};
alias({'case', On, _, _}, _Columns) ->
    {var, ?ephemeral_alias('case', On)};
alias({'fun', Name, _}, _Columns) ->
    {var, ?ephemeral_alias('fun', Name)};
alias(_, _Columns) ->
    ?ephemeral_alias(unknown, unknown).

handle_path_alias({path, [{key, <<"payload">>} | Rest]}, #{payload := _Payload} = _Columns) ->
    {path, [{key, payload} | Rest]};
handle_path_alias(Path, _Columns) ->
    Path.

eval_case_clauses([], ElseClauses, Columns) ->
    case ElseClauses of
        {} -> undefined;
        _ -> eval(ElseClauses, Columns)
    end;
eval_case_clauses([{Cond, Clause} | CaseClauses], ElseClauses, Columns) ->
    case match_conditions(Cond, Columns) of
        true ->
            eval(Clause, Columns);
        _ ->
            eval_case_clauses(CaseClauses, ElseClauses, Columns)
    end.

eval_switch_clauses(_CaseOn, [], ElseClauses, Columns) ->
    case ElseClauses of
        {} -> undefined;
        _ -> eval(ElseClauses, Columns)
    end;
eval_switch_clauses(CaseOn, [{Cond, Clause} | CaseClauses], ElseClauses, Columns) ->
    ConResult = eval(Cond, Columns),
    case eval(CaseOn, Columns) of
        ConResult ->
            eval(Clause, Columns);
        _ ->
            eval_switch_clauses(CaseOn, CaseClauses, ElseClauses, Columns)
    end.

apply_func(Name, Args, Columns) when is_binary(Name) ->
    FuncName = parse_function_name(?DEFAULT_SQL_FUNC_PROVIDER, Name),
    apply_func(FuncName, Args, Columns);
apply_func([{key, ModuleName0}, {key, FuncName0}], Args, Columns) ->
    ModuleName = parse_module_name(ModuleName0),
    FuncName = parse_function_name(ModuleName, FuncName0),
    do_apply_func(ModuleName, FuncName, Args, Columns);
apply_func(Name, Args, Columns) when is_atom(Name) ->
    do_apply_func(?DEFAULT_SQL_FUNC_PROVIDER, Name, Args, Columns);
apply_func(Other, _, _) ->
    ?RAISE_BAD_SQL(#{
        reason => bad_sql_function_reference,
        reference => Other
    }).

do_apply_func(Module, Name, Args, Columns) ->
    try
        case erlang:apply(Module, Name, Args) of
            Func when is_function(Func) ->
                erlang:apply(Func, [Columns]);
            Result ->
                Result
        end
    catch
        error:function_clause ->
            ?RAISE_BAD_SQL(#{
                reason => bad_sql_function_argument,
                arguments => Args,
                function_name => Name
            })
    end.

add_metadata(Columns, Metadata) when is_map(Columns), is_map(Metadata) ->
    NewMetadata = maps:merge(maps:get(metadata, Columns, #{}), Metadata),
    Columns#{metadata => NewMetadata}.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
maybe_decode_payload(Payload) when is_binary(Payload) ->
    case get_cached_payload() of
        undefined -> safe_decode_and_cache(Payload);
        DecodedP -> DecodedP
    end;
maybe_decode_payload(Payload) ->
    Payload.

get_cached_payload() ->
    erlang:get(rule_payload).

cache_payload(DecodedP) ->
    erlang:put(rule_payload, DecodedP),
    DecodedP.

safe_decode_and_cache(MaybeJson) ->
    try
        cache_payload(emqx_utils_json:decode(MaybeJson))
    catch
        _:_ -> error({decode_json_failed, MaybeJson})
    end.

ensure_list({var, <<"payload">>}, Payload) ->
    ensure_list(maybe_decode_payload(Payload));
ensure_list(_, Any) ->
    ensure_list(Any).

ensure_list(List) when is_list(List) -> List;
ensure_list(_NotList) -> [].

nested_put(Alias, Val, Columns0) ->
    Columns = ensure_decoded_payload(Alias, Columns0),
    emqx_rule_maps:nested_put(Alias, Val, Columns).

eval_action_reply_to(_TraceCtx, #{is_fallback := true} = _RequestContext) ->
    %% If this is the result of running a fallback action, we don't want to bump any
    %% metrics from the rule containing the primary action that triggered this.
    %% XXX:
    %% FOR OTEL fallback actions, should end it's span here or in buffer worker?
    ?EXT_TRACE_HANDLE_ACTION_STOP(#{}, _RequestContext),
    ok;
eval_action_reply_to(TraceCtx, #{result := Result} = _RequestContext) ->
    %% end otel span `handle_action_end' here
    do_eval_action_reply_to(TraceCtx, Result),
    ?EXT_TRACE_HANDLE_ACTION_STOP(#{}, _RequestContext),
    Result.

do_eval_action_reply_to(TraceCtx, Result) ->
    SavedMetaData = logger:get_process_metadata(),
    try
        %% To not pollute the trace we temporary remove the process meta data
        logger:unset_process_metadata(),
        _ = do_inc_action_metrics(TraceCtx, Result)
    after
        %% Setting process metadata to undefined yields an error
        case SavedMetaData of
            undefined ->
                ok;
            _ ->
                logger:set_process_metadata(SavedMetaData)
        end
    end,
    Result.

do_inc_action_metrics(
    #{rule_res_id := RuleResId, action_id := ActId} = TraceContext,
    {error, ?EMQX_TRACE_STOP_ACTION(Explanation) = _Reason}
) ->
    TraceContext1 = maps:without([action_id, rule_res_id], TraceContext),
    trace_action(
        ActId,
        "action_stopped_after_template_rendering",
        maps:merge(#{reason => Explanation}, TraceContext1)
    ),
    emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
    emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.unknown');
do_inc_action_metrics(
    #{rule_res_id := RuleResId, action_id := ActId},
    ?RESOURCE_ERROR_M(R, _)
) when ?IS_RES_DOWN(R) ->
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.out_of_service'),
    trace_action(ActId, "out_of_service", #{}, warning);
do_inc_action_metrics(
    #{rule_res_id := RuleResId, action_id := ActId} = TraceContext,
    {error, {recoverable_error, _}} = Reason
) ->
    FormatterRes = #emqx_trace_format_func_data{
        function = fun trace_formatted_result/1,
        data = {ActId, Reason}
    },
    TraceContext1 = maps:without([action_id, rule_res_id], TraceContext),
    trace_action(ActId, "out_of_service", TraceContext1#{reason => FormatterRes}),
    emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.out_of_service');
do_inc_action_metrics(
    #{rule_res_id := RuleResId, action_id := ActId} = TraceContext,
    {error, {unrecoverable_error, _}} = Reason
) ->
    TraceContext1 = maps:without([action_id, rule_res_id], TraceContext),
    FormatterRes = #emqx_trace_format_func_data{
        function = fun trace_formatted_result/1,
        data = {ActId, Reason}
    },
    trace_action(ActId, "action_failed", maps:merge(#{reason => FormatterRes}, TraceContext1)),
    emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
    emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.unknown');
do_inc_action_metrics(#{rule_res_id := RuleResId, action_id := ActId} = TraceContext, R) ->
    TraceContext1 = maps:without([action_id, rule_res_id], TraceContext),
    FormatterRes = #emqx_trace_format_func_data{
        function = fun trace_formatted_result/1,
        data = {ActId, R}
    },
    case is_ok_result(R) of
        false ->
            trace_action(
                ActId,
                "action_failed",
                maps:merge(#{reason => FormatterRes}, TraceContext1)
            ),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed'),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.failed.unknown');
        true ->
            trace_action(
                ActId,
                "action_success",
                maps:merge(#{result => FormatterRes}, TraceContext1)
            ),
            emqx_metrics_worker:inc(rule_metrics, RuleResId, 'actions.success')
    end.

trace_formatted_result({{bridge_v2, Type, _Name}, R}) ->
    ConnectorType = emqx_action_info:action_type_to_connector_type(Type),
    ResourceModule = emqx_connector_info:resource_callback_module(ConnectorType),
    clean_up_error_tuple(emqx_resource:call_format_query_result(ResourceModule, R));
trace_formatted_result({{bridge, BridgeType, _BridgeName, _ResId}, R}) ->
    BridgeV2Type = emqx_action_info:bridge_v1_type_to_action_type(BridgeType),
    ConnectorType = emqx_action_info:action_type_to_connector_type(BridgeV2Type),
    ResourceModule = emqx_connector_info:resource_callback_module(ConnectorType),
    clean_up_error_tuple(emqx_resource:call_format_query_result(ResourceModule, R));
trace_formatted_result({_, R}) ->
    R.

is_ok_result(ok) ->
    true;
is_ok_result({async_return, R}) ->
    is_ok_result(R);
is_ok_result(R) when is_tuple(R) ->
    ok == erlang:element(1, R);
is_ok_result(_) ->
    false.

clean_up_error_tuple({error, {unrecoverable_error, Reason}}) ->
    Reason;
clean_up_error_tuple({error, {recoverable_error, Reason}}) ->
    Reason;
clean_up_error_tuple({error, Reason}) ->
    Reason;
clean_up_error_tuple(Result) ->
    Result.

parse_module_name(Name) when is_binary(Name) ->
    case ?IS_VALID_SQL_FUNC_PROVIDER_MODULE_NAME(Name) of
        true ->
            ok;
        false ->
            ?RAISE_BAD_SQL(#{
                reason => sql_function_provider_module_not_allowed,
                module => Name
            })
    end,
    try
        parse_module_name(binary_to_existing_atom(Name, utf8))
    catch
        error:badarg ->
            ?RAISE_BAD_SQL(#{
                reason => sql_function_provider_module_not_loaded,
                module => Name
            })
    end;
parse_module_name(Name) when is_atom(Name) ->
    Name.

parse_function_name(Module, Name) when is_binary(Name) ->
    try
        parse_function_name(Module, binary_to_existing_atom(Name, utf8))
    catch
        error:badarg ->
            ?RAISE_BAD_SQL(#{
                reason => sql_function_not_supported,
                module => Module,
                function => Name
            })
    end;
parse_function_name(_Module, Name) when is_atom(Name) ->
    Name.

trace_action(ActId, Message) ->
    trace_action_bridge("ACTION", ActId, Message).

trace_action(ActId, Message, Extra) ->
    trace_action_bridge("ACTION", ActId, Message, Extra, debug).

trace_action(ActId, Message, Extra, Level) ->
    trace_action_bridge("ACTION", ActId, Message, Extra, Level).

trace_action_bridge(Tag, ActId, Message) ->
    trace_action_bridge(Tag, ActId, Message, #{}, debug).

trace_action_bridge(Tag, ActId, Message, Extra, Level) ->
    ?TRACE(
        Level,
        Tag,
        Message,
        maps:merge(
            #{
                action_info => action_info(ActId)
            },
            Extra
        )
    ).

trace_rule_sql(Message) ->
    trace_rule_sql(Message, #{}, debug).

trace_rule_sql(Message, Extra, Level) ->
    ?TRACE(
        Level,
        "RULE_SQL_EXEC",
        Message,
        Extra
    ).

metrics_inc_no_result(RuleResId) ->
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'failed.no_result'),
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'failed').

metrics_inc_exception(RuleResId) ->
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'failed.exception'),
    ok = emqx_metrics_worker:inc(rule_metrics, RuleResId, 'failed').

rule_attrs(Rule) ->
    emqx_external_trace:rule_attrs(Rule).

action_attrs(Action) ->
    emqx_external_trace:action_attrs(Action).
