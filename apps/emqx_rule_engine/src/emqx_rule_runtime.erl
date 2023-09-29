%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_runtime).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource_errors.hrl").

-export([
    apply_rule/3,
    apply_rules/3,
    inc_action_metrics/2
]).

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

-elvis([
    {elvis_style, invalid_dynamic_call, #{ignore => [emqx_rule_runtime]}}
]).

-define(ephemeral_alias(TYPE, NAME),
    iolist_to_binary(io_lib:format("_v_~ts_~p_~p", [TYPE, NAME, erlang:system_time()]))
).

%%------------------------------------------------------------------------------
%% Apply rules
%%------------------------------------------------------------------------------
-spec apply_rules(list(rule()), columns(), envs()) -> ok.
apply_rules([], _Columns, _Envs) ->
    ok;
apply_rules([#{enable := false} | More], Columns, Envs) ->
    apply_rules(More, Columns, Envs);
apply_rules([Rule | More], Columns, Envs) ->
    apply_rule_discard_result(Rule, Columns, Envs),
    apply_rules(More, Columns, Envs).

apply_rule_discard_result(Rule, Columns, Envs) ->
    _ = apply_rule(Rule, Columns, Envs),
    ok.

apply_rule(Rule = #{id := RuleID}, Columns, Envs) ->
    ok = emqx_metrics_worker:inc(rule_metrics, RuleID, 'matched'),
    clear_rule_payload(),
    try
        do_apply_rule(Rule, add_metadata(Columns, #{rule_id => RuleID}), Envs)
    catch
        %% ignore the errors if select or match failed
        _:Reason = {select_and_transform_error, Error} ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleID, 'failed.exception'),
            ?SLOG(warning, #{
                msg => "SELECT_clause_exception",
                rule_id => RuleID,
                reason => Error
            }),
            {error, Reason};
        _:Reason = {match_conditions_error, Error} ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleID, 'failed.exception'),
            ?SLOG(warning, #{
                msg => "WHERE_clause_exception",
                rule_id => RuleID,
                reason => Error
            }),
            {error, Reason};
        _:Reason = {select_and_collect_error, Error} ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleID, 'failed.exception'),
            ?SLOG(warning, #{
                msg => "FOREACH_clause_exception",
                rule_id => RuleID,
                reason => Error
            }),
            {error, Reason};
        _:Reason = {match_incase_error, Error} ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleID, 'failed.exception'),
            ?SLOG(warning, #{
                msg => "INCASE_clause_exception",
                rule_id => RuleID,
                reason => Error
            }),
            {error, Reason};
        Class:Error:StkTrace ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleID, 'failed.exception'),
            ?SLOG(error, #{
                msg => "apply_rule_failed",
                rule_id => RuleID,
                exception => Class,
                reason => Error,
                stacktrace => StkTrace
            }),
            {error, {Error, StkTrace}}
    end.

do_apply_rule(
    #{
        id := RuleId,
        is_foreach := true,
        fields := Fields,
        doeach := DoEach,
        incase := InCase,
        conditions := Conditions,
        actions := Actions
    },
    Columns,
    Envs
) ->
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
            Collection2 = filter_collection(ColumnsAndSelected, InCase, DoEach, Collection),
            case Collection2 of
                [] ->
                    ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'failed.no_result');
                _ ->
                    ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'passed')
            end,
            NewEnvs = maps:merge(ColumnsAndSelected, Envs),
            {ok, [handle_action_list(RuleId, Actions, Coll, NewEnvs) || Coll <- Collection2]};
        false ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'failed.no_result'),
            {error, nomatch}
    end;
do_apply_rule(
    #{
        id := RuleId,
        is_foreach := false,
        fields := Fields,
        conditions := Conditions,
        actions := Actions
    },
    Columns,
    Envs
) ->
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
            ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'passed'),
            {ok, handle_action_list(RuleId, Actions, Selected, maps:merge(Columns, Envs))};
        false ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'failed.no_result'),
            {error, nomatch}
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
    {nested_put(Alias, Val, Action), {A, ensure_list(Val)}};
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
    {nested_put(Key, Val, Action), {'item', ensure_list(Val)}};
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

handle_action_list(RuleId, Actions, Selected, Envs) ->
    [handle_action(RuleId, Act, Selected, Envs) || Act <- Actions].

handle_action(RuleId, ActId, Selected, Envs) ->
    ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.total'),
    try
        do_handle_action(RuleId, ActId, Selected, Envs)
    catch
        throw:out_of_service ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed'),
            ok = emqx_metrics_worker:inc(
                rule_metrics, RuleId, 'actions.failed.out_of_service'
            ),
            ?SLOG(warning, #{msg => "out_of_service", action => ActId});
        Err:Reason:ST ->
            ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed'),
            ok = emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed.unknown'),
            ?SLOG(error, #{
                msg => "action_failed",
                action => ActId,
                exception => Err,
                reason => Reason,
                stacktrace => ST
            })
    end.

-define(IS_RES_DOWN(R), R == stopped; R == not_connected; R == not_found; R == unhealthy_target).
do_handle_action(RuleId, {bridge, BridgeType, BridgeName, ResId}, Selected, _Envs) ->
    ?TRACE(
        "BRIDGE",
        "bridge_action",
        #{bridge_id => emqx_bridge_resource:bridge_id(BridgeType, BridgeName)}
    ),
    ReplyTo = {fun ?MODULE:inc_action_metrics/2, [RuleId], #{reply_dropped => true}},
    case
        emqx_bridge:send_message(BridgeType, BridgeName, ResId, Selected, #{reply_to => ReplyTo})
    of
        {error, Reason} when Reason == bridge_not_found; Reason == bridge_stopped ->
            throw(out_of_service);
        ?RESOURCE_ERROR_M(R, _) when ?IS_RES_DOWN(R) ->
            throw(out_of_service);
        Result ->
            Result
    end;
do_handle_action(RuleId, #{mod := Mod, func := Func} = Action, Selected, Envs) ->
    %% the function can also throw 'out_of_service'
    Args = maps:get(args, Action, []),
    Result = Mod:Func(Selected, Envs, Args),
    inc_action_metrics(RuleId, Result),
    Result.

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
        cache_payload(emqx_utils_json:decode(MaybeJson, [return_maps]))
    catch
        _:_ -> error({decode_json_failed, MaybeJson})
    end.

ensure_list(List) when is_list(List) -> List;
ensure_list(_NotList) -> [].

nested_put(Alias, Val, Columns0) ->
    Columns = ensure_decoded_payload(Alias, Columns0),
    emqx_rule_maps:nested_put(Alias, Val, Columns).

inc_action_metrics(RuleId, Result) ->
    _ = do_inc_action_metrics(RuleId, Result),
    Result.

do_inc_action_metrics(RuleId, {error, {recoverable_error, _}}) ->
    emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed.out_of_service');
do_inc_action_metrics(RuleId, {error, {unrecoverable_error, _}}) ->
    emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed');
do_inc_action_metrics(RuleId, R) ->
    case is_ok_result(R) of
        false ->
            emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed'),
            emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.failed.unknown');
        true ->
            emqx_metrics_worker:inc(rule_metrics, RuleId, 'actions.success')
    end.

is_ok_result(ok) ->
    true;
is_ok_result({async_return, R}) ->
    is_ok_result(R);
is_ok_result(R) when is_tuple(R) ->
    ok == erlang:element(1, R);
is_ok_result(_) ->
    false.

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
