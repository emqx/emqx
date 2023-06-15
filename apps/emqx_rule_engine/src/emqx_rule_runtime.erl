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
-include("rule_actions.hrl").
-include_lib("emqx/include/emqx.hrl").

-export([ apply_rule/2
        , apply_rules/2
        , clear_rule_payload/0
        ]).

-import(emqx_rule_maps,
        [ nested_get/2
        , range_gen/2
        , range_get/3
        ]).

-compile({no_auto_import,[alias/1]}).

-type(input() :: map()).
-type(alias() :: atom()).
-type(collection() :: {alias(), [term()]}).

-define(ephemeral_alias(TYPE, NAME),
    iolist_to_binary(io_lib:format("_v_~s_~p_~p", [TYPE, NAME, erlang:system_time()]))).

-define(ActionMaxRetry, 3).

%%------------------------------------------------------------------------------
%% Apply rules
%%------------------------------------------------------------------------------
-spec(apply_rules(list(emqx_rule_engine:rule()), input()) -> ok).
apply_rules([], _Input) ->
    ok;
apply_rules([#rule{enabled = false}|More], Input) ->
    apply_rules(More, Input);
apply_rules([Rule|More], Input) ->
    _ = apply_rule(Rule, Input),
    apply_rules(More, Input).

apply_rule(Rule = #rule{id = RuleId}, Input) ->
    clear_rule_payload(),
    ok = emqx_rule_metrics:inc_rules_matched(RuleId),
    %% Add metadata here caused we need support `metadata` and `rule_id` in SQL
    try do_apply_rule(Rule, emqx_rule_utils:add_metadata(Input, #{rule_id => RuleId}))
    catch
        %% ignore the errors if select or match failed
        _:Reason = {select_and_transform_error, Error} ->
            emqx_rule_metrics:inc_rules_exception(RuleId),
            ?LOG(warning, "SELECT clause exception for ~s failed: ~p",
                 [RuleId, Error]),
            {error, Reason};
        _:Reason = {match_conditions_error, Error} ->
            emqx_rule_metrics:inc_rules_exception(RuleId),
            ?LOG(warning, "WHERE clause exception for ~s failed: ~p",
                 [RuleId, Error]),
            {error, Reason};
        _:Reason = {select_and_collect_error, Error} ->
            emqx_rule_metrics:inc_rules_exception(RuleId),
            ?LOG(warning, "FOREACH clause exception for ~s failed: ~p",
                 [RuleId, Error]),
            {error, Reason};
        _:Reason = {match_incase_error, Error} ->
            emqx_rule_metrics:inc_rules_exception(RuleId),
            ?LOG(warning, "INCASE clause exception for ~s failed: ~p",
                 [RuleId, Error]),
            {error, Reason};
        _:Error:StkTrace ->
            emqx_rule_metrics:inc_rules_exception(RuleId),
            ?LOG(error, "Apply rule ~s failed: ~p. Stacktrace:~n~p",
                 [RuleId, Error, StkTrace]),
            {error, {Error, StkTrace}}
    end.

do_apply_rule(#rule{id = RuleId,
                    is_foreach = true,
                    fields = Fields,
                    doeach = DoEach,
                    incase = InCase,
                    conditions = Conditions,
                    on_action_failed = OnFailed,
                    actions = Actions}, Input) ->
    {Selected, Collection} = ?RAISE(select_and_collect(Fields, Input),
                                        {select_and_collect_error, {_EXCLASS_,_EXCPTION_,_ST_}}),
    ColumnsAndSelected = maps:merge(Input, Selected),
    case ?RAISE(match_conditions(Conditions, ColumnsAndSelected),
                {match_conditions_error, {_EXCLASS_,_EXCPTION_,_ST_}}) of
        true ->
            Collection2 = filter_collection(ColumnsAndSelected, InCase, DoEach, Collection),
            case Collection2 of
                [] -> emqx_rule_metrics:inc_rules_no_result(RuleId);
                _ -> emqx_rule_metrics:inc_rules_passed(RuleId)
            end,
            {ok, [take_actions(Actions, Coll, ColumnsAndSelected, OnFailed) || Coll <- Collection2]};
        false ->
            ok = emqx_rule_metrics:inc_rules_no_result(RuleId),
            {error, nomatch}
    end;

do_apply_rule(#rule{id = RuleId,
                    is_foreach = false,
                    fields = Fields,
                    conditions = Conditions,
                    on_action_failed = OnFailed,
                    actions = Actions}, Input) ->
    Selected = ?RAISE(select_and_transform(Fields, Input),
                      {select_and_transform_error, {_EXCLASS_,_EXCPTION_,_ST_}}),
    case ?RAISE(match_conditions(Conditions, maps:merge(Input, Selected)),
                {match_conditions_error, {_EXCLASS_,_EXCPTION_,_ST_}}) of
        true ->
            ok = emqx_rule_metrics:inc_rules_passed(RuleId),
            {ok, take_actions(Actions, Selected, Input, OnFailed)};
        false ->
            ok = emqx_rule_metrics:inc_rules_no_result(RuleId),
            {error, nomatch}
    end.

clear_rule_payload() ->
    erlang:erase(rule_payload).

%% SELECT Clause
select_and_transform(Fields, Input) ->
    select_and_transform(Fields, Input, #{}).

select_and_transform([], _Input, Output) ->
    Output;
select_and_transform(['*'|More], Input, Output) ->
    select_and_transform(More, Input, maps:merge(Output, Input));
select_and_transform([{as, Field, Alias}|More], Input, Output) ->
    Val = eval(Field, Input),
    select_and_transform(More,
        nested_put(Alias, Val, Input),
        nested_put(Alias, Val, Output));
select_and_transform([Field|More], Input, Output) ->
    Val = eval(Field, Input),
    Key = alias(Field),
    select_and_transform(More,
        nested_put(Key, Val, Input),
        nested_put(Key, Val, Output)).

%% FOREACH Clause
-spec select_and_collect(list(), input()) -> {input(), collection()}.
select_and_collect(Fields, Input) ->
    select_and_collect(Fields, Input, {#{}, {'item', []}}).

select_and_collect([{as, Field, {_, A} = Alias}], Input, {Output, _}) ->
    Val = eval(Field, Input),
    {nested_put(Alias, Val, Output), {A, ensure_list(Val)}};
select_and_collect([{as, Field, Alias}|More], Input, {Output, LastKV}) ->
    Val = eval(Field, Input),
    select_and_collect(More,
        nested_put(Alias, Val, Input),
        {nested_put(Alias, Val, Output), LastKV});
select_and_collect([Field], Input, {Output, _}) ->
    Val = eval(Field, Input),
    Key = alias(Field),
    {nested_put(Key, Val, Output), {'item', ensure_list(Val)}};
select_and_collect([Field|More], Input, {Output, LastKV}) ->
    Val = eval(Field, Input),
    Key = alias(Field),
    select_and_collect(More,
        nested_put(Key, Val, Input),
        {nested_put(Key, Val, Output), LastKV}).

%% Filter each item got from FOREACH
filter_collection(Input, InCase, DoEach, {CollKey, CollVal}) ->
    lists:filtermap(
        fun(Item) ->
            InputAndItem = maps:merge(Input, #{CollKey => Item}),
            case ?RAISE(match_conditions(InCase, InputAndItem),
                    {match_incase_error, {_EXCLASS_,_EXCPTION_,_ST_}}) of
                true when DoEach == [] -> {true, InputAndItem};
                true ->
                    {true, ?RAISE(select_and_transform(DoEach, InputAndItem),
                                  {doeach_error, {_EXCLASS_,_EXCPTION_,_ST_}})};
                false -> false
            end
        end, CollVal).

%% Conditional Clauses such as WHERE, WHEN.
match_conditions({'and', L, R}, Data) ->
    match_conditions(L, Data) andalso match_conditions(R, Data);
match_conditions({'or', L, R}, Data) ->
    match_conditions(L, Data) orelse match_conditions(R, Data);
match_conditions({'not', Var}, Data) ->
    case eval(Var, Data) of
        Bool when is_boolean(Bool) ->
            not Bool;
        _other -> false
    end;
match_conditions({in, Var, {list, Vals}}, Data) ->
    lists:member(eval(Var, Data), [eval(V, Data) || V <- Vals]);
match_conditions({'fun', {_, Name}, Args}, Data) ->
    apply_func(Name, [eval(Arg, Data) || Arg <- Args], Data);
match_conditions({Op, L, R}, Data) when ?is_comp(Op) ->
    compare(Op, eval(L, Data), eval(R, Data));
%%match_conditions({'like', Var, Pattern}, Data) ->
%%    match_like(eval(Var, Data), Pattern);
match_conditions({}, _Data) ->
    true.

%% comparing numbers against strings
compare(Op, L, R) when L == undefined; R == undefined ->
    do_compare(Op, L, R);
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
do_compare('>', L, R) when L == undefined; R == undefined -> false;
do_compare('>', L, R) -> L > R;
do_compare('<', L, R) when L == undefined; R == undefined -> false;
do_compare('<', L, R) -> L < R;
do_compare('<=', L, R) ->
    do_compare('=', L, R) orelse do_compare('<', L, R);
do_compare('>=', L, R) ->
    do_compare('=', L, R) orelse do_compare('>', L, R);
do_compare('<>', L, R) -> L /= R;
do_compare('!=', L, R) -> L /= R;
do_compare('=~', undefined, undefined) -> true;
do_compare('=~', T, F) when T == undefined; F == undefined -> false;
do_compare('=~', T, F) ->
    emqx_topic:match(T, F).

number(Bin) ->
    try binary_to_integer(Bin)
    catch error:badarg -> binary_to_float(Bin)
    end.

%% %% Step3 -> Take actions
%% fallback actions already have `rule_id` in `metadata`
take_actions(Actions, Selected, Envs, OnFailed) ->
    [take_action(ActInst, Selected, emqx_rule_utils:add_metadata(Envs, ActInst), OnFailed, ?ActionMaxRetry)
     || ActInst <- Actions].

take_action(#action_instance{id = Id, name = ActName, fallbacks = Fallbacks} = ActInst,
            Selected, Envs, OnFailed, RetryN) when RetryN >= 0 ->
    try
        {ok, #action_instance_params{apply = Apply}}
            = emqx_rule_registry:get_action_instance_params(Id),
        emqx_rule_metrics:inc_actions_taken(Id),
        apply_action_func(Selected, Envs, Apply, ActName)
    of
        badact_quiet ->
            handle_action_failure(OnFailed, Id, Fallbacks, Selected, Envs, badact_quiet);
        {badact, Reason} ->
            handle_action_failure(OnFailed, Id, Fallbacks, Selected, Envs, Reason);
        Result -> Result
    catch
        error:{badfun, _Func}:_ST ->
            %?LOG(warning, "Action ~p maybe outdated, refresh it and try again."
            %              "Func: ~p~nST:~0p", [Id, Func, ST]),
            _ = trans_action_on(Id, fun() ->
                emqx_rule_engine:refresh_actions([ActInst])
            end, 5000),
            emqx_rule_metrics:inc_actions_retry(Id),
            take_action(ActInst, Selected, Envs, OnFailed, RetryN-1);
        Error:Reason:Stack ->
            emqx_rule_metrics:inc_actions_exception(Id),
            handle_action_failure(OnFailed, Id, Fallbacks, Selected, Envs, {Error, Reason, Stack})
    end;

take_action(#action_instance{id = Id, fallbacks = Fallbacks}, Selected, Envs, OnFailed, _RetryN) ->
    emqx_rule_metrics:inc_actions_error(Id),
    handle_action_failure(OnFailed, Id, Fallbacks, Selected, Envs, {max_try_reached, ?ActionMaxRetry}).

apply_action_func(Data, Envs, #{mod := Mod, bindings := Bindings}, Name) ->
    %% TODO: Build the Func Name when creating the action
    Func = cbk_on_action_triggered(Name),
    Mod:Func(Data, Envs#{'__bindings__' => Bindings});
apply_action_func(Data, Envs, Func, _Name) when is_function(Func) ->
    erlang:apply(Func, [Data, Envs]).

cbk_on_action_triggered(Name) ->
    list_to_atom("on_action_" ++ atom_to_list(Name)).

trans_action_on(Id, Callback, Timeout) ->
    case emqx_rule_locker:lock(Id) of
        true -> try Callback() after emqx_rule_locker:unlock(Id) end;
        _ ->
            wait_action_on(Id, Timeout div 10)
    end.

wait_action_on(_, 0) ->
    {error, timeout};
wait_action_on(Id, RetryN) ->
    timer:sleep(10),
    case emqx_rule_registry:get_action_instance_params(Id) of
        not_found ->
            {error, not_found};
        {ok, #action_instance_params{apply = Apply}} ->
            case catch apply_action_func(baddata, #{}, Apply, tryit) of
                {'EXIT', {{badfun, _}, _}} ->
                    wait_action_on(Id, RetryN-1);
                _ ->
                    ok
            end
    end.

handle_action_failure(continue, _Id, Fallbacks, Selected, Envs = #{metadata := Metadata}, Reason) ->
    Reason =/= badact_quiet andalso
        ?LOG_RULE_ACTION(error, Metadata, "Continue next action, reason: ~0p", [Reason]),
    _ = take_actions(Fallbacks, Selected, Envs, continue),
    failed;
handle_action_failure(stop, Id, Fallbacks, Selected, Envs = #{metadata := Metadata}, Reason) ->
    Reason =/= badact_quiet andalso
        ?LOG_RULE_ACTION(error, Metadata, "Skip all actions, reason: ~0p", [Reason]),
    _ = take_actions(Fallbacks, Selected, Envs, continue),
    error({take_action_failed, {Id, Reason}}).

eval({path, [{key, <<"payload">>} | Path]}, #{payload := Payload}) ->
    nested_get({path, Path}, may_decode_payload(Payload));
eval({path, [{key, <<"payload">>} | Path]}, #{<<"payload">> := Payload}) ->
    nested_get({path, Path}, may_decode_payload(Payload));
eval({path, _} = Path, Input) ->
    nested_get(Path, Input);
eval({range, {Begin, End}}, _Input) ->
    range_gen(Begin, End);
eval({get_range, {Begin, End}, Data}, Input) ->
    range_get(Begin, End, eval(Data, Input));
eval({var, _} = Var, Input) ->
    nested_get(Var, Input);
eval({const, Val}, _Input) ->
    Val;
%% unary add
eval({'+', L}, Input) ->
    eval(L, Input);
%% unary subtract
eval({'-', L}, Input) ->
    -(eval(L, Input));
eval({Op, L, R}, Input) when ?is_arith(Op) ->
    apply_func(Op, [eval(L, Input), eval(R, Input)], Input);
eval({Op, L, R}, Input) when ?is_comp(Op) ->
    compare(Op, eval(L, Input), eval(R, Input));
eval({list, List}, Input) ->
    [eval(L, Input) || L <- List];
eval({'case', <<>>, CaseClauses, ElseClauses}, Input) ->
    eval_case_clauses(CaseClauses, ElseClauses, Input);
eval({'case', CaseOn, CaseClauses, ElseClauses}, Input) ->
    eval_switch_clauses(CaseOn, CaseClauses, ElseClauses, Input);
eval({'fun', {_, Name}, Args}, Input) ->
    apply_func(Name, [eval(Arg, Input) || Arg <- Args], Input).

handle_alias({path, [{key, <<"payload">>} | _]}, #{payload := Payload} = Input) ->
    Input#{payload => may_decode_payload(Payload)};
handle_alias({path, [{key, <<"payload">>} | _]}, #{<<"payload">> := Payload} = Input) ->
    Input#{<<"payload">> => may_decode_payload(Payload)};
handle_alias(_, Input) ->
    Input.

alias({var, Var}) ->
    {var, Var};
alias({const, Val}) when is_binary(Val) ->
    {var, Val};
alias({list, L}) ->
    {var, ?ephemeral_alias(list, length(L))};
alias({range, R}) ->
    {var, ?ephemeral_alias(range, R)};
alias({get_range, _, {var, Key}}) ->
    {var, Key};
alias({get_range, _, {path, Path}}) ->
    {path, Path};
alias({path, Path}) ->
    {path, Path};
alias({const, Val}) ->
    {var, ?ephemeral_alias(const, Val)};
alias({Op, _L, _R}) when ?is_arith(Op); ?is_comp(Op) ->
    {var, ?ephemeral_alias(op, Op)};
alias({'case', On, _, _}) ->
    {var, ?ephemeral_alias('case', On)};
alias({'fun', Name, _}) ->
    {var, ?ephemeral_alias('fun', Name)};
alias(_) ->
    ?ephemeral_alias(unknown, unknown).

eval_case_clauses([], ElseClauses, Input) ->
    case ElseClauses of
        {} -> undefined;
        _ -> eval(ElseClauses, Input)
    end;
eval_case_clauses([{Cond, Clause} | CaseClauses], ElseClauses, Input) ->
    case match_conditions(Cond, Input) of
        true ->
            eval(Clause, Input);
        _ ->
            eval_case_clauses(CaseClauses, ElseClauses, Input)
    end.

eval_switch_clauses(_CaseOn, [], ElseClauses, Input) ->
    case ElseClauses of
        {} -> undefined;
        _ -> eval(ElseClauses, Input)
    end;
eval_switch_clauses(CaseOn, [{Cond, Clause} | CaseClauses], ElseClauses, Input) ->
    ConResult = eval(Cond, Input),
    case eval(CaseOn, Input) of
        ConResult ->
            eval(Clause, Input);
        _ ->
            eval_switch_clauses(CaseOn, CaseClauses, ElseClauses, Input)
    end.

apply_func(Name, Args, Input) when is_atom(Name) ->
    do_apply_func(Name, Args, Input);
apply_func(Name, Args, Input) when is_binary(Name) ->
    FunName =
        try binary_to_existing_atom(Name, utf8)
        catch error:badarg -> error({sql_function_not_supported, Name})
        end,
    do_apply_func(FunName, Args, Input).

do_apply_func(Name, Args, Input) ->
    case erlang:apply(emqx_rule_funcs, Name, Args) of
        Func when is_function(Func) ->
            erlang:apply(Func, [Input]);
        Result -> Result
    end.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
may_decode_payload(Payload) when is_binary(Payload) ->
    case get_cached_payload() of
        undefined -> safe_decode_and_cache(Payload);
        DecodedP -> DecodedP
    end;
may_decode_payload(Payload) ->
    Payload.

get_cached_payload() ->
    erlang:get(rule_payload).

cache_payload(DecodedP) ->
    erlang:put(rule_payload, DecodedP),
    DecodedP.

safe_decode_and_cache(MaybeJson) ->
    try cache_payload(emqx_json:decode(MaybeJson, [return_maps]))
    catch
        _:_:_-> error({decode_json_failed, MaybeJson})
    end.

ensure_list(List) when is_list(List) -> List;
ensure_list(_NotList) -> [].

nested_put(Alias, Val, Input0) ->
    Input = handle_alias(Alias, Input0),
    emqx_rule_maps:nested_put(Alias, Val, Input).
