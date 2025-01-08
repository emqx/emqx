%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module provides a single-line expression string rendering engine.
%% A predefined set of functions are allowed to be called in the expressions.
%% Only simple string expressions are supported, and no control flow is allowed.
%% However, with the help from the functions, some control flow can be achieved.
%% For example, the `coalesce` function can be used to provide a default value,
%% or used to choose the first non-empty value from a list of variables.
-module(emqx_variform).

-export([
    inject_allowed_module/1,
    inject_allowed_modules/1,
    erase_allowed_module/1,
    erase_allowed_modules/1
]).

-export([render/2, render/3]).
-export([compile/1, decompile/1]).

-export_type([compiled/0]).

-type compiled() :: #{expr := string(), form := term()}.
-define(BIF_MOD, emqx_variform_bif).
-define(IS_ALLOWED_MOD(M),
    (M =:= ?BIF_MOD orelse
        M =:= lists orelse
        M =:= maps)
).

-define(IS_EMPTY(X), (X =:= <<>> orelse X =:= "" orelse X =:= undefined orelse X =:= null)).

%% @doc Render a variform expression with bindings.
%% A variform expression is a template string which supports variable substitution
%% and function calls.
%%
%% The function calls are in the form of `module.function(arg1, arg2, ...)` where `module`
%% is optional, and if not provided, the function is assumed to be in the `emqx_variform_bif` module.
%% Both module and function must be existing atoms, and only whitelisted functions are allowed.
%%
%% A function arg can be a constant string or a number.
%% Strings can be quoted with single quotes or double quotes, without support of escape characters.
%% If some special characters are needed, the function `unescape' can be used convert a escaped string
%% to raw bytes.
%% For example, to get the first line of a multi-line string, the expression can be
%% `coalesce(tokens(variable_name, unescape("\n")))'.
%%
%% The bindings is a map of variables to their values.
%%
%% For unresolved variables, empty string (but not "undefined") is used.
%% In case of runtime exeption, an error is returned.
%% In case of unbound variable is referenced, error is returned.
-spec render(string(), map()) -> {ok, binary()} | {error, term()}.
render(Expression, Bindings) ->
    render(Expression, Bindings, #{}).

render(#{form := Form}, Bindings, Opts) ->
    eval_render(Form, Bindings, Opts);
render(Expression, Bindings, Opts) ->
    case compile(Expression) of
        {ok, Compiled} ->
            render(Compiled, Bindings, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

eval_render(Expr, Bindings, Opts) ->
    EvalAsStr = maps:get(eval_as_string, Opts, true),
    try
        Result = eval(Expr, Bindings, #{}),
        case EvalAsStr of
            true ->
                {ok, return_str(Result)};
            false ->
                {ok, Result}
        end
    catch
        throw:Reason ->
            {error, Reason};
        C:E:S ->
            {error, #{exception => C, reason => E, stack_trace => S}}
    end.

%% Force the expression to return binary string (in most cases).
return_str(X) when ?IS_EMPTY(X) -> <<"">>;
return_str(Str) when is_binary(Str) -> Str;
return_str(Num) when is_integer(Num) -> integer_to_binary(Num);
return_str(Num) when is_float(Num) -> float_to_binary(Num, [{decimals, 10}, compact]);
return_str(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
return_str(Other) ->
    throw(#{
        reason => bad_return,
        expected => string,
        got => Other
    }).

%% @doc Compile varifom expression.
-spec compile(string() | binary() | compiled()) -> {ok, compiled()} | {error, any()}.
compile(#{form := _} = Compiled) ->
    {ok, Compiled};
compile(Expression) when is_binary(Expression) ->
    compile(unicode:characters_to_list(Expression));
compile(Expression) when is_list(Expression) ->
    do_compile(Expression);
compile(_Expression) ->
    {error, invalid_expression}.

do_compile(Expression) ->
    case emqx_variform_scan:string(Expression) of
        {ok, Tokens, _Line} ->
            case emqx_variform_parser:parse(Tokens) of
                {ok, Form} ->
                    {ok, #{expr => Expression, form => Form}};
                {error, {_, emqx_variform_parser, Msg}} ->
                    %% syntax error
                    {error, lists:flatten(Msg)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason, _Line} ->
            {error, Reason}
    end.

decompile(#{expr := Expression}) ->
    Expression;
decompile(Expression) ->
    Expression.

eval(Atom, _Bindings, _Opts) when is_atom(Atom) ->
    %% There is no atom literals in variform,
    %% but some bif functions such as regex_match may return an atom.
    atom_to_binary(Atom, utf8);
eval({str, Str}, _Bindings, _Opts) ->
    unicode:characters_to_binary(Str);
eval({integer, Num}, _Bindings, _Opts) ->
    Num;
eval({float, Num}, _Bindings, _Opts) ->
    Num;
eval({array, Args}, Bindings, Opts) ->
    eval_loop(Args, Bindings, Opts);
eval({call, FuncNameStr, Args}, Bindings, Opts) ->
    {Mod, Fun} = resolve_func_name(FuncNameStr),
    ok = assert_func_exported(Mod, Fun, length(Args)),
    case {Mod, Fun} of
        {?BIF_MOD, iif} ->
            eval_iif(Args, Bindings, Opts);
        {?BIF_MOD, coalesce} ->
            eval_coalesce(Args, Bindings, Opts);
        {?BIF_MOD, is_empty} ->
            eval_is_empty(Args, Bindings, Opts);
        _ ->
            call(Mod, Fun, eval_loop(Args, Bindings, Opts))
    end;
eval({var, VarName}, Bindings, Opts) ->
    resolve_var_value(VarName, Bindings, Opts).

eval_loop([], _, _) -> [];
eval_loop([H | T], Bindings, Opts) -> [eval(H, Bindings, Opts) | eval_loop(T, Bindings, Opts)].

%% coalesce treats var_unbound exception as empty string ''
eval_coalesce([{array, Args}], Bindings, Opts) ->
    %% The input arg is an array
    eval_coalesce_loop(Args, Bindings, Opts);
eval_coalesce([Arg], Bindings, Opts) ->
    %% Arg is an expression (which is expected to return an array)
    case try_eval(Arg, Bindings, Opts) of
        List when is_list(List) ->
            case lists:dropwhile(fun(I) -> ?IS_EMPTY(I) end, List) of
                [] ->
                    <<>>;
                [H | _] ->
                    H
            end;
        <<>> ->
            <<>>;
        _ ->
            throw(#{
                reason => coalesce_badarg,
                explain => "the arg expression did not yield an array"
            })
    end;
eval_coalesce(Args, Bindings, Opts) ->
    %% It also accepts arbitrary number of args
    %% equivalent to [{array, Args}]
    eval_coalesce_loop(Args, Bindings, Opts).

eval_coalesce_loop([], _Bindings, _Opts) ->
    <<>>;
eval_coalesce_loop([Arg | Args], Bindings, Opts) ->
    Result = try_eval(Arg, Bindings, Opts),
    case ?IS_EMPTY(Result) of
        true ->
            eval_coalesce_loop(Args, Bindings, Opts);
        false ->
            Result
    end.

try_eval(Arg, Bindings, Opts) ->
    try
        eval(Arg, Bindings, Opts)
    catch
        throw:#{reason := var_unbound} ->
            <<>>
    end.

eval_is_empty([Arg], Bindings, Opts) ->
    Val = eval_coalesce_loop([Arg], Bindings, Opts),
    ?IS_EMPTY(Val).

eval_iif([Cond, If, Else], Bindings, Opts) ->
    CondVal = try_eval(Cond, Bindings, Opts),
    case is_iif_condition_met(CondVal) of
        true ->
            eval(If, Bindings, Opts);
        false ->
            eval(Else, Bindings, Opts)
    end.

%% If iif condition expression yielded boolean, use the boolean value.
%% otherwise it's met as long as it's not an empty string.
is_iif_condition_met(true) -> true;
is_iif_condition_met(false) -> false;
is_iif_condition_met(V) -> not ?IS_EMPTY(V).

%% Some functions accept arbitrary number of arguments but implemented as /1.
call(Mod, Fun, Args) ->
    erlang:apply(Mod, Fun, Args).

resolve_func_name(FuncNameStr) ->
    case string:tokens(FuncNameStr, ".") of
        [Mod0, Fun0] ->
            Mod =
                try
                    list_to_existing_atom(Mod0)
                catch
                    error:badarg ->
                        throw(#{
                            reason => unknown_variform_module,
                            module => Mod0
                        })
                end,
            ok = assert_module_allowed(Mod),
            Fun =
                try
                    list_to_existing_atom(Fun0)
                catch
                    error:badarg ->
                        throw(#{
                            reason => unknown_variform_function,
                            module => Mod,
                            function => Fun0
                        })
                end,
            {Mod, Fun};
        [Fun] ->
            FuncName =
                try
                    list_to_existing_atom(Fun)
                catch
                    error:badarg ->
                        throw(#{
                            reason => unknown_variform_function,
                            module => ?BIF_MOD,
                            function => Fun
                        })
                end,
            {?BIF_MOD, FuncName};
        _ ->
            throw(#{reason => invalid_function_reference, function => FuncNameStr})
    end.

%% _Opts can be extended in the future. For example, unbound var as 'undfeined'
resolve_var_value(VarName, Bindings, _Opts) ->
    case emqx_template:lookup_var(split(VarName), Bindings) of
        {ok, Value} when ?IS_EMPTY(Value) ->
            <<"">>;
        {ok, Value} ->
            Value;
        {error, _Reason} ->
            throw(#{
                var_name => iolist_to_binary(VarName),
                reason => var_unbound
            })
    end.

assert_func_exported(?BIF_MOD, coalesce, _Arity) ->
    ok;
assert_func_exported(?BIF_MOD, iif, _Arity) ->
    ok;
assert_func_exported(?BIF_MOD, is_empty, _Arity) ->
    ok;
assert_func_exported(Mod, Fun, Arity) ->
    %% call emqx_utils:interactive_load to make sure it will work in tests.
    %% in production, the module has to be pre-loaded by plugin management code
    ok = emqx_utils:interactive_load(Mod),
    case erlang:function_exported(Mod, Fun, Arity) of
        true ->
            ok;
        false ->
            throw(#{
                reason => unknown_variform_function,
                module => Mod,
                function => Fun,
                arity => Arity
            })
    end.

assert_module_allowed(Mod) when ?IS_ALLOWED_MOD(Mod) ->
    ok;
assert_module_allowed(Mod) ->
    Allowed = get_allowed_modules(),
    case lists:member(Mod, Allowed) of
        true ->
            ok;
        false ->
            throw(#{
                reason => unallowed_variform_module,
                module => Mod
            })
    end.

inject_allowed_module(Module) when is_atom(Module) ->
    inject_allowed_modules([Module]).

inject_allowed_modules(Modules) when is_list(Modules) ->
    Allowed0 = get_allowed_modules(),
    Allowed = lists:usort(Allowed0 ++ Modules),
    persistent_term:put({emqx_variform, allowed_modules}, Allowed).

erase_allowed_module(Module) when is_atom(Module) ->
    erase_allowed_modules([Module]).

erase_allowed_modules(Modules) when is_list(Modules) ->
    Allowed0 = get_allowed_modules(),
    Allowed = Allowed0 -- Modules,
    persistent_term:put({emqx_variform, allowed_modules}, Allowed).

get_allowed_modules() ->
    persistent_term:get({emqx_variform, allowed_modules}, []).

split(VarName) ->
    lists:map(fun erlang:iolist_to_binary/1, string:tokens(VarName, ".")).
