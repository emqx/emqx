%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(COALESCE_BADARG,
    throw(#{
        reason => coalesce_badarg,
        explain =>
            "must be an array, or a call to a function which returns an array, "
            "for example: coalesce([a,b,c]) or coalesce(tokens(var,','))"
    })
).

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
    eval_as_string(Form, Bindings, Opts);
render(Expression, Bindings, Opts) ->
    case compile(Expression) of
        {ok, Compiled} ->
            render(Compiled, Bindings, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

eval_as_string(Expr, Bindings, _Opts) ->
    try
        {ok, return_str(eval(Expr, Bindings, #{}))}
    catch
        throw:Reason ->
            {error, Reason};
        C:E:S ->
            {error, #{exception => C, reason => E, stack_trace => S}}
    end.

%% Force the expression to return binary string.
return_str(Str) when is_binary(Str) -> Str;
return_str(Num) when is_integer(Num) -> integer_to_binary(Num);
return_str(Num) when is_float(Num) -> float_to_binary(Num, [{decimals, 10}, compact]);
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
compile(Expression) ->
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
        {?BIF_MOD, coalesce} ->
            eval_coalesce(Args, Bindings, Opts);
        _ ->
            call(Mod, Fun, eval_loop(Args, Bindings, Opts))
    end;
eval({var, VarName}, Bindings, Opts) ->
    resolve_var_value(VarName, Bindings, Opts).

eval_loop([], _, _) -> [];
eval_loop([H | T], Bindings, Opts) -> [eval(H, Bindings, Opts) | eval_loop(T, Bindings, Opts)].

%% coalesce treats var_unbound exception as empty string ''
eval_coalesce([{array, Args}], Bindings, Opts) ->
    NewArgs = [lists:map(fun(Arg) -> try_eval(Arg, Bindings, Opts) end, Args)],
    call(?BIF_MOD, coalesce, NewArgs);
eval_coalesce([Arg], Bindings, Opts) ->
    case try_eval(Arg, Bindings, Opts) of
        List when is_list(List) ->
            call(?BIF_MOD, coalesce, List);
        <<>> ->
            <<>>;
        _ ->
            ?COALESCE_BADARG
    end;
eval_coalesce(_Args, _Bindings, _Opts) ->
    ?COALESCE_BADARG.

try_eval(Arg, Bindings, Opts) ->
    try
        eval(Arg, Bindings, Opts)
    catch
        throw:#{reason := var_unbound} ->
            <<>>
    end.

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
        {ok, Value} ->
            Value;
        {error, _Reason} ->
            throw(#{
                var_name => VarName,
                reason => var_unbound
            })
    end.

assert_func_exported(Mod, Fun, Arity) ->
    ok = try_load(Mod),
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

%% best effort to load the module because it might not be loaded as a part of the release modules
%% e.g. from a plugin.
%% do not call code server, just try to call a function in the module.
try_load(Mod) ->
    try
        _ = erlang:apply(Mod, module_info, [md5]),
        ok
    catch
        _:_ ->
            ok
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
                reason => unallowed_veriform_module,
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
