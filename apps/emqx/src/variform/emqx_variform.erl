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

%% @doc This module provides a single-line expression string rendering engine.
%% A predefined set of functions are allowed to be called in the expressions.
%% Only simple string expressions are supported, and no control flow is allowed.
%% However, with the help from the functions, some control flow can be achieved.
%% For example, the `coalesce` function can be used to provide a default value,
%% or used to choose the first non-empty value from a list of variables.
-module(emqx_variform).

-export([inject_allowed_modules/1]).
-export([render/2, render/3]).

%% @doc Render a variform expression with bindings.
%% A variform expression is a template string which supports variable substitution
%% and function calls.
%%
%% The function calls are in the form of `module.function(arg1, arg2, ...)` where `module`
%% is optional, and if not provided, the function is assumed to be in the `emqx_variform_str` module.
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
-spec render(string(), map()) -> {ok, binary()} | {error, term()}.
render(Expression, Bindings) ->
    render(Expression, Bindings, #{}).

render(Expression, Bindings, Opts) ->
    case emqx_variform_scan:string(Expression) of
        {ok, Tokens, _Line} ->
            case emqx_variform_parser:parse(Tokens) of
                {ok, Expr} ->
                    eval_as_string(Expr, Bindings, Opts);
                {error, {_, emqx_variform_parser, Msg}} ->
                    %% syntax error
                    {error, lists:flatten(Msg)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason, _Line} ->
            {error, Reason}
    end.

eval_as_string(Expr, Bindings, _Opts) ->
    try
        {ok, iolist_to_binary(eval(Expr, Bindings))}
    catch
        throw:Reason ->
            {error, Reason};
        C:E:S ->
            {error, #{exception => C, reason => E, stack_trace => S}}
    end.

eval({str, Str}, _Bindings) ->
    str(Str);
eval({num, Num}, _Bindings) ->
    str(Num);
eval({call, FuncNameStr, Args}, Bindings) ->
    {Mod, Fun} = resolve_func_name(FuncNameStr),
    ok = assert_func_exported(Mod, Fun, length(Args)),
    call(Mod, Fun, eval(Args, Bindings));
eval({var, VarName}, Bindings) ->
    resolve_var_value(VarName, Bindings);
eval([Arg | Args], Bindings) ->
    [eval(Arg, Bindings) | eval(Args, Bindings)];
eval([], _Bindings) ->
    [].

%% Some functions accept arbitrary number of arguments but implemented as /1.
call(emqx_variform_str, concat, Args) ->
    str(emqx_variform_str:concat(Args));
call(emqx_variform_str, coalesce, Args) ->
    str(emqx_variform_str:coalesce(Args));
call(Mod, Fun, Args) ->
    str(erlang:apply(Mod, Fun, Args)).

resolve_func_name(FuncNameStr) ->
    case string:tokens(FuncNameStr, ".") of
        [Mod0, Fun0] ->
            Mod =
                try
                    list_to_existing_atom(Mod0)
                catch
                    error:badarg ->
                        throw(#{unknown_module => Mod0})
                end,
            ok = assert_module_allowed(Mod),
            Fun =
                try
                    list_to_existing_atom(Fun0)
                catch
                    error:badarg ->
                        throw(#{unknown_function => Fun0})
                end,
            {Mod, Fun};
        [Fun] ->
            FuncName =
                try
                    list_to_existing_atom(Fun)
                catch
                    error:badarg ->
                        throw(#{
                            reason => "unknown_variform_function",
                            function => Fun
                        })
                end,
            {emqx_variform_str, FuncName}
    end.

resolve_var_value(VarName, Bindings) ->
    case emqx_template:lookup_var(split(VarName), Bindings) of
        {ok, Value} ->
            str(Value);
        {error, _Reason} ->
            <<>>
    end.

assert_func_exported(emqx_variform_str, concat, _Arity) ->
    ok;
assert_func_exported(emqx_variform_str, coalesce, _Arity) ->
    ok;
assert_func_exported(Mod, Fun, Arity) ->
    _ = Mod:module_info(md5),
    case erlang:function_exported(Mod, Fun, Arity) of
        true ->
            ok;
        false ->
            throw(#{
                reason => "unknown_variform_function",
                module => Mod,
                function => Fun,
                arity => Arity
            })
    end.

assert_module_allowed(emqx_variform_str) ->
    ok;
assert_module_allowed(Mod) ->
    Allowed = get_allowed_modules(),
    case lists:member(Mod, Allowed) of
        true ->
            ok;
        false ->
            throw(#{
                reason => "unallowed_veriform_module",
                module => Mod
            })
    end.

inject_allowed_modules(Modules) ->
    Allowed0 = get_allowed_modules(),
    Allowed = lists:usort(Allowed0 ++ Modules),
    persistent_term:put({emqx_variform, allowed_modules}, Allowed).

get_allowed_modules() ->
    persistent_term:get({emqx_variform, allowed_modules}, []).

str(Value) ->
    emqx_utils_conv:bin(Value).

split(VarName) ->
    lists:map(fun erlang:iolist_to_binary/1, string:tokens(VarName, ".")).
