%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @hidden This parse transform generates BPAPI metadata function for
%% a module, and helps dialyzer typechecking RPC calls
-module(emqx_bpapi_trans).

-export([parse_transform/2, format_error/1]).

%%-define(debug, true).

-define(META_FUN, bpapi_meta).

-type semantics() :: call | cast.

-record(s, {
    api :: emqx_bpapi:api(),
    module :: module(),
    version :: emqx_bpapi:api_version() | undefined,
    targets = [] :: [{semantics(), emqx_bpapi:call(), emqx_bpapi:call()}],
    errors = [] :: list(),
    file
}).

format_error(invalid_name) ->
    "BPAPI module name should follow <API>_proto_v<number> pattern";
format_error({invalid_fun, Name, Arity}) ->
    io_lib:format(
        "malformed function ~p/~p. "
        "BPAPI functions should have exactly one clause "
        "and call (emqx_|e)rpc at the top level",
        [Name, Arity]
    ).

parse_transform(Forms, _Options) ->
    log("Original:~n~p", [Forms]),
    State = #s{file = File} = lists:foldl(fun go/2, #s{}, Forms),
    log("parse_trans state: ~p", [State]),
    case check(State) of
        [] ->
            finalize(Forms, State);
        Errors ->
            {error, [{File, [{Line, ?MODULE, Msg} || {Line, Msg} <- Errors]}], []}
    end.

%% Scan erl_forms:
go({attribute, _, file, {File, _}}, S) ->
    S#s{file = File};
go({attribute, Line, module, Mod}, S) ->
    case api_and_version(Mod) of
        {ok, API, Vsn} -> S#s{api = API, version = Vsn, module = Mod};
        error -> push_err(Line, invalid_name, S)
    end;
go({function, _Line, introduced_in, 0, _}, S) ->
    S;
go({function, _Line, deprecated_since, 0, _}, S) ->
    S;
go({function, Line, Name, Arity, Clauses}, S) ->
    analyze_fun(Line, Name, Arity, Clauses, S);
go(_, S) ->
    S.

check(#s{errors = Err}) ->
    %% Post-processing checks can be placed here
    Err.

finalize(Forms, S) ->
    {Attrs, Funcs} = lists:splitwith(fun is_attribute/1, Forms),
    AST = mk_meta_fun(S),
    log("Meta fun:~n~p", [AST]),
    Attrs ++ [mk_export()] ++ [AST | Funcs].

mk_meta_fun(#s{api = API, version = Vsn, targets = Targets}) ->
    Line = 0,
    Calls = [{From, To} || {call, From, To} <- Targets],
    Casts = [{From, To} || {cast, From, To} <- Targets],
    Ret = erlang_qq:const(Line, #{
        api => API,
        version => Vsn,
        calls => Calls,
        casts => Casts
    }),
    {function, Line, ?META_FUN, _Arity = 0, [{clause, Line, _Args = [], _Guards = [], [Ret]}]}.

mk_export() ->
    {attribute, 0, export, [{?META_FUN, 0}]}.

is_attribute({attribute, _Line, _Attr, _Val}) -> true;
is_attribute(_) -> false.

%% Extract the target function of the RPC call
analyze_fun(Line, Name, Arity, [{clause, Line, Head, _Guards, Exprs}], S) ->
    analyze_exprs(Line, Name, Arity, Head, Exprs, S);
analyze_fun(Line, Name, Arity, _Clauses, S) ->
    invalid_fun(Line, Name, Arity, S).

analyze_exprs(Line, Name, Arity, Head, Exprs, S) ->
    log("~p/~p (~p):~n~p", [Name, Arity, Head, Exprs]),
    try
        [{call, _, CallToBackend, CallArgs}] = Exprs,
        OuterArgs = extract_outer_args(Head),
        Key = {S#s.module, Name, OuterArgs},
        {Semantics, Target} = extract_target_call(CallToBackend, CallArgs),
        push_target({Semantics, Key, Target}, S)
    catch
        _:Err:Stack ->
            log("Failed to process function call:~n~s~nStack: ~p", [Err, Stack]),
            invalid_fun(Line, Name, Arity, S)
    end.

-spec extract_outer_args([erl_parse:abstract_form()]) -> [atom()].
extract_outer_args(Abs) ->
    lists:map(
        fun
            ({var, _, Var}) ->
                Var;
            ({match, _, {var, _, Var}, _}) ->
                Var;
            ({match, _, _, {var, _, Var}}) ->
                Var
        end,
        Abs
    ).

-spec extract_target_call(_AST, [_AST]) -> {semantics(), emqx_bpapi:call()}.
extract_target_call(RPCBackend, OuterArgs) ->
    {Semantics, {atom, _, M}, {atom, _, F}, A} = extract_mfa(RPCBackend, OuterArgs),
    {Semantics, {M, F, list_to_args(A)}}.

-define(BACKEND(MOD, FUN), {remote, _, {atom, _, MOD}, {atom, _, FUN}}).
-define(IS_RPC(MOD), (MOD =:= erpc orelse MOD =:= rpc)).

%% gen_rpc:
extract_mfa(?BACKEND(gen_rpc, _), _) ->
    %% gen_rpc has an extremely messy API, thankfully it's fully wrapped
    %% by emqx_rpc, so we simply forbid direct calls to it:
    error("direct call to gen_rpc");
%% emqx_rpc:
extract_mfa(?BACKEND(emqx_rpc, CallOrCast), [_Node, M, F, A]) ->
    {call_or_cast(CallOrCast), M, F, A};
extract_mfa(?BACKEND(emqx_rpc, CallOrCast), [_Tag, _Node, M, F, A]) ->
    {call_or_cast(CallOrCast), M, F, A};
extract_mfa(?BACKEND(emqx_rpc, call), [_Tag, _Node, M, F, A, _Timeout]) ->
    {call_or_cast(call), M, F, A};
%% (e)rpc:
extract_mfa(?BACKEND(rpc, multicall), [M, F, A]) ->
    {call_or_cast(multicall), M, F, A};
extract_mfa(?BACKEND(rpc, multicall), [M, F, A, {integer, _, _Timeout}]) ->
    {call_or_cast(multicall), M, F, A};
extract_mfa(?BACKEND(RPC, CallOrCast), [_Node, M, F, A]) when ?IS_RPC(RPC) ->
    {call_or_cast(CallOrCast), M, F, A};
extract_mfa(?BACKEND(RPC, CallOrCast), [_Node, M, F, A, _Timeout]) when ?IS_RPC(RPC) ->
    {call_or_cast(CallOrCast), M, F, A};
%% emqx_cluster_rpc:
extract_mfa(?BACKEND(emqx_cluster_rpc, multicall), [M, F, A]) ->
    {call, M, F, A};
extract_mfa(?BACKEND(emqx_cluster_rpc, multicall), [M, F, A, _RequiredNum, _Timeout]) ->
    {call, M, F, A};
extract_mfa(_, _) ->
    error("unrecognized RPC call").

call_or_cast(cast) -> cast;
call_or_cast(multicast) -> cast;
call_or_cast(multicall) -> call;
call_or_cast(call) -> call.

list_to_args({cons, _, {var, _, A}, T}) ->
    [A | list_to_args(T)];
list_to_args({nil, _}) ->
    [].

invalid_fun(Line, Name, Arity, S) ->
    push_err(Line, {invalid_fun, Name, Arity}, S).

push_err(Line, Err, S = #s{errors = Errs}) ->
    S#s{errors = [{Line, Err} | Errs]}.

push_target(Target, S = #s{targets = Targets}) ->
    S#s{targets = [Target | Targets]}.

-spec api_and_version(module()) -> {ok, emqx_bpapi:api(), emqx_bpapi:api_version()} | error.
api_and_version(Module) ->
    Opts = [{capture, all_but_first, list}],
    case re:run(atom_to_list(Module), "(.*)_proto_v([0-9]+)$", Opts) of
        {match, [API, VsnStr]} ->
            {ok, list_to_atom(API), list_to_integer(VsnStr)};
        nomatch ->
            error
    end.

-ifdef(debug).
log(Fmt, Args) ->
    io:format(user, "!! " ++ Fmt ++ "~n", Args).
-else.
log(_, _) ->
    ok.
-endif.
