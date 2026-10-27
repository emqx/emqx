#!/usr/bin/env escript
%% -*- erlang -*-
%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% Check that no new transaction fun is shipped from a replicant to a
%% core node by value.
%%
%% On a replicant node, mria:transaction/2,3, mria:sync_transaction/2,3,4,
%% mria:async_dirty/2,3 and mria:sync_dirty/2,3 send the fun argument to
%% a core node, which runs it inside a mnesia transaction. An anonymous
%% fun (`fun() -> ... end'), a local fun
%% (`fun f/1') or a variable bound to one of them carries the identity
%% of the module version that created it. When the core runs a different
%% build of that module, the call fails with `badfun'. Only an external
%% fun (`fun m:f/1') is called by name and survives a version difference.
%% The place for such a fun is a transaction BPAPI module,
%% `<api>_tx_proto_v<N>', see apps/emqx_bpapi/README.md.
%%
%% The check reads the sources, so it does not need a build. It
%% resolves a variable to the expression bound to it in the same
%% clause, and it follows a local wrapper function that forwards a fun
%% parameter to mria (`trans(Fun, Args) -> mria:transaction(?SHARD,
%% Fun, Args)') to the callers of the wrapper. A wrapper that is
%% exported is reported, because its callers in other modules cannot be
%% checked. A call on `mria:local_content_shard()' never leaves the
%% node and is skipped.
%%
%% The remaining call sites are recorded per module in the allowlist,
%% which is maintained by hand and only ever shrinks. A module that
%% gains a call site fails this check. A module that loses one fails
%% too, until its count in the allowlist is lowered to match.
%%
%% Usage:
%%   ./scripts/check-mria-tx-funs.escript            # check against the allowlist
%%   ./scripts/check-mria-tx-funs.escript --list     # print every call site

-mode(compile).

-define(ALLOWLIST, "scripts/mria-tx-fun-allowlist.eterm").
-define(SRC_GLOB, "apps/*/src/**/*.erl").
%% mria:ro_transaction/2 is not checked. It takes a 0-arity fun only, so
%% a closure cannot be avoided there. On a replicant it runs locally,
%% and reaches a core node only while the shard is disconnected, where
%% a `badfun' fails the read as the disconnection already does.
-define(MRIA_FUNS, [transaction, sync_transaction, async_dirty, sync_dirty]).

%% A call site whose fun is not passed by name.
-record(site, {
    file :: file:filename(),
    line :: pos_integer(),
    mria_fun :: atom(),
    kind :: atom(),
    %% direct, or the local wrapper function the fun goes through
    via :: direct | {atom(), arity()}
}).

%% A local function that forwards its parameter number `index' to mria.
-record(wrapper, {
    name :: atom(),
    arity :: arity(),
    index :: pos_integer(),
    mria_fun :: atom(),
    line :: pos_integer()
}).

main([]) ->
    Sites = scan(),
    Actual = count(Sites),
    Allowed = read_allowlist(),
    case diff(Actual, Allowed) of
        [] ->
            io:format("OK: ~p module(s), ~p mria transaction fun(s) passed by value~n", [
                length(Actual), length(Sites)
            ]),
            halt(0);
        Diff ->
            io:format(
                standard_error, "ERROR: mria transaction funs passed by value changed:~n", []
            ),
            lists:foreach(fun report_diff/1, Diff),
            Grown = [M || {M, Was, Now} <- Diff, Now > Was],
            Shrunk = [M || {M, Was, Now} <- Diff, Now < Was],
            Grown =/= [] andalso
                begin
                    io:format(standard_error, "~nCall sites:~n", []),
                    [print_site(S) || S <- Sites, lists:member(module_of(S), Grown)],
                    io:format(
                        standard_error,
                        "~nA fun passed by value fails with `badfun' on a core node that runs "
                        "another build of the module.~n"
                        "Put the transaction fun in a transaction BPAPI module, "
                        "`<api>_tx_proto_vN' (see apps/emqx_bpapi/README.md), and pass it by "
                        "name: `fun <api>_tx_proto_vN:f/N' with an argument list.~n"
                        "The allowlist in ~s is not meant to grow.~n",
                        [?ALLOWLIST]
                    )
                end,
            Shrunk =/= [] andalso
                io:format(
                    standard_error,
                    "~nTo accept a removal, lower the count of the module in ~s by hand, "
                    "or delete its line when it reaches 0.~n",
                    [?ALLOWLIST]
                ),
            halt(1)
    end;
main(["--list"]) ->
    lists:foreach(fun print_site/1, scan());
main(_) ->
    io:format(standard_error, "Usage: check-mria-tx-funs.escript [--list]~n", []),
    halt(1).

print_site(#site{file = File, line = Line, mria_fun = Fun, kind = Kind, via = Via}) ->
    ViaStr =
        case Via of
            direct -> "";
            {Name, Arity} -> io_lib:format(" (via ~p/~p)", [Name, Arity])
        end,
    io:format("~s:~p: mria:~p ~s~s~n", [File, Line, Fun, Kind, ViaStr]).

%%--------------------------------------------------------------------
%% Scan
%%--------------------------------------------------------------------

%% Returns one entry per call site whose fun argument is not an
%% external fun.
scan() ->
    cd_repo_root(),
    Files = lists:sort(filelib:wildcard(?SRC_GLOB)),
    lists:append([scan_file(F) || F <- Files]).

scan_file(File) ->
    case epp_dodger:parse_file(File) of
        {ok, Forms0} ->
            Forms = drop_test_forms(Forms0),
            Funs = [F || F <- Forms, erl_syntax:type(F) =:= function],
            Exports = exports(Forms),
            {Sites, Wrappers} = lists:foldl(
                fun(Fun, Acc) -> scan_function(File, Fun, Acc) end,
                {[], []},
                Funs
            ),
            lists:reverse(follow_wrappers(File, Funs, Exports, Wrappers, Sites, []));
        {error, Reason} ->
            io:format(standard_error, "ERROR: cannot parse ~s: ~p~n", [File, Reason]),
            halt(1)
    end.

%% Forms under `-ifdef(TEST)', and under the `-else' of `-ifndef(TEST)',
%% are not in a release build and are not checked.
drop_test_forms(Forms) ->
    drop_test_forms(Forms, [], []).

drop_test_forms([], _Stack, Acc) ->
    lists:reverse(Acc);
drop_test_forms([F | Rest], Stack, Acc) ->
    case preprocessor(F) of
        {ifdef, Macro} ->
            drop_test_forms(Rest, [{Macro, Macro =:= 'TEST'} | Stack], Acc);
        {ifndef, Macro} ->
            drop_test_forms(Rest, [{Macro, false} | Stack], Acc);
        'else' ->
            [{Macro, Skip} | Stack1] = Stack,
            drop_test_forms(Rest, [{Macro, Macro =:= 'TEST' andalso not Skip} | Stack1], Acc);
        endif ->
            drop_test_forms(Rest, tl(Stack), Acc);
        none ->
            case lists:any(fun({_, Skip}) -> Skip end, Stack) of
                true -> drop_test_forms(Rest, Stack, Acc);
                false -> drop_test_forms(Rest, Stack, [F | Acc])
            end
    end.

preprocessor(F) ->
    case erl_syntax:type(F) of
        attribute ->
            Name = atom_value(erl_syntax:attribute_name(F)),
            case {Name, erl_syntax:attribute_arguments(F)} of
                {ifdef, [M]} -> {ifdef, macro_name(M)};
                {ifndef, [M]} -> {ifndef, macro_name(M)};
                %% `-if(Cond)' and `-elif(Cond)' never select test code
                {'if', _} -> {ifdef, '$if'};
                {elif, _} -> 'else';
                {'else', _} -> 'else';
                {endif, _} -> endif;
                _ -> none
            end;
        _ ->
            none
    end.

%% `-ifdef(TEST)' parses as a variable, `-ifdef(test)' as an atom.
macro_name(Node) ->
    case erl_syntax:type(Node) of
        variable -> erl_syntax:variable_name(Node);
        atom -> erl_syntax:atom_value(Node);
        _ -> undefined
    end.

exports(Forms) ->
    lists:append([
        [
            {
                erl_syntax:atom_value(erl_syntax:arity_qualifier_body(AQ)),
                erl_syntax:integer_value(erl_syntax:arity_qualifier_argument(AQ))
            }
         || AQ <- erl_syntax:list_elements(hd(erl_syntax:attribute_arguments(F)))
        ]
     || F <- Forms,
        erl_syntax:type(F) =:= attribute,
        atom_value(erl_syntax:attribute_name(F)) =:= export
    ]).

%% The direct mria calls of one function: sites, and the parameters the
%% function forwards to mria (wrappers).
scan_function(File, Fun, Acc) ->
    Name = erl_syntax:atom_value(erl_syntax:function_name(Fun)),
    Arity = erl_syntax:function_arity(Fun),
    lists:foldl(
        fun(Clause, {Sites, Wrappers}) ->
            Ctx = clause_context(Clause),
            erl_syntax_lib:fold(
                fun(Node, {S, W}) ->
                    case mria_tx_call(Node) of
                        {ok, MriaFun, FunArg} ->
                            case classify(FunArg, Ctx) of
                                external ->
                                    {S, W};
                                {param, Index} ->
                                    Wrapper = #wrapper{
                                        name = Name,
                                        arity = Arity,
                                        index = Index,
                                        mria_fun = MriaFun,
                                        line = line(Node)
                                    },
                                    {S, [Wrapper | W]};
                                Kind ->
                                    Site = #site{
                                        file = File,
                                        line = line(Node),
                                        mria_fun = MriaFun,
                                        kind = Kind,
                                        via = direct
                                    },
                                    {[Site | S], W}
                            end;
                        false ->
                            {S, W}
                    end
                end,
                {Sites, Wrappers},
                Clause
            )
        end,
        Acc,
        erl_syntax:function_clauses(Fun)
    ).

%% Find the callers of each wrapper and classify the argument they pass
%% in the forwarded position. A caller that forwards its own parameter
%% is a wrapper too, and is followed in turn.
follow_wrappers(_File, _Funs, _Exports, [], Sites, _Seen) ->
    Sites;
follow_wrappers(File, Funs, Exports, [W | Rest], Sites0, Seen) ->
    Key = {W#wrapper.name, W#wrapper.arity, W#wrapper.index},
    case lists:member(Key, Seen) of
        true ->
            follow_wrappers(File, Funs, Exports, Rest, Sites0, Seen);
        false ->
            Sites1 =
                case lists:member({W#wrapper.name, W#wrapper.arity}, Exports) of
                    true ->
                        [
                            #site{
                                file = File,
                                line = W#wrapper.line,
                                mria_fun = W#wrapper.mria_fun,
                                kind = exported_wrapper,
                                via = direct
                            }
                            | Sites0
                        ];
                    false ->
                        Sites0
                end,
            {Sites, NewWrappers} = lists:foldl(
                fun(Fun, Acc) -> scan_wrapper_callers(File, Fun, W, Acc) end,
                {Sites1, []},
                Funs
            ),
            follow_wrappers(File, Funs, Exports, NewWrappers ++ Rest, Sites, [Key | Seen])
    end.

scan_wrapper_callers(File, Fun, W, Acc) ->
    Name = erl_syntax:atom_value(erl_syntax:function_name(Fun)),
    Arity = erl_syntax:function_arity(Fun),
    lists:foldl(
        fun(Clause, {Sites, Wrappers}) ->
            Ctx = clause_context(Clause),
            erl_syntax_lib:fold(
                fun(Node, {S, Ws}) ->
                    case local_call(Node, W#wrapper.name, W#wrapper.arity) of
                        {ok, Args} ->
                            FunArg = lists:nth(W#wrapper.index, Args),
                            case classify(FunArg, Ctx) of
                                external ->
                                    {S, Ws};
                                {param, Index} ->
                                    Wrapper = #wrapper{
                                        name = Name,
                                        arity = Arity,
                                        index = Index,
                                        mria_fun = W#wrapper.mria_fun,
                                        line = line(Node)
                                    },
                                    {S, [Wrapper | Ws]};
                                Kind ->
                                    Site = #site{
                                        file = File,
                                        line = line(Node),
                                        mria_fun = W#wrapper.mria_fun,
                                        kind = Kind,
                                        via = {W#wrapper.name, W#wrapper.arity}
                                    },
                                    {[Site | S], Ws}
                            end;
                        false ->
                            {S, Ws}
                    end
                end,
                {Sites, Wrappers},
                Clause
            )
        end,
        Acc,
        erl_syntax:function_clauses(Fun)
    ).

%% mria:F(Shard, Fun, ...) with F in ?MRIA_FUNS. The fun is always the
%% second argument. A call on the local content shard runs on the node
%% itself and is not a site.
mria_tx_call(Node) ->
    case erl_syntax:type(Node) of
        application ->
            Op = erl_syntax:application_operator(Node),
            Args = erl_syntax:application_arguments(Node),
            case {erl_syntax:type(Op), Args} of
                {module_qualifier, [Shard, FunArg | _]} ->
                    Mod = erl_syntax:module_qualifier_argument(Op),
                    Fun = erl_syntax:module_qualifier_body(Op),
                    case {atom_value(Mod), atom_value(Fun)} of
                        {mria, F} ->
                            case lists:member(F, ?MRIA_FUNS) andalso not is_local_shard(Shard) of
                                true -> {ok, F, FunArg};
                                false -> false
                            end;
                        _ ->
                            false
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.

is_local_shard(Node) ->
    erl_syntax:type(Node) =:= application andalso
        erl_syntax:application_arguments(Node) =:= [] andalso
        qualified_name(erl_syntax:application_operator(Node)) =:= {mria, local_content_shard}.

%% A call to the local function Name/Arity: `Name(...)' or
%% `?MODULE:Name(...)'.
local_call(Node, Name, Arity) ->
    case erl_syntax:type(Node) of
        application ->
            Op = erl_syntax:application_operator(Node),
            Args = erl_syntax:application_arguments(Node),
            IsCall =
                length(Args) =:= Arity andalso
                    case erl_syntax:type(Op) of
                        atom ->
                            erl_syntax:atom_value(Op) =:= Name;
                        module_qualifier ->
                            is_module_macro(erl_syntax:module_qualifier_argument(Op)) andalso
                                atom_value(erl_syntax:module_qualifier_body(Op)) =:= Name;
                        _ ->
                            false
                    end,
            case IsCall of
                true -> {ok, Args};
                false -> false
            end;
        _ ->
            false
    end.

is_module_macro(Node) ->
    erl_syntax:type(Node) =:= macro andalso
        atom_value(erl_syntax:macro_name(Node)) =:= 'MODULE'.

qualified_name(Op) ->
    case erl_syntax:type(Op) of
        module_qualifier ->
            {
                atom_value(erl_syntax:module_qualifier_argument(Op)),
                atom_value(erl_syntax:module_qualifier_body(Op))
            };
        _ ->
            undefined
    end.

atom_value(Node) ->
    case erl_syntax:type(Node) of
        atom -> erl_syntax:atom_value(Node);
        _ -> undefined
    end.

%% What a clause tells about its variables: the parameters it binds in
%% its head (name to position), and the expressions matched to a
%% variable in its body (`Fun = fun() -> ... end').
clause_context(Clause) ->
    Params = maps:from_list(
        lists:filtermap(
            fun({Index, Pattern}) ->
                case erl_syntax:type(Pattern) of
                    variable -> {true, {erl_syntax:variable_name(Pattern), Index}};
                    _ -> false
                end
            end,
            lists:enumerate(erl_syntax:clause_patterns(Clause))
        )
    ),
    Bindings = erl_syntax_lib:fold(
        fun(Node, Acc) ->
            case erl_syntax:type(Node) of
                match_expr ->
                    Pattern = erl_syntax:match_expr_pattern(Node),
                    case erl_syntax:type(Pattern) of
                        variable ->
                            Var = erl_syntax:variable_name(Pattern),
                            Body = erl_syntax:match_expr_body(Node),
                            %% Bound in more than one branch: do not guess.
                            maps:update_with(Var, fun(_) -> ambiguous end, Body, Acc);
                        _ ->
                            Acc
                    end;
                _ ->
                    Acc
            end
        end,
        #{},
        Clause
    ),
    #{params => Params, bindings => Bindings}.

%% external: `fun m:f/1' or `fun ?MODULE:f/1'. {param, N}: the N-th
%% parameter of the enclosing function. Everything else is sent by
%% value, or cannot be told apart from it without running the code.
classify(Node, Ctx = #{params := Params, bindings := Bindings}) ->
    case erl_syntax:type(Node) of
        implicit_fun ->
            Name = erl_syntax:implicit_fun_name(Node),
            case erl_syntax:type(Name) of
                module_qualifier -> external;
                _ -> local_fun
            end;
        fun_expr ->
            anonymous_fun;
        named_fun_expr ->
            anonymous_fun;
        variable ->
            Var = erl_syntax:variable_name(Node),
            case Bindings of
                #{Var := ambiguous} ->
                    variable;
                #{Var := Expr} ->
                    classify(Expr, Ctx#{bindings := maps:remove(Var, Bindings)});
                #{} ->
                    case Params of
                        #{Var := Index} -> {param, Index};
                        #{} -> variable
                    end
            end;
        _ ->
            expression
    end.

line(Node) ->
    erl_anno:line(erl_syntax:get_pos(Node)).

%%--------------------------------------------------------------------
%% Allowlist
%%--------------------------------------------------------------------

module_of(#site{file = File}) ->
    list_to_atom(filename:basename(File, ".erl")).

%% [{Module, Count}], sorted by module.
count(Sites) ->
    Counts = lists:foldl(
        fun(Site, Acc) ->
            maps:update_with(module_of(Site), fun(N) -> N + 1 end, 1, Acc)
        end,
        #{},
        Sites
    ),
    lists:sort(maps:to_list(Counts)).

read_allowlist() ->
    case file:consult(?ALLOWLIST) of
        {ok, [List]} when is_list(List) ->
            lists:sort(List);
        {error, enoent} ->
            io:format(standard_error, "ERROR: ~s not found~n", [?ALLOWLIST]),
            halt(1);
        Other ->
            io:format(standard_error, "ERROR: cannot read ~s: ~p~n", [?ALLOWLIST, Other]),
            halt(1)
    end.

diff(Actual, Allowed) ->
    Mods = lists:usort([M || {M, _} <- Actual ++ Allowed]),
    [
        {M, Was, Now}
     || M <- Mods,
        Was <- [proplists:get_value(M, Allowed, 0)],
        Now <- [proplists:get_value(M, Actual, 0)],
        Was =/= Now
    ].

report_diff({Mod, Was, Now}) ->
    io:format(standard_error, "  ~p: allowed ~p, found ~p~n", [Mod, Was, Now]).

cd_repo_root() ->
    Root = filename:dirname(filename:dirname(filename:absname(escript:script_name()))),
    ok = file:set_cwd(Root).
