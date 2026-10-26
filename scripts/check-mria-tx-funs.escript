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
%% mria:async_dirty/2,3, mria:sync_dirty/2,3 and mria:ro_transaction/2
%% send the fun argument to a core node, which runs it inside a mnesia
%% transaction. An anonymous fun (`fun() -> ... end'), a local fun
%% (`fun f/1') or a variable bound to one of them carries the identity
%% of the module version that created it. When the core runs a different
%% build of that module, the call fails with `badfun'. Only an external
%% fun (`fun ?MODULE:f/1', `fun m:f/1') is called by name and survives a
%% version difference.
%%
%% The existing call sites are recorded per module in the allowlist,
%% which is maintained by hand and only ever shrinks. A module that
%% gains a call site fails this check. A module that loses one fails
%% too, until its count in the allowlist is lowered to match.
%%
%% The check reads the sources, so it does not need a build.
%%
%% Usage:
%%   ./scripts/check-mria-tx-funs.escript            # check against the allowlist
%%   ./scripts/check-mria-tx-funs.escript --list     # print every call site

-mode(compile).

-define(ALLOWLIST, "scripts/mria-tx-fun-allowlist.eterm").
-define(SRC_GLOB, "apps/*/src/**/*.erl").
-define(MRIA_FUNS, [transaction, sync_transaction, async_dirty, sync_dirty, ro_transaction]).

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
                io:format(
                    standard_error,
                    "~nA fun passed by value fails with `badfun' on a core node that runs "
                    "another build of the module.~n"
                    "An operation that a replicant needs a core to run is an inter-broker API: "
                    "define a BPAPI module for it (see apps/emqx_bpapi) and run the transaction "
                    "on the core behind it. When it must stay a mria call, pass "
                    "`fun ?MODULE:f/N' with an argument list.~n"
                    "The allowlist in ~s is not meant to grow.~n",
                    [?ALLOWLIST]
                ),
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
    lists:foreach(
        fun({File, Line, Fun, Kind}) ->
            io:format("~s:~p: mria:~p ~s~n", [File, Line, Fun, Kind])
        end,
        scan()
    );
main(_) ->
    io:format(standard_error, "Usage: check-mria-tx-funs.escript [--list]~n", []),
    halt(1).

%%--------------------------------------------------------------------
%% Scan
%%--------------------------------------------------------------------

%% Returns one entry per call site whose fun argument is not an
%% external fun: {File, Line, MriaFun, Kind}.
scan() ->
    cd_repo_root(),
    Files = lists:sort(filelib:wildcard(?SRC_GLOB)),
    lists:append([scan_file(F) || F <- Files]).

scan_file(File) ->
    case epp_dodger:parse_file(File) of
        {ok, Forms} ->
            lists:append([scan_form(File, F) || F <- Forms]);
        {error, Reason} ->
            io:format(standard_error, "ERROR: cannot parse ~s: ~p~n", [File, Reason]),
            halt(1)
    end.

scan_form(File, Form) ->
    erl_syntax_lib:fold(
        fun(Node, Acc) ->
            case mria_tx_call(Node) of
                {ok, Fun, FunArg} ->
                    case classify(FunArg) of
                        external -> Acc;
                        Kind -> [{File, line(Node), Fun, Kind} | Acc]
                    end;
                false ->
                    Acc
            end
        end,
        [],
        Form
    ).

%% mria:F(Shard, Fun, ...) with F in ?MRIA_FUNS. The fun is always the
%% second argument.
mria_tx_call(Node) ->
    case erl_syntax:type(Node) of
        application ->
            Op = erl_syntax:application_operator(Node),
            Args = erl_syntax:application_arguments(Node),
            case {erl_syntax:type(Op), Args} of
                {module_qualifier, [_Shard, FunArg | _]} ->
                    Mod = erl_syntax:module_qualifier_argument(Op),
                    Fun = erl_syntax:module_qualifier_body(Op),
                    case {atom_value(Mod), atom_value(Fun)} of
                        {mria, F} ->
                            case lists:member(F, ?MRIA_FUNS) of
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

atom_value(Node) ->
    case erl_syntax:type(Node) of
        atom -> erl_syntax:atom_value(Node);
        _ -> undefined
    end.

%% external: `fun m:f/1' or `fun ?MODULE:f/1'. Everything else is sent
%% by value, or cannot be told apart from it without running the code.
classify(Node) ->
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
            variable;
        _ ->
            expression
    end.

line(Node) ->
    erl_anno:line(erl_syntax:get_pos(Node)).

%%--------------------------------------------------------------------
%% Allowlist
%%--------------------------------------------------------------------

%% [{Module, Count}], sorted by module.
count(Sites) ->
    Counts = lists:foldl(
        fun({File, _, _, _}, Acc) ->
            Mod = list_to_atom(filename:basename(File, ".erl")),
            maps:update_with(Mod, fun(N) -> N + 1 end, 1, Acc)
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
