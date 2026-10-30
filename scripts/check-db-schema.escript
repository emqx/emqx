#!/usr/bin/env escript
%% -*- erlang -*-
%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% Check that the set of transaction BPAPI versions has not changed
%% since the baseline of the current db_schema version was taken.
%%
%% A transaction BPAPI module (`<api>_tx_proto_v<N>', see
%% apps/emqx_bpapi/README.md) holds the funs that a replicant passes to
%% mria by name. A core node runs them inside its own transaction, so it
%% must have the module: a replicant that ships `fun x_tx_proto_v2:f/1'
%% to a core without v2 gets `undef'. EMQX_DB_SCHEMA_VSN
%% (apps/emqx/include/emqx_release.hrl) is what replicant and core
%% compare before they connect, so it must change when a transaction
%% API version is added or removed. This check makes that bump hard to
%% forget: the baseline records every `{API, Version}', and a different
%% set fails the sanity-checks job until the version is bumped and the
%% baseline regenerated.
%%
%% The table half of the baseline needs a running node and is checked
%% by scripts/test/db-schema-baseline-test.sh.
%%
%% Usage:
%%   ./scripts/check-db-schema.escript            # check
%%   ./scripts/check-db-schema.escript --update   # rewrite tx_apis.eterm for the current version

-mode(compile).

-define(RELEASE_HRL, "apps/emqx/include/emqx_release.hrl").
-define(BASELINE_DIR, "apps/emqx_machine/priv/db_schema").
-define(PROTO_GLOB, "apps/*/src/**/*_tx_proto_v*.erl").

main([]) ->
    cd_repo_root(),
    Vsn = db_schema_vsn(),
    Dir = baseline_dir(Vsn),
    ok = ensure_baseline_files(Vsn, Dir),
    Actual = tx_apis(),
    Baseline = read_tx_apis(Dir),
    case {Actual -- Baseline, Baseline -- Actual} of
        {[], []} ->
            io:format("OK: db_schema ~s, ~p transaction API version(s) unchanged~n", [
                Vsn, length(Actual)
            ]),
            halt(0);
        {Added, Removed} ->
            io:format(
                standard_error,
                "ERROR: transaction API versions changed since db_schema ~s was baselined:~n",
                [Vsn]
            ),
            [io:format(standard_error, "  added ~p v~p~n", [A, V]) || {A, V} <- Added],
            [io:format(standard_error, "  removed ~p v~p~n", [A, V]) || {A, V} <- Removed],
            io:format(
                standard_error,
                "~nA replicant ships these funs by name to a core node, which must have "
                "the module, so a changed set needs a new db_schema version.~n"
                "Bump EMQX_DB_SCHEMA_VSN in ~s, then run:~n"
                "  ./scripts/check-db-schema.escript --update~n"
                "  ./scripts/test/db-schema-baseline-test.sh --update~n",
                [?RELEASE_HRL]
            ),
            halt(1)
    end;
main(["--update"]) ->
    cd_repo_root(),
    Vsn = db_schema_vsn(),
    Dir = baseline_dir(Vsn),
    ok = filelib:ensure_path(Dir),
    File = tx_apis_file(Dir),
    ok = write_tx_apis(File, Vsn, tx_apis()),
    io:format("Wrote ~s~n", [File]);
main(_) ->
    io:format(standard_error, "Usage: check-db-schema.escript [--update]~n", []),
    halt(1).

%%--------------------------------------------------------------------
%% Version and baseline files
%%--------------------------------------------------------------------

db_schema_vsn() ->
    {ok, Hrl} = file:read_file(?RELEASE_HRL),
    case re:run(Hrl, <<"-define\\(EMQX_DB_SCHEMA_VSN, \"([^\"]+)\"\\)">>, [{capture, [1], list}]) of
        {match, [Vsn]} ->
            Vsn;
        nomatch ->
            io:format(standard_error, "ERROR: EMQX_DB_SCHEMA_VSN not found in ~s~n", [?RELEASE_HRL]),
            halt(1)
    end.

baseline_dir(Vsn) ->
    filename:join(?BASELINE_DIR, Vsn).

tx_apis_file(Dir) ->
    filename:join(Dir, "tx_apis.eterm").

tables_file(Dir) ->
    filename:join(Dir, "tables.eterm").

ensure_baseline_files(Vsn, Dir) ->
    Missing = [F || F <- [tx_apis_file(Dir), tables_file(Dir)], not filelib:is_regular(F)],
    case Missing of
        [] ->
            ok;
        _ ->
            io:format(standard_error, "ERROR: baseline for db_schema ~s is incomplete:~n", [Vsn]),
            [io:format(standard_error, "  missing ~s~n", [F]) || F <- Missing],
            io:format(
                standard_error,
                "Generate it with:~n"
                "  ./scripts/check-db-schema.escript --update~n"
                "  ./scripts/test/db-schema-baseline-test.sh --update~n",
                []
            ),
            halt(1)
    end.

read_tx_apis(Dir) ->
    File = tx_apis_file(Dir),
    case file:consult(File) of
        {ok, [List]} when is_list(List) ->
            lists:sort(List);
        Other ->
            io:format(standard_error, "ERROR: cannot read ~s: ~p~n", [File, Other]),
            halt(1)
    end.

write_tx_apis(File, Vsn, APIs) ->
    Header = io_lib:format(
        "%% Every transaction BPAPI version (<api>_tx_proto_v<N> module) in the tree~n"
        "%% when db_schema ~s was baselined. Generated by scripts/check-db-schema.escript --update.~n",
        [Vsn]
    ),
    Body = [io_lib:format("    {~p, ~p}", [A, V]) || {A, V} <- APIs],
    ok = file:write_file(File, [Header, "[\n", lists:join(",\n", Body), "\n].\n"]),
    %% make fmt formats .eterm files too; write what it would produce.
    case os:cmd("./scripts/erlfmt -w " ++ File ++ " && echo formatted") of
        "formatted\n" ->
            ok;
        Output ->
            io:format(standard_error, "ERROR: erlfmt failed on ~s:~n~s~n", [File, Output]),
            halt(1)
    end.

%%--------------------------------------------------------------------
%% Transaction APIs
%%--------------------------------------------------------------------

%% [{API, Version}], sorted. One entry per `<api>_tx_proto_v<N>' module.
tx_apis() ->
    lists:sort(
        lists:filtermap(
            fun(File) ->
                Mod = filename:basename(File, ".erl"),
                case re:run(Mod, "^(.*_tx)_proto_v([0-9]+)$", [{capture, all_but_first, list}]) of
                    {match, [API, Vsn]} ->
                        {true, {list_to_atom(API), list_to_integer(Vsn)}};
                    nomatch ->
                        io:format(standard_error, "ERROR: ~s is not a *_tx_proto_v<N> module~n", [
                            File
                        ]),
                        halt(1)
                end
            end,
            filelib:wildcard(?PROTO_GLOB)
        )
    ).

cd_repo_root() ->
    Root = filename:dirname(filename:dirname(filename:absname(escript:script_name()))),
    ok = file:set_cwd(Root).
