#!/usr/bin/env escript
%%! -mode(compile)
%%
%% Convert a directory of .coverdata files into a single Cobertura XML report.
%%
%% Bypasses OTP's cover_server gen_server (which serialises every import and
%% analyse through a single ETS owner) by reading the well-known coverdata
%% on-disk format directly, in parallel, and summing per-{module,line} hit
%% counts in plain Erlang maps.
%%
%% Output schema is line-level Cobertura - the only part Codecov consumes for
%% line coverage. <methods> blocks are not emitted.
%%
%% Usage:
%%   cover-to-cobertura.escript -cover DIR [-output FILE] [-appname NAME]
%%                              [-lookup DIR] [-parallel N]
%%
%% The .coverdata on-disk format is defined by lib/tools/src/cover.erl's
%% `get_term/1` and `write/2`: a 1-byte size header followed by either a
%% small term-to-binary payload or a `{'$size', Size2}` marker pointing at a
%% larger compressed payload. Each entry is one of:
%%   {file, Module, BeamPath}
%%   {#bump{module=M, function=F, arity=A, clause=C, line=L}, Count}

-record(bump, {module = '_', function = '_', arity = '_', clause = '_', line = '_'}).
-record(opts, {
    cover_dir,
    output = "coverage.xml",
    appname = "Application",
    lookup = "src",
    parallel = erlang:system_info(schedulers)
}).

main(Args) ->
    Opts = parse_args(Args, #opts{}),
    case Opts#opts.cover_dir of
        undefined ->
            usage(),
            halt(1);
        Dir ->
            ok = run(Opts#opts{cover_dir = Dir})
    end.

usage() ->
    io:format(
        "Usage: cover-to-cobertura.escript -cover DIR [-output FILE] "
        "[-appname NAME] [-lookup DIR] [-parallel N]~n"
    ).

parse_args([], Acc) ->
    Acc;
parse_args(["-cover", V | R], A) ->
    parse_args(R, A#opts{cover_dir = V});
parse_args(["-output", V | R], A) ->
    parse_args(R, A#opts{output = V});
parse_args(["-appname", V | R], A) ->
    parse_args(R, A#opts{appname = V});
parse_args(["-lookup", V | R], A) ->
    parse_args(R, A#opts{lookup = V});
parse_args(["-parallel", V | R], A) ->
    parse_args(R, A#opts{parallel = list_to_integer(V)});
parse_args(Other, _) ->
    io:format(standard_error, "Unknown args: ~p~n", [Other]),
    usage(),
    halt(1).

run(#opts{
    cover_dir = Dir,
    output = Out,
    appname = App,
    lookup = Lk,
    parallel = Par
}) ->
    Files = list_coverdata(Dir),
    case Files of
        [] ->
            io:format(standard_error, "No .coverdata files in ~s~n", [Dir]),
            halt(1);
        _ ->
            ok
    end,
    io:format(
        "Reading ~p .coverdata files with ~p workers...~n",
        [length(Files), Par]
    ),
    T0 = mono_ms(),
    LineCounts = parallel_read(Files, Par),
    T1 = mono_ms(),
    io:format(
        "  read+merge: ~p ms (~p {module,line} entries)~n",
        [T1 - T0, maps:size(LineCounts)]
    ),
    SrcLookup = build_lookup(Lk),
    T2 = mono_ms(),
    io:format(
        "  source lookup: ~p ms (~p .erl files indexed)~n",
        [T2 - T1, maps:size(SrcLookup)]
    ),
    ByModule = group_by_module(LineCounts),
    Modules = [M || M <- maps:keys(ByModule), maps:is_key(M, SrcLookup)],
    io:format(
        "  modules with source: ~p / ~p (others skipped)~n",
        [length(Modules), maps:size(ByModule)]
    ),
    write_cobertura(Out, App, Modules, ByModule, SrcLookup),
    T3 = mono_ms(),
    io:format("  wrote ~s in ~p ms~n", [Out, T3 - T2]),
    io:format("Done in ~p ms.~n", [T3 - T0]),
    ok.

mono_ms() -> erlang:monotonic_time(millisecond).

list_coverdata(Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            [filename:join(Dir, N) || N <- Names, filename:extension(N) =:= ".coverdata"];
        {error, Reason} ->
            io:format(standard_error, "Cannot list ~s: ~p~n", [Dir, Reason]),
            halt(1)
    end.

%%---------------------------------------------------------------- parallel read

parallel_read(Files, N) ->
    Chunks = partition(Files, max(N, 1)),
    Parent = self(),
    Refs = [{erlang:make_ref(), Chunk} || Chunk <- Chunks],
    [
        spawn_link(fun() -> Parent ! {Ref, read_chunk(Chunk)} end)
     || {Ref, Chunk} <- Refs
    ],
    lists:foldl(
        fun({Ref, _}, Acc) ->
            receive
                {Ref, M} -> merge_counts(Acc, M)
            end
        end,
        #{},
        Refs
    ).

%% Round-robin partition keeps work even when shards differ in size.
partition(Files, N) ->
    Buckets = list_to_tuple(lists:duplicate(N, [])),
    Final = element(
        1,
        lists:foldl(
            fun(F, {B, I}) ->
                Pos = (I rem N) + 1,
                {setelement(Pos, B, [F | element(Pos, B)]), I + 1}
            end,
            {Buckets, 0},
            Files
        )
    ),
    [Bucket || Bucket <- tuple_to_list(Final), Bucket =/= []].

read_chunk(Files) ->
    lists:foldl(fun read_file/2, #{}, Files).

read_file(Path, Acc) ->
    case file:open(Path, [read, binary, raw, {read_ahead, 1048576}]) of
        {ok, Fd} ->
            try
                read_loop(Fd, Acc)
            after
                file:close(Fd)
            end;
        {error, Reason} ->
            io:format(standard_error, "Skip ~s: ~p~n", [Path, Reason]),
            Acc
    end.

read_loop(Fd, Acc) ->
    case get_term(Fd) of
        eof ->
            Acc;
        {file, _Mod, _Path} ->
            read_loop(Fd, Acc);
        {#bump{module = M, line = L}, Count} when is_integer(L), L > 0 ->
            Key = {M, L},
            read_loop(Fd, maps:update_with(Key, fun(C) -> C + Count end, Count, Acc));
        _Other ->
            read_loop(Fd, Acc)
    end.

%% Mirrors lib/tools/src/cover.erl `get_term/1`.
get_term(Fd) ->
    case file:read(Fd, 1) of
        {ok, <<Size1:8>>} ->
            {ok, Bin1} = file:read(Fd, Size1),
            case binary_to_term(Bin1) of
                {'$size', Size2} ->
                    {ok, Bin2} = file:read(Fd, Size2),
                    binary_to_term(Bin2);
                Term ->
                    Term
            end;
        eof ->
            eof
    end.

merge_counts(A, B) when map_size(A) >= map_size(B) ->
    maps:fold(
        fun(K, V, Acc) ->
            maps:update_with(K, fun(C) -> C + V end, V, Acc)
        end,
        A,
        B
    );
merge_counts(A, B) ->
    merge_counts(B, A).

group_by_module(LineCounts) ->
    maps:fold(
        fun({M, L}, V, Acc) ->
            Inner = maps:get(M, Acc, #{}),
            maps:put(M, maps:update_with(L, fun(C) -> C + V end, V, Inner), Acc)
        end,
        #{},
        LineCounts
    ).

%%---------------------------------------------------------------- source lookup

build_lookup(Dir) ->
    Paths = filelib:wildcard(filename:join([Dir, "**", "*.erl"])),
    lists:foldl(
        fun(P, Acc) ->
            Mod = list_to_atom(filename:basename(P, ".erl")),
            %% First match wins (consistent with covertool).
            case maps:is_key(Mod, Acc) of
                true -> Acc;
                false -> maps:put(Mod, P, Acc)
            end
        end,
        #{},
        Paths
    ).

%%------------------------------------------------------------------ write XML

write_cobertura(OutFile, AppName, Modules, ByModule, SrcLookup) ->
    {ok, Fd} = file:open(OutFile, [write, binary, raw, delayed_write]),
    try
        {TotCov, TotValid} = totals(Modules, ByModule),
        Rate = rate(TotCov, TotValid),
        Now = erlang:system_time(millisecond),
        ok = file:write(
            Fd,
            [
                <<"<?xml version=\"1.0\" encoding=\"utf-8\"?>\n">>,
                <<"<!DOCTYPE coverage SYSTEM \"http://cobertura.sourceforge.net/xml/coverage-04.dtd\">\n">>,
                io_lib:format(
                    "<coverage line-rate=\"~s\" lines-covered=\"~p\" lines-valid=\"~p\" "
                    "branch-rate=\"0.0\" branches-covered=\"0\" branches-valid=\"0\" "
                    "complexity=\"0\" version=\"1.9.4.1\" timestamp=\"~p\">\n"
                    "  <sources>\n    <source>.</source>\n  </sources>\n"
                    "  <packages>\n"
                    "    <package name=\"~s\" line-rate=\"~s\" branch-rate=\"0.0\" complexity=\"0\">\n"
                    "      <classes>\n",
                    [Rate, TotCov, TotValid, Now, xml_escape(AppName), Rate]
                )
            ]
        ),
        lists:foreach(
            fun(M) ->
                write_class(
                    Fd,
                    M,
                    maps:get(M, ByModule),
                    maps:get(M, SrcLookup)
                )
            end,
            lists:sort(Modules)
        ),
        ok = file:write(
            Fd,
            <<"      </classes>\n    </package>\n  </packages>\n</coverage>\n">>
        )
    after
        ok = file:close(Fd)
    end.

totals(Modules, ByModule) ->
    lists:foldl(
        fun(M, {C, V}) ->
            {Mc, Mv} = module_totals(maps:get(M, ByModule)),
            {C + Mc, V + Mv}
        end,
        {0, 0},
        Modules
    ).

module_totals(Lines) ->
    maps:fold(
        fun
            (_L, 0, {C, V}) -> {C, V + 1};
            (_L, _Hit, {C, V}) -> {C + 1, V + 1}
        end,
        {0, 0},
        Lines
    ).

write_class(Fd, M, Lines, SrcPath) ->
    {Cov, Valid} = module_totals(Lines),
    Rate = rate(Cov, Valid),
    Header = io_lib:format(
        "        <class name=\"~s\" filename=\"~s\" line-rate=\"~s\" "
        "branch-rate=\"0.0\" complexity=\"0\">\n"
        "          <methods/>\n          <lines>\n",
        [atom_to_list(M), xml_escape(SrcPath), Rate]
    ),
    ok = file:write(Fd, Header),
    LineRecords = lists:sort(maps:to_list(Lines)),
    LineXml = [
        io_lib:format(
            "            <line number=\"~p\" hits=\"~p\" branch=\"false\"/>\n",
            [L, H]
        )
     || {L, H} <- LineRecords
    ],
    ok = file:write(Fd, LineXml),
    ok = file:write(Fd, <<"          </lines>\n        </class>\n">>).

rate(_, 0) -> "0.0";
rate(Cov, Valid) -> io_lib:format("~.3f", [Cov / Valid]).

xml_escape(S) when is_atom(S) -> xml_escape(atom_to_list(S));
xml_escape(S) ->
    [
        case C of
            $< -> "&lt;";
            $> -> "&gt;";
            $& -> "&amp;";
            $" -> "&quot;";
            _ -> C
        end
     || C <- S
    ].
