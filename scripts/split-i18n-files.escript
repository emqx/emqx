#!/usr/bin/env escript

%% This script is for one-time use.
%% will be deleted after the migration is done.

-mode(compile).

main([]) ->
    %% we need to parse hocon
    %% so we'll just add all compiled libs to path
    code:add_pathsz(find_ebin_paths("_build/default/lib/*")),
    Files = filelib:wildcard("rel/i18n/*.hocon"),
    ok = lists:foreach(fun split_file/1, Files),
    ok.

find_ebin_paths(DirPattern) ->
    LibDirs = filelib:wildcard(DirPattern),
    lists:filtermap(fun add_ebin/1, LibDirs).

add_ebin(Dir) ->
    EbinDir = filename:join(Dir, "ebin"),
    case filelib:is_dir(EbinDir) of
        true -> {true, EbinDir};
        false -> false
    end.

split_file(Path) ->
    {ok, DescMap} = hocon:load(Path),
    [{Module, Descs}] = maps:to_list(DescMap),
    try
        ok = split(Path, Module, <<"en">>, Descs),
        ok = split(Path, Module, <<"zh">>, Descs)
    catch
        throw : already_done ->
            ok
    end.

split(Path, Module, Lang, Fields) when is_map(Fields) ->
    split(Path, Module, Lang, maps:to_list(Fields));
split(Path, Module, Lang, Fields) when is_list(Fields) ->
    Split = lists:map(fun({Name, Desc})-> do_split(Path, Name, Lang, Desc) end, Fields),
    IoData = [Module, " {\n\n", Split, "}\n"],
    %% assert it's a valid HOCON object
    {ok, _} = hocon:binary(IoData),
    %io:format(user, "~s", [IoData]).
    WritePath = case Lang of
                    <<"en">> ->
                        Path;
                    <<"zh">> ->
                        rename(Path, "zh")
                end,
    ok = filelib:ensure_dir(WritePath),
    ok = file:write_file(WritePath, IoData),
    ok.

rename(FilePath, Lang) ->
    Dir = filename:dirname(FilePath),
    BaseName = filename:basename(FilePath),
    filename:join([Dir, Lang, BaseName]).

do_split(_Path, _Name, _Lang, #{<<"desc">> := Desc}) when is_binary(Desc) ->
    throw(already_done);
do_split(Path, Name, Lang, #{<<"desc">> := Desc} = D) ->
    try
        Label = maps:get(<<"label">>, D, #{}),
        DescL = maps:get(Lang, Desc),
        LabelL = maps:get(Lang, Label, undefined),
        [fmt([Name, ".desc:\n"], DescL),
        fmt([Name, ".label:\n"], LabelL)
        ]
    catch
        C : E : S->
            erlang:raise(C, {Path, Name, E}, S)
    end.


tq() ->
    "\"\"\"".

fmt(_Key, undefined) ->
    [];
fmt(Key, Content) ->
    [Key, tq(), Content, tq(), "\n\n"].

