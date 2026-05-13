-module(emqx_relup_file_utils).

-include_lib("kernel/include/file.hrl").

-export([
    ensure_dir/1,
    cp_r/2,
    tmp_dir/0,
    real_dir_path/1,
    ensure_dir_deleted/1,
    ensure_file_deleted/1,
    expand_env_variable/3,
    sh/2,
    chr/2
]).

-import(emqx_relup_utils, [make_error/2, bin/1]).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

ensure_dir(Path) ->
    filelib:ensure_dir(filename:join(Path, "fake_file")).

-spec cp_r(list(string()), file:filename()) -> 'ok'.
cp_r([], _Dest) ->
    ok;
cp_r(Sources, Dest) ->
    case os:type() of
        {unix, Os} ->
            % ensure destination exists before copying files into it
            {ok, []} = sh(
                ?FMT("mkdir -p ~ts", [escape_chars(Dest)]),
                [{use_stdout, false}]
            ),
            case filter_cp_dirs(Sources, Dest) of
                [] ->
                    ok;
                Sources1 ->
                    EscSources = [escape_chars(Src) || Src <- Sources1],
                    SourceStr = join(EscSources, " "),
                    % On darwin the following cp command will cp everything inside
                    % target vs target and everything inside, so we chop the last char
                    % off if it is a '/'
                    Source =
                        case {Os == darwin, lists:last(SourceStr) == $/} of
                            {true, true} -> string:trim(SourceStr, trailing, "/");
                            {true, false} -> SourceStr;
                            {false, _} -> SourceStr
                        end,
                    {ok, []} = sh(
                        ?FMT("cp -Rp ~ts \"~ts\"", [Source, escape_double_quotes(Dest)]),
                        [{use_stdout, true}]
                    ),
                    ok
            end;
        {win32, _} ->
            lists:foreach(fun(Src) -> ok = cp_r_win32(Src, Dest) end, Sources),
            ok
    end.

tmp_dir() ->
    case tmp_dir_1() of
        false -> throw(make_error(cannot_get_writable_tmp_dir, #{}));
        Dir -> Dir
    end.

ensure_dir_deleted(Dir) ->
    case file:del_dir_r(Dir) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw(make_error(failed_to_delete_dir, #{dir => Dir, reason => Reason}))
    end.

ensure_file_deleted(FileName) ->
    case file:delete(FileName) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            throw(make_error(failed_to_delete_file, #{file => FileName, reason => Reason}))
    end.

%% @doc gets the real path of a directory. This is mostly useful for
%% resolving symlinks. Be aware that this temporarily changes the
%% current working directory to figure out what the actual path
%% is. That means that it can be quite slow.
-spec real_dir_path(file:name() | binary()) -> file:name().
real_dir_path(Path) ->
    {ok, CurCwd} = file:get_cwd(),
    ok = file:set_cwd(Path),
    {ok, RealPath} = file:get_cwd(),
    ok = file:set_cwd(CurCwd),
    filename:absname(RealPath).

%% escape\ as\ a\ shell\?
escape_chars(Str) when is_atom(Str) ->
    escape_chars(atom_to_list(Str));
escape_chars(Str) ->
    re:replace(
        Str,
        "([ ()?`!$&;\"\'\|\\t|~<>])",
        "\\\\&",
        [global, {return, list}, unicode]
    ).

%% "escape inside these"
escape_double_quotes(Str) ->
    re:replace(
        Str,
        "([\"\\\\`!$&*;])",
        "\\\\&",
        [global, {return, list}, unicode]
    ).

%% @doc Given env. variable `FOO' we want to expand all references to
%% it in `InStr'. References can have two forms: `$FOO' and `${FOO}'
%% The end of form `$FOO' is delimited with whitespace or EOL
expand_env_variable(InStr, VarName, RawVarValue) ->
    case chr(InStr, $$) of
        0 ->
            %% No variables to expand
            InStr;
        _ ->
            ReOpts = [global, unicode, {return, list}],
            VarValue = re:replace(RawVarValue, "\\\\", "\\\\\\\\", ReOpts),
            %% Use a regex to match/replace:
            %% Given variable "FOO": match $FOO\s | $FOOeol | ${FOO}
            RegEx = ?FMT("\\\$(~ts(\\W|$)|{~ts})", [VarName, VarName]),
            re:replace(InStr, RegEx, [VarValue, "\\2"], ReOpts)
    end.

chr(S, C) when is_integer(C) -> chr(S, C, 1).
chr([C | _Cs], C, I) -> I;
chr([_ | Cs], C, I) -> chr(Cs, C, I + 1);
chr([], _C, _I) -> 0.

sh(Command0, Options0) ->
    DefaultOptions = [
        {use_stdout, false},
        {abort_on_error, ?FMT("sh: run command failed: ~ts", [Command0])}
    ],
    Options = [expand_sh_flag(V) || V <- proplists:compact(Options0 ++ DefaultOptions)],
    ErrorHandler = proplists:get_value(error_handler, Options),
    OutputHandler = proplists:get_value(output_handler, Options),
    Command = lists:flatten(patch_on_windows(Command0, proplists:get_value(env, Options0, []))),
    PortSettings =
        proplists:get_all_values(port_settings, Options) ++
            [exit_status, {line, 16384}, use_stdio, stderr_to_stdout, hide, eof, binary],
    Port = open_port({spawn, Command}, PortSettings),
    try
        case sh_loop(Port, OutputHandler, []) of
            {ok, _Output} = Ok ->
                Ok;
            {error, {_Rc, _Output} = Err} ->
                ErrorHandler(Command, Err)
        end
    after
        port_close(Port)
    end.

%%==============================================================================
%% Internal functions
%%==============================================================================
cp_r_win32({true, SourceDir}, {true, DestDir}) ->
    %% from directory to directory
    ok =
        case file:make_dir(DestDir) of
            {error, eexist} -> ok;
            Other -> Other
        end,
    ok = xcopy_win32(SourceDir, DestDir);
cp_r_win32({false, Source} = S, {true, DestDir}) ->
    %% from file to directory
    cp_r_win32(S, {false, filename:join(DestDir, filename:basename(Source))});
cp_r_win32({false, Source}, {false, Dest}) ->
    %% from file to file
    {ok, _} = file:copy(Source, Dest),
    ok;
cp_r_win32({true, SourceDir}, {false, DestDir}) ->
    case filelib:is_regular(DestDir) of
        true ->
            %% From directory to file? This shouldn't happen
            {error,
                lists:flatten(
                    ?FMT(
                        "Cannot copy dir (~p) to file (~p)\n",
                        [SourceDir, DestDir]
                    )
                )};
        false ->
            %% Specifying a target directory that doesn't currently exist.
            %% So let's attempt to create this directory
            case ensure_dir(DestDir) of
                ok ->
                    ok = xcopy_win32(SourceDir, DestDir);
                {error, Reason} ->
                    {error,
                        lists:flatten(
                            ?FMT(
                                "Unable to create dir ~p: ~p\n",
                                [DestDir, Reason]
                            )
                        )}
            end
    end;
cp_r_win32(Source, Dest) ->
    Dst = {filelib:is_dir(Dest), Dest},
    lists:foreach(
        fun(Src) ->
            ok = cp_r_win32({filelib:is_dir(Src), Src}, Dst)
        end,
        filelib:wildcard(Source)
    ),
    ok.

xcopy_win32(Source, Dest) ->
    %% "xcopy \"~ts\" \"~ts\" /q /y /e 2> nul", Changed to robocopy to
    %% handle long names. May have issues with older windows.
    Cmd =
        case filelib:is_dir(Source) of
            true ->
                %% For robocopy, copying /a/b/c/ to /d/e/f/ recursively does not
                %% create /d/e/f/c/*, but rather copies all files to /d/e/f/*.
                %% The usage we make here expects the former, not the later, so we
                %% must manually add the last fragment of a directory to the `Dest`
                %% in order to properly replicate POSIX platforms
                NewDest = filename:join([Dest, filename:basename(Source)]),
                ?FMT(
                    "robocopy \"~ts\" \"~ts\" /e 1> nul",
                    [
                        escape_double_quotes(filename:nativename(Source)),
                        escape_double_quotes(filename:nativename(NewDest))
                    ]
                );
            false ->
                ?FMT(
                    "robocopy \"~ts\" \"~ts\" \"~ts\" /e 1> nul",
                    [
                        escape_double_quotes(filename:nativename(filename:dirname(Source))),
                        escape_double_quotes(filename:nativename(Dest)),
                        escape_double_quotes(filename:basename(Source))
                    ]
                )
        end,
    Res = sh(Cmd, [{use_stdout, false}, return_on_error]),
    case win32_ok(Res) of
        true ->
            ok;
        false ->
            {error,
                lists:flatten(
                    ?FMT(
                        "Failed to copy ~ts to ~ts~n",
                        [Source, Dest]
                    )
                )}
    end.

win32_ok({ok, _}) -> true;
win32_ok({error, {Rc, _}}) when Rc < 9; Rc =:= 16 -> true;
win32_ok(_) -> false.

join([], Sep) when is_list(Sep) ->
    [];
join([H | T], Sep) ->
    H ++ lists:append([Sep ++ X || X <- T]).

filter_cp_dirs(Sources, Dest) ->
    RealSrcDirs = resolve_real_dirs(Sources),
    RealDstDir = real_dir_path(Dest),
    lists:filter(
        fun(Src) ->
            Dir = bin(filename:dirname(Src)),
            RealDir = maps:get(Dir, RealSrcDirs),
            RealDir =/= RealDstDir
        end,
        Sources
    ).

resolve_real_dirs(Srcs) ->
    resolve_real_dirs(Srcs, #{}).

resolve_real_dirs([], Acc) ->
    Acc;
resolve_real_dirs([H | T], Acc) ->
    Dir = bin(filename:dirname(H)),
    case maps:get(Dir, Acc, false) of
        false ->
            %% real_dir_path can be slow, use Acc as a cache
            RealDir = real_dir_path(Dir),
            resolve_real_dirs(T, Acc#{Dir => RealDir});
        _RealDir ->
            resolve_real_dirs(T, Acc)
    end.

%% We do the shell variable substitution ourselves on Windows and hope that the
%% command doesn't use any other shell magic.
patch_on_windows(Cmd, Env) ->
    case os:type() of
        {win32, nt} ->
            Cmd1 =
                "cmd /q /c " ++
                    lists:foldl(
                        fun({Key, Value}, Acc) ->
                            expand_env_variable(Acc, Key, Value)
                        end,
                        Cmd,
                        Env
                    ),
            %% Remove left-over vars
            re:replace(
                Cmd1,
                "\\\$\\w+|\\\${\\w+}",
                "",
                [global, {return, list}, unicode]
            );
        _ ->
            Cmd
    end.

expand_sh_flag({abort_on_error, Message}) ->
    {error_handler, log_msg_and_abort(Message)};
expand_sh_flag(return_on_error) ->
    {error_handler, fun(_Command, Err) ->
        {error, Err}
    end};
expand_sh_flag(use_stdout) ->
    {output_handler, fun(Line, Acc) ->
        %% Line already has a newline so don't use ?CONSOLE which adds one
        io:format("~ts", [Line]),
        [Line | Acc]
    end};
expand_sh_flag({use_stdout, false}) ->
    {output_handler, fun(Line, Acc) ->
        [Line | Acc]
    end};
expand_sh_flag({cd, _CdArg} = Cd) ->
    {port_settings, Cd};
expand_sh_flag({env, _EnvArg} = Env) ->
    {port_settings, Env}.

log_msg_and_abort(Message) ->
    fun(_Command, {_Rc, _Output}) ->
        logger:error(#{msg => sh_failed, details => Message}),
        throw(sh_aborted)
    end.

port_line_to_list(Line) ->
    case unicode:characters_to_list(Line) of
        LineList when is_list(LineList) ->
            LineList;
        _ ->
            binary_to_list(Line)
    end.

sh_loop(Port, Fun, Acc) ->
    receive
        {Port, {data, {eol, Line}}} ->
            sh_loop(Port, Fun, Fun(port_line_to_list(Line) ++ "\n", Acc));
        {Port, {data, {noeol, Line}}} ->
            sh_loop(Port, Fun, Fun(port_line_to_list(Line), Acc));
        {Port, eof} ->
            Data = lists:flatten(lists:reverse(Acc)),
            receive
                {Port, {exit_status, 0}} ->
                    {ok, Data};
                {Port, {exit_status, Rc}} ->
                    {error, {Rc, Data}}
            end
    end.

tmp_dir_1() ->
    case get_writable_tmp_dir(["TMPDIR", "TEMP", "TMP"]) of
        false ->
            case writable_tmp_dir("/tmp") of
                false ->
                    {ok, Dir} = file:get_cwd(),
                    writable_tmp_dir(Dir);
                Tmp ->
                    Tmp
            end;
        Tmp ->
            Tmp
    end.

get_writable_tmp_dir([Env | Envs]) ->
    case writable_env_tmp_dir(Env) of
        false -> get_writable_tmp_dir(Envs);
        Tmp -> Tmp
    end;
get_writable_tmp_dir([]) ->
    false.

writable_env_tmp_dir(Env) ->
    case os:getenv(Env) of
        false -> false;
        Tmp -> writable_tmp_dir(Tmp)
    end.

writable_tmp_dir(Tmp) ->
    case file:read_file_info(Tmp) of
        {ok, Info} ->
            case Info#file_info.access of
                write -> Tmp;
                read_write -> Tmp;
                _ -> false
            end;
        {error, _} ->
            false
    end.
