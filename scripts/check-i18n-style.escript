#!/usr/bin/env escript

%% called from check-i18n-style.sh

-mode(compile).

% -define(YELLOW, "\e[33m"). % not used
-define(RED, "\e[31m").
-define(RESET, "\e[39m").

main(Files) ->
    io:format(user, "checking i18n file styles~n", []),
    _ = put(errors, 0),
    ok = load_hocon(),
    ok = lists:foreach(fun check/1, Files),
    case get(errors) of
        1 ->
            die("1 error found~n", []);
        N when is_integer(N) andalso N > 1 ->
            die("~p errors found~n", [N]);
        _ ->
            io:format(user, "~nOK~n", [])
    end.

load_hocon() ->
    Dir = "_build/default/lib/hocon/ebin",
    File = filename:join([Dir, "hocon.beam"]),
    case filelib:is_regular(File) of
        true ->
            code:add_path(Dir),
            ok;
        false ->
            die("HOCON is not compiled in " ++ Dir ++ "~n")
    end.

die(Msg) ->
    die(Msg, []).

die(Msg, Args) ->
    ok = logerr(Msg, Args),
    halt(1).

logerr(Fmt, Args) ->
    io:format(standard_error, "~n" ++ ?RED ++ "ERROR: " ++ Fmt ++ ?RESET, Args),
    N = get(errors),
    _ = put(errors, N + 1),
    ok.

check(File) ->
    io:format(user, ".", []),
    {ok, C} = hocon:load(File),
    maps:foreach(fun check_one_field/2, C),
    ok.

check_one_field(Name, Field) ->
    maps:foreach(
        fun(SubName, DescAndLabel) ->
            check_desc_and_label([Name, ".", SubName], DescAndLabel)
        end,
        Field
    ).

check_desc_and_label(Name, D) ->
    case maps:keys(D) -- [<<"desc">>, <<"label">>] of
        [] ->
            ok;
        Unknown ->
            die("~s: unknown tags ~p~n", [Name, Unknown])
    end,
    ok = check_desc(Name, D),
    ok = check_label(Name, D).

check_label(_Name, #{<<"label">> := _Label}) ->
    ok;
check_label(_Name, _) ->
    %% some may not have label
    ok.

check_desc(Name, #{<<"desc">> := Desc}) ->
    check_desc_string(Name, Desc);
check_desc(Name, _) ->
    die("~s: no 'desc'~n", [Name]).

check_desc_string(Name, <<>>) ->
    logerr("~s: empty string", [Name]);
check_desc_string(Name, <<"~", _/binary>> = Line) ->
    logerr("~s: \"~s\" is a bad multi-line string? '~~' must be followed by NL", [Name, Line]);
check_desc_string(Name, BinStr) ->
    Str = unicode:characters_to_list(BinStr, utf8),
    Err = fun(Reason) ->
        logerr("~s: ~s~n", [Name, Reason])
    end,
    case Str of
        [$\s | _] ->
            Err("remove leading whitespace");
        [$\n | _] ->
            Err("remove leading line-break");
        "<br/>" ->
            Err("remove leading <br/>");
        "<br />" ->
            Err("remove leading <br />");
        _ ->
            ok
    end,
    case lists:reverse(Str) of
        [$\s | _] ->
            Err("remove trailing whitespace");
        [$\n | _] ->
            Err("remove trailing line-break");
        ">/rb<" ++ _ ->
            Err("remove trailing <br/>");
        ">/ rb<" ++ _ ->
            Err("remove trailing <br />");
        [$~ | _] ->
            Err("unpairdd '~\"\"\"' for multi-line stirng?");
        _ ->
            ok
    end.
