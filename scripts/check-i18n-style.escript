#!/usr/bin/env escript

%% called from check-i18n-style.sh

-mode(compile).

-define(YELLOW, "\e[33m").
-define(RED, "\e[31m").
-define(RESET, "\e[39m").

main([Files0]) ->
    _ = put(errors, 0),
    Files = string:tokens(Files0, "\n"),
    ok = load_hocon(),
    ok = lists:foreach(fun check/1, Files),
    case get(errors) of
        1 ->
            logerr("1 error found~n", []);
        N when is_integer(N) andalso N > 1 ->
            logerr("~p errors found~n", [N]);
        _ ->
            io:format(user, "OK~n", [])
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
    io:format(standard_error, ?RED ++ "ERROR: " ++ Fmt ++ ?RESET, Args),
    N = get(errors),
    _ = put(errors, N + 1),
    ok.


check(File) ->
    io:format(user, "checking: ~s~n", [File]),
    {ok, C} = hocon:load(File),
    maps:foreach(fun check_one_field/2, C),
    ok.

check_one_field(Name, Field) ->
    maps:foreach(fun(SubName, DescAndLabel) ->
                         check_desc_and_label([Name, ".", SubName], DescAndLabel)
                 end, Field).

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
    do_check_desc(Name, Desc);
check_desc(Name, _) ->
    die("~s: no 'desc'~n", [Name]).

do_check_desc(Name, #{<<"zh">> := Zh, <<"en">> := En}) ->
    ok = check_desc_string(Name, "zh", Zh),
    ok = check_desc_string(Name, "en", En);
do_check_desc(Name, _) ->
    die("~s: missing 'zh' or 'en'~n", [Name]).

check_desc_string(Name, Tr, <<>>) ->
    io:format(standard_error, ?YELLOW ++ "WARNING: ~s.~s: empty string~n" ++ ?RESET, [Name, Tr]);
check_desc_string(Name, Tr, BinStr) ->
    Str = unicode:characters_to_list(BinStr, utf8),
    Err = fun(Reason) ->
            logerr("~s.~s: ~s~n", [Name, Tr, Reason])
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
        _ ->
            ok
    end.
