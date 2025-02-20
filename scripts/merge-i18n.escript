#!/usr/bin/env escript

%% This script is only used at build time to generate the merged desc.en.hocon in JSON format
%% but NOT the file generated to _build/$PROFILE/lib/emqx_dashboard/priv (which is HOCON format).
%%
%% The generated JSON file is used as the source of truth when translating to other languages.

-mode(compile).

-define(RED, "\e[31m").
-define(RESET, "\e[39m").

main(_) ->
    try
        _ = hocon:module_info(module)
    catch
        _:_ ->
            fail("hocon module not found, please make sure the project is compiled")
    end,
    %% wildcard all .hocon files in rel/i18n
    Files = filelib:wildcard("rel/i18n/*.hocon"),
    case Files of
        [_ | _] ->
            ok;
        [] ->
            fail("No .hocon files found in rel/i18n")
    end,
    case hocon:files(Files) of
        {ok, Map} ->
            JSON = jiffy:encode(Map),
            io:format("~s~n", [JSON]);
        {error, Reason} ->
            fail("~p~n", [Reason])
    end.

fail(Str) ->
    fail(Str, []).

fail(Str, Args) ->
    io:format(standard_error, ?RED ++ "ERROR: " ++ Str ++ ?RESET ++ "~n", Args),
    halt(1).
