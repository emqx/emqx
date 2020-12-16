#!/usr/bin/env escript
%%! -pa _build/test/lib/bbmustache/ebin

%% this is used as a rebar3 post-compile hook
%% work dir is project root, not where the script resides
-mode(compile).

main(_Args) ->
    Files = filelib:wildcard("_build/test/lib/*/etc/*.conf"),
    case os:getenv("DIAGNOSTIC") of
        false -> ok;
        "" -> ok;
        _ -> io:format("rendering config templates:~n~p~n", [Files])
    end,
    lists:foreach(fun render/1, Files).

render(File) ->
    {ok, Input} = file:read_file(File),
    {ok, Vars0} = file:consult("vars/vars-bin.config"),
    Vars = [{atom_to_list(N), list_to_binary(V)} || {N, V} <- Vars0],
    Output = bbmustache:render(Input, Vars),
    ok = file:write_file(File, Output).
