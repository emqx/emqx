#!/usr/bin/env -S escript -c
%% A script that adds changed modules to the corresponding appup files

main(_Args) ->
    ChangedFiles = string:lexemes(os:cmd("git diff --name-only origin/master..HEAD"), "\n"),
    AppModules0 = lists:filtermap(fun filter_erlang_modules/1, ChangedFiles),
    %% emqx_app must always be included as we bump version number in emqx_release.hrl for each release
    AppModules1 = [{emqx, emqx_app} | AppModules0],
    AppModules = group_modules(AppModules1),
    io:format("Changed modules: ~p~n", [AppModules]),
    _ = maps:map(fun process_app/2, AppModules),
    ok.

process_app(App, Modules) ->
    AppupFiles = filelib:wildcard(lists:concat(["{src,apps,lib-*}/**/", App, ".appup.src"])),
    case AppupFiles of
        [AppupFile] ->
          update_appup(AppupFile, Modules);
        []          ->
          io:format("~nWARNING: Please create an stub appup src file for ~p~n", [App])
    end.

filter_erlang_modules(Filename) ->
    case lists:reverse(filename:split(Filename)) of
        [Module, "src"] ->
            erl_basename("emqx", Module);
        [Module, "src", App|_] ->
            erl_basename(App, Module);
        [Module, _, "src", App|_] ->
            erl_basename(App, Module);
        _ ->
            false
    end.

erl_basename(App, Name) ->
    case filename:basename(Name, ".erl") of
        Name   -> false;
        Module -> {true, {list_to_atom(App), list_to_atom(Module)}}
    end.

group_modules(L) ->
    lists:foldl(fun({App, Mod}, Acc) ->
                        maps:update_with(App, fun(Tl) -> [Mod|Tl] end, [Mod], Acc)
                end, #{}, L).

update_appup(File, Modules) ->
    io:format("~nUpdating appup: ~p~n", [File]),
    {_, Upgrade0, Downgrade0} = read_appup(File),
    Upgrade = update_actions(Modules, Upgrade0),
    Downgrade = update_actions(Modules, Downgrade0),
    IOList = io_lib:format("%% -*- mode: erlang -*-
{VSN,~n  ~p,~n  ~p}.~n", [Upgrade, Downgrade]),
    ok = file:write_file(File, IOList).

update_actions(Modules, Versions) ->
    lists:map(fun(L) -> do_update_actions(Modules, L) end, Versions).

do_update_actions(_, Ret = {<<".*">>, _}) ->
    Ret;
do_update_actions(Modules, {Vsn, Actions}) ->
    {Vsn, add_modules(Modules, Actions)}.

add_modules(NewModules, OldActions) ->
    OldModules = lists:map(fun(It) -> element(2, It) end, OldActions),
    Modules = NewModules -- OldModules,
    OldActions ++ [{load_module, M, brutal_purge, soft_purge, []} || M <- Modules].

read_appup(File) ->
    {ok, Bin0} = file:read_file(File),
    %% Hack:
    Bin1 = re:replace(Bin0, "VSN", "\"VSN\""),
    TmpFile = filename:join("/tmp", filename:basename(File)),
    ok = file:write_file(TmpFile, Bin1),
    {ok, [Terms]} = file:consult(TmpFile),
    Terms.
