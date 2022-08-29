#!/usr/bin/env escript

main(_) ->
    {ok, [Jobs]} = file:consult("scripts/xref_check.eterm"),
    lists:foreach(fun(#{ name := Name
                       , analysis := Analysis
                       , excl_apps := ExclApps
                       , excl_mods := ExclMods
                       , filters := Filters
                       }) ->
                          xref:start(Name),
                          Tid = ets:new(Name, [ordered_set, named_table]),
                          xref:set_default(Name, [{verbose,false}, {warnings,false}]),
                          xref:add_release(Name, "_build/emqx/rel/emqx/lib/"),
                          xref:add_application(Name, code:lib_dir(erts)),
                          [ok = xref:remove_application(Name, App) ||
                              App <- ExclApps
                          ],

                          [ok = xref:remove_module(Name, M) ||
                              M <- ExclMods
                          ],

                          ModuleInfos = xref:info(Name, modules),
                          LibInfos = xref:info(Name, modules),
                          true = ets:insert(Tid, ModuleInfos ++ LibInfos),
                          {ok, Res0} = xref:analyse(Name, Analysis),
                          Res = Res0 -- Filters,
                          Res =/= [] andalso
                              begin
                                  put(is_warn_found, true),
                                  io:format("** Warnings for ~p~n : ~p~n", [Name, Res])
                              end,
                          xref:stop(Name)
                  end, Jobs),
    case get(is_warn_found) of
        true ->
            halt(1);
        _ ->
            ok
    end.
