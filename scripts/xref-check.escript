#!/usr/bin/env escript

%%% @doc
%%% Unlike rebar3 xref check, this script runs the full xref checks in the EMQX release dir,
%%% meaning all the modules for release are analysed.
%%% For behavior configuration, all rebar3 related modules attributes, filters are not used in this script,
%%% instead all the filters, checks are defined in `xref_check.eterm`
main(_) ->
    {ok, [Jobs]} = file:consult("scripts/xref_check.eterm"),
    lists:foreach(fun(J) -> do_check(J) end, Jobs),
    case get(is_warn_found) of
        true ->
            halt(1);
        _ ->
            ok
    end.

do_check(#{ name := Name
          , analysis := Analysis
          , excl_apps := ExclApps
          , excl_mods := ExclMods
          , filters := Filters
          }) ->
    xref:start(Name),
    %% Build a table for later printing more informative warnings.
    %% The table is currently not in use.
    Tid = ets:new(Name, [ordered_set, named_table]),
    xref:set_default(Name, [{verbose,false}, {warnings,false}]),
    Profile = case filelib:is_file("EMQX_ENTERPRISE") of
                  true -> 'emqx-ee';
                  false -> emqx
              end,
    Dir = filename:join(["_build/",  Profile, "rel/emqx/lib/"]),
    xref:add_release(Name, Dir),
    xref:add_application(Name, code:lib_dir(erts)),
    [ case xref:remove_application(Name, App) of
          ok -> ok;
          {error, xref_base, {no_such_application, _}} -> ok
      end || App <- ExclApps
    ],

    [case xref:remove_module(Name, M) of
         ok -> ok;
         %% but in doc it should return '{error, module(), Reason}`
         {error, xref_base, {no_such_module, M}} -> ok
     end || M <- ExclMods
    ],
    ModuleInfos = xref:info(Name, modules),
    LibInfos = xref:info(Name, libraries),
    true = ets:insert(Tid, ModuleInfos ++ LibInfos),
    {ok, Res0} = xref:analyse(Name, Analysis),
    Res = Res0 -- Filters,
    Res =/= [] andalso
        begin
            put(is_warn_found, true),
            io:format("** Warnings for ~p~n : ~p~n", [Name, Res])
        end,
    xref:stop(Name).
