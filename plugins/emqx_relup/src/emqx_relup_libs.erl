-module(emqx_relup_libs).

-export([
    make_libs_info/2,
    get_app_mods/2,
    get_app_ebin/2,
    rel_libs/1,
    rel_vsn/1,
    rel_erts_vsn/1,
    lib_app_name/1,
    lib_app_vsn/1
]).

-import(lists, [concat/1]).
-import(emqx_relup_utils, [make_error/2]).

make_libs_info(Rel, Dir) ->
    AppDescList = make_app_desc_list(rel_libs(Rel), Dir),
    #{
        mod_app_mapping => make_mod_app_mapping(AppDescList, Dir),
        app_ebin_mapping => make_app_ebin_mapping(AppDescList, Dir),
        app_desc_list => AppDescList
    }.

get_app_mods(AppName, #{app_desc_list := AppDescL}) ->
    case lists:keyfind(AppName, 2, AppDescL) of
        false ->
            throw(make_error(app_not_found, #{app_name => AppName}));
        {application, AppName, Attrs} ->
            emqx_relup_utils:assert_propl_get(
                modules,
                Attrs,
                no_modules_in_app_desc,
                #{func => get_app_mods, app => AppName}
            )
    end.

get_app_ebin(AppName, #{app_ebin_mapping := AppEbinMapping}) ->
    case maps:get(AppName, AppEbinMapping, undefined) of
        undefined ->
            throw(make_error(app_not_found, #{app_name => AppName}));
        EbinDir ->
            EbinDir
    end.

rel_libs({release, _Emqx, _Erts, Libs}) ->
    Libs.
rel_vsn({release, {"emqx", Vsn}, _Erts, _Libs}) ->
    Vsn.
rel_erts_vsn({release, _Emqx, {erts, Vsn}, _Libs}) ->
    Vsn.

lib_app_name(Lib) ->
    element(1, Lib).
lib_app_vsn(Lib) ->
    element(2, Lib).

%%==============================================================================
%% Internal functions
%%==============================================================================
make_app_desc_list(Libs, Dir) ->
    lists:map(
        fun(Lib) ->
            AppName = lib_app_name(Lib),
            AppVsn = lib_app_vsn(Lib),
            AppDescFile = filename:join([
                Dir, "lib", concat([AppName, "-", AppVsn]), "ebin", concat([AppName, ".app"])
            ]),
            case file:consult(AppDescFile) of
                {ok, [AppDesc]} ->
                    AppDesc;
                {error, Reason} ->
                    throw(
                        make_error(
                            failed_to_read_app_desc_file,
                            #{file => AppDescFile, reason => Reason}
                        )
                    )
            end
        end,
        Libs
    ).

make_mod_app_mapping(AppDescList, Dir) ->
    lists:foldl(
        fun({application, AppName, Attrs}, Map) ->
            Mods = emqx_relup_utils:assert_propl_get(modules, Attrs, no_modules_in_app_desc, #{
                app => AppName
            }),
            AppVsn = emqx_relup_utils:assert_propl_get(vsn, Attrs, no_vsn_in_app_desc, #{
                app => AppName
            }),
            BeamFile = fun(Mod) ->
                filename:join([
                    Dir,
                    "lib",
                    concat([AppName, "-", AppVsn]),
                    "ebin",
                    concat([Mod, code:objfile_extension()])
                ])
            end,
            ModMaps = maps:from_list([{M, {AppName, AppVsn, BeamFile(M)}} || M <- Mods]),
            maps:merge(Map, ModMaps)
        end,
        #{},
        AppDescList
    ).

make_app_ebin_mapping(AppDescList, Dir) ->
    maps:from_list(
        lists:map(
            fun({application, AppName, Attrs}) ->
                AppVsn = emqx_relup_utils:assert_propl_get(vsn, Attrs, no_vsn_in_app_desc, #{
                    app => AppName
                }),
                EbinDir = filename:join([
                    Dir,
                    "lib",
                    concat([AppName, "-", AppVsn]),
                    "ebin"
                ]),
                {AppName, EbinDir}
            end,
            AppDescList
        )
    ).
