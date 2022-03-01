#!/usr/bin/env escript

%% This script injects implicit relup instructions for emqx applications.

-mode(compile).

-define(ERROR(FORMAT, ARGS), io:format(standard_error, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).
-define(INFO(FORMAT, ARGS), io:format(user, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).

usage() ->
    "Usage: " ++ escript:script_name() ++ " <path-to-release-dir> <release-vsn>".

main([RelRootDir, CurrRelVsn]) ->
    case filelib:is_dir(filename:join([RelRootDir, "releases"])) andalso
         filelib:is_dir(filename:join([RelRootDir, "lib"])) of
        true ->
            EmqxAppVsns = get_emqx_app_vsns(RelRootDir),
            ok = inject_relup_file(RelRootDir, CurrRelVsn, EmqxAppVsns);
        false ->
            ?ERROR("not a valid root dir of release: ~p, for example: _build/emqx/rel/emqx",
                [RelRootDir]),
            erlang:halt(1)
    end;
main(_Args) ->
    ?ERROR("~s", [usage()]),
    erlang:halt(1).

inject_relup_file(RelRootDir, CurrRelVsn, EmqxAppVsns) ->
    RelupFile = filename:join([RelRootDir, "releases", CurrRelVsn, "relup"]),
    inject_file(RelupFile, EmqxAppVsns).

inject_file(File, EmqxAppVsns) ->
    case file:script(File) of
        {ok, {CurrRelVsn, UpVsnRUs, DnVsnRUs}} ->
            ?INFO("injecting instructions to: ~p", [File]),
            UpdatedContent = {CurrRelVsn,
                inject_relup_instrs(up, EmqxAppVsns, CurrRelVsn, UpVsnRUs),
                inject_relup_instrs(down, EmqxAppVsns, CurrRelVsn, DnVsnRUs)},
            file:write_file(File, term_to_text(UpdatedContent));
        {ok, _BadFormat} ->
            ?ERROR("bad formatted relup file: ~p", [File]),
            error({bad_relup_format, File});
        {error, enoent} ->
            ?INFO("relup file not found: ~p", [File]),
            ok;
        {error, Reason} ->
            ?ERROR("read relup file ~p failed: ~p", [File, Reason]),
            error({read_relup_error, Reason})
    end.

inject_relup_instrs(Type, EmqxAppVsns, CurrRelVsn, RUs) ->
    lists:map(fun
        ({Vsn, "(relup-injected) " ++ _ = Desc, Instrs}) -> %% already injected
            {Vsn, Desc, Instrs};
        ({Vsn, Desc, Instrs}) ->
            {Vsn, "(relup-injected) " ++ Desc,
                append_emqx_relup_instrs(Type, EmqxAppVsns, CurrRelVsn, Vsn, Instrs)}
    end, RUs).

%% The `{apply, emqx_relup, post_release_upgrade, []}` will be appended to the end of
%% the instruction lists.
append_emqx_relup_instrs(up, EmqxAppVsns, CurrRelVsn, FromRelVsn, Instrs0) ->
    {EmqxVsn, true} = maps:get(CurrRelVsn, EmqxAppVsns),
    Extra = #{}, %% we may need some extended args
    LoadObjEmqxMods = {load_object_code, {emqx, EmqxVsn, [emqx_relup, emqx_app]}},
    LoadCodeEmqxRelup = {load, {emqx_relup, brutal_purge, soft_purge}},
    LoadCodeEmqxApp = {load, {emqx_app, brutal_purge, soft_purge}},
    ApplyEmqxRelup = {apply, {emqx_relup, post_release_upgrade, [FromRelVsn, Extra]}},
    Instrs1 = Instrs0 -- [LoadCodeEmqxRelup, LoadCodeEmqxApp],
    %% we have to put 'load_object_code' before 'point_of_no_return'
    %% so here we simply put them to the beginning of the instruction list
    Instrs2 = [ LoadObjEmqxMods
              | Instrs1],
    %% the `load` must be put after the 'point_of_no_return'
    Instrs2 ++
        [ LoadCodeEmqxRelup
        , LoadCodeEmqxApp
        , ApplyEmqxRelup
        ];

append_emqx_relup_instrs(down, EmqxAppVsns, _CurrRelVsn, ToRelVsn, Instrs0) ->
    Extra = #{}, %% we may need some extended args
    ApplyEmqxRelup = {apply, {emqx_relup, post_release_downgrade, [ToRelVsn, Extra]}},
    case maps:get(ToRelVsn, EmqxAppVsns) of
        {EmqxVsn, true} ->
            LoadObjEmqxMods = {load_object_code, {emqx, EmqxVsn, [emqx_relup, emqx_app]}},
            LoadCodeEmqxRelup = {load, {emqx_relup, brutal_purge, soft_purge}},
            LoadCodeEmqxApp = {load, {emqx_app, brutal_purge, soft_purge}},
            Instrs1 = Instrs0 -- [LoadCodeEmqxRelup, LoadCodeEmqxApp, ApplyEmqxRelup],
            Instrs2 = [ LoadObjEmqxMods
                      | Instrs1],
            %% NOTE: We apply emqx_relup:post_release_downgrade/2 first, and then reload
            %%  the old vsn code of emqx_relup.
            Instrs2 ++
                [ LoadCodeEmqxApp
                , ApplyEmqxRelup
                , LoadCodeEmqxRelup
                ];
        {EmqxVsn, false} ->
            LoadObjEmqxApp = {load_object_code, {emqx, EmqxVsn, [emqx_app]}},
            LoadCodeEmqxApp = {load, {emqx_app, brutal_purge, soft_purge}},
            RemoveCodeEmqxRelup = {remove, {emqx_relup, brutal_purge, soft_purge}},
            Instrs1 = Instrs0 -- [LoadCodeEmqxApp, RemoveCodeEmqxRelup, ApplyEmqxRelup],
            Instrs2 = [ LoadObjEmqxApp
                      | Instrs1],
            Instrs2 ++
                [ LoadCodeEmqxApp
                , ApplyEmqxRelup
                , RemoveCodeEmqxRelup
                ]
    end.

get_emqx_app_vsns(RelRootDir) ->
    RelFiles = filelib:wildcard(filename:join([RelRootDir, "releases", "*", "emqx.rel"])),
    lists:foldl(fun(RelFile, AppVsns) ->
        {ok, RelVsn, EmqxVsn} = read_emqx_vsn_from_rel_file(RelFile),
        AppVsns#{RelVsn => {EmqxVsn, has_relup_module(RelRootDir, EmqxVsn)}}
    end, #{}, RelFiles).

read_emqx_vsn_from_rel_file(RelFile) ->
    case file:script(RelFile) of
        {ok, {release, {_RelName, RelVsn}, _Erts, Apps}} ->
            case lists:keysearch(emqx, 1, Apps) of
                {value, {emqx, EmqxVsn}} ->
                    {ok, RelVsn, EmqxVsn};
                false ->
                    error({emqx_vsn_cannot_found, RelFile})
            end;
        {ok, _BadFormat} ->
            ?ERROR("bad formatted .rel file: ~p", [RelFile]);
        {error, Reason} ->
            ?ERROR("read .rel file ~p failed: ~p", [RelFile, Reason])
    end.

has_relup_module(RelRootDir, EmqxVsn) ->
    AppFile = filename:join([RelRootDir, "lib", "emqx-" ++ EmqxVsn, "ebin", "emqx.app"]),
    case file:script(AppFile) of
        {ok, {application, emqx, AppInfo}} ->
            {value, {_, EmqxVsn}} = lists:keysearch(vsn, 1, AppInfo), %% assert
            {value, {_, Modules}} = lists:keysearch(modules, 1, AppInfo),
            lists:member(emqx_relup, Modules);
        {error, Reason} ->
            ?ERROR("read .app file ~p failed: ~p", [AppFile, Reason]),
            error({read_app_file_error, AppFile, Reason})
    end.

term_to_text(Term) ->
    io_lib:format("~p.", [Term]).
