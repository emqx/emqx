#!/usr/bin/env escript

%% This script injects implicit relup instructions for emqx applications.

-mode(compile).

-define(ERROR(FORMAT, ARGS), io:format(standard_error, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).
-define(INFO(FORMAT, ARGS), io:format(user, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).

usage() ->
    "Usage: " ++ escript:script_name() ++ " <path-to-relup-file>".

main([RelupFile]) ->
    case filelib:is_regular(RelupFile) of
        true ->
            ok = inject_relup_file(RelupFile);
        false ->
            ?ERROR("not a valid file: ~p", [RelupFile]),
            erlang:halt(1)
    end;
main(_Args) ->
    ?ERROR("~s", [usage()]),
    erlang:halt(1).

inject_relup_file(File) ->
    case file:script(File) of
        {ok, {CurrRelVsn, UpVsnRUs, DnVsnRUs}} ->
            ?INFO("injecting instructions to: ~p", [File]),
            UpdatedContent = {CurrRelVsn,
                inject_relup_instrs(up, UpVsnRUs),
                inject_relup_instrs(down, DnVsnRUs)},
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

inject_relup_instrs(Type, RUs) ->
    lists:map(fun({Vsn, Desc, Instrs}) ->
        {Vsn, Desc, append_emqx_relup_instrs(Type, Vsn, Instrs)}
    end, RUs).

%% The `{apply, emqx_relup, post_release_upgrade, []}` will be appended to the end of
%% the instruction lists.
append_emqx_relup_instrs(up, FromRelVsn, Instrs0) ->
    Extra = #{}, %% we may need some extended args
    filter_and_check_instrs(up, Instrs0) ++
        [ {load, {emqx_app, brutal_purge, soft_purge}}
        , {load, {emqx_relup, brutal_purge, soft_purge}}
        , {apply, {emqx_relup, post_release_upgrade, [FromRelVsn, Extra]}}
        ];

append_emqx_relup_instrs(down, ToRelVsn, Instrs0) ->
    Extra = #{}, %% we may need some extended args
    %% NOTE: When downgrading, we apply emqx_relup:post_release_downgrade/2 before reloading
    %%  or removing the emqx_relup module.
    Instrs1 = filter_and_check_instrs(down, Instrs0) ++
        [ {load, {emqx_app, brutal_purge, soft_purge}}
        , {apply, {emqx_relup, post_release_downgrade, [ToRelVsn, Extra]}}
        ],
    %% emqx_relup does not exist before release "4.4.2"
    LoadInsts =
        case ToRelVsn of
            ToRelVsn when ToRelVsn =:= "4.4.1"; ToRelVsn =:= "4.4.0" ->
                [{remove, {emqx_relup, brutal_purge, brutal_purge}}];
            _ ->
                [{load, {emqx_relup, brutal_purge, soft_purge}}]
        end,
    Instrs1 ++ LoadInsts.

filter_and_check_instrs(Type, Instrs) ->
    case take_emqx_vsn_and_modules(Instrs) of
        {EmqxAppVsn, EmqxMods, RemainInstrs} when EmqxAppVsn =/= not_found, EmqxMods =/= [] ->
            assert_mandatory_modules(Type, EmqxMods),
            [{load_object_code, {emqx, EmqxAppVsn, EmqxMods}} | RemainInstrs];
        {_, _, _} ->
            ?ERROR("cannot found 'load_module' instructions for app emqx", []),
            error({instruction_not_found, load_object_code})
    end.

take_emqx_vsn_and_modules(Instrs) ->
    lists:foldl(fun
        ({load_object_code, {emqx, AppVsn, Mods}}, {_EmqxAppVsn, EmqxMods, RemainInstrs}) ->
            {AppVsn, EmqxMods ++ Mods, RemainInstrs};
        ({load, {Mod, _, _}}, {EmqxAppVsn, EmqxMods, RemainInstrs})
                when Mod =:= emqx_relup; Mod =:= emqx_app ->
            {EmqxAppVsn, EmqxMods, RemainInstrs};
        ({remove, {emqx_relup, _, _}}, {EmqxAppVsn, EmqxMods, RemainInstrs}) ->
            {EmqxAppVsn, EmqxMods, RemainInstrs};
        ({apply, {emqx_relup, _, _}}, {EmqxAppVsn, EmqxMods, RemainInstrs}) ->
            {EmqxAppVsn, EmqxMods, RemainInstrs};
        (Instr, {EmqxAppVsn, EmqxMods, RemainInstrs}) ->
            {EmqxAppVsn, EmqxMods, RemainInstrs ++ [Instr]}
    end, {not_found, [], []}, Instrs).

assert_mandatory_modules(up, Mods) ->
    assert(lists:member(emqx_relup, Mods) andalso lists:member(emqx_app, Mods),
        "cannot found 'load_module' instructions for emqx_app and emqx_rel: ~p", [Mods]);

assert_mandatory_modules(down, Mods) ->
    assert(lists:member(emqx_app, Mods),
        "cannot found 'load_module' instructions for emqx_app", []).

assert(true, _, _) ->
    ok;
assert(false, Msg, Args) ->
    ?ERROR(Msg, Args),
    error(assert_failed).

term_to_text(Term) ->
    io_lib:format("~p.", [Term]).
