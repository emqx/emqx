#!/usr/bin/env escript

%% This script injects implicit relup instructions for emqx applications.

-mode(compile).

-define(ERROR(FORMAT, ARGS), io:format(standard_error, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).
-define(INFO(FORMAT, ARGS), io:format(user, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).

usage() ->
    "Usage: " ++ escript:script_name() ++ " <path-to-relup-file>".

main([RelupFile]) ->
    ok = inject_relup_file(RelupFile);
main(_Args) ->
    ?ERROR("~s", [usage()]),
    erlang:halt(1).

inject_relup_file(File) ->
    case file:script(File) of
        {ok, {CurrRelVsn, UpVsnRUs, DnVsnRUs}} ->
            ?INFO("Injecting instructions to: ~p", [File]),
            UpdatedContent = {CurrRelVsn,
                inject_relup_instrs(up, UpVsnRUs),
                inject_relup_instrs(down, DnVsnRUs)},
            file:write_file(File, term_to_text(UpdatedContent));
        {ok, _BadFormat} ->
            ?ERROR("Bad formatted relup file: ~p", [File]),
            error({bad_relup_format, File});
        {error, enoent} ->
            ?INFO("Cannot find relup file: ~p", [File]),
            ok;
        {error, Reason} ->
            ?ERROR("Read relup file ~p failed: ~p", [File, Reason]),
            error({read_relup_error, Reason})
    end.

inject_relup_instrs(Type, RUs) ->
    lists:map(fun({Vsn, Desc, Instrs}) ->
        {Vsn, Desc, append_emqx_relup_instrs(Type, Vsn, Instrs)}
    end, RUs).

append_emqx_relup_instrs(up, FromRelVsn, Instrs0) ->
     {{UpExtra, _}, Instrs1} = filter_and_check_instrs(up, Instrs0),
     Instrs1 ++
        [ {load, {emqx_release, brutal_purge, soft_purge}}
        , {load, {emqx_relup, brutal_purge, soft_purge}}
        , {apply, {emqx_relup, post_release_upgrade, [FromRelVsn, UpExtra]}}
        ];

append_emqx_relup_instrs(down, ToRelVsn, Instrs0) ->
    {{_, DnExtra}, Instrs1} = filter_and_check_instrs(down, Instrs0),
    %% NOTE: When downgrading, we apply emqx_relup:post_release_downgrade/2 before reloading
    %%  or removing the emqx_relup module.
    Instrs2 = Instrs1 ++
        [ {load, {emqx_release, brutal_purge, soft_purge}}
        , {apply, {emqx_relup, post_release_downgrade, [ToRelVsn, DnExtra]}}
        , {load, {emqx_relup, brutal_purge, soft_purge}}
        ],
    Instrs2.

filter_and_check_instrs(Type, Instrs) ->
    case filter_fetch_emqx_mods_and_extra(Instrs) of
        {_, DnExtra, _, _} when Type =:= up, DnExtra =/= undefined ->
            ?ERROR("Got '{apply,{emqx_relup,post_release_downgrade,[_,Extra]}}'"
                   " from the upgrade instruction list, should be 'post_release_upgrade'", []),
            error({instruction_not_found, load_object_code});
        {UpExtra, _, _, _} when Type =:= down, UpExtra =/= undefined ->
            ?ERROR("Got '{apply,{emqx_relup,post_release_upgrade,[_,Extra]}}'"
                   " from the downgrade instruction list, should be 'post_release_downgrade'", []),
            error({instruction_not_found, load_object_code});
        {_, _, [], _} ->
            ?ERROR("Cannot find any 'load_object_code' instructions for app emqx", []),
            error({instruction_not_found, load_object_code});
        {UpExtra, DnExtra, EmqxMods, RemainInstrs} ->
            assert_mandatory_modules(Type, EmqxMods),
            {{UpExtra, DnExtra}, RemainInstrs}
    end.

filter_fetch_emqx_mods_and_extra(Instrs) ->
    lists:foldl(fun do_filter_and_get/2, {undefined, undefined, [], []}, Instrs).

%% collect modules for emqx app
do_filter_and_get({load_object_code, {emqx, _AppVsn, Mods}} = Instr,
        {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods ++ Mods, RemainInstrs ++ [Instr]};
%% remove 'load' instrs for emqx_relup and emqx_release
do_filter_and_get({load, {Mod, _, _}}, {UpExtra, DnExtra, EmqxMods, RemainInstrs})
        when Mod =:= emqx_relup; Mod =:= emqx_release ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs};
%% remove 'remove' and 'purge' instrs for emqx_relup
do_filter_and_get({remove, {emqx_relup, _, _}}, {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs};
do_filter_and_get({purge, [emqx_relup]}, {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs};
%% remove 'apply' instrs for upgrade, and collect the 'Extra' parameter
do_filter_and_get({apply, {emqx_relup, post_release_upgrade, [_, UpExtra0]}},
        {_, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra0, DnExtra, EmqxMods, RemainInstrs};
%% remove 'apply' instrs for downgrade, and collect the 'Extra' parameter
do_filter_and_get({apply, {emqx_relup, post_release_downgrade, [_, DnExtra0]}},
        {UpExtra, _, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra0, EmqxMods, RemainInstrs};
%% keep all other instrs unchanged
do_filter_and_get(Instr, {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs ++ [Instr]}.

assert_mandatory_modules(_, Mods) ->
    MandInstrs = [{load_module,emqx_release,brutal_purge,soft_purge,[]},
                  {load_module,emqx_relup}],
    assert(lists:member(emqx_relup, Mods) andalso lists:member(emqx_release, Mods),
        "The following instructions are mandatory in every clause of the emqx.appup.src: ~p", [MandInstrs]).

assert(true, _, _) ->
    ok;
assert(false, Msg, Args) ->
    ?ERROR(Msg, Args),
    error(assert_failed).

term_to_text(Term) ->
    io_lib:format("~p.", [Term]).
