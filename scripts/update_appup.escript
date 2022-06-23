#!/usr/bin/env -S escript -c
%% -*- erlang-indent-level:4 -*-

usage() ->
"A script that fills in boilerplate for appup files.

Algorithm: this script compares md5s of beam files of each
application, and creates a `{load_module, Module, brutal_purge,
soft_purge, []}` action for the changed and new modules. For deleted
modules it creates `{delete_module, M}` action. These entries are
added to each patch release preceding the current release. If an entry
for a module already exists, this module is ignored. The existing
actions are kept.

Please note that it only compares the current release with its
predecessor, assuming that the upgrade actions for the older releases
are correct.

Note: The defaults are set up for emqx, but they can be tuned to
support other repos too.

Usage:

   update_appup.escript [--check] [--repo URL] [--remote NAME] [--skip-build] [--make-commad SCRIPT] [--release-dir DIR] <previous_release_tag>

Options:

  --check            Don't update the appfile, just check that they are complete
  --repo             Upsteam git repo URL
  --remote           Get upstream repo URL from the specified git remote
  --skip-build       Don't rebuild the releases. May produce wrong results
  --make-command     A command used to assemble the release
  --prev-release-dir Previous version's release dir (if already built/extracted)
  --release-dir      Release directory
  --src-dirs         Directories where source code is found. Defaults to '{src,apps,lib-*}/**/'
".

-record(app,
        { modules       :: #{module() => binary()}
        , version       :: string()
        }).

default_options() ->
    #{ clone_url      => find_upstream_repo("origin")
     , make_command   => "make emqx-rel"
     , beams_dir      => "_build/emqx/rel/emqx/lib/"
     , check          => false
     , prev_tag       => undefined
     , src_dirs       => "{src,apps,lib-*}/**/"
     , prev_beams_dir => undefined
     }.

%% App-specific actions that should be added unconditionally to any update/downgrade:
app_specific_actions(_) ->
    [].

ignored_apps() ->
    [gpb %% only a build tool
    ] ++ otp_standard_apps().

main(Args) ->
    #{prev_tag := Baseline} = Options = parse_args(Args, default_options()),
    init_globals(Options),
    main(Options, Baseline).

parse_args([PrevTag = [A|_]], State) when A =/= $- ->
    State#{prev_tag => PrevTag};
parse_args(["--check"|Rest], State) ->
    parse_args(Rest, State#{check => true});
parse_args(["--skip-build"|Rest], State) ->
    parse_args(Rest, State#{make_command => undefined});
parse_args(["--repo", Repo|Rest], State) ->
    parse_args(Rest, State#{clone_url => Repo});
parse_args(["--remote", Remote|Rest], State) ->
    parse_args(Rest, State#{clone_url => find_upstream_repo(Remote)});
parse_args(["--make-command", Command|Rest], State) ->
    parse_args(Rest, State#{make_command => Command});
parse_args(["--release-dir", Dir|Rest], State) ->
    parse_args(Rest, State#{beams_dir => Dir});
parse_args(["--prev-release-dir", Dir|Rest], State) ->
    parse_args(Rest, State#{prev_beams_dir => Dir});
parse_args(["--src-dirs", Pattern|Rest], State) ->
    parse_args(Rest, State#{src_dirs => Pattern});
parse_args(_, _) ->
    fail(usage()).

main(Options, Baseline) ->
    {CurrRelDir, PrevRelDir} = prepare(Baseline, Options),
    putopt(prev_beams_dir, PrevRelDir),
    log("~n===================================~n"
        "Processing changes..."
        "~n===================================~n"),
    CurrAppsIdx = index_apps(CurrRelDir),
    PrevAppsIdx = index_apps(PrevRelDir),
    %% log("Curr: ~p~nPrev: ~p~n", [CurrAppsIdx, PrevAppsIdx]),
    AppupChanges = find_appup_actions(CurrAppsIdx, PrevAppsIdx),
    ok = update_appups(AppupChanges),
    ok = check_appup_files(),
    ok = warn_and_exit(is_valid()).

warn_and_exit(true) ->
    log("
NOTE: Please review the changes manually. This script does not know about NIF
changes, supervisor changes, process restarts and so on. Also the load order of
the beam files might need updating.~n"),
    halt(0);
warn_and_exit(false) ->
    log("~nERROR: Incomplete appups found. Please inspect the output for more details.~n"),
    halt(1).

prepare(Baseline, Options = #{make_command := MakeCommand, beams_dir := BeamDir}) ->
    log("~n===================================~n"
        "Baseline: ~s"
        "~n===================================~n", [Baseline]),
    log("Building the current version...~n"),
    ok = bash(MakeCommand),
    PrevRelDir =
        case maps:get(prev_beams_dir, Options, undefined) of
            undefined ->
                log("Building the previous release...~n"),
                {ok, PrevRootDir} = build_prev_release(Baseline, Options),
                filename:join(PrevRootDir, BeamDir);
            Dir ->
                %% already built
                Dir
        end,
    {BeamDir, PrevRelDir}.

build_prev_release(Baseline, #{clone_url := Repo, make_command := MakeCommand}) ->
    BaseDir = "/tmp/emqx-appup-base/",
    Dir = filename:basename(Repo, ".git") ++ [$-|Baseline],
    Script = "mkdir -p ${BASEDIR} &&
              cd ${BASEDIR} &&
              { [ -d ${DIR} ] || git clone --depth 1 --branch ${TAG} ${REPO} ${DIR}; } &&
              cd ${DIR} &&" ++ MakeCommand,
    Env = [{"REPO", Repo}, {"TAG", Baseline}, {"BASEDIR", BaseDir}, {"DIR", Dir}],
    ok = bash(Script, Env),
    {ok, filename:join([BaseDir, Dir, "_build/*/lib"])}.

find_upstream_repo(Remote) ->
    string:trim(os:cmd("git remote get-url " ++ Remote)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Appup action creation and updating
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_appup_actions(CurrApps, PrevApps) ->
    maps:fold(
      fun(App, CurrAppIdx, Acc) ->
              case PrevApps of
                  #{App := PrevAppIdx} ->
                      find_appup_actions(App, CurrAppIdx, PrevAppIdx) ++ Acc;
                  _ ->
                      %% New app, nothing to upgrade here.
                      Acc
              end
      end,
      [],
      CurrApps).

find_appup_actions(_App, AppIdx, AppIdx) ->
    %% No changes to the app, ignore:
    [];
find_appup_actions(App,
                   CurrAppIdx = #app{version = CurrVersion},
                   PrevAppIdx = #app{version = PrevVersion}) ->
    {OldUpgrade0, OldDowngrade0} = find_base_appup_actions(App, PrevVersion),
    OldUpgrade = ensure_all_patch_versions(App, CurrVersion, OldUpgrade0),
    OldDowngrade = ensure_all_patch_versions(App, CurrVersion, OldDowngrade0),
    UpDiff = diff_app(up, App, CurrAppIdx, PrevAppIdx),
    DownDiff = diff_app(down, App, PrevAppIdx, CurrAppIdx),
    Upgrade = merge_update_actions(App, UpDiff, OldUpgrade, PrevVersion),
    Downgrade = merge_update_actions(App, DownDiff, OldDowngrade, PrevVersion),
    case OldUpgrade =:= Upgrade andalso OldDowngrade =:= Downgrade of
        true -> [];
        false -> [{App, {Upgrade, Downgrade, OldUpgrade, OldDowngrade}}]
    end.

%% To avoid missing one patch version when upgrading, we try to
%% optimistically generate the list of expected versions that should
%% be covered by the upgrade.
ensure_all_patch_versions(App, CurrVsn, OldActions) ->
    case is_app_external(App) of
        true ->
            %% we do not attempt to predict the version list for
            %% external dependencies, as those may not follow our
            %% conventions.
            OldActions;
        false ->
            do_ensure_all_patch_versions(App, CurrVsn, OldActions)
    end.

do_ensure_all_patch_versions(App, CurrVsn, OldActions) ->
    case enumerate_past_versions(CurrVsn) of
        {ok, ExpectedVsns} ->
            CoveredVsns = [V || {V, _} <- OldActions, V =/= <<".*">>],
            ExpectedVsnStrs = [vsn_number_to_string(V) || V <- ExpectedVsns],
            MissingActions = [{V, []} || V <- ExpectedVsnStrs, not contains_version(V, CoveredVsns)],
            MissingActions ++ OldActions;
        {error, bad_version} ->
            log("WARN: Could not infer expected versions to upgrade from for ~p~n", [App]),
            OldActions
    end.

%% For external dependencies, show only the changes that are missing
%% in their current appup.
diff_appup_instructions(ComputedChanges, PresentChanges) ->
    lists:foldr(
      fun({VsnOrRegex, ComputedActions}, Acc) ->
              case find_matching_version(VsnOrRegex, PresentChanges) of
                  undefined ->
                      [{VsnOrRegex, ComputedActions} | Acc];
                  PresentActions ->
                      DiffActions = ComputedActions -- PresentActions,
                      case DiffActions of
                          [] ->
                              %% no diff
                              Acc;
                          _ ->
                              [{VsnOrRegex, DiffActions} | Acc]
                      end
              end
      end,
      [],
      ComputedChanges).

%% checks if any missing diffs are present
%% and groups them by `up' and `down' types.
parse_appup_diffs(Upgrade, OldUpgrade, Downgrade, OldDowngrade) ->
    DiffUp = diff_appup_instructions(Upgrade, OldUpgrade),
    DiffDown = diff_appup_instructions(Downgrade, OldDowngrade),
    case {DiffUp, DiffDown} of
        {[], []} ->
            %% no diff for external dependency; ignore
            ok;
        _ ->
            Diffs = #{ up => DiffUp
                     , down => DiffDown
                     },
            {diffs, Diffs}
    end.

%% TODO: handle regexes
%% Since the first argument may be a regex itself, we would need to
%% check if it is "contained" within other regexes inside list of
%% versions in the second argument.
find_matching_version(VsnOrRegex, PresentChanges) ->
    proplists:get_value(VsnOrRegex, PresentChanges).

find_base_appup_actions(App, PrevVersion) ->
    {Upgrade, Downgrade} =
        case locate_appup(App) of
            {ok, AppupSrcFile} ->
                log("INFO: Using ~s as a source of previous update actions~n", [AppupSrcFile]),
                read_appup(AppupSrcFile);
            undefined ->
                log("INFO: no appup base found for ~p~n", [App]),
                {[], []}
        end,
    {ensure_version(PrevVersion, Upgrade), ensure_version(PrevVersion, Downgrade)}.

merge_update_actions(App, Changes, Vsns, PrevVersion) ->
    lists:map(fun(Ret = {<<".*">>, _}) ->
                      Ret;
                 ({Vsn, Actions}) ->
                      case is_skipped_version(App, Vsn, PrevVersion) of
                          true ->
                              log("WARN: ~p has version ~s skipped over?~n", [App, Vsn]),
                              {Vsn, Actions};
                          false ->
                              {Vsn, do_merge_update_actions(App, Changes, Actions)}
                      end
              end,
              Vsns).

%% say current version is 1.1.3, and the compare base is version 1.1.1,
%% but there is a 1.1.2 in appup we may skip merging instructions for
%% 1.1.2 because it's not used and no way to know what has been changed
is_skipped_version(App, Vsn, PrevVersion) when is_list(Vsn) andalso is_list(PrevVersion) ->
    case is_app_external(App) andalso parse_version_number(Vsn) of
        {ok, VsnTuple} ->
            case parse_version_number(PrevVersion) of
                {ok, PrevVsnTuple} ->
                    VsnTuple > PrevVsnTuple;
                _ ->
                    false
            end;
        _ ->
            false
    end;
is_skipped_version(_App, _Vsn, _PrevVersion) ->
    %% if app version is a regexp, we don't know for sure
    %% return 'false' to be on the safe side
    false.

do_merge_update_actions(App, {New0, Changed0, Deleted0}, OldActions) ->
    AppSpecific = app_specific_actions(App) -- OldActions,
    AlreadyHandled = lists:flatten(lists:map(fun process_old_action/1, OldActions)),
    New = New0 -- AlreadyHandled,
    Changed = Changed0 -- AlreadyHandled,
    Deleted = Deleted0 -- AlreadyHandled,
    HasRestart = contains_restart_application(App, OldActions),
    Actions =
        case HasRestart of
            true ->
                [];
            false ->
                [{load_module, M, brutal_purge, soft_purge, []} || M <- Changed] ++
                [{add_module, M} || M <- New]
        end,
    {OldActionsWithStop, OldActionsAfterStop} =
        find_application_stop_instruction(App, OldActions),
    OldActionsWithStop ++
        Actions ++
        OldActionsAfterStop ++
        case HasRestart of
            true ->
                [];
            false ->
                [{delete_module, M} || M <- Deleted]
        end ++
        AppSpecific.

%% If an entry restarts an application, there's no need to use
%% `load_module' instructions.
contains_restart_application(Application, Actions) ->
    lists:member({restart_application, Application}, Actions).

%% If there is an `application:stop(Application)' call in the
%% instructions, we insert `load_module' instructions after it.
find_application_stop_instruction(Application, Actions) ->
    {Before, After0} =
        lists:splitwith(
          fun({apply, {application, stop, [App]}}) when App =:= Application ->
                  false;
             (_) ->
                  true
          end, Actions),
    case After0 of
        [StopInst | After] ->
            {Before ++ [StopInst], After};
        [] ->
            {[], Before}
    end.

%% @doc Process the existing actions to exclude modules that are
%% already handled
process_old_action({purge, Modules}) ->
    Modules;
process_old_action({add_module, Module}) ->
    [Module];
process_old_action({delete_module, Module}) ->
    [Module];
process_old_action({update, Module, _Change}) ->
    [Module];
process_old_action(LoadModule) when is_tuple(LoadModule) andalso
                                    element(1, LoadModule) =:= load_module ->
    element(2, LoadModule);
process_old_action(_) ->
    [].

ensure_version(Version, OldInstructions) ->
    OldVersions = [element(1, I) || I <- OldInstructions],
    case contains_version(Version, OldVersions) of
        false ->
            [{Version, []} | OldInstructions];
        true ->
            OldInstructions
    end.

contains_version(Needle, Haystack) when is_list(Needle) ->
    lists:any(
      fun(Regex) when is_binary(Regex) ->
              case re:run(Needle, Regex) of
                  {match, _} ->
                      true;
                  nomatch ->
                      false
              end;
         (Vsn) ->
              Vsn =:= Needle
      end,
      Haystack).

%% As a best effort approach, we assume that we only bump patch
%% version numbers between release upgrades for our dependencies and
%% that we deal only with 3-part version schemas
%% (`Major.Minor.Patch').  Using those assumptions, we enumerate the
%% past versions that should be covered by regexes in .appup file
%% instructions.
enumerate_past_versions(Vsn) when is_list(Vsn) ->
    case parse_version_number(Vsn) of
        {ok, ParsedVsn} ->
            {ok, enumerate_past_versions(ParsedVsn)};
        Error ->
            Error
    end;
enumerate_past_versions({Major, Minor, Patch}) ->
    [{Major, Minor, P} || P <- lists:seq(Patch - 1, 0, -1)].

parse_version_number(Vsn) when is_list(Vsn) ->
    Nums = string:split(Vsn, ".", all),
    Results = lists:map(fun string:to_integer/1, Nums),
    case Results of
        [{Major, []}, {Minor, []}, {Patch, []}] ->
            {ok, {Major, Minor, Patch}};
        _ ->
            {error, bad_version}
    end.

vsn_number_to_string({Major, Minor, Patch}) ->
    io_lib:format("~b.~b.~b", [Major, Minor, Patch]).

read_appup(File) ->
    %% NOTE: appup file is a script, it may contain variables or functions.
   case do_read_appup(File) of
       {ok, {U, D}} -> {U, D};
       {error, Reason} -> fail("Failed to parse appup file ~p~n~p", [File, Reason])
    end.

do_read_appup(File) ->
    case file:script(File, [{'VSN', "VSN"}]) of
        {ok, {_, U, D}} ->
            {ok, {U, D}};
        {ok, Other} ->
            {error, {bad_appup_format, Other}};
        {error, Reason} ->
            {error, Reason}
    end.

check_appup_files() ->
    AppupFiles = filelib:wildcard(getopt(src_dirs) ++ "/*.appup.src"),
    lists:foreach(fun read_appup/1, AppupFiles).

update_appups(Changes) ->
    lists:foreach(
      fun({App, {Upgrade, Downgrade, OldUpgrade, OldDowngrade}}) ->
              do_update_appup(App, Upgrade, Downgrade, OldUpgrade, OldDowngrade)
      end,
      Changes).

do_update_appup(App, Upgrade, Downgrade, OldUpgrade, OldDowngrade) ->
    case locate_current_src(App, ".appup.src") of
        {ok, AppupFile} ->
            case contains_contents(AppupFile, Upgrade, Downgrade) of
                true ->
                    ok;
                false ->
                    render_appup(App, AppupFile, Upgrade, Downgrade)
            end;
        undefined ->
            maybe_create_appup(App, Upgrade, Downgrade, OldUpgrade, OldDowngrade)
    end.

maybe_create_appup(App, Upgrade, Downgrade, OldUpgrade, OldDowngrade) ->
    case create_stub(App) of
        {ok, AppupFile} ->
            render_appup(App, AppupFile, Upgrade, Downgrade);
        external ->
            %% for external appup, the best we can do is to validate it
            _ = check_appup(App, Upgrade, Downgrade, OldUpgrade, OldDowngrade),
            ok
    end.

check_appup(App, Upgrade, Downgrade, OldUpgrade, OldDowngrade) ->
    case parse_appup_diffs(Upgrade, OldUpgrade, Downgrade, OldDowngrade) of
        ok ->
            %% no diff for external dependency; ignore
            ok;
        {diffs, Diffs} ->
            set_invalid(),
            log("ERROR: Appup file for '~p' is not complete.~n"
                "Missing:~100p~n", [App, Diffs]),
            notok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Appup file creation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

render_appup(App, File, Up, Down) ->
    case getopt(check) of
        true ->
            case do_read_appup(File) of
                {ok, {U, D}} when U =:= Up andalso D =:= Down ->
                    ok;
                {ok, {OldU, OldD}} ->
                    check_appup(App, Up, Down, OldU, OldD);
                {error, enoent} ->
                    %% failed to read old file, exit
                    log("ERROR: ~s is missing", [File]),
                    set_invalid()
            end;
        false ->
            do_render_appup(File, Up, Down)
    end.

do_render_appup(File, Up, Down) ->
    IOList = io_lib:format("%% -*- mode: erlang -*-~n"
                           "%% Unless you know what you are doing, DO NOT edit manually!!~n"
                           "{VSN,~n  ~p,~n  ~p}.~n", [Up, Down]),
    ok = file:write_file(File, IOList).

create_stub(App) ->
    Ext = ".app.src",
    case locate_current_src(App, Ext) of
        {ok, AppSrc} ->
            DirName = filename:dirname(AppSrc),
            AppupFile = filename:basename(AppSrc, Ext) ++ ".appup.src",
            Default = {<<".*">>, []},
            AppupFileFullpath = filename:join(DirName, AppupFile),
            render_appup(App, AppupFileFullpath, [Default], [Default]),
            {ok, AppupFileFullpath};
        undefined ->
            external
    end.

%% we check whether the destination file already has the contents we
%% want to write to avoid writing and losing indentation and comments.
contains_contents(File, Upgrade, Downgrade) ->
    %% the file may contain the VSN variable, so it's a script
    case file:script(File, [{'VSN', 'VSN'}]) of
        {ok, {_, Upgrade, Downgrade}} ->
            true;
        _ ->
            false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% application and release indexing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

index_apps(ReleaseDir) ->
    log("INFO: indexing apps in ~s~n", [ReleaseDir]),
    AppFiles0 = filelib:wildcard("**/ebin/*.app", ReleaseDir),
    %% everything in _build sub-dir e.g. cuttlefish/_build should be ignored
    AppFiles = lists:filter(fun(File) -> re:run(File, "_build") =:= nomatch end, AppFiles0),
    Apps0 = maps:from_list([index_app(filename:join(ReleaseDir, AppFile)) || AppFile <- AppFiles]),
    maps:without(ignored_apps(), Apps0).

index_app(AppFile) ->
    {ok, [{application, App, Properties}]} = file:consult(AppFile),
    Vsn = proplists:get_value(vsn, Properties),
    %% Note: assuming that beams are always located in the same directory where app file is:
    EbinDir = filename:dirname(AppFile),
    Modules = hashsums(EbinDir),
    {App, #app{ version       = Vsn
              , modules       = Modules
              }}.

diff_app(UpOrDown, App,
         #app{version = NewVersion, modules = NewModules},
         #app{version = OldVersion, modules = OldModules}) ->
    {New, Changed} =
        maps:fold( fun(Mod, MD5, {New, Changed}) ->
                           case OldModules of
                               #{Mod := OldMD5} when MD5 =:= OldMD5 ->
                                   {New, Changed};
                               #{Mod := _} ->
                                   {New, [Mod | Changed]};
                               _ ->
                                   {[Mod | New], Changed}
                           end
                   end
                 , {[], []}
                 , NewModules
                 ),
    Deleted = maps:keys(maps:without(maps:keys(NewModules), OldModules)),
    Changes = lists:filter(fun({_T, L}) -> length(L) > 0 end,
                           [{added, New}, {changed, Changed}, {deleted, Deleted}]),
    case NewVersion =:= OldVersion of
        true when Changes =:= [] ->
            %% no change
            ok;
        true ->
            set_invalid(),
            case UpOrDown =:= up of
                true ->
                    %% only log for the upgrade case because it would be the same result
                    log("ERROR: Application '~p' contains changes, but its version is not updated. ~s",
                        [App, format_changes(Changes)]);
                false ->
                    ok
            end;
        false ->
            log("INFO: Application '~p' has been updated: ~p --[~p]--> ~p~n", [App, OldVersion, UpOrDown, NewVersion]),
            log("INFO: changes [~p]: ~p~n", [UpOrDown, Changes]),
            ok
    end,
    {New, Changed, Deleted}.

format_changes(Changes) ->
    lists:map(fun({Tag, List}) -> io_lib:format("~p: ~p~n", [Tag, List]) end, Changes).

-spec hashsums(file:filename()) -> #{module() => binary()}.
hashsums(EbinDir) ->
    maps:from_list(lists:map(
                     fun(Beam) ->
                             File = filename:join(EbinDir, Beam),
                             {ok, Ret = {_Module, _MD5}} = beam_lib:md5(File),
                             Ret
                     end,
                     filelib:wildcard("*.beam", EbinDir)
                    )).

is_app_external(App) ->
    Ext = ".app.src",
    case locate_current_src(App, Ext) of
        {ok, _} ->
            false;
        undefined ->
            true
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Global state
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_globals(Options) ->
    ets:new(globals, [named_table, set, public]),
    ets:insert(globals, {valid, true}),
    ets:insert(globals, {options, Options}).

putopt(Option, Value) ->
    ets:insert(globals, {{option, Option}, Value}).

getopt(Option) ->
    case ets:lookup(globals, {option, Option}) of
        [] ->
            maps:get(Option, ets:lookup_element(globals, options, 2));
        [{_, V}] ->
            V
    end.

%% Set a global flag that something about the appfiles is invalid
set_invalid() ->
    ets:insert(globals, {valid, false}).

is_valid() ->
    ets:lookup_element(globals, valid, 2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

locate_appup(App) ->
    case locate_current_rel(App, ".appup.src") of
        {ok, File} ->
            {ok, File};
        undefined ->
            %% fallback to .appup
            locate_current_rel(App, ".appup")
    end.

locate_current_rel(App, Suffix) ->
    CurDir = getopt(beams_dir),
    do_locate(filename:join([CurDir, "**"]), App, Suffix).

%% Locate a file in a specified application
locate_current_src(App, Suffix) ->
    SrcDirs = getopt(src_dirs),
    do_locate(SrcDirs, App, Suffix).

do_locate(Dir, App, Suffix) ->
    AppStr = atom_to_list(App),
    Pattern = filename:join(Dir, AppStr ++ Suffix),
    case find_app(Pattern) of
        [File] ->
            {ok, File};
        [] ->
            undefined;
        Files ->
            error({more_than_one_app_found, Files})
    end.

find_app(Pattern) ->
    lists:filter(fun(D) -> re:run(D, "apps/.*/_build") =:= nomatch end,
                 filelib:wildcard(Pattern)).

bash(undefined) -> ok;
bash(Script) ->
    bash(Script, []).

bash(Script, Env) ->
    log("+ ~s~n+ Env: ~p~n", [Script, Env]),
    case cmd("bash", #{args => ["-c", Script], env => Env}) of
        0 -> ok;
        _ -> fail("Failed to run command: ~s", [Script])
    end.

%% Spawn an executable and return the exit status
cmd(Exec, Params) ->
    case os:find_executable(Exec) of
        false ->
            fail("Executable not found in $PATH: ~s", [Exec]);
        Path ->
            Params1 = maps:to_list(maps:with([env, args, cd], Params)),
            Port = erlang:open_port( {spawn_executable, Path}
                                   , [ exit_status
                                     , nouse_stdio
                                     | Params1
                                     ]
                                   ),
            receive
                {Port, {exit_status, Status}} ->
                    Status
            end
    end.

fail(Str) ->
    fail(Str, []).

fail(Str, Args) ->
    log(Str ++ "~n", Args),
    halt(1).

log(Msg) ->
    log(Msg, []).

log(Msg, Args) ->
    io:format(standard_error, Msg, Args).

otp_standard_apps() ->
    [ssl, mnesia, kernel, asn1, stdlib].
