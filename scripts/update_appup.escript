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

   update_appup.escript [--check] [--repo URL] [--remote NAME] [--skip-build] [--make-commad SCRIPT] [--release-dir DIR] <current_release_tag>

Options:

  --check         Don't update the appup files, just check that they are complete
  --repo          Upsteam git repo URL
  --remote        Get upstream repo URL from the specified git remote
  --skip-build    Don't rebuild the releases. May produce wrong results
  --make-command  A command used to assemble the release
  --release-dir   Release directory
".

default_options() ->
    #{ check        => false
     , clone_url    => find_upstream_repo("origin")
     , make_command => "make emqx-rel"
     , beams_dir    => "_build/emqx/rel/emqx/lib/"
     }.

main(Args) ->
    #{current_release := CurrentRelease} = Options = parse_args(Args, default_options()),
    case find_pred_tag(CurrentRelease) of
        {ok, Baseline} ->
            main(Options, Baseline);
        undefined ->
            log("No appup update is needed for this release, nothing to be done~n", []),
            ok
    end.

parse_args([CurrentRelease = [A|_]], State) when A =/= $- ->
    State#{current_release => CurrentRelease};
parse_args(["--check"|Rest], State) ->
    parse_args(Rest, State#{check => true});
parse_args(["--skip-build"|Rest], State) ->
    parse_args(Rest, State#{make_command => "true"});
parse_args(["--repo", Repo|Rest], State) ->
    parse_args(Rest, State#{clone_url => Repo});
parse_args(["--remote", Remote|Rest], State) ->
    parse_args(Rest, State#{clone_url => find_upstream_repo(Remote)});
parse_args(["--make-command", Command|Rest], State) ->
    parse_args(Rest, State#{make_command => Command});
parse_args(["--release-dir", Dir|Rest], State) ->
    parse_args(Rest, State#{beams_dir => Dir});
parse_args(_, _) ->
    fail(usage()).

main(Options = #{check := Check}, Baseline) ->
    {CurrDir, PredDir} = prepare(Baseline, Options),
    CurrBeams = hashsums(find_beams(CurrDir)),
    PredBeams = hashsums(find_beams(PredDir)),
    Upgrade = diff_releases(CurrBeams, PredBeams),
    Downgrade = diff_releases(PredBeams, CurrBeams),
    Apps = maps:keys(Upgrade),
    lists:foreach( fun(App) ->
                           #{App := AppUpgrade} = Upgrade,
                           #{App := AppDowngrade} = Downgrade,
                           process_app(Baseline, Check, App, AppUpgrade, AppDowngrade)
                   end
                 , Apps
                 ),
    log("
NOTE: Please review the changes manually. This script does not know about NIF
changes, supervisor changes, process restarts and so on. Also the load order of
the beam files might need updating.
").

process_app(_, _, App, {[], [], []}, {[], [], []}) ->
    %% No changes, just check the appup file if present:
    case locate(App, ".appup.src") of
        {ok, AppupFile} ->
            _ = read_appup(AppupFile),
            ok;
        undefined ->
            ok
    end;
process_app(PredVersion, Check, App, Upgrade, Downgrade) ->
    case locate(App, ".appup.src") of
        {ok, AppupFile} ->
            update_appup(Check, PredVersion, AppupFile, Upgrade, Downgrade);
        undefined ->
            case create_stub(App) of
                false ->
                    %% External dependency, skip
                    ok;
                AppupFile ->
                    update_appup(Check, PredVersion, AppupFile, Upgrade, Downgrade)
            end
    end.

create_stub(App) ->
    case locate(App, ".app.src") of
        {ok, AppSrc} ->
            AppupFile = filename:basename(AppSrc) ++ ".appup.src",
            Default = {<<".*">>, []},
            render_appfile(AppupFile, [Default], [Default]),
            AppupFile;
        undefined ->
            false
    end.

update_appup(Check, PredVersion, File, UpgradeChanges, DowngradeChanges) ->
    log("Updating appup: ~p~n", [File]),
    {_, Upgrade0, Downgrade0} = read_appup(File),
    Upgrade = update_actions(PredVersion, UpgradeChanges, Upgrade0),
    Downgrade = update_actions(PredVersion, DowngradeChanges, Downgrade0),
    render_appfile(File, Upgrade, Downgrade),
    %% Check appup syntax:
    _ = read_appup(File).

render_appfile(File, Upgrade, Downgrade) ->
    IOList = io_lib:format("%% -*- mode: erlang -*-\n{VSN,~n  ~p,~n  ~p}.~n", [Upgrade, Downgrade]),
    ok = file:write_file(File, IOList).

update_actions(PredVersion, Changes, Actions) ->
    lists:map( fun(L) -> do_update_actions(Changes, L) end
             , ensure_pred_versions(PredVersion, Actions)
             ).

do_update_actions(_, Ret = {<<".*">>, _}) ->
    Ret;
do_update_actions(Changes, {Vsn, Actions}) ->
    {Vsn, process_changes(Changes, Actions)}.

process_changes({New0, Changed0, Deleted0}, OldActions) ->
    AlreadyHandled = lists:flatten(lists:map(fun process_old_action/1, OldActions)),
    New = New0 -- AlreadyHandled,
    Changed = Changed0 -- AlreadyHandled,
    Deleted = Deleted0 -- AlreadyHandled,
    [{load_module, M, brutal_purge, soft_purge, []} || M <- Changed ++ New] ++
        OldActions ++
        [{delete_module, M} || M <- Deleted].

%% @doc Process the existing actions to exclude modules that are
%% already handled
process_old_action({purge, Modules}) ->
    Modules;
process_old_action({delete_module, Module}) ->
    [Module];
process_old_action(LoadModule) when is_tuple(LoadModule) andalso
                                    element(1, LoadModule) =:= load_module ->
    element(2, LoadModule);
process_old_action(_) ->
    [].

ensure_pred_versions(PredVersion, Versions) ->
    {Maj, Min, Patch} = parse_semver(PredVersion),
    PredVersions = [semver(Maj, Min, P) || P <- lists:seq(0, Patch)],
    lists:foldl(fun ensure_version/2, Versions, PredVersions).

ensure_version(Version, Versions) ->
    case lists:keyfind(Version, 1, Versions) of
        false ->
            [{Version, []}|Versions];
        _ ->
            Versions
    end.

read_appup(File) ->
    case file:script(File, [{'VSN', "VSN"}]) of
        {ok, Terms} ->
            Terms;
        Error ->
            fail("Failed to parse appup file ~s: ~p", [File, Error])
    end.

diff_releases(Curr, Old) ->
    Fun = fun(App, Modules, Acc) ->
                  OldModules = maps:get(App, Old, #{}),
                  Acc#{App => diff_app_modules(Modules, OldModules)}
          end,
    maps:fold(Fun, #{}, Curr).

diff_app_modules(Modules, OldModules) ->
    {New, Changed} =
        maps:fold( fun(Mod, MD5, {New, Changed}) ->
                           case OldModules of
                               #{Mod := OldMD5} when MD5 =:= OldMD5 ->
                                   {New, Changed};
                               #{Mod := _} ->
                                   {New, [Mod|Changed]};
                               _ -> {[Mod|New], Changed}
                           end
                   end
                 , {[], []}
                 , Modules
                 ),
    Deleted = maps:keys(maps:without(maps:keys(Modules), OldModules)),
    {New, Changed, Deleted}.

find_beams(Dir) ->
    [filename:join(Dir, I) || I <- filelib:wildcard("**/ebin/*.beam", Dir)].

prepare(Baseline, Options = #{make_command := MakeCommand, beams_dir := BeamDir}) ->
    log("~n===================================~n"
        "Baseline: ~s"
        "~n===================================~n", [Baseline]),
    log("Building the current version...~n"),
    bash(MakeCommand),
    log("Downloading and building the previous release...~n"),
    {ok, PredRootDir} = build_pred_release(Baseline, Options),
    {BeamDir, filename:join(PredRootDir, BeamDir)}.

build_pred_release(Baseline, #{clone_url := Repo, make_command := MakeCommand}) ->
    BaseDir = "/tmp/emqx-baseline/",
    Dir = filename:basename(Repo, ".git") ++ [$-|Baseline],
    %% TODO: shallow clone
    Script = "mkdir -p ${BASEDIR} &&
              cd ${BASEDIR} &&
              { git clone --branch ${TAG} ${REPO} ${DIR} || true; } &&
              cd ${DIR} &&" ++ MakeCommand,
    Env = [{"REPO", Repo}, {"TAG", Baseline}, {"BASEDIR", BaseDir}, {"DIR", Dir}],
    bash(Script, Env),
    {ok, filename:join(BaseDir, Dir)}.

find_upstream_repo(Remote) ->
    string:trim(os:cmd("git remote get-url " ++ Remote)).

find_pred_tag(CurrentRelease) ->
    {Maj, Min, Patch} = parse_semver(CurrentRelease),
    case Patch of
        0 -> undefined;
        _ -> {ok, semver(Maj, Min, Patch - 1)}
    end.

-spec hashsums(file:filename()) -> #{App => #{module() => binary()}}
              when App :: atom().
hashsums(Files) ->
    hashsums(Files, #{}).

hashsums([], Acc) ->
    Acc;
hashsums([File|Rest], Acc0) ->
    [_, "ebin", Dir|_] = lists:reverse(filename:split(File)),
    {match, [AppStr]} = re(Dir, "^(.*)-[^-]+$"),
    App = list_to_atom(AppStr),
    {ok, {Module, MD5}} = beam_lib:md5(File),
    Acc = maps:update_with( App
                          , fun(Old) -> Old #{Module => MD5} end
                          , #{Module => MD5}
                          , Acc0
                          ),
    hashsums(Rest, Acc).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_semver(Version) ->
    case re(Version, "^([0-9]+)\\.([0-9]+)\\.([0-9]+)(\\.[0-9]+)?$") of
        {match, [Maj, Min, Patch|_]} ->
            {list_to_integer(Maj), list_to_integer(Min), list_to_integer(Patch)};
        _ ->
            error({not_a_semver, Version})
    end.

semver(Maj, Min, Patch) ->
    lists:flatten(io_lib:format("~p.~p.~p", [Maj, Min, Patch])).

%% Locate a file in a specified application
locate(App, Suffix) ->
    AppStr = atom_to_list(App),
    case filelib:wildcard("{src,apps,lib-*}/**/" ++ AppStr ++ Suffix) of
        [File] ->
            {ok, File};
        [] ->
            undefined
    end.

bash(Script) ->
    bash(Script, []).

bash(Script, Env) ->
    case cmd("bash", #{args => ["-c", Script], env => Env}) of
        0 -> true;
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

re(Subject, RE) ->
    re:run(Subject, RE, [{capture, all_but_first, list}]).

log(Msg) ->
    log(Msg, []).

log(Msg, Args) ->
    io:format(standard_error, Msg, Args).
