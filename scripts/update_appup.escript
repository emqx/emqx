#!/usr/bin/env -S escript -c
%% -*- erlang-indent-level:4 -*-
%% A script that adds changed modules to the corresponding appup files

main(Args) ->
    #{check := Check, current_release := CurrentRelease, prepare := Prepare} =
        parse_args(Args, #{check => false, prepare => true}),
    case find_pred_tag(CurrentRelease) of
        {ok, Baseline} ->
            {CurrDir, PredDir} = prepare(Baseline, Prepare),
            Upgrade = diff_releases(CurrDir, PredDir),
            Downgrade = diff_releases(PredDir, CurrDir),
            Apps = maps:keys(Upgrade),
            lists:foreach( fun(App) ->
                                   #{App := AppUpgrade} = Upgrade,
                                   #{App := AppDowngrade} = Downgrade,
                                   process_app(Baseline, Check, App, AppUpgrade, AppDowngrade)
                           end
                         , Apps
                         );
        undefined ->
            log("No appup update is needed for this release, nothing to be done~n", []),
            ok
    end.

parse_args([CurrentRelease = [A|_]], State) when A =/= $- ->
    State#{current_release => CurrentRelease};
parse_args(["--check"|Rest], State) ->
    parse_args(Rest, State#{check => true});
parse_args(["--skip-build"|Rest], State) ->
    parse_args(Rest, State#{prepare => false});
parse_args([], _) ->
    fail("A script that creates stubs for appup files

Usage: update_appup.escript [--check] [--skip-build] <current_release_tag>

  --check       Don't update the appup files, just check that they are complete
  --skip-build  Don't rebuild the releases. May produce wrong appup files if changes are made.
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
process_app(PredVersion, _Check, App, Upgrade, Downgrade) ->
    case locate(App, ".appup.src") of
        {ok, AppupFile} ->
            update_appup(PredVersion, AppupFile, Upgrade, Downgrade);
        undefined ->
            case create_stub(App) of
                false ->
                    %% External dependency, skip
                    ok;
                AppupFile ->
                    update_appup(PredVersion, AppupFile, Upgrade, Downgrade)
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

update_appup(_, File, {[], [], []}, {[], [], []}) ->
    %% No changes in the app. Just check syntax of the existing appup:
    _ = read_appup(File);
update_appup(PredVersion, File, UpgradeChanges, DowngradeChanges) ->
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
    AlreadyHandled = lists:map(fun(It) -> element(2, It) end, OldActions),
    New = New0 -- AlreadyHandled,
    Changed = Changed0 -- AlreadyHandled,
    Deleted = Deleted0 -- AlreadyHandled,
    OldActions ++ [{load_module, M, brutal_purge, soft_purge, []} || M <- Changed ++ New]
               ++ [{delete_module, M} || M <- Deleted].

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

diff_releases(CurrDir, OldDir) ->
    Curr = hashsums(find_beams(CurrDir)),
    Old = hashsums(find_beams(OldDir)),
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

prepare(Baseline, Prepare) ->
    log("~n===================================~n"
        "Baseline: ~s"
        "~n===================================~n", [Baseline]),
    log("Building the current version...~n"),
    Prepare andalso bash("make emqx-rel"),
    log("Downloading the preceding release...~n"),
    {ok, PredRootDir} = build_pred_release(Baseline, Prepare),
    BeamDir = "_build/emqx/rel/emqx/lib/",
    {BeamDir, filename:join(PredRootDir, BeamDir)}.

build_pred_release(Baseline, Prepare) ->
    Repo = find_upstream_repo(),
    BaseDir = "/tmp/emqx-baseline/",
    Dir = filename:basename(Repo, ".git") ++ [$-|Baseline],
    %% TODO: shallow clone
    Script = "mkdir -p ${BASEDIR} &&
              cd ${BASEDIR} &&
              { git clone --branch ${TAG} ${REPO} ${DIR} || true; } &&
              cd ${DIR} &&
              make emqx-rel",
    Env = [{"REPO", Repo}, {"TAG", Baseline}, {"BASEDIR", BaseDir}, {"DIR", Dir}],
    Prepare andalso bash(Script, Env),
    {ok, filename:join(BaseDir, Dir)}.

%% @doc Find whether we are in emqx or emqx-ee
find_upstream_repo() ->
    Str = os:cmd("git remote get-url origin"),
    case re(Str, "/([^/]+).git$") of
        {match, ["emqx"]}    -> "git@github.com:emqx/emqx.git";
        {match, ["emqx-ee"]} -> "git@github.com:emqx/emqx-ee.git";
        Ret                  -> fail("Cannot detect the correct upstream repo: ~p", [Ret])
    end.

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
    case re(Version, "^([0-9]+)\.([0-9]+)\.([0-9]+)$") of
        {match, [Maj, Min, Patch]} ->
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
