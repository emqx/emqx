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

  --check           Don't update the appfile, just check that they are complete
  --repo            Upsteam git repo URL
  --remote          Get upstream repo URL from the specified git remote
  --skip-build      Don't rebuild the releases. May produce wrong results
  --make-command    A command used to assemble the release
  --release-dir     Release directory
  --src-dirs        Directories where source code is found. Defaults to '{src,apps,lib-*}/**/'
  --binary-rel-url  Binary release URL pattern. %TAG% variable is substituted with the release tag.
                    E.g. \"https://github.com/emqx/emqx/releases/download/v%TAG%/emqx-centos7-%TAG%-amd64.zip\"
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
     , binary_rel_url => undefined
     }.

%% App-specific actions that should be added unconditionally to any update/downgrade:
app_specific_actions(_) ->
    [].

ignored_apps() ->
    [emqx_dashboard, emqx_management] ++ otp_standard_apps().

main(Args) ->
    #{prev_tag := Baseline} = Options = parse_args(Args, default_options()),
    init_globals(Options),
    main(Options, Baseline).

parse_args([PrevTag = [A|_]], State) when A =/= $- ->
    State#{prev_tag => PrevTag};
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
parse_args(["--src-dirs", Pattern|Rest], State) ->
    parse_args(Rest, State#{src_dirs => Pattern});
parse_args(["--binary-rel-url", URL|Rest], State) ->
    parse_args(Rest, State#{binary_rel_url => {ok, URL}});
parse_args(_, _) ->
    fail(usage()).

main(Options, Baseline) ->
    {CurrRelDir, PrevRelDir} = prepare(Baseline, Options),
    log("~n===================================~n"
        "Processing changes..."
        "~n===================================~n"),
    CurrAppsIdx = index_apps(CurrRelDir),
    PrevAppsIdx = index_apps(PrevRelDir),
    %% log("Curr: ~p~nPrev: ~p~n", [CurrAppsIdx, PrevAppsIdx]),
    AppupChanges = find_appup_actions(CurrAppsIdx, PrevAppsIdx),
    case getopt(check) of
        true ->
            case AppupChanges of
                [] ->
                    ok;
                _ ->
                    set_invalid(),
                    log("ERROR: The appup files are incomplete. Missing changes:~n   ~p", [AppupChanges])
            end;
        false ->
            update_appups(AppupChanges)
    end,
    check_appup_files(),
    warn_and_exit(is_valid()).

warn_and_exit(true) ->
    log("
NOTE: Please review the changes manually. This script does not know about NIF
changes, supervisor changes, process restarts and so on. Also the load order of
the beam files might need updating.~n"),
    halt(0);
warn_and_exit(false) ->
    log("~nERROR: Incomplete appups found. Please inspect the output for more details.~n"),
    halt(1).

prepare(Baseline, Options = #{make_command := MakeCommand, beams_dir := BeamDir, binary_rel_url := BinRel}) ->
    log("~n===================================~n"
        "Baseline: ~s"
        "~n===================================~n", [Baseline]),
    log("Building the current version...~n"),
    bash(MakeCommand),
    log("Downloading and building the previous release...~n"),
    PrevRelDir =
        case BinRel of
            undefined ->
                {ok, PrevRootDir} = build_prev_release(Baseline, Options),
                filename:join(PrevRootDir, BeamDir);
            {ok, _URL} ->
                {ok, PrevRootDir} = download_prev_release(Baseline, Options),
                PrevRootDir
        end,
    {BeamDir, PrevRelDir}.

build_prev_release(Baseline, #{clone_url := Repo, make_command := MakeCommand}) ->
    BaseDir = "/tmp/emqx-baseline/",
    Dir = filename:basename(Repo, ".git") ++ [$-|Baseline],
    %% TODO: shallow clone
    Script = "mkdir -p ${BASEDIR} &&
              cd ${BASEDIR} &&
              { [ -d ${DIR} ] || git clone --branch ${TAG} ${REPO} ${DIR}; } &&
              cd ${DIR} &&" ++ MakeCommand,
    Env = [{"REPO", Repo}, {"TAG", Baseline}, {"BASEDIR", BaseDir}, {"DIR", Dir}],
    bash(Script, Env),
    {ok, filename:join(BaseDir, Dir)}.

download_prev_release(Tag, #{binary_rel_url := {ok, URL0}, clone_url := Repo}) ->
    URL = string:replace(URL0, "%TAG%", Tag, all),
    BaseDir = "/tmp/emqx-baseline-bin/",
    Dir = filename:basename(Repo, ".git") ++ [$-|Tag],
    Filename = filename:join(BaseDir, Dir),
    Script = "mkdir -p ${OUTFILE} &&
              wget -c -O ${OUTFILE}.zip ${URL} &&
              unzip -n -d ${OUTFILE} ${OUTFILE}.zip",
    Env = [{"TAG", Tag}, {"OUTFILE", Filename}, {"URL", URL}],
    bash(Script, Env),
    {ok, Filename}.

find_upstream_repo(Remote) ->
    string:trim(os:cmd("git remote get-url " ++ Remote)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Appup action creation and updating
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_appup_actions(CurrApps, PrevApps) ->
    maps:fold(
      fun(App, CurrAppIdx, Acc) ->
              case PrevApps of
                  #{App := PrevAppIdx} -> find_appup_actions(App, CurrAppIdx, PrevAppIdx) ++ Acc;
                  _                    -> Acc %% New app, nothing to upgrade here.
              end
      end,
      [],
      CurrApps).

find_appup_actions(_App, AppIdx, AppIdx) ->
    %% No changes to the app, ignore:
    [];
find_appup_actions(App, CurrAppIdx, PrevAppIdx = #app{version = PrevVersion}) ->
    {OldUpgrade, OldDowngrade} = find_old_appup_actions(App, PrevVersion),
    Upgrade = merge_update_actions(App, diff_app(App, CurrAppIdx, PrevAppIdx), OldUpgrade),
    Downgrade = merge_update_actions(App, diff_app(App, PrevAppIdx, CurrAppIdx), OldDowngrade),
    if OldUpgrade =:= Upgrade andalso OldDowngrade =:= Downgrade ->
            %% The appup file has been already updated:
            [];
       true ->
            [{App, {Upgrade, Downgrade}}]
    end.

find_old_appup_actions(App, PrevVersion) ->
    {Upgrade0, Downgrade0} =
        case locate(ebin_current, App, ".appup") of
            {ok, AppupFile} ->
                log("Found the previous appup file: ~s~n", [AppupFile]),
                {_, U, D} = read_appup(AppupFile),
                {U, D};
            undefined ->
                %% Fallback to the app.src file, in case the
                %% application doesn't have a release (useful for the
                %% apps that live outside the EMQX monorepo):
                case locate(src, App, ".appup.src") of
                    {ok, AppupSrcFile} ->
                        log("Using ~s as a source of previous update actions~n", [AppupSrcFile]),
                        {_, U, D} = read_appup(AppupSrcFile),
                        {U, D};
                    undefined ->
                        {[], []}
                end
        end,
    {ensure_version(PrevVersion, Upgrade0), ensure_version(PrevVersion, Downgrade0)}.

merge_update_actions(App, Changes, Vsns) ->
    lists:map(fun(Ret = {<<".*">>, _}) ->
                      Ret;
                 ({Vsn, Actions}) ->
                      {Vsn, do_merge_update_actions(App, Changes, Actions)}
              end,
              Vsns).

do_merge_update_actions(App, {New0, Changed0, Deleted0}, OldActions) ->
    AppSpecific = app_specific_actions(App) -- OldActions,
    AlreadyHandled = lists:flatten(lists:map(fun process_old_action/1, OldActions)),
    New = New0 -- AlreadyHandled,
    Changed = Changed0 -- AlreadyHandled,
    Deleted = Deleted0 -- AlreadyHandled,
    [{load_module, M, brutal_purge, soft_purge, []} || M <- Changed ++ New] ++
        OldActions ++
        [{delete_module, M} || M <- Deleted] ++
        AppSpecific.


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

ensure_version(Version, OldInstructions) ->
    OldVersions = [ensure_string(element(1, I)) || I <- OldInstructions],
    case lists:member(Version, OldVersions) of
        false ->
            [{Version, []}|OldInstructions];
        _ ->
            OldInstructions
    end.

read_appup(File) ->
    %% NOTE: appup file is a script, it may contain variables or functions.
    case file:script(File, [{'VSN', "VSN"}]) of
        {ok, Terms} ->
            Terms;
        Error ->
            fail("Failed to parse appup file ~s: ~p", [File, Error])
    end.

check_appup_files() ->
    AppupFiles = filelib:wildcard(getopt(src_dirs) ++ "/*.appup.src"),
    lists:foreach(fun read_appup/1, AppupFiles).

update_appups(Changes) ->
    lists:foreach(
      fun({App, {Upgrade, Downgrade}}) ->
              do_update_appup(App, Upgrade, Downgrade)
      end,
      Changes).

do_update_appup(App, Upgrade, Downgrade) ->
    case locate(src, App, ".appup.src") of
        {ok, AppupFile} ->
            render_appfile(AppupFile, Upgrade, Downgrade);
        undefined ->
            case create_stub(App) of
                {ok, AppupFile} ->
                    render_appfile(AppupFile, Upgrade, Downgrade);
                false ->
                    set_invalid(),
                    log("ERROR: Appup file for the external dependency '~p' is not complete.~n       Missing changes: ~p~n", [App, Upgrade])
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Appup file creation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

render_appfile(File, Upgrade, Downgrade) ->
    IOList = io_lib:format("%% -*- mode: erlang -*-\n{VSN,~n  ~p,~n  ~p}.~n", [Upgrade, Downgrade]),
    ok = file:write_file(File, IOList).

create_stub(App) ->
    Ext = ".app.src",
    case locate(src, App, Ext) of
        {ok, AppSrc} ->
            DirName = filename:dirname(AppSrc),
            AppupFile = filename:basename(AppSrc, Ext) ++ ".appup.src",
            Default = {<<".*">>, []},
            AppupFileFullpath = filename:join(DirName, AppupFile),
            render_appfile(AppupFileFullpath, [Default], [Default]),
            {ok, AppupFileFullpath};
        undefined ->
            false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% application and release indexing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

index_apps(ReleaseDir) ->
    Apps0 = maps:from_list([index_app(filename:join(ReleaseDir, AppFile)) ||
                               AppFile <- filelib:wildcard("**/ebin/*.app", ReleaseDir)]),
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

diff_app(App, #app{version = NewVersion, modules = NewModules}, #app{version = OldVersion, modules = OldModules}) ->
    {New, Changed} =
        maps:fold( fun(Mod, MD5, {New, Changed}) ->
                           case OldModules of
                               #{Mod := OldMD5} when MD5 =:= OldMD5 ->
                                   {New, Changed};
                               #{Mod := _} ->
                                   {New, [Mod|Changed]};
                               _ ->
                                   {[Mod|New], Changed}
                           end
                   end
                 , {[], []}
                 , NewModules
                 ),
    Deleted = maps:keys(maps:without(maps:keys(NewModules), OldModules)),
    NChanges = length(New) + length(Changed) + length(Deleted),
    if NewVersion =:= OldVersion andalso NChanges > 0 ->
            set_invalid(),
            log("ERROR: Application '~p' contains changes, but its version is not updated~n", [App]);
       NewVersion > OldVersion ->
            log("INFO: Application '~p' has been updated: ~p -> ~p~n", [App, OldVersion, NewVersion]),
            ok;
       true ->
            ok
    end,
    {New, Changed, Deleted}.

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Global state
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_globals(Options) ->
    ets:new(globals, [named_table, set, public]),
    ets:insert(globals, {valid, true}),
    ets:insert(globals, {options, Options}).

getopt(Option) ->
    maps:get(Option, ets:lookup_element(globals, options, 2)).

%% Set a global flag that something about the appfiles is invalid
set_invalid() ->
    ets:insert(globals, {valid, false}).

is_valid() ->
    ets:lookup_element(globals, valid, 2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Locate a file in a specified application
locate(ebin_current, App, Suffix) ->
    ReleaseDir = getopt(beams_dir),
    AppStr = atom_to_list(App),
    case filelib:wildcard(ReleaseDir ++ "/**/ebin/" ++ AppStr ++ Suffix) of
        [File] ->
            {ok, File};
        [] ->
            undefined
    end;
locate(src, App, Suffix) ->
    AppStr = atom_to_list(App),
    SrcDirs = getopt(src_dirs),
    case filelib:wildcard(SrcDirs ++ AppStr ++ Suffix) of
        [File] ->
            {ok, File};
        [] ->
            undefined
    end.

bash(Script) ->
    bash(Script, []).

bash(Script, Env) ->
    log("+ ~s~n+ Env: ~p~n", [Script, Env]),
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

log(Msg) ->
    log(Msg, []).

log(Msg, Args) ->
    io:format(standard_error, Msg, Args).

ensure_string(Str) when is_binary(Str) ->
    binary_to_list(Str);
ensure_string(Str) when is_list(Str) ->
    Str.

otp_standard_apps() ->
    [ssl, mnesia, kernel, asn1, stdlib].
