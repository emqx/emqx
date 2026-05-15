-module(emqx_relup_handler).

-export([
    list_supported_paths/0,
    check_and_unpack/3,
    perform_upgrade/4,
    permanent_upgrade/4,
    validate_priv_catalog/0
]).

-import(lists, [concat/1]).
-import(emqx_relup_utils, [str/1, bin/1, exception_to_error/3, make_error/2]).

%%==============================================================================
%% API
%%==============================================================================

%% Enumerate the {From, Target} hops this plugin's priv catalog
%% supports. Each `priv/relup/*.relup` file is a single hop. Files
%% that fail to parse are skipped here (the boot-time validator logs
%% them as warnings — see `validate_priv_catalog/0`).
list_supported_paths() ->
    [
        #{
            from_version => maps:get(from_version, R),
            target_version => maps:get(target_version, R)
        }
     || R <- load_priv_catalog()
    ].

check_and_unpack(CurrVsn, RootDir, #{tarball := TarFile} = Opts) ->
    try
        ok = assert_no_upgrade_pending(RootDir),
        {ok, UnpackDir} = unpack_release(TarFile),
        TargetVsn = read_rel_vsn(UnpackDir),
        ok = assert_not_same_vsn(CurrVsn, TargetVsn),
        ok = check_write_permission(RootDir),
        ok = check_otp_comaptibility(CurrVsn, RootDir, UnpackDir, TargetVsn),
        {ok, OldRel} = consult_rel_file(RootDir, CurrVsn),
        {ok, NewRel} = consult_rel_file(UnpackDir, TargetVsn),
        ok = deploy_files(TargetVsn, RootDir, UnpackDir, OldRel, NewRel, Opts),
        Relup = get_relup_entry(CurrVsn, TargetVsn),
        {ok, Opts#{
            target_vsn => TargetVsn,
            unpack_dir => UnpackDir,
            old_rel => OldRel,
            new_rel => NewRel,
            relup => Relup
        }}
    catch
        throw:Reason ->
            {error, Reason};
        Err:Reason:ST ->
            exception_to_error(Err, Reason, ST)
    end.

perform_upgrade(CurrVsn, TargetVsn, RootDir, Opts) ->
    try
        UpgradeType = maps:get(upgrade_type, Opts, eval_upgrade),
        #{new_rel := NewRel, relup := Relup} = Opts,
        Dir = get_deploy_dir(RootDir, TargetVsn),
        {emqx_relup_libs:make_libs_info(NewRel, Dir), UpgradeType}
    of
        {LibModInfo, eval_upgrade} ->
            eval_relup(CurrVsn, TargetVsn, Relup, LibModInfo);
        {_, deploy_only} ->
            ok
    catch
        throw:Reason ->
            {error, Reason};
        Err:Reason:ST ->
            exception_to_error(Err, Reason, ST)
    end.

%%==============================================================================
%% Check Upgrade
%%==============================================================================
%% A previous successful upgrade leaves `<RootDir>/relup/version` pointing at
%% the deployed target. The `bin/emqx` wrapper consumes that marker on the
%% next start and execs into `<RootDir>/relup/<TargetVsn>/bin/emqx`. Until that
%% restart happens, `code:root_dir()` still points at the original install and
%% another upgrade attempt would re-run the same hop (or, worse, plan a hop on
%% top of stale `emqx_release:version()`). Refuse early.
%%
%% After the restart, the new `bin/emqx` resolves `code:root_dir()` to the
%% deploy dir, which has no `relup/version` of its own, so this check passes
%% and the next hop can be planned from the freshly booted version.
assert_no_upgrade_pending(RootDir) ->
    VersionFile = filename:join([independent_deploy_root(RootDir), "version"]),
    case file:read_file(VersionFile) of
        {ok, Bin} ->
            PendingTarget = string:trim(Bin),
            throw(
                make_error(upgrade_pending_restart, #{
                    pending_target => PendingTarget,
                    version_marker => bin(VersionFile),
                    hint =>
                        <<
                            "a previous upgrade is deployed and pending node restart; "
                            "restart the node before another upgrade, or remove "
                            "the version marker file to override"
                        >>
                })
            );
        {error, enoent} ->
            ok
    end.

assert_not_same_vsn(TargetVsn, TargetVsn) ->
    throw(make_error(already_upgraded_to_target_vsn, #{vsn => TargetVsn}));
assert_not_same_vsn(_CurrVsn, _TargetVsn) ->
    ok.

check_write_permission(RootDir) ->
    SubDirs = ["relup"],
    lists:foreach(
        fun(SubDir) ->
            do_check_write_permission(RootDir, SubDir)
        end,
        SubDirs
    ).

do_check_write_permission(RootDir, SubDir) ->
    Dir = filename:join([RootDir, SubDir]),
    File = filename:join([Dir, "relup_test_perm"]),
    case filelib:ensure_dir(File) of
        ok ->
            case file:write_file(File, "t") of
                {error, eacces} ->
                    throw(make_error(no_write_permission, #{dir => Dir}));
                {error, Reason} ->
                    throw(make_error(cannot_write_file, #{dir => Dir, reason => Reason}));
                ok ->
                    ok = file:delete(File)
            end;
        {error, Reason} ->
            throw(make_error(cannot_create_dir, #{dir => Dir, reason => Reason}))
    end.

check_otp_comaptibility(CurrVsn, RootDir, UnpackDir, TargetVsn) ->
    CurrBuildInfo = read_build_info(RootDir, CurrVsn),
    NewBuildInfo = read_build_info(UnpackDir, TargetVsn),
    CurrOTPVsn = maps:get("erlang", CurrBuildInfo),
    NewOTPVsn = maps:get("erlang", NewBuildInfo),
    %% 1. We may need to update the OTP version to fix some bugs so here we only check the major version.
    assert_same_major_vsn(CurrOTPVsn, NewOTPVsn),
    %% 2. We have our own OTP fork, so here we make sure the new OTP version is also from our fork,
    %%    otherwise the emqx may failed to get started due to mira problems.
    assert_same_otp_fork(UnpackDir),
    %% 3. We need to make sure the os arch the same, otherwise the emqx will fail to load NIFs.
    assert_same_os_arch(CurrBuildInfo, NewBuildInfo).

assert_same_major_vsn(CurrOTPVsn, NewOTPVsn) ->
    case emqx_relup_utils:major_vsn(CurrOTPVsn) =:= emqx_relup_utils:major_vsn(NewOTPVsn) of
        true -> ok;
        false -> throw(make_error(otp_major_vsn_mismatch, #{curr => CurrOTPVsn, new => NewOTPVsn}))
    end.

assert_same_otp_fork(UnpackDir) ->
    CurrCompatible = running_otp_compatible(),
    PkgCompatible = pkg_otp_compatible(UnpackDir),
    case CurrCompatible =:= PkgCompatible of
        true ->
            ok;
        false ->
            throw(
                make_error(
                    otp_compatible_mismatch,
                    #{
                        curr_compatible => CurrCompatible,
                        new => PkgCompatible,
                        details =>
                            <<
                                "Please make sure the running emqx is built using EMQX's official Erlang/OTP,\n"
                                "                  and also the hot-upgrade package is got from EMQX team."
                            >>
                    }
                )
            )
    end.

assert_same_os_arch(CurrBuildInfo, NewBuildInfo) ->
    case
        maps:get("os", CurrBuildInfo) =:= maps:get("os", NewBuildInfo) andalso
            emqx_relup_utils:is_arch_compatible(
                maps:get("arch", CurrBuildInfo), maps:get("arch", NewBuildInfo)
            )
    of
        true -> ok;
        false -> throw(make_error(os_arch_mismatch, #{curr => CurrBuildInfo, new => NewBuildInfo}))
    end.

running_otp_compatible() ->
    try mnesia_hook:module_info() of
        _ -> yes
    catch
        error:undef -> no
    end.

pkg_otp_compatible(UnpackDir) ->
    %% the mnesia_hook is added by emqx team
    MnesiaDir = filename:join([UnpackDir, "lib", "mnesia-*"]),
    BeamFile = filename:join([MnesiaDir, "ebin", "mnesia_hook.beam"]),
    case filelib:wildcard(MnesiaDir) of
        [] ->
            %% the relup does not include mneisa dir, maybe compatible
            yes;
        [_ | _] ->
            case filelib:wildcard(BeamFile) of
                [] -> no;
                [_ | _] -> yes
            end
    end.

%%==============================================================================
%% Deploy Libs and Release Files
%%==============================================================================
%% Carry only the runtime tree forward: bin (the wrapper scripts the
%% next start execs into), erts-* (new Erlang VM), lib (new app code),
%% releases (new boot scripts + emqx_vars). Anything else in the
%% tarball — data/, etc/, log/, plugins/ — would be misleading: after
%% `bin/emqx` re-execs into the upgraded tree, those still resolve to
%% the *original* install via emqx_vars's absolute paths.
deploy_files(TargetVsn, RootDir, UnpackDir, _OldRel, _NewRel, _Opts) ->
    TargetDir = get_deploy_dir(RootDir, TargetVsn),
    logger:notice("add independent code dir: ~s", [TargetDir]),
    ok = emqx_relup_file_utils:ensure_dir_deleted(TargetDir),
    Sources = runtime_subdirs(UnpackDir),
    ok = emqx_relup_file_utils:cp_r(Sources, TargetDir).

runtime_subdirs(UnpackDir) ->
    Fixed = ["bin", "lib", "releases"],
    Erts = [
        filename:basename(D)
     || D <- filelib:wildcard(filename:join([UnpackDir, "erts-*"]))
    ],
    [
        filename:join([UnpackDir, Name])
     || Name <- Fixed ++ Erts,
        filelib:is_dir(filename:join([UnpackDir, Name]))
    ].

unpack_release(TarFile) ->
    case filelib:is_regular(TarFile) of
        false ->
            throw(make_error(target_tarball_not_found, #{file => TarFile}));
        true ->
            FullDigest = sha256_hex(TarFile),
            ok = verify_sha256_sidecar(TarFile, FullDigest),
            TmpDir = emqx_relup_file_utils:tmp_dir(),
            HashPrefix = binary:part(FullDigest, 0, 16),
            UnpackDir = filename:join([
                TmpDir, "emqx_relup_" ++ binary_to_list(HashPrefix)
            ]),
            ok = maybe_extract_tar(TarFile, UnpackDir),
            {ok, UnpackDir}
    end.

maybe_extract_tar(TarFile, UnpackDir) ->
    case already_extracted(UnpackDir) of
        true ->
            ok;
        false ->
            ok = emqx_relup_file_utils:ensure_dir_deleted(UnpackDir),
            ok = filelib:ensure_dir(filename:join([UnpackDir, "dummy"])),
            ok = erl_tar:extract(TarFile, [{cwd, UnpackDir}, compressed])
    end.

%% The unpack dir is sha256-keyed, so its presence is sufficient
%% evidence of a complete extraction — re-extracting would be wasteful.
already_extracted(UnpackDir) ->
    filelib:is_regular(filename:join([UnpackDir, "releases", "emqx_vars"])).

%% Read `<UnpackDir>/releases/emqx_vars` and extract the target
%% version from the `REL_VSN="..."` line written by the relx overlay.
%% Authoritative: the tarball declares its own version, no operator
%% sidecar or CLI argument required.
read_rel_vsn(UnpackDir) ->
    File = filename:join([UnpackDir, "releases", "emqx_vars"]),
    case file:read_file(File) of
        {error, Reason} ->
            throw(make_error(cannot_read_emqx_vars, #{file => File, reason => Reason}));
        {ok, Bin} ->
            case
                re:run(Bin, <<"^REL_VSN=\"([^\"]+)\"">>, [multiline, {capture, all_but_first, list}])
            of
                {match, [Vsn]} -> Vsn;
                nomatch -> throw(make_error(rel_vsn_not_found, #{file => File}))
            end
    end.

%% Lowercase hex sha256 of `TarFile`'s contents.
sha256_hex(TarFile) ->
    {ok, Bin} = file:read_file(TarFile),
    binary:encode_hex(crypto:hash(sha256, Bin), lowercase).

%% Refuse to proceed unless `<TarFile>.sha256` exists and matches the
%% computed digest. The sidecar may contain just the digest, or
%% `<digest>  <filename>` (the `sha256sum` output format) — we accept
%% both and only inspect the leading 64-hex-char field.
verify_sha256_sidecar(TarFile, ComputedHex) ->
    SidecarPath = TarFile ++ ".sha256",
    case file:read_file(SidecarPath) of
        {error, enoent} ->
            throw(make_error(missing_sha256_sidecar, #{file => SidecarPath}));
        {error, Reason} ->
            throw(make_error(cannot_read_sha256_sidecar, #{file => SidecarPath, reason => Reason}));
        {ok, Bin} ->
            case parse_sha256(Bin) of
                {ok, ExpectedHex} when ExpectedHex =:= ComputedHex ->
                    ok;
                {ok, ExpectedHex} ->
                    throw(
                        make_error(sha256_mismatch, #{
                            file => TarFile,
                            sidecar => SidecarPath,
                            expected => ExpectedHex,
                            actual => ComputedHex
                        })
                    );
                error ->
                    throw(make_error(invalid_sha256_sidecar, #{file => SidecarPath}))
            end
    end.

parse_sha256(Bin) ->
    Trimmed = string:trim(Bin),
    case re:run(Trimmed, <<"^([0-9a-fA-F]{64})">>, [{capture, all_but_first, binary}]) of
        {match, [Hex]} -> {ok, string:lowercase(Hex)};
        nomatch -> error
    end.

get_relup_entry(CurrVsn, TargetVsn) ->
    Catalog = load_priv_catalog(),
    Match = lists:search(
        fun(#{target_version := T, from_version := F}) ->
            str(F) =:= str(CurrVsn) andalso str(T) =:= str(TargetVsn)
        end,
        Catalog
    ),
    case Match of
        false ->
            throw(
                make_error(no_relup_entry, #{
                    from_vsn => CurrVsn,
                    target_vsn => TargetVsn,
                    supported_paths => list_supported_paths()
                })
            );
        {value, Relup} ->
            Relup
    end.

load_priv_catalog() ->
    {Valid, _Errors} = scan_priv_catalog(),
    Valid.

%% Boot-time validator. Returns `{[ValidEntry], [ErrorMap]}` so the
%% caller can log warnings without preventing app start.
validate_priv_catalog() ->
    scan_priv_catalog().

scan_priv_catalog() ->
    Pattern = filename:join([code:priv_dir(emqx_relup), "relup", "*.relup"]),
    lists:foldl(
        fun(File, {Acc, Errs}) ->
            case load_one_relup(File) of
                {ok, Relup} -> {[Relup | Acc], Errs};
                {error, ErrMap} -> {Acc, [ErrMap | Errs]}
            end
        end,
        {[], []},
        filelib:wildcard(Pattern)
    ).

load_one_relup(File) ->
    try file:script(File) of
        {ok, #{from_version := _, target_version := _} = Relup} ->
            {ok, Relup};
        {ok, Other} ->
            {error, #{err_type => invalid_relup_file, file => File, value => Other}};
        {error, Reason} ->
            {error, #{err_type => failed_to_read_relup_file, file => File, reason => Reason}}
    catch
        Err:Reason:_ST ->
            {error, #{
                err_type => relup_file_eval_crashed,
                file => File,
                exception => {Err, Reason}
            }}
    end.

%%==============================================================================
%% Permanent Release
%%==============================================================================
permanent_upgrade(_CurrVsn, TargetVsn, RootDir, _) ->
    file:write_file(filename:join([independent_deploy_root(RootDir), "version"]), TargetVsn).

%%==============================================================================
%% Eval Relup Instructions
%%==============================================================================
eval_relup(CurrVsn, TargetVsn, Relup, LibModInfo) ->
    %% NOTE: Exceptions in eval_code_changes/2 will not be caught and the VM will be restarted!
    ok = eval_code_changes(Relup, LibModInfo, CurrVsn),
    try
        eval_post_upgrade_actions(TargetVsn, CurrVsn, Relup)
    catch
        Err:Reason:ST ->
            exception_to_error(Err, Reason, ST)
    end.

eval_code_changes(Relup, LibModInfo, CurrVsn) ->
    CodeChanges = maps:get(code_changes, Relup),
    Instrs = prepare_code_change(CodeChanges, LibModInfo, []),
    ok = write_troubleshoot_file("relup", strip_instrs(Instrs)),
    eval(Instrs, #{from_vsn => CurrVsn}).

prepare_code_change([{load_module, Mod} | CodeChanges], LibModInfo, Instrs) ->
    {Bin, FName} = load_object_code(Mod, LibModInfo),
    %% TODO: we can chose to stop if some processes are still running old code
    _ = code:soft_purge(Mod),
    prepare_code_change(CodeChanges, LibModInfo, [{load, Mod, Bin, FName} | Instrs]);
prepare_code_change([{restart_application, AppName} | CodeChanges], LibModInfo, Instrs) ->
    Mods = emqx_relup_libs:get_app_mods(AppName, LibModInfo),
    CodeChanges1 = [{load_module, Mod} || Mod <- Mods] ++ CodeChanges,
    ExpandedInstrs =
        [{stop_app, AppName}, {remove_app, AppName} | CodeChanges1] ++ [{start_app, AppName}],
    prepare_code_change(ExpandedInstrs, LibModInfo, Instrs);
prepare_code_change([{update, Mod, Change} | CodeChanges], LibModInfo, Instrs) ->
    ModProcs = get_supervised_procs(),
    Pids = pids_of_callback_mod(Mod, ModProcs),
    ExpandedInstrs =
        [
            {suspend, Pids},
            {load_module, Mod},
            {code_change, Pids, Mod, Change},
            {resume, Pids}
        ] ++ CodeChanges,
    prepare_code_change(ExpandedInstrs, LibModInfo, Instrs);
prepare_code_change([Instr | CodeChanges], LibModInfo, Instrs) ->
    prepare_code_change(CodeChanges, LibModInfo, [assert_valid_instrs(Instr) | Instrs]);
prepare_code_change([], _, Instrs) ->
    lists:reverse(Instrs).

curr_mod_md5(Mod) ->
    case code:is_loaded(Mod) of
        {file, _} -> Mod:module_info(md5);
        false -> not_loaded
    end.

pids_of_callback_mod(Mod, ModProcs) ->
    lists:filtermap(
        fun({_Sup, _Name, Pid, Mods}) ->
            case lists:member(Mod, Mods) of
                true -> {true, Pid};
                false -> false
            end
        end,
        ModProcs
    ).

load_object_code(Mod, #{mod_app_mapping := ModAppMapping}) ->
    case maps:get(Mod, ModAppMapping, undefined) of
        {_AppName, _AppVsn, File} ->
            case erl_prim_loader:get_file(File) of
                {ok, Bin, FName2} ->
                    {Bin, FName2};
                error ->
                    throw(make_error(no_such_file, #{file => File}))
            end;
        undefined ->
            throw(make_error(module_not_found, #{module => Mod}))
    end.

assert_valid_instrs({load, _, _, _} = Instr) ->
    Instr;
assert_valid_instrs({suspend, Pids} = Instr) when is_list(Pids) ->
    Instr;
assert_valid_instrs({resume, Pids} = Instr) when is_list(Pids) ->
    Instr;
assert_valid_instrs({code_change, Pids, Mod, {advanced, _Extra}} = Instr) when
    is_list(Pids), is_atom(Mod)
->
    Instr;
assert_valid_instrs({stop_app, AppName} = Instr) when is_atom(AppName) ->
    Instr;
assert_valid_instrs({remove_app, AppName} = Instr) when is_atom(AppName) ->
    Instr;
assert_valid_instrs({start_app, AppName} = Instr) when is_atom(AppName) ->
    Instr;
assert_valid_instrs(Instr) ->
    throw(make_error(invalid_instr, #{instruction => Instr})).

eval([], _Opts) ->
    add_patch_code_path();
eval([{load, Mod, Bin, FName} | Instrs], Opts) ->
    case code:module_md5(Bin) =:= curr_mod_md5(Mod) of
        true ->
            logger:notice("there's no change in module: ~p, skip loading", [Mod]),
            ok;
        false ->
            % load_binary kills all procs running old code
            {module, _} = code:load_binary(Mod, FName, Bin),
            true = code:add_patha(filename:dirname(FName)),
            logger:debug("loaded module at: ~p", [FName]),
            ok
    end,
    eval(Instrs, Opts);
eval([{suspend, Pids} | Instrs], Opts) ->
    lists:foreach(
        fun(Pid) ->
            case catch sys:suspend(Pid) of
                ok ->
                    {true, Pid};
                _ ->
                    % If the proc hangs, make sure to
                    % resume it when it gets suspended!
                    catch sys:resume(Pid)
            end
        end,
        Pids
    ),
    eval(Instrs, Opts);
eval([{resume, Pids} | Instrs], Opts) ->
    lists:foreach(
        fun(Pid) ->
            catch sys:resume(Pid)
        end,
        Pids
    ),
    eval(Instrs, Opts);
eval([{code_change, Pids, Mod, {advanced, Extra}} | Instrs], #{from_vsn := FromVsn} = Opts) ->
    lists:foreach(
        fun(Pid) ->
            change_code(Pid, Mod, FromVsn, Extra)
        end,
        Pids
    ),
    eval(Instrs, Opts);
eval([{stop_app, AppName} | Instrs], Opts) ->
    case is_excluded_app(AppName) orelse application:stop(AppName) of
        true ->
            ok;
        ok ->
            ok;
        {error, {not_started, _}} ->
            ok;
        {error, Reason} ->
            throw(make_error(failed_to_stop_app, #{app => AppName, reason => Reason}))
    end,
    eval(Instrs, Opts);
eval([{remove_app, AppName} | Instrs], Opts) ->
    case is_excluded_app(AppName) orelse application:get_key(AppName, modules) of
        true ->
            ok;
        undefined ->
            ok;
        {ok, Mods} ->
            lists:foreach(
                fun(M) ->
                    _ = code:purge(M),
                    true = code:delete(M)
                end,
                Mods
            )
    end,
    eval(Instrs, Opts);
eval([{start_app, AppName} | Instrs], Opts) ->
    case is_excluded_app(AppName) of
        true -> ok;
        false -> {ok, _} = application:ensure_all_started(AppName)
    end,
    eval(Instrs, Opts).

change_code(Pid, Mod, FromVsn, Extra) ->
    case sys:change_code(Pid, Mod, FromVsn, Extra) of
        ok ->
            ok;
        {error, Reason} ->
            throw(
                make_error(code_change_failed, #{
                    pid => Pid,
                    mod => Mod,
                    from_vsn => FromVsn,
                    extra => Extra,
                    reason => Reason
                })
            )
    end.

% add_code_paths(RootDir, TargetVsn, Opts) ->
%     LibDirs = filename:join([get_deploy_dir(RootDir, TargetVsn, Opts), "lib", "*", "ebin"]),
%     ok = code:add_pathsa(filelib:wildcard(LibDirs)).

add_patch_code_path() ->
    true = code:add_patha(filename:join([emqx:data_dir(), "patches"])),
    ok.

get_supervised_procs() ->
    lists:foldl(
        fun({AppName, _Desc, _Vsn}, Procs) ->
            MasterPid = application_controller:get_master(AppName),
            get_master_procs(AppName, Procs, MasterPid)
        end,
        [],
        application:which_applications()
    ).

get_supervised_procs(AppName, SupPid, Procs, undefined) ->
    get_procs(which_children(SupPid, AppName, SupPid), SupPid) ++ Procs;
get_supervised_procs(_, SupPid, Procs, SupMod) ->
    get_procs(which_children(SupPid, SupMod, SupPid), SupPid) ++
        [{undefined, undefined, SupPid, [SupMod]} | Procs].

get_master_procs(AppName, Procs, MasterPid) when is_pid(MasterPid) ->
    {SupPid, _AppMod} = application_master:get_child(MasterPid),
    get_supervised_procs(AppName, SupPid, Procs, get_supervisor_module(AppName, SupPid));
get_master_procs(_, Procs, _) ->
    Procs.

get_supervisor_module(AppName, SupPid) ->
    try
        supervisor:get_callback_module(SupPid)
    catch
        Err:Reason:ST ->
            logger:error(
                "get_callback_module failed, app: ~p, supervisor ~p, error: ~0p~n",
                [AppName, SupPid, {Err, Reason, ST}]
            ),
            undefined
    end.

get_procs([{Name, Pid, worker, dynamic} | T], Sup) when is_pid(Pid) ->
    Mods = maybe_get_dynamic_mods(Name, Pid),
    [{Sup, Name, Pid, Mods} | get_procs(T, Sup)];
get_procs([{Name, Pid, worker, Mods} | T], Sup) when is_pid(Pid), is_list(Mods) ->
    [{Sup, Name, Pid, Mods} | get_procs(T, Sup)];
get_procs([{Name, Pid, supervisor, Mods} | T], Sup) when is_pid(Pid) ->
    [{Sup, Name, Pid, Mods} | get_procs(T, Sup)] ++
        get_procs(which_children(Pid, Name, Pid), Pid);
get_procs([_H | T], Sup) ->
    get_procs(T, Sup);
get_procs(_, _Sup) ->
    [].

-define(NOT_TIMEOUT_OR_NODE_DOWN(REASON),
    REASON =/= timeout andalso
        not (is_tuple(REASON) andalso element(1, REASON) =:= nodedown)
).

which_children(Proc, Name, Pid) ->
    case get_proc_state(Proc) of
        noproc ->
            logger:warning("a process (~p) exited during supervision tree interrogation. ", [Proc]),
            [];
        suspended ->
            throw(
                make_error(suspended_supervisor, #{
                    reason => <<"which_children failed, supervisor suspended">>,
                    name => Name,
                    pid => Pid
                })
            );
        running ->
            call_which_children(Name, Pid)
    end.

-dialyzer([{nowarn_function, [call_which_children/2]}]).
call_which_children(Name, Pid) ->
    try supervisor:which_children(Pid) of
        Res when is_list(Res) -> Res;
        _ ->
            %% emqtt_quic_stream returns {error, unimpl, which_children}.
            []
    catch
        exit:Reason when ?NOT_TIMEOUT_OR_NODE_DOWN(Reason) ->
            [];
        exit:Other:ST ->
            throw(
                make_error(which_children_failed, #{
                    reason => <<"which_children failed">>,
                    name => Name,
                    pid => Pid,
                    error => {Other, ST}
                })
            )
    end.

get_proc_state(Proc) ->
    try sys:get_status(Proc) of
        {status, _, {module, _}, [_, State, _, _, _]} when
            State == running;
            State == suspended
        ->
            State
    catch
        exit:{Reason, {sys, get_status, [Proc]}} when ?NOT_TIMEOUT_OR_NODE_DOWN(Reason) ->
            noproc
    end.

maybe_get_dynamic_mods(Name, Pid) ->
    try gen:call(Pid, self(), get_modules) of
        {ok, Mods} when is_list(Mods) -> Mods;
        {ok, Res} -> logger:warning("got invalid dynamic_mods: ~p", Res)
    catch
        exit:Reason when ?NOT_TIMEOUT_OR_NODE_DOWN(Reason) ->
            [];
        exit:Other:ST ->
            throw(
                make_error(get_modules_failed, #{
                    reason => <<"maybe invalid childspec">>,
                    name => Name,
                    pid => Pid,
                    error => {Other, ST}
                })
            )
    end.

%%==============================================================================
%% Eval Post Upgrade Actions
%%==============================================================================
eval_post_upgrade_actions(TargetVsn, CurrVsn, Relup) ->
    case get_upgrade_mod(TargetVsn) of
        {ok, Mod} ->
            lists:foreach(
                fun
                    ({Func, Args}) ->
                        erlang:apply(Mod, Func, [CurrVsn] ++ Args);
                    (Func) ->
                        erlang:apply(Mod, Func, [CurrVsn])
                end,
                maps:get(post_upgrade_callbacks, Relup, [])
            );
        {error, Reason} ->
            {error, Reason}
    end.

get_upgrade_mod(TargetVsn) ->
    %% If we find a issue in the emqx_post_upgrade, we can quickly fix it by adding
    %% a `emqx_post_upgrade_<TargeVsn>.erl` module in this plugin.
    TaggedMod = list_to_atom(concat(["emqx_post_upgrade_", TargetVsn])),
    Mod = emqx_post_upgrade,
    case {code:is_loaded(TaggedMod), code:is_loaded(Mod)} of
        {{file, _}, _} ->
            {ok, TaggedMod};
        {false, {file, _}} ->
            {ok, Mod};
        {false, false} ->
            {error, make_error(post_upgrade_module_not_loaded, #{mod => Mod})}
    end.

%%==============================================================================
%% Internal functions
%%==============================================================================
independent_deploy_root(RootDir) ->
    filename:join([RootDir, "relup"]).

get_deploy_dir(RootDir, TargetVsn) ->
    filename:join([independent_deploy_root(RootDir), TargetVsn]).

read_build_info(RootDir, Vsn) ->
    BuildInfoFile = filename:join([RootDir, "releases", Vsn, "BUILD_INFO"]),
    Lines = readlines(BuildInfoFile),
    lists:foldl(
        fun(Line, Map) ->
            case string:split(Line, ":") of
                [Key, Value] ->
                    Map#{str(trim(Key)) => trim(Value)};
                _ ->
                    Map
            end
        end,
        #{},
        Lines
    ).

trim(Str) ->
    string:trim(Str, both, " \"").

readlines(FileName) ->
    {ok, Device} = file:open(FileName, [read]),
    try
        get_all_lines(Device)
    after
        file:close(Device)
    end.

get_all_lines(Device) ->
    case file:read_line(Device) of
        eof -> [];
        {ok, Line} -> [Line | get_all_lines(Device)];
        {error, Reason} -> throw(make_error(failed_to_read_file, #{reason => Reason}))
    end.

consult_rel_file(RootDir, TargetVsn) ->
    RelFile = filename:join([RootDir, "releases", TargetVsn, "emqx.rel"]),
    case file:consult(RelFile) of
        {ok, [Release]} ->
            {ok, Release};
        {error, Reason} ->
            throw(make_error(failed_to_read_rel_file, #{file => RelFile, reason => Reason}))
    end.

write_troubleshoot_file(Name, Term) ->
    FName = emqx_relup_utils:ts_filename(Name),
    file:write_file(FName, io_lib:format("~p", [Term])).

%% sticky directories that are not allowed to be upgraded
is_excluded_app(kernel) -> true;
is_excluded_app(stdlib) -> true;
is_excluded_app(compiler) -> true;
is_excluded_app(_) -> false.

strip_instrs([{load, Mod, _Bin, FName} | Instrs]) ->
    %% Bin makes no sense and is too large to be printed
    [{load, Mod, "...", FName} | strip_instrs(Instrs)];
strip_instrs([Instr | Instrs]) ->
    [Instr | strip_instrs(Instrs)];
strip_instrs([]) ->
    [].
