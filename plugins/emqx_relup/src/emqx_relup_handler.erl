-module(emqx_relup_handler).

-export([
    get_package_info/1,
    get_target_vsn/0,
    check_and_unpack/4,
    perform_upgrade/4,
    permanent_upgrade/4
]).

-import(lists, [concat/1]).
-import(emqx_relup_utils, [str/1, bin/1, exception_to_error/3, make_error/2]).

%%==============================================================================
%% API
%%==============================================================================
get_target_vsn() ->
    PrivDir = code:priv_dir(emqx_relup),
    case filelib:wildcard(filename:join([PrivDir, "*.tar.gz"])) of
        [] -> throw(make_error(no_relup_tar_file_found, #{dir => PrivDir}));
        [TarFile] -> str(filename:basename(TarFile, ".tar.gz"));
        TarFiles -> throw(make_error(multiple_relup_tar_files_found, #{files => TarFiles}))
    end.

get_package_info(TargetVsn) ->
    try
        {ok, UnpackDir} = unpack_release(TargetVsn),
        RelupL = load_relup_files(TargetVsn, UnpackDir),
        ChangeLogs = read_change_log_files(TargetVsn, UnpackDir),
        {ok, #{
            unpack_dir => UnpackDir,
            full_relup => RelupL,
            base_vsns => get_base_vsns(RelupL),
            change_logs => ChangeLogs
        }}
    catch
        throw:Reason ->
            {error, Reason};
        Err:Reason:ST ->
            exception_to_error(Err, Reason, ST)
    end.

check_and_unpack(CurrVsn, TargetVsn, RootDir, Opts) ->
    try
        ok = assert_not_same_vsn(CurrVsn, TargetVsn),
        {ok, UnpackDir} = unpack_release(TargetVsn),
        ok = check_write_permission(RootDir),
        ok = check_otp_comaptibility(CurrVsn, RootDir, UnpackDir, TargetVsn),
        {ok, OldRel} = consult_rel_file(RootDir, CurrVsn),
        {ok, NewRel} = consult_rel_file(UnpackDir, TargetVsn),
        ok = deploy_files(TargetVsn, RootDir, UnpackDir, OldRel, NewRel, Opts),
        Relup = get_relup_entry(CurrVsn, TargetVsn, get_deploy_dir(RootDir, TargetVsn, Opts)),
        {ok, Opts#{unpack_dir => UnpackDir, old_rel => OldRel, new_rel => NewRel, relup => Relup}}
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
        Dir = get_deploy_dir(RootDir, TargetVsn, Opts),
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
get_base_vsns(Relup) ->
    lists:map(fun(#{from_version := Vsn}) -> bin(Vsn) end, Relup).

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
    File = filename:join([RootDir, SubDir, "relup_test_perm"]),
    case filelib:ensure_dir(File) of
        ok ->
            case file:write_file(File, "t") of
                {error, eacces} ->
                    throw(
                        make_error(
                            no_write_permission,
                            #{
                                dir => SubDir,
                                msg =>
                                    "Please set emqx as the owner of the dir by running:"
                                    " 'sudo chown -R emqx:emqx " ++ SubDir ++ "'"
                            }
                        )
                    );
                {error, Reason} ->
                    throw(make_error(cannot_write_file, #{dir => SubDir, reason => Reason}));
                ok ->
                    ok = file:delete(File)
            end;
        {error, Reason} ->
            throw(make_error(cannot_create_dir, #{dir => SubDir, reason => Reason}))
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
deploy_files(TargetVsn, RootDir, UnpackDir, OldRel, NewRel, #{deploy_inplace := true}) ->
    ok = copy_libs(TargetVsn, RootDir, UnpackDir, OldRel, NewRel),
    ok = copy_release(TargetVsn, RootDir, UnpackDir),
    {OldRel, NewRel};
deploy_files(TargetVsn, RootDir, UnpackDir, _OldRel, _NewRel, _Opts) ->
    DstDir = independent_deploy_root(RootDir),
    logger:notice("add independent code dir: ~s", [DstDir]),
    ok = emqx_relup_file_utils:ensure_dir_deleted(filename:basename([DstDir, TargetVsn])),
    ok = emqx_relup_file_utils:cp_r([UnpackDir], DstDir),
    DirName = filename:basename(UnpackDir),
    file:rename(
        filename:join([DstDir, DirName]),
        filename:join([DstDir, TargetVsn])
    ).

unpack_release(TargetVsn) ->
    TarFile = filename:join([code:priv_dir(emqx_relup), concat([TargetVsn, ".tar.gz"])]),
    case filelib:is_regular(TarFile) of
        false ->
            throw(make_error(relup_tar_file_not_found, #{file => TarFile}));
        true ->
            TmpDir = emqx_relup_file_utils:tmp_dir(),
            UnpackDir = filename:join([TmpDir, TargetVsn ++ "_hash_" ++ str(calc_hash(TarFile))]),
            ok = maybe_extract_tar(TarFile, UnpackDir, TargetVsn),
            {ok, UnpackDir}
    end.

maybe_extract_tar(TarFile, UnpackDir, TargetVsn) ->
    case already_extracted(UnpackDir, TargetVsn) of
        true ->
            ok;
        false ->
            ok = emqx_relup_file_utils:ensure_dir_deleted(UnpackDir),
            ok = filelib:ensure_dir(filename:join([UnpackDir, "dummy"])),
            ok = erl_tar:extract(TarFile, [{cwd, UnpackDir}, compressed])
    end.

already_extracted(UnpackDir, TargetVsn) ->
    case filelib:is_dir(UnpackDir) of
        false ->
            false;
        true ->
            case filelib:wildcard(filename:join([UnpackDir, "releases", TargetVsn, "*"])) of
                [] -> false;
                _ -> true
            end
    end.

calc_hash(TarFile) ->
    {ok, Bin} = file:read_file(TarFile),
    <<HashPrefix:16/binary, _/binary>> = binary:encode_hex(crypto:hash(sha256, Bin)),
    HashPrefix.

copy_libs(_TargetVsn, RootDir, UnpackDir, OldRel, NewRel) ->
    OldLibs = emqx_relup_libs:rel_libs(OldRel),
    NewLibs = emqx_relup_libs:rel_libs(NewRel),
    do_copy_libs(NewLibs, OldLibs, RootDir, UnpackDir).

do_copy_libs([NLib | Libs], OldLibs, RootDir, UnpackDir) ->
    AppName = emqx_relup_libs:lib_app_name(NLib),
    case lists:keyfind(AppName, 1, OldLibs) of
        %% this lib is newly added, copy it
        false ->
            ok = copy_lib(NLib, RootDir, UnpackDir);
        %% this lib is already in the old release, copy it only if the version is changed
        OLib ->
            case emqx_relup_libs:lib_app_vsn(OLib) =:= emqx_relup_libs:lib_app_vsn(NLib) of
                true -> ok;
                false -> ok = copy_lib(NLib, RootDir, UnpackDir)
            end
    end,
    do_copy_libs(Libs, OldLibs, RootDir, UnpackDir);
do_copy_libs([], _, _, _) ->
    ok.

copy_lib(NLib, RootDir, UnpackDir) ->
    LibDirName = concat([emqx_relup_libs:lib_app_name(NLib), "-", emqx_relup_libs:lib_app_vsn(NLib)]),
    DstDir = filename:join([RootDir, "lib", LibDirName]),
    SrcDir = filename:join([UnpackDir, "lib", LibDirName]),
    logger:notice("add lib dir: ~s", [DstDir]),
    emqx_relup_file_utils:cp_r([SrcDir], DstDir).

get_relup_entry(CurrVsn, TargetVsn, Dir) ->
    RelupL = load_relup_files(TargetVsn, Dir),
    case
        lists:search(
            fun(#{target_version := TargetVsn0, from_version := FromVsn}) ->
                FromVsn =:= CurrVsn andalso TargetVsn0 =:= TargetVsn
            end,
            RelupL
        )
    of
        false ->
            throw(
                make_error(no_relup_entry, #{
                    relup_dir => Dir, from_vsn => CurrVsn, target_vsn => TargetVsn
                })
            );
        {value, Relup} ->
            Relup
    end.

load_relup_files(TargetVsn, Dir) ->
    RelupFile = filename:join([Dir, "releases", TargetVsn, concat([TargetVsn, ".relup"])]),
    case file:script(RelupFile) of
        {ok, RelupL} ->
            RelupL;
        {error, Reason} ->
            throw(make_error(failed_to_read_relup_file, #{file => RelupFile, reason => Reason}))
    end.

read_change_log_files(TargetVsn, Dir) ->
    FileNamesWc = filename:join([Dir, "releases", TargetVsn, "change_log*.md"]),
    ChangeLogFiles = filelib:wildcard(FileNamesWc),
    [read_change_log_file(F) || F <- ChangeLogFiles].

read_change_log_file(FileName) ->
    case file:read_file(FileName) of
        {ok, Bin} ->
            Bin;
        {error, Reason} ->
            throw(make_error(failed_to_read_change_log_file, #{file => FileName, reason => Reason}))
    end.

copy_release(TargetVsn, RootDir, UnpackDir) ->
    SrcDir = filename:join([UnpackDir, "releases", TargetVsn]),
    DstDir = filename:join([RootDir, "releases", TargetVsn]),
    emqx_relup_file_utils:cp_r([SrcDir], DstDir).

%%==============================================================================
%% Permanent Release
%%==============================================================================
permanent_upgrade(_CurrVsn, TargetVsn, RootDir, #{deploy_inplace := true, unpack_dir := UnpackDir}) ->
    overwrite_files(TargetVsn, RootDir, UnpackDir);
permanent_upgrade(_CurrVsn, TargetVsn, RootDir, _) ->
    file:write_file(filename:join([independent_deploy_root(RootDir), "version"]), TargetVsn).

overwrite_files(_TargetVsn, RootDir, UnpackDir) ->
    %% The RELEASES file is not required by OTP to start a release but it is
    %% used by bin/nodetool. We also won't write release info to it as we don't
    %% use release_handler anymore.
    TmpDir0 = emqx_relup_file_utils:tmp_dir(),
    TmpDir = filename:join([TmpDir0, emqx_relup_utils:ts_filename("_relup_bk")]),
    ReleaseFiles0 = ["emqx_vars", "start_erl.data", "RELEASES"],
    ReleaseFiles = [{"releases", File} || File <- ReleaseFiles0],
    Bins0 = [
        "emqx",
        "emqx_ctl",
        "node_dump",
        "emqx.cmd",
        "emqx_ctl.cmd",
        "nodetool",
        "emqx_cluster_rescue"
    ],
    Bins = [{"bin", File} || File <- Bins0],
    case filelib:ensure_dir(filename:join([TmpDir, "dummy"])) of
        ok ->
            try
                ok = do_overwrite_files(ReleaseFiles ++ Bins, RootDir, UnpackDir, TmpDir),
                %% We already have emqx.rel files in "releases/<vsn>/emqx.rel",
                %% so we don't need the one in "releases/".
                ExraRel = filename:join([RootDir, "releases", "emqx.rel"]),
                emqx_relup_file_utils:ensure_file_deleted(ExraRel)
            catch
                throw:#{error := copy_failed, history := History} = Details ->
                    ok = recover_overwritten_files(History),
                    {error, make_error(copy_failed, #{details => maps:remove(history, Details)})}
            end;
        {error, _} = Err ->
            Err
    end.

do_overwrite_files(Files, RootDir, UnpackDir, TmpDir) ->
    lists:foldl(
        fun({SubDir, File}, Copied) ->
            NewFile = filename:join([UnpackDir, SubDir, File]),
            OldFile = filename:join([RootDir, SubDir, File]),
            TmpFile = filename:join([TmpDir, File]),
            copy_file(OldFile, TmpFile, Copied),
            copy_file(NewFile, OldFile, Copied),
            [{TmpFile, OldFile} | Copied]
        end,
        [],
        Files
    ),
    ok.

recover_overwritten_files(History) ->
    lists:foreach(
        fun({TmpFile, OldFile}) ->
            {ok, _} = file:copy(TmpFile, OldFile)
        end,
        History
    ).

copy_file(SrcFile, DstFile, Copied) ->
    case file:copy(SrcFile, DstFile) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            throw(
                make_error(copy_failed, #{
                    reason => Reason, src => SrcFile, dst => DstFile, history => Copied
                })
            )
    end.

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

get_deploy_dir(RootDir, _TargetVsn, #{deploy_inplace := true}) ->
    RootDir;
get_deploy_dir(RootDir, TargetVsn, _) ->
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
