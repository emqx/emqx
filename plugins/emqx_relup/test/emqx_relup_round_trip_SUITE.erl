%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Round-trip CT for the upgrade pipeline: check_and_unpack →
%% perform_upgrade → permanent_upgrade against a forged target tarball
%% with a no-op .relup catalog entry. Stages all artefacts under a
%% per-test temp RootDir; the running EMQX node's code:root_dir() is
%% never touched.
%%
%% emqx_relup is loaded (so code:priv_dir/1 resolves) but not started,
%% to avoid emqx_relup_app:start/2 writing to the real <code:root_dir()>
%% on the CT host.
%%--------------------------------------------------------------------
-module(emqx_relup_round_trip_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CURR_VSN, "9.9.0-fake-curr").
-define(TARGET_VSN, "9.9.1-fake-target").

%%==============================================================================
%% suite setup
%%==============================================================================
all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_conf, emqx],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = ensure_loaded(emqx_relup),
    %% emqx_post_upgrade is loaded lazily; the handler's
    %% get_upgrade_mod/1 calls code:is_loaded/1 which only returns
    %% {file, _} once the module has been referenced. Force-load it
    %% here so the round-trip's no-op post-upgrade phase resolves.
    {module, emqx_post_upgrade} = code:ensure_loaded(emqx_post_upgrade),
    %% the handler's add_patch_code_path/0 calls
    %% code:add_patha(emqx:data_dir() ++ "/patches"); ensure that dir
    %% exists or add_patha returns {error, bad_directory}.
    PatchesDir = filename:join(emqx:data_dir(), "patches"),
    ok = filelib:ensure_path(PatchesDir),
    [{apps, Apps}, {patches_dir, PatchesDir} | Config].

end_per_suite(Config) ->
    cleanup_test_catalog_entries(),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    RootDir = filename:join(?config(priv_dir, Config), atom_to_list(Case)),
    ok = filelib:ensure_path(RootDir),
    ok = forge_release_files(RootDir, ?CURR_VSN, default_arch()),
    [{root_dir, RootDir} | Config].

end_per_testcase(_Case, _Config) ->
    cleanup_test_catalog_entries(),
    ok.

%%==============================================================================
%% happy path
%%==============================================================================
-doc "Forge a fake target tarball + sha256 sidecar + a no-op .relup "
"catalog entry; drive the three-stage handler API and assert the "
"target version is read out of `releases/emqx_vars`, the version "
"marker file is written, and the deploy dir is populated.".
t_round_trip_no_op(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    ok = write_sha256_sidecar(Tarball),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),

    {ok, Opts1} = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch(
        #{
            target_vsn := ?TARGET_VSN,
            unpack_dir := _,
            tarball := Tarball,
            old_rel := {release, _, _, _},
            new_rel := {release, _, _, _},
            relup := #{from_version := ?CURR_VSN, target_version := ?TARGET_VSN}
        },
        Opts1
    ),

    ok = emqx_relup_handler:perform_upgrade(?CURR_VSN, ?TARGET_VSN, RootDir, Opts1),
    ok = emqx_relup_handler:permanent_upgrade(?CURR_VSN, ?TARGET_VSN, RootDir, Opts1),

    Marker = filename:join([RootDir, "relup", "current"]),
    ?assertEqual({ok, list_to_binary(?TARGET_VSN)}, file:read_file(Marker)),
    DeployDir = filename:join([RootDir, "relup", ?TARGET_VSN]),
    ?assert(filelib:is_dir(DeployDir), DeployDir ++ " should be a dir"),
    ?assert(filelib:is_regular(filename:join([DeployDir, "releases", ?TARGET_VSN, "emqx.rel"]))).

%%==============================================================================
%% negative cases
%%==============================================================================
-doc "No `.sha256` sidecar → `missing_sha256_sidecar`.".
t_missing_sha256_rejects(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    %% no .sha256 file
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    Result = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch({error, #{err_type := missing_sha256_sidecar}}, Result).

-doc "`.sha256` digest doesn't match the tarball → `sha256_mismatch`.".
t_mismatched_sha256_rejects(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    BadDigest = <<"deadbeef00000000000000000000000000000000000000000000000000000000\n">>,
    ok = file:write_file(Tarball ++ ".sha256", BadDigest),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    Result = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch({error, #{err_type := sha256_mismatch}}, Result).

-doc "`sha256sum`-format sidecar (digest + space + filename) is accepted "
"verbatim — the handler only inspects the leading 64 hex chars.".
t_sha256_sidecar_sha256sum_format(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    {ok, Bin} = file:read_file(Tarball),
    Digest = binary:encode_hex(crypto:hash(sha256, Bin), lowercase),
    Sha256SumLine =
        <<Digest/binary, "  ", (list_to_binary(filename:basename(Tarball)))/binary, "\n">>,
    ok = file:write_file(Tarball ++ ".sha256", Sha256SumLine),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    {ok, _Opts1} = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ).

-doc "Tarball without `releases/emqx_vars` → `cannot_read_emqx_vars`. "
"This is the tarball's authoritative declaration of its target "
"version, so without it the upgrade can't proceed.".
t_missing_emqx_vars_rejects(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball_without_emqx_vars(RootDir, ?TARGET_VSN, default_arch()),
    ok = write_sha256_sidecar(Tarball),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    Result = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch({error, #{err_type := cannot_read_emqx_vars}}, Result).

-doc "No catalog entry for {From, Target} → `no_relup_entry`.".
t_no_matching_catalog_entry(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    ok = write_sha256_sidecar(Tarball),
    %% no .relup file — list_supported_paths is empty
    Result = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch({error, #{err_type := no_relup_entry}}, Result).

-doc "Tarball contains non-runtime dirs (data/, etc/, log/, plugins/) — "
"deploy_files must copy only bin/, erts-*/, lib/, releases/. The "
"re-exec'd `bin/emqx` still resolves data/etc/log via emqx_vars's "
"absolute paths to the original install; carrying copies into the "
"relup tree only confuses operators.".
t_deploy_only_runtime_dirs(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball_with_noise(RootDir, ?TARGET_VSN, default_arch()),
    ok = write_sha256_sidecar(Tarball),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    {ok, Opts1} = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ok = emqx_relup_handler:perform_upgrade(?CURR_VSN, ?TARGET_VSN, RootDir, Opts1),
    ok = emqx_relup_handler:permanent_upgrade(?CURR_VSN, ?TARGET_VSN, RootDir, Opts1),
    DeployDir = filename:join([RootDir, "relup", ?TARGET_VSN]),
    ?assert(filelib:is_dir(filename:join(DeployDir, "releases"))),
    ?assert(filelib:is_dir(filename:join(DeployDir, "lib"))),
    [
        ?assertNot(
            filelib:is_dir(filename:join(DeployDir, Sub)),
            "non-runtime dir " ++ Sub ++ " must not be deployed"
        )
     || Sub <- ["data", "etc", "log", "plugins"]
    ].

-doc "Tarball BUILD_INFO arch differs from running side → `os_arch_mismatch`.".
t_os_arch_mismatch_rejects(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, "different-arch"),
    ok = write_sha256_sidecar(Tarball),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    Result = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch({error, #{err_type := os_arch_mismatch}}, Result).

-doc "An existing `<RootDir>/relup/current` marker pointing at the "
"same target means the operator is re-running the exact same "
"upgrade against a VM that's already been migrated by it. Refuse "
"with `duplicate_pending_target`. Chaining to a different target "
"is allowed and exercised by `t_chain_relup_from_pending_marker`.".
t_refuse_duplicate_pending_target(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    ok = write_sha256_sidecar(Tarball),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    Marker = filename:join([RootDir, "relup", "current"]),
    ok = filelib:ensure_dir(Marker),
    ok = file:write_file(Marker, ?TARGET_VSN),
    Result = emqx_relup_handler:check_and_unpack(
        ?CURR_VSN, RootDir, #{tarball => Tarball}
    ),
    ?assertMatch(
        {error, #{
            err_type := duplicate_pending_target,
            pending_target := <<?TARGET_VSN>>
        }},
        Result
    ),
    %% Once the marker is gone, the same inputs proceed normally.
    ok = file:delete(Marker),
    ?assertMatch(
        {ok, #{target_vsn := ?TARGET_VSN}},
        emqx_relup_handler:check_and_unpack(?CURR_VSN, RootDir, #{tarball => Tarball})
    ).

-doc "A marker pointing at a *different* version than the requested "
"target means the operator wants to chain another hop on top of "
"the pending one. The framework allows it; the .relup hop author "
"is responsible for `from_version` and `code_changes` that match "
"whatever base state the running node is in.".
t_chain_relup_from_pending_marker(Config) ->
    RootDir = ?config(root_dir, Config),
    Tarball = forge_target_tarball(RootDir, ?TARGET_VSN, default_arch()),
    ok = write_sha256_sidecar(Tarball),
    _ = write_no_op_relup(?CURR_VSN, ?TARGET_VSN),
    %% A pending marker for some unrelated earlier hop must not block
    %% the new target.
    Marker = filename:join([RootDir, "relup", "current"]),
    ok = filelib:ensure_dir(Marker),
    ok = file:write_file(Marker, "9.9.0-some-earlier-hop"),
    ?assertMatch(
        {ok, #{target_vsn := ?TARGET_VSN}},
        emqx_relup_handler:check_and_unpack(?CURR_VSN, RootDir, #{tarball => Tarball})
    ).

%%==============================================================================
%% helpers
%%==============================================================================
ensure_loaded(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, App}} -> ok;
        Other -> Other
    end.

default_arch() ->
    "x86_64-test".

%% Forge `<RootDir>/releases/<Vsn>/{emqx.rel, BUILD_INFO}` for the
%% running side so consult_rel_file/2 and read_build_info/2 succeed.
forge_release_files(RootDir, Vsn, Arch) ->
    Dir = filename:join([RootDir, "releases", Vsn]),
    ok = filelib:ensure_path(Dir),
    ok = file:write_file(filename:join(Dir, "emqx.rel"), rel_file_content(Vsn)),
    ok = file:write_file(filename:join(Dir, "BUILD_INFO"), build_info_content(Arch)).

rel_file_content(Vsn) ->
    %% empty Libs list keeps make_libs_info a no-op for the round-trip.
    iolist_to_binary(
        io_lib:format("{release, {\"emqx\", \"~s\"}, {erts, \"~s\"}, []}.~n", [
            Vsn, erlang:system_info(version)
        ])
    ).

build_info_content(Arch) ->
    %% `erlang` on both sides must share the same major; any same value
    %% works. `os` and `arch` must match between curr and new sides.
    OtpVsn = erlang:system_info(otp_release) ++ ".0",
    iolist_to_binary(
        io_lib:format("erlang: ~s~nos: testos~narch: ~s~n", [OtpVsn, Arch])
    ).

emqx_vars_content(Vsn) ->
    iolist_to_binary(io_lib:format("REL_VSN=\"~s\"~nERTS_VSN=\"15.0\"~n", [Vsn])).

%% Build a minimal target tarball anywhere readable (per-test priv
%% dir). Members: releases/<Vsn>/{emqx.rel, BUILD_INFO},
%% releases/emqx_vars (carries REL_VSN), and
%% lib/mnesia-9.9/ebin/mnesia_hook.beam (presence-only sentinel for
%% assert_same_otp_fork/1).
forge_target_tarball(RootDir, TargetVsn, Arch) ->
    do_forge_target_tarball(RootDir, TargetVsn, Arch, #{
        include_emqx_vars => true, with_noise => false
    }).

%% Variant used by `t_missing_emqx_vars_rejects` — omits the
%% releases/emqx_vars member so check_and_unpack can't learn the
%% target version.
forge_target_tarball_without_emqx_vars(RootDir, TargetVsn, Arch) ->
    do_forge_target_tarball(RootDir, TargetVsn, Arch, #{
        include_emqx_vars => false, with_noise => false
    }).

%% Variant used by `t_deploy_only_runtime_dirs` — adds dummy files
%% under non-runtime dirs (data/, etc/, log/, plugins/) so the
%% deploy-filter assertion has something concrete to check against.
forge_target_tarball_with_noise(RootDir, TargetVsn, Arch) ->
    do_forge_target_tarball(RootDir, TargetVsn, Arch, #{
        include_emqx_vars => true, with_noise => true
    }).

do_forge_target_tarball(RootDir, TargetVsn, Arch, Flags) when is_map(Flags) ->
    StageDir = filename:join([RootDir, "_stage", TargetVsn]),
    RelDir = filename:join([StageDir, "releases", TargetVsn]),
    MneDir = filename:join([StageDir, "lib", "mnesia-9.9", "ebin"]),
    ok = filelib:ensure_path(RelDir),
    ok = filelib:ensure_path(MneDir),
    RelFile = filename:join(RelDir, "emqx.rel"),
    BuildFile = filename:join(RelDir, "BUILD_INFO"),
    BeamFile = filename:join(MneDir, "mnesia_hook.beam"),
    ok = file:write_file(RelFile, rel_file_content(TargetVsn)),
    ok = file:write_file(BuildFile, build_info_content(Arch)),
    ok = file:write_file(BeamFile, <<>>),
    BaseMembers = [RelFile, BuildFile, BeamFile],
    Members1 =
        case maps:get(include_emqx_vars, Flags) of
            true ->
                EmqxVarsFile = filename:join([StageDir, "releases", "emqx_vars"]),
                ok = file:write_file(EmqxVarsFile, emqx_vars_content(TargetVsn)),
                [EmqxVarsFile | BaseMembers];
            false ->
                BaseMembers
        end,
    Members2 =
        case maps:get(with_noise, Flags) of
            true -> Members1 ++ forge_noise_members(StageDir);
            false -> Members1
        end,
    %% Tarball lives wherever the operator put it — placed next to
    %% the per-case RootDir, never under <RootDir>/relup/uploads/.
    TarPath = filename:join(tarball_dir(RootDir), "emqx-test-" ++ TargetVsn ++ "-tag.tar.gz"),
    Members = [{archive_name(F, StageDir), F} || F <- Members2],
    ok = erl_tar:create(TarPath, Members, [compressed]),
    TarPath.

%% Dummy files under non-runtime subdirs (data/, etc/, log/,
%% plugins/) so the deploy-filter assertion has something concrete
%% to check against in t_deploy_only_runtime_dirs.
forge_noise_members(StageDir) ->
    Noise = ["data/dummy", "etc/dummy", "log/dummy", "plugins/dummy"],
    [
        begin
            Path = filename:join([StageDir, Rel]),
            ok = filelib:ensure_dir(Path),
            ok = file:write_file(Path, <<>>),
            Path
        end
     || Rel <- Noise
    ].

tarball_dir(RootDir) ->
    %% Models "operator scp'd to some path readable by emqx" — the
    %% handler doesn't care where, only that the file is readable
    %% and its `.sha256` sidecar sits next to it. Per-case dir so
    %% sidecars from one case don't bleed into the next (CT shares
    %% the suite-level priv_dir across all cases).
    Dir = filename:join([RootDir, "staging"]),
    ok = filelib:ensure_path(Dir),
    Dir.

archive_name(F, StageDir) ->
    Prefix = StageDir ++ "/",
    case lists:prefix(Prefix, F) of
        true -> lists:nthtail(length(Prefix), F);
        false -> filename:basename(F)
    end.

write_sha256_sidecar(Tarball) ->
    {ok, Bin} = file:read_file(Tarball),
    Digest = binary:encode_hex(crypto:hash(sha256, Bin), lowercase),
    file:write_file(Tarball ++ ".sha256", <<Digest/binary, "\n">>).

write_no_op_relup(FromVsn, TargetVsn) ->
    Body = io_lib:format(
        "#{from_version => \"~s\","
        "  target_version => \"~s\","
        "  code_changes => [],"
        "  post_upgrade_callbacks => []}.",
        [FromVsn, TargetVsn]
    ),
    Dir = filename:join([code:priv_dir(emqx_relup), "relup"]),
    ok = filelib:ensure_path(Dir),
    Name = "test-" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".relup",
    File = filename:join(Dir, Name),
    ok = file:write_file(File, iolist_to_binary(Body)),
    File.

cleanup_test_catalog_entries() ->
    Dir = filename:join([code:priv_dir(emqx_relup), "relup"]),
    case filelib:wildcard(filename:join(Dir, "test-*.relup")) of
        [] ->
            ok;
        Files ->
            lists:foreach(fun(F) -> _ = file:delete(F) end, Files),
            ok
    end.
