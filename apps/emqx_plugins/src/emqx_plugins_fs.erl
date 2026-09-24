%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_fs).

-feature(maybe_expr, enable).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Packages are unpacked below this hidden directory of the install directory
%% before they are published (see `staging_root/0').
-define(STAGING_DIR, ".emqx-plugin-staging").

%% A directory which does not exist, used to resolve the paths of tar entries
%% without depending on the state of the install directory.
%%
%% The path is absolute on purpose: `filelib:safe_relative_path/2' resolves the
%% entries against it, and a relative one would make that resolution depend on
%% the current working directory of the process which happens to run the check.
%% It does not exist either, so the check does not depend on anything on disk.
-define(PATH_PROBE_DIR, "/nonexistent-emqx-plugin-staging").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Tarfile operations
-export([
    get_tar/1,
    write_tar/2,
    delete_tar/1,
    is_tar_present/1,
    backup_package/1,
    restore_package/2
]).

%% Unpack plugin tar/delete unpacked content
-export([
    ensure_installed_from_tar/2,
    install_state/1,
    is_extraction_complete/1,
    is_in_use/1,
    prepare_replacement/1,
    purge_installed/1,
    is_installed/1
]).

%% Read individual plugin entries
-export([
    read_info/1,
    read_readme/1,
    read_md5sum/1,
    read_avsc_map/1,
    read_avsc_bin/1,
    read_avsc_bin_all/0,
    read_i18n/1,
    read_hocon/1,
    read_default_hocon/1
]).

%% List all installed plugins
-export([
    list_name_vsn/0
]).

%% Plugin's directories that are used directly by other modules
-export([
    %% To load and start plugin's apps
    lib_dir/1,
    %% To store plugin's configs
    default_config_file_path/1,
    config_file_path/1,
    ensure_config_dir/1
]).

%% Intelnal export
-export([
    install_dir/0,
    tar_file_path/1,
    info_file_path/1,
    plugin_dir/1,
    %% Where a package is unpacked before it is published.  Exposed so that the
    %% attempts which are in progress can be enumerated at a stable path; the
    %% location is load bearing, see the comment at its definition.
    staging_root/0,
    %% Called by the plugins application when it starts.
    cleanup_stale_staging/0
]).

%% The cluster wide installation lock, for the callers which have to run a whole
%% sequence of steps which read or write an installation
%% (`emqx_plugins:with_installation_lock/2').
-export([with_installation_lock/2]).

%% What the plugin's install directory currently holds
-type install_state() :: installed | incomplete | absent.

%% The package file (and its checksum) as it is on disk, kept so that a failed
%% replacement can put it back.  Each file is `none' when it was not there.
-type package_backup() :: #{tar := none | binary(), md5sum := none | binary()}.

-export_type([install_state/0, package_backup/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Read plugin entries

-spec read_info(name_vsn()) -> {ok, map()} | {error, term()}.
read_info(NameVsn) ->
    InfoFilePath = info_file_path(NameVsn),
    read_file_map(InfoFilePath, "bad_info_file").

-spec read_md5sum(name_vsn()) -> binary().
read_md5sum(NameVsn) ->
    case file:read_file(md5sum_file_path(NameVsn)) of
        {ok, MD5} -> MD5;
        _ -> <<>>
    end.

-spec read_readme(name_vsn()) -> binary().
read_readme(NameVsn) ->
    ReadmeFilePath = readme_file_path(NameVsn),
    case file:read_file(ReadmeFilePath) of
        {ok, Content} ->
            Content;
        {error, _} ->
            <<>>
    end.

-spec read_avsc_map(name_vsn()) -> {ok, map()} | {error, term()}.
read_avsc_map(NameVsn) ->
    AvscFilePath = avsc_file_path(NameVsn),
    read_file_map(AvscFilePath, "bad_avsc_file").

-spec read_avsc_bin(name_vsn()) -> {ok, binary()} | {error, term()}.
read_avsc_bin(NameVsn) ->
    AvscFilePath = avsc_file_path(NameVsn),
    read_file_bin(AvscFilePath, "bad_avsc_file").

-spec read_avsc_bin_all() -> [{name_vsn(), binary()}].
read_avsc_bin_all() ->
    lists:filtermap(
        fun(NameVsn) ->
            case read_avsc_bin(NameVsn) of
                {ok, AvscBin} -> {true, {NameVsn, AvscBin}};
                {error, _} -> false
            end
        end,
        list_name_vsn()
    ).

-spec read_i18n(name_vsn()) -> {ok, map()} | {error, term()}.
read_i18n(NameVsn) ->
    I18nFilePath = i18n_file_path(NameVsn),
    read_file_map(I18nFilePath, "bad_i18n_file").

-spec read_hocon(name_vsn()) -> {ok, map()} | {error, term()}.
read_hocon(NameVsn) ->
    HoconFilePath = config_file_path(NameVsn),
    read_file_map(HoconFilePath, "bad_hocon_file").

-spec read_default_hocon(name_vsn()) -> {ok, map()} | {error, term()}.
read_default_hocon(NameVsn) ->
    HoconFilePath = default_config_file_path(NameVsn),
    case read_file_map(HoconFilePath, "bad_default_hocon_file") of
        {error, Error} ->
            {error, Error#{
                kind => invalid_package
            }};
        Result ->
            Result
    end.

%% List all installed plugins

-spec list_name_vsn() -> [name_vsn()].
list_name_vsn() ->
    Pattern = filename:join([install_dir(), "*", "release.json"]),
    lists:map(
        fun(JsonFilePath) ->
            [_, NameVsn | _] = lists:reverse(filename:split(JsonFilePath)),
            NameVsn
        end,
        filelib:wildcard(Pattern)
    ).

%% Tarfile operations

-spec get_tar(name_vsn()) -> {ok, binary()} | {error, any}.
get_tar(NameVsn) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        TarGz = tar_file_path(NameVsn),
        case file:read_file(TarGz) of
            {ok, Content} ->
                {ok, Content};
            {error, _} ->
                case create_tar(NameVsn, TarGz) of
                    ok ->
                        file:read_file(TarGz);
                    Err ->
                        Err
                end
        end
    end).

-spec is_tar_present(name_vsn()) ->
    false | {true, [file:filename()]}.
is_tar_present(NameVsn) ->
    TarGz = tar_file_path(NameVsn),
    case filelib:is_regular(TarGz) of
        true -> {true, [TarGz]};
        false -> false
    end.

-spec write_tar(name_vsn(), iodata()) -> ok | {error, map()}.
write_tar(NameVsn, Content) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        maybe
            {ok, PreviousPackage} ?= backup_package(NameVsn),
            case write_tar_files(NameVsn, Content) of
                ok ->
                    ok;
                {error, _} = Error ->
                    case restore_package(NameVsn, PreviousPackage) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "failed_to_restore_plugin_package",
                                name_vsn => NameVsn,
                                reason => Reason
                            })
                    end,
                    Error
            end
        end
    end).

write_tar_files(NameVsn, Content) ->
    TarFilePath = tar_file_path(NameVsn),
    MD5 = emqx_utils:bin_to_hexstr(crypto:hash(md5, Content), lower),
    maybe
        ok ?= write_package_file(TarFilePath, Content),
        ok ?= write_package_file(md5sum_file_path(NameVsn), MD5)
    end.

%% Write one file of the package file, creating its directory first.  A failure
%% is reported instead of crashing the caller with a `badmatch': the caller may
%% have a package file to put back.
write_package_file(Path, Content) ->
    case filelib:ensure_dir(Path) of
        ok ->
            case file:write_file(Path, Content) of
                ok -> ok;
                {error, Reason} -> write_package_file_failed(Path, Reason)
            end;
        {error, Reason} ->
            write_package_file_failed(Path, Reason)
    end.

write_package_file_failed(Path, Reason) ->
    {error, #{
        msg => "failed_to_write_plugin_package",
        path => Path,
        reason => Reason
    }}.

%% @doc Snapshot the package file (and its checksum) as it is on disk, so that
%% it can be put back when a replacement of the installation fails.
%%
%% A package file which is not there is part of the snapshot as `none' (the
%% restore deletes it again); a file which is there but can not be read is a
%% failure instead, see `read_package_file/1'.
-spec backup_package(name_vsn()) -> {ok, package_backup()} | {error, map()}.
backup_package(NameVsn) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        maybe
            {ok, Tar} ?= read_package_file(tar_file_path(NameVsn)),
            {ok, Md5sum} ?= read_package_file(md5sum_file_path(NameVsn)),
            {ok, #{tar => Tar, md5sum => Md5sum}}
        end
    end).

%% @doc Put the package file (and its checksum) back as it was before a failed
%% installation attempt: a file which was not there is deleted.
-spec restore_package(name_vsn(), package_backup()) -> ok | {error, term()}.
restore_package(NameVsn, #{tar := Tar, md5sum := Md5sum}) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        maybe
            ok ?= restore_file(tar_file_path(NameVsn), Tar),
            ok ?= restore_file(md5sum_file_path(NameVsn), Md5sum)
        end
    end).

restore_file(Path, none) ->
    delete_file_if_exists(Path);
restore_file(Path, Content) ->
    maybe
        ok ?= filelib:ensure_dir(Path),
        ok ?= file:write_file(Path, Content)
    end.

%% A missing file is part of the snapshot as `none': the restore then deletes
%% it.  Any other error is reported instead of recorded as `none', because the
%% restore would delete the package (or the checksum) it was meant to preserve.
read_package_file(Path) ->
    case file:read_file(Path) of
        {ok, Content} ->
            {ok, Content};
        {error, enoent} ->
            {ok, none};
        {error, Reason} ->
            {error, #{
                msg => "failed_to_backup_plugin_package",
                path => Path,
                reason => Reason,
                hint => <<"check the permissions of the plugins install directory">>
            }}
    end.

%%--------------------------------------------------------------------
%% Plugin package extraction
%%--------------------------------------------------------------------

%% Install the package of `NameVsn' from the install directory.
%%
%% The package is unpacked into a staging directory and only published with a
%% single `file:rename/2': the installation directory of a plugin only ever
%% holds a tree which was written by one rename, every entry of the package
%% stays below the package's own name-vsn, and a package which is refused (a
%% wrong package root, an unsafe entry, an incomplete or an invalid package)
%% leaves nothing behind.  No entry of one package can ever touch the directory
%% of another plugin.
%%
%% NOTE: the caller has to hold the installation lock (`with_installation_lock/2'):
%% this is only one step of a sequence which has to be serialized, see
%% `ensure_installed_from_tar/2'.
install_from_local_tar(NameVsn, InstallValidator) ->
    %% The name is checked before it is used to build the package path:
    %% `tar_file_path/1' must not look for a package outside the install dir.
    with_valid_package_name(NameVsn, fun() ->
        case read_tar_content(NameVsn) of
            {ok, TarContent} ->
                do_install_from_tar_content(NameVsn, TarContent, InstallValidator);
            {error, Reason} ->
                {error, Reason}
        end
    end).

%% Validate the package identifier before constructing its paths.
with_valid_package_name(NameVsn, Fun) ->
    case check_package_root_name(NameVsn) of
        ok ->
            Fun();
        {error, Reason} = Error ->
            ?SLOG(warning, Reason),
            Error
    end.

%% Unpack `TarContent' into a staging directory and publish it.
%%
%% The caller has to hold the installation lock: from the package root check to
%% the rollback of a rejected tree, every step reads or writes the same plugin
%% directory and the same staging directories, so it must not interleave with
%% the attempt of another process.
do_install_from_tar_content(NameVsn, TarContent, InstallValidator) ->
    case check_package_entries(NameVsn, TarContent) of
        {ok, SafeEntries} ->
            case make_staging_dir(NameVsn) of
                {ok, StagingDir} ->
                    try
                        install_staged_package(NameVsn, StagingDir, SafeEntries, InstallValidator)
                    after
                        %% Only this attempt's staging directory is removed,
                        %% and never by the top level directory of an entry: a
                        %% failed attempt leaves the install directory exactly
                        %% as it was.
                        ok = remove_staging_dir(StagingDir)
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} = Error ->
            %% Report the package validation result before creating staging files.
            ?SLOG(warning, Reason),
            Error
    end.

install_staged_package(NameVsn, StagingDir, SafeEntries, InstallValidator) ->
    case write_tar_file_content(StagingDir, SafeEntries) of
        ok ->
            do_publish_validated_package(NameVsn, StagingDir, InstallValidator);
        {error, Reason} ->
            {error, Reason}
    end.

read_tar_content(NameVsn) ->
    TarGz = tar_file_path(NameVsn),
    case erl_tar:extract(TarGz, [compressed, memory]) of
        {ok, TarContent} ->
            {ok, TarContent};
        {error, {_, enoent}} ->
            {error, #{
                msg => "failed_to_extract_plugin_package",
                path => TarGz,
                reason => plugin_tarball_not_found
            }};
        {error, Reason} ->
            {error, #{
                msg => "bad_plugin_package",
                path => TarGz,
                reason => Reason
            }}
    end.

%% The freshly unpacked content must be a usable installation: it has to pass
%% the caller's validation (metadata, config schema, ...) and to contain every
%% application the package declares.  Without the completeness check a package
%% which does not contain the files its own metadata declares would be reported
%% as installed, and the next state check would classify it as incomplete
%% again.
%%
%% NOTE: the caller's validation runs first, on the published tree, and a
%% failure is rolled back by the caller (`validate_published_package/3').  The
%% order matters: checking the structure first would report a package with a
%% broken application resource file as `incomplete_plugin_package' instead of
%% the `bad_plugin_app_file' its validator reports.
validate_unpacked(NameVsn, InstallValidator) ->
    maybe
        ok ?= InstallValidator(),
        case apps_not_extracted(NameVsn) of
            {ok, []} ->
                ok;
            {ok, MissingApps} ->
                {error, #{
                    msg => "incomplete_plugin_package",
                    name_vsn => NameVsn,
                    missing_apps => MissingApps
                }};
            error ->
                {error, #{
                    msg => "bad_plugin_package_metadata",
                    name_vsn => NameVsn,
                    reason => metadata_not_readable
                }}
        end
    end.

-spec ensure_installed_from_tar(name_vsn(), fun(() -> ok | {error, term()})) -> ok | {error, map()}.
ensure_installed_from_tar(NameVsn, InstallValidator) ->
    %% `install_state/1' and `purge_installed/1' build the plugin directory from
    %% the name as well: a name which is not a single path component must not
    %% reach them either (leftovers are purged before the package is read).
    %%
    %% What the state says is acted upon under the same installation lock as the
    %% publication (`with_installation_lock/2').  A concurrent attempt which has
    %% already published its tree but has not validated it yet holds that lock,
    %% and the tree it has temporarily put in place is not an installation: it
    %% can still be rolled back.  Reading the state outside of the lock would
    %% return `ok' here for such a tree, and the caller would be told that a
    %% plugin is installed which the other attempt then removes.
    with_valid_package_name(NameVsn, fun() ->
        with_installation_lock(NameVsn, fun() ->
            do_ensure_installed_from_tar(NameVsn, InstallValidator)
        end)
    end).

do_ensure_installed_from_tar(NameVsn, InstallValidator) ->
    case install_state(NameVsn) of
        installed ->
            ok;
        absent ->
            install_from_local_tar(NameVsn, InstallValidator);
        incomplete ->
            recover_incomplete_installation(NameVsn, InstallValidator)
    end.

%% @doc Classify what the plugin's install directory holds on disk.
%%
%% `installed' means that the metadata is readable *and* that every application
%% declared by the package has been extracted.  A directory holding only the
%% leftovers of an interrupted or failed installation (no readable
%% `release.json', or a manifest without the application files it declares) is
%% `incomplete'; `absent' means that there is no directory at all.
%%
%% Only what is on disk is inspected: validation which depends on the runtime
%% state (an application loaded from another package) or which changes the
%% runtime state (installing or deleting a config schema) must not decide
%% whether an installation exists, or a refused replacement could destroy a
%% complete installation.
-spec install_state(name_vsn()) -> install_state().
install_state(NameVsn) ->
    case is_installed(NameVsn) of
        false ->
            absent;
        true ->
            case is_extraction_complete(NameVsn) of
                true -> installed;
                false -> incomplete
            end
    end.

%% @doc Whether every application declared by the plugin's `release.json' has
%% been extracted into the install directory.
%%
%% A readable `release.json' alone does not prove that the unpack finished: the
%% extraction may have been interrupted after the manifest had been written, so
%% the applications it declares (and the beam files those applications declare)
%% are checked as well.
-spec is_extraction_complete(name_vsn()) -> boolean().
is_extraction_complete(NameVsn) ->
    case apps_not_extracted(NameVsn) of
        {ok, []} -> true;
        _ -> false
    end.

%% @doc The applications declared by the plugin's `release.json' whose files
%% have not been extracted.  `error' when the metadata can not be read.
-spec apps_not_extracted(name_vsn()) -> {ok, [name_vsn()]} | error.
apps_not_extracted(NameVsn) ->
    case read_info(NameVsn) of
        {ok, #{<<"rel_apps">> := Apps}} when is_list(Apps) ->
            {ok, [App || App <- Apps, not is_app_extracted(NameVsn, App)]};
        _ ->
            error
    end.

%% @doc Whether one of the plugin's applications is running on this node.
-spec is_in_use(name_vsn()) -> boolean().
is_in_use(NameVsn) ->
    emqx_plugins_apps:running_apps_from(plugin_dir(NameVsn)) =/= [].

%% @doc Check that the plugin's install directory can be replaced, and unload
%% the applications that are still loaded from it.
%%
%% An error is returned when one of the plugin's applications is running: the
%% files of a running plugin must not be deleted, the plugin would keep running
%% from replaced or missing code.  The applications that are merely loaded are
%% unloaded, so that the package which is unpacked afterwards is loaded cleanly
%% instead of the old modules staying in the code server.
%%
%% NOTE: the check and the replacement that follows it are not atomic.  A
%% plugin started concurrently, after the running applications have been read
%% but before the install directory is replaced, can still lose its files.
%% Serializing the plugin lifecycle operations is left to the callers.
-spec prepare_replacement(name_vsn()) -> ok | {error, map()}.
prepare_replacement(NameVsn) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        PluginDir = plugin_dir(NameVsn),
        case emqx_plugins_apps:running_apps_from(PluginDir) of
            [] ->
                case emqx_plugins_apps:stop_and_unload_loaded(PluginDir) of
                    {ok, _} ->
                        ok;
                    {error, Reason} ->
                        %% The applications are still loaded from the directory
                        %% which is about to be purged: the code they run would be
                        %% replaced or deleted.
                        ?SLOG(warning, #{
                            msg => "failed_to_unload_plugin_apps",
                            name_vsn => NameVsn,
                            reason => Reason
                        }),
                        {error, #{
                            msg => "failed_to_unload_plugin_apps",
                            name_vsn => NameVsn,
                            reason => Reason,
                            hint => <<"stop the plugin first">>
                        }}
                end;
            Running ->
                ?SLOG(warning, #{
                    msg => "refusing_to_replace_running_plugin",
                    name_vsn => NameVsn,
                    running_apps => Running
                }),
                {error, #{
                    msg => "plugin_is_in_use",
                    name_vsn => NameVsn,
                    running_apps => Running,
                    hint => <<"stop the plugin first">>
                }}
        end
    end).

recover_incomplete_installation(NameVsn, InstallValidator) ->
    case prepare_replacement(NameVsn) of
        ok ->
            %% The install dir only holds leftovers of an interrupted or failed
            %% installation: that is not an installation, so purge it and unpack
            %% the package into a clean directory instead of letting stale files
            %% shadow the new content.
            ?SLOG(warning, #{
                msg => "purging_incomplete_plugin_installation",
                name_vsn => NameVsn,
                reason => incomplete_installation_reason(NameVsn)
            }),
            case purge_installed(NameVsn) of
                ok ->
                    install_from_local_tar(NameVsn, InstallValidator);
                {error, PurgeError} ->
                    {error, #{
                        msg => "failed_to_purge_plugin_dir",
                        name_vsn => NameVsn,
                        reason => PurgeError
                    }}
            end;
        {error, _} = Error ->
            Error
    end.

-spec is_installed(name_vsn()) -> boolean().
is_installed(NameVsn) ->
    filelib:is_dir(plugin_dir(NameVsn)).

-spec delete_tar(name_vsn()) -> ok | {error, term()}.
delete_tar(NameVsn) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        TarFilePath = tar_file_path(NameVsn),
        MD5FilePath = md5sum_file_path(NameVsn),
        maybe
            ok ?= delete_file_if_exists(TarFilePath),
            ok ?= delete_file_if_exists(MD5FilePath),
            ok
        else
            {error, Reason} ->
                ?SLOG(error, #{
                    msg => "failed_to_delete_package_file",
                    package => NameVsn,
                    reason => Reason
                }),
                {error, Reason}
        end
    end).

-spec purge_installed(name_vsn()) -> ok | {error, term()}.
purge_installed(NameVsn) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        Dir = plugin_dir(NameVsn),
        purge_plugin_dir(Dir)
    end).

-spec ensure_config_dir(name_vsn()) -> ok | {error, term()}.
ensure_config_dir(NameVsn) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        ConfigDir = plugin_data_dir(NameVsn),
        case filelib:ensure_path(ConfigDir) of
            ok ->
                ok;
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "failed_to_create_plugin_config_dir",
                    dir => ConfigDir,
                    reason => Reason
                }),
                {error, {mkdir_failed, ConfigDir, Reason}}
        end
    end).

-spec lib_dir(name_vsn()) -> string().
lib_dir(NameVsn) ->
    plugin_dir(NameVsn).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

install_dir() ->
    emqx_config:get([?CONF_ROOT, install_dir], "").

plugin_dir(NameVsn) ->
    ok = assert_package_root_name(NameVsn),
    wrap_to_list(filename:join([install_dir(), NameVsn])).

tar_file_path(NameVsn) ->
    plugin_dir(NameVsn) ++ ".tar.gz".

info_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_dir(NameVsn), "release.json"])).

avsc_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_priv_dir(NameVsn), "config_schema.avsc"])).

config_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_data_dir(NameVsn), "config.hocon"])).

%% should only used when plugin installing
default_config_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_priv_dir(NameVsn), "config.hocon"])).

i18n_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_priv_dir(NameVsn), "config_i18n.json"])).

md5sum_file_path(NameVsn) ->
    tar_file_path(NameVsn) ++ ".md5sum".

readme_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_dir(NameVsn), "README.md"])).

read_file_bin(Path, Msg) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            {ok, Bin};
        {error, Reason} ->
            {error, #{msg => Msg, reason => Reason}}
    end.

read_file_map(Path, Msg) ->
    case hocon:load(Path, #{format => richmap}) of
        {ok, RichMap} ->
            {ok, hocon_maps:ensure_plain(RichMap)};
        {error, Reason} ->
            {error, #{msg => Msg, reason => Reason}}
    end.

plugin_priv_dir(NameVsn) ->
    maybe
        {ok, #{<<"name">> := Name, <<"rel_apps">> := Apps}} ?= read_info(NameVsn),
        case app_dir(Name, Apps) of
            {ok, AppDir} ->
                wrap_to_list(filename:join([plugin_dir(NameVsn), AppDir, "priv"]));
            {error, not_found} ->
                case
                    [
                        PrivDir
                     || AppDir <- Apps,
                        PrivDir <- [filename:join([plugin_dir(NameVsn), AppDir, "priv"])],
                        filelib:is_dir(PrivDir)
                    ]
                of
                    [PrivDir] -> wrap_to_list(PrivDir);
                    _ -> filename:join(plugin_dir(NameVsn), "priv")
                end
        end
    else
        %% Otherwise assume the priv directory is under the plugin root directory
        _ -> filename:join(plugin_dir(NameVsn), "priv")
    end.

plugin_data_dir(NameVsn) ->
    case emqx_plugins_utils:validate_name_vsn(NameVsn) of
        ok ->
            [Name, _Vsn] = binary:split(iolist_to_binary(NameVsn), <<"-">>),
            wrap_to_list(filename:join([emqx:data_dir(), "plugins", Name]));
        {error, _} ->
            {error, Reason} = bad_package_name(NameVsn),
            error(Reason)
    end.

purge_plugin_dir(Dir) ->
    case file:del_dir_r(Dir) of
        ok ->
            ?SLOG(info, #{
                msg => "purged_plugin_dir",
                dir => Dir
            });
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_purge_plugin_dir",
                dir => Dir,
                reason => Reason
            }),
            {error, Reason}
    end.

create_tar(NameVsn, TarGzName) ->
    InstallDir = string:trim(install_dir(), trailing, "/") ++ "/",
    case filelib:wildcard(filename:join(plugin_dir(NameVsn), "**")) of
        [_ | _] = PluginFiles ->
            PluginFiles1 = [{string:prefix(F, InstallDir), F} || F <- PluginFiles],
            erl_tar:create(TarGzName, PluginFiles1, [compressed]);
        _ ->
            {error, plugin_not_found}
    end.

%%--------------------------------------------------------------------
%% Package content validation
%%--------------------------------------------------------------------

%% Every entry of the package must be below the authorized name-vsn, and the
%% name-vsn itself must be a single path component.  Returns the entries with
%% their paths normalized (a leading `./' and redundant `..' are resolved), so
%% that the write phase only ever joins a plain relative path.
%%
%% The validation is complete before staging files are written.
check_package_entries(NameVsn, TarContent) ->
    case check_package_root_name(NameVsn) of
        ok ->
            case
                lists:foldl(
                    fun(Entry, Acc) -> classify_entry(NameVsn, Entry, Acc) end,
                    {ok, []},
                    TarContent
                )
            of
                {ok, Safe} -> {ok, lists:reverse(Safe)};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

classify_entry(_NameVsn, _Entry, {error, _} = Error) ->
    Error;
classify_entry(NameVsn, {Name, Bin}, {ok, Acc}) ->
    case safe_entry_path(NameVsn, Name) of
        {ok, SafeName} ->
            {ok, [{SafeName, Bin} | Acc]};
        {error, Reason} ->
            {error, Reason#{name_vsn => NameVsn, entry => Name}}
    end.

%% The authorized name must be one path component, and it must not name a
%% directory which is not a plugin's own: `filename:split/1' does not normalize,
%% so `"."' and `".."' have to be excluded by hand, and `?STAGING_DIR' is the
%% directory the staging attempts live in (a plugin installed under that name
%% would be its own staging root).
%%
%% The name also has to be in its normalized form already: `filename:join/1'
%% resolves a trailing or a duplicated separator, so `"foo-1.0/"' would build
%% the paths of `"foo-1.0"' while being a different package file name
%% (`"foo-1.0/.tar.gz"').  Two spellings which the file system can not tell
%% apart must not reach the code which builds paths from the name.
assert_package_root_name(NameVsn) ->
    case check_package_root_name(NameVsn) of
        ok -> ok;
        {error, Reason} -> error(Reason)
    end.

check_package_root_name(NameVsn) ->
    try
        Name = wrap_to_list(NameVsn),
        case filename:split(Name) of
            [Root] when Root =/= ".", Root =/= "..", Root =/= ?STAGING_DIR ->
                case
                    filename:pathtype(Name) =:= relative andalso
                        filename:join([Name]) =:= Name andalso
                        lists:all(fun(C) -> C > 32 andalso C =/= 127 andalso C =/= $\\ end, Name)
                of
                    true -> ok;
                    false -> bad_package_name(NameVsn)
                end;
            _ ->
                bad_package_name(NameVsn)
        end
    catch
        error:_ -> bad_package_name(NameVsn)
    end.

bad_package_name(NameVsn) ->
    {error, #{
        msg => "bad_plugin_package_name",
        name_vsn => NameVsn,
        hint => "the package name must be a single path component"
    }}.

%% Whether a tar entry stays below the package root, and where it resolves to:
%% a path which escapes the package is unsafe, and so is one which resolves back
%% to the package root itself (a plain file would be written over the
%% directory).
%%
%% The path is resolved against a fixed directory which does not exist, so that
%% the decision does not depend on the state of the install directory: nothing
%% of the package is on disk yet, and the archive is read into memory (which
%% does not materialize directory or symlink entries), so there is no symbolic
%% link on the way which could be followed.
safe_entry_path(NameVsn, Name) ->
    case filelib:safe_relative_path(Name, ?PATH_PROBE_DIR) of
        unsafe ->
            {error, #{msg => "unsafe_tar_entry_path"}};
        [] ->
            {error, #{msg => "unsafe_tar_entry_path", reason => resolves_to_install_root}};
        Safe ->
            NameVsnBin = emqx_plugins_utils:bin(NameVsn),
            [Root | _] = filename:split(Safe),
            case emqx_plugins_utils:bin(Root) =:= NameVsnBin of
                false ->
                    {error, #{msg => "plugin_package_entry_outside_root"}};
                true ->
                    case emqx_plugins_utils:bin(Safe) =:= NameVsnBin of
                        true ->
                            {error, #{
                                msg => "unsafe_tar_entry_path",
                                reason => resolves_to_package_root
                            }};
                        false ->
                            {ok, Safe}
                    end
            end
    end.

%% Write the entries of a package below BaseDir.  The entries have already been
%% normalized and checked by `check_package_entries/2'.  Any failure is returned
%% instead of crashing the caller with a `badmatch'; the caller removes the
%% staging directory it wrote into.
write_tar_file_content(BaseDir, TarContent) ->
    lists:foldl(
        fun
            (_Entry, {error, _} = Error) ->
                Error;
            ({Name, Bin}, ok) ->
                Filename = filename:join(BaseDir, Name),
                case filelib:ensure_dir(Filename) of
                    ok ->
                        case file:write_file(Filename, Bin) of
                            ok -> ok;
                            {error, Reason} -> write_failed(Name, Reason)
                        end;
                    {error, Reason} ->
                        write_failed(Name, Reason)
                end
        end,
        ok,
        TarContent
    ).

write_failed(Name, Reason) ->
    {error, #{
        msg => "failed_to_write_plugin_package",
        entry => Name,
        reason => Reason
    }}.

%%--------------------------------------------------------------------
%% Staging directory
%%--------------------------------------------------------------------

%% Packages are unpacked below one hidden directory of the install directory.
%%
%% Two properties of this path are load bearing and both are easy to break from
%% a distance:
%%
%% * the staging root is below the install directory, so the `file:rename/2'
%%   which publishes a staged tree stays within one file system and is atomic.
%%   Moving the staging root somewhere else (such as `emqx:data_dir()') would
%%   silently turn the publish into a cross device error, or into a copy.
%% * a staging attempt is never mistaken for an installation: its tree is
%%   `<name-vsn>/<attempt>/<name-vsn>/...', while `list_name_vsn/0' lists
%%   `install_dir()/*/release.json', so the `release.json' of a staged package
%%   is one level too deep to be found.  The leading dot is not what keeps the
%%   directory out of that listing: `filelib:wildcard/1' does match dot prefixed
%%   names.  The depth is.
-spec staging_root() -> file:filename().
staging_root() ->
    wrap_to_list(filename:join(install_dir(), ?STAGING_DIR)).

%% The staging directory of one installation attempt.  Its first level is the
%% exact name-vsn (not a prefix of it: `foo-1.0' and `foo-1.0-extra-2.0' must not
%% see each other), the second level makes the attempt unique, so that a
%% directory left behind by a crashed process is never reused.
make_staging_dir(NameVsn) ->
    StagingDir = filename:join([staging_root(), NameVsn, unique()]),
    %% `filelib:ensure_dir/1' creates the staging directory of the name-vsn,
    %% `file:make_dir/1' creates the attempt directory itself: an attempt
    %% directory which is already there (an id collision) is an error, not a
    %% directory two installations would share.
    Result =
        case filelib:ensure_dir(StagingDir) of
            ok -> file:make_dir(StagingDir);
            {error, _} = Error -> Error
        end,
    case Result of
        ok ->
            {ok, StagingDir};
        {error, Reason} ->
            {error, #{
                msg => "failed_to_create_plugin_staging_dir",
                dir => StagingDir,
                reason => Reason
            }}
    end.

%% Remove this attempt's staging directory.  Cleanup is best effort: it never
%% changes the result of the installation.
%%
%% Only this attempt's directory is removed: the (shared) staging directory of
%% the name-vsn and the staging root are not touched, so a concurrent
%% installation of the same plugin can not lose its attempt directory to this
%% cleanup.  What is left of them is swept when the application starts.
remove_staging_dir(StagingDir) ->
    _ = file:del_dir_r(StagingDir),
    ok.

%% @doc Remove the staging directories left behind on this node.
%%
%% Called once while the plugins application starts, before anything can be
%% installed: at that point no installation is using them, so they can be swept
%% by name.  A running installation does not sweep staging directories, because
%% an installation of the same plugin may be writing its own attempt at the
%% same time.
-spec cleanup_stale_staging() -> ok.
cleanup_stale_staging() ->
    case filelib:wildcard(filename:join(staging_root(), "*")) of
        [] ->
            ok;
        Dirs ->
            Count = length([ok || Dir <- Dirs, file:del_dir_r(Dir) =:= ok]),
            _ = file:del_dir(staging_root()),
            ?SLOG(warning, #{
                msg => "cleaned_stale_plugin_staging",
                dir => staging_root(),
                count => Count
            }),
            ok
    end.

unique() ->
    lists:flatten(
        io_lib:format("~s-~b-~b", [
            os:getpid(),
            erlang:system_time(millisecond),
            erlang:unique_integer([positive, monotonic])
        ])
    ).

%%--------------------------------------------------------------------
%% The cluster wide install lock, and publishing a staged package
%%--------------------------------------------------------------------

%% Run `Fun' while holding the plugin installation lock of the cluster.
%%
%% The lock covers the state check (a tree which a concurrent attempt has
%% published but not validated yet is not an installation), the whole sequence of
%% an attempt which reaches publication (move the installed tree away, rename the
%% staged tree into place, validate it, roll back or discard the replaced tree),
%% and the recovery of an `incomplete' installation (`prepare_replacement/1' and
%% the purge which follows it).  A rollback outside of the lock could remove a
%% tree which a concurrent installation has just published, and a state check
%% outside of it could report such a tree as an installation.
%%
%% The lock is held by `emqx_plugins_install_serializer' on the coordinator of
%% the cluster, so it serializes installations on every node, not only on this
%% one.  It is fail closed: when the coordinator can not be determined or
%% reached, the installation is refused instead of running without a lock.
%%
%% NOTE: `ensure_installed_from_tar/2' takes the lock for its own sequence, and
%% the operations of `emqx_plugins' which also write or snapshot the package file
%% take it around the whole sequence: snapshot, write, unpack, publish, validate
%% and roll back all happen in one critical section.  Taking the lock only around
%% the unpack would leave the package file the unpack reads outside of it, and a
%% concurrent upload of the same name-vsn could be installed instead.
%%
%% NOTE: the uninstall paths (`purge_installed/1' and `delete_tar/1') are still
%% called without it: they are not ordered against an installation, and
%% serializing the whole plugin lifecycle is out of scope here.
with_installation_lock(NameVsn, Fun) ->
    emqx_plugins_install_serializer:run(NameVsn, Fun).

do_publish_validated_package(NameVsn, StagingDir, InstallValidator) ->
    Target = plugin_dir(NameVsn),
    Staged = filename:join(StagingDir, NameVsn),
    case move_away(NameVsn, Target) of
        {ok, Previous} ->
            case file:rename(Staged, Target) of
                ok ->
                    validate_published_package(NameVsn, Previous, InstallValidator);
                {error, Reason} ->
                    restore_previous(NameVsn, Target, Previous),
                    {error, #{
                        msg => "failed_to_publish_plugin_package",
                        name_vsn => NameVsn,
                        reason => Reason
                    }}
            end;
        {error, Reason} ->
            {error, #{
                msg => "failed_to_replace_plugin_package",
                name_vsn => NameVsn,
                reason => Reason
            }}
    end.

%% The staged tree is already published, so the caller's validation runs on it.
%%
%% The validator is the caller's code: whatever it does (it may raise, it talks
%% to the config schema store), the installation which was there before must not
%% be lost.  Removing only this attempt's staging directory would leave the tree
%% which failed the validation in place and the previous one in a directory
%% which the next boot sweeps.
%%
%% What this function owns is the file rollback of the plugin directory: the
%% previous tree is put back when the validation is refused or raises, and the
%% side effects of the validator itself are not restored.  Whether a published
%% tree stays authoritative across a restart is decided by `install_state/1';
%% changing that would need a persisted in-progress state which the CLI and the
%% HTTP API would then report for an installation under validation, so it is out
%% of scope here.
validate_published_package(NameVsn, Previous, InstallValidator) ->
    try validate_unpacked(NameVsn, InstallValidator) of
        ok ->
            ?SLOG(info, #{
                msg => "plugin_package_published",
                name_vsn => NameVsn,
                previous => Previous
            }),
            discard_replaced(NameVsn, Previous);
        {error, Reason} ->
            %% The files of the previous installation are put back, but the side
            %% effects of a validator which failed (a deleted config schema) are
            %% not.
            ?SLOG(warning, #{
                msg => "rolled_back_plugin_install",
                name_vsn => NameVsn,
                reason => Reason
            }),
            unpublish_staged_package(NameVsn, Previous),
            {error, Reason}
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "rolled_back_plugin_install_after_validator_crash",
                name_vsn => NameVsn,
                class => Class,
                reason => Reason
            }),
            unpublish_staged_package(NameVsn, Previous),
            erlang:raise(Class, Reason, Stacktrace)
    end.

%% Move the installed tree of the plugin (if any) into the staging directory.
%%
%% A target which is not a directory (a leftover file, a broken symlink) is
%% moved away just the same: `file:rename/2' refuses a non-empty target
%% directory, so the target has to be gone before the new tree is renamed into
%% place.
move_away(NameVsn, Target) ->
    case file:read_link_info(Target) of
        {error, enoent} ->
            {ok, none};
        {error, Reason} ->
            {error, Reason};
        {ok, _} ->
            Replaced = filename:join([staging_root(), NameVsn, "replaced-" ++ unique()]),
            case filelib:ensure_dir(Replaced) of
                ok ->
                    case file:rename(Target, Replaced) of
                        ok -> {ok, {replaced, Replaced}};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

restore_previous(_NameVsn, _Target, none) ->
    ok;
restore_previous(NameVsn, Target, {replaced, Path}) ->
    _ = file:del_dir_r(Target),
    case file:rename(Path, Target) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_restore_replaced_plugin_package",
                name_vsn => NameVsn,
                path => Path,
                reason => Reason
            }),
            ok
    end.

discard_replaced(_NameVsn, none) ->
    ok;
discard_replaced(NameVsn, {replaced, Path}) ->
    case file:del_dir_r(Path) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_discard_replaced_plugin_package",
                name_vsn => NameVsn,
                path => Path,
                reason => Reason
            }),
            ok
    end.

unpublish_staged_package(NameVsn, Previous) ->
    _ = file:del_dir_r(plugin_dir(NameVsn)),
    restore_previous(NameVsn, plugin_dir(NameVsn), Previous).

app_dir(AppName, Apps) ->
    case
        lists:filter(
            fun(AppNameVsn) -> nomatch =/= string:prefix(AppNameVsn, AppName) end,
            Apps
        )
    of
        [AppNameVsn] ->
            {ok, AppNameVsn};
        _ ->
            {error, not_found}
    end.

%% Check that the files of the given application are all on disk: its resource
%% file must be there, and so must the beam files it declares.  A declared
%% application whose name-vsn can not be parsed is not an extracted
%% application.
is_app_extracted(NameVsn, AppNameVsn) ->
    try
        {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
        EbinDir = filename:join([
            plugin_dir(NameVsn), emqx_plugins_utils:bin(AppNameVsn), "ebin"
        ]),
        case app_modules(EbinDir, AppName) of
            {ok, Modules} ->
                lists:all(
                    fun(Module) ->
                        Beam = atom_to_list(Module) ++ ".beam",
                        filelib:is_regular(filename:join(EbinDir, Beam))
                    end,
                    Modules
                );
            error ->
                false
        end
    catch
        _:_ ->
            false
    end.

app_modules(EbinDir, AppName) ->
    AppFile = filename:join(EbinDir, atom_to_list(AppName) ++ ".app"),
    case file:consult(AppFile) of
        {ok, [{application, _AppName, Props}]} when is_list(Props) ->
            case proplists:get_value(modules, Props, []) of
                %% A module which is not an atom can not name a beam file.
                Modules when is_list(Modules) ->
                    case lists:all(fun erlang:is_atom/1, Modules) of
                        true -> {ok, Modules};
                        false -> error
                    end;
                _ ->
                    error
            end;
        _ ->
            error
    end.

%% Why an installation is not complete, for the logs: the metadata is not
%% readable at all, or the files of the given applications are missing.
incomplete_installation_reason(NameVsn) ->
    case apps_not_extracted(NameVsn) of
        {ok, []} -> metadata_not_readable;
        {ok, Apps} -> {apps_not_extracted, Apps};
        error -> metadata_not_readable
    end.

wrap_to_list(Path) ->
    binary_to_list(iolist_to_binary(Path)).

delete_file_if_exists(File) ->
    case file:delete(File) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            {error, {delete_file_failed, File, Reason}}
    end.

-ifdef(TEST).
check_package_root_name_test_() ->
    [
        ?_assertEqual(ok, check_package_root_name("candidate-1.0")),
        ?_assertEqual(ok, check_package_root_name(<<"candidate-1.0">>)),
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}}, check_package_root_name("../evil-1.0")
        ),
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}}, check_package_root_name("a/b-1.0")
        ),
        ?_assertMatch({error, #{msg := "bad_plugin_package_name"}}, check_package_root_name("")),
        ?_assertMatch({error, #{msg := "bad_plugin_package_name"}}, check_package_root_name(".")),
        ?_assertMatch({error, #{msg := "bad_plugin_package_name"}}, check_package_root_name("..")),
        %% the staging directory is not a plugin
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}}, check_package_root_name(?STAGING_DIR)
        ),
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}}, check_package_root_name(<<"a/b">>)
        ),
        %% a spelling which `filename:join/1' would resolve to another name
        %% addresses the same directory with a different package file, so it
        %% must not be accepted as a package name
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}}, check_package_root_name("candidate-1.0/")
        ),
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}},
            check_package_root_name(<<"candidate-1.0//">>)
        )
    ].

check_package_entries_test_() ->
    [
        ?_assertEqual(
            {ok, [{"candidate-1.0/a", <<"1">>}, {"candidate-1.0/b", <<"2">>}]},
            check_package_entries("candidate-1.0", [
                {"candidate-1.0/a", <<"1">>}, {"./candidate-1.0/b", <<"2">>}
            ])
        ),
        %% a duplicate entry is written twice, which is not an error
        ?_assertEqual(
            {ok, [{"candidate-1.0/a", <<"1">>}, {"candidate-1.0/a", <<"2">>}]},
            check_package_entries("candidate-1.0", [
                {"candidate-1.0/a", <<"1">>}, {"candidate-1.0/a", <<"2">>}
            ])
        ),
        ?_assertMatch(
            {error, #{
                msg := "plugin_package_entry_outside_root",
                name_vsn := "candidate-1.0",
                entry := "existing-1.0/x"
            }},
            check_package_entries("candidate-1.0", [
                {"candidate-1.0/a", <<"1">>}, {"existing-1.0/x", <<"2">>}
            ])
        ),
        ?_assertMatch(
            {error, #{msg := "bad_plugin_package_name"}},
            check_package_entries("../evil-1.0", [])
        )
    ].

safe_entry_path_test_() ->
    [
        ?_assertEqual(
            {ok, "candidate-1.0/lib/x.beam"},
            safe_entry_path("candidate-1.0", "candidate-1.0/lib/x.beam")
        ),
        %% a leading `./' and a redundant `..' are resolved, not rejected
        ?_assertEqual(
            {ok, "candidate-1.0/x"}, safe_entry_path("candidate-1.0", "./candidate-1.0/x")
        ),
        ?_assertEqual(
            {ok, "candidate-1.0/b"}, safe_entry_path("candidate-1.0", "candidate-1.0/a/../b")
        ),
        %% `safe_entry_path/2' reports the bare reason; the caller adds the
        %% name-vsn and the entry to it (`check_package_entries_test_' covers
        %% that enriched error)
        ?_assertMatch(
            {error, #{msg := "plugin_package_entry_outside_root"}},
            safe_entry_path("candidate-1.0", "existing-1.0/x")
        ),
        ?_assertMatch(
            {error, #{msg := "plugin_package_entry_outside_root"}},
            safe_entry_path("candidate-1.0", "evil-1.0.0/release.json")
        ),
        ?_assertMatch(
            {error, #{msg := "plugin_package_entry_outside_root"}},
            safe_entry_path("candidate-1.0", "candidate-1.0/../existing-1.0/x")
        ),
        %% unsafe paths carry no `reason', as they did before
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path"}},
            safe_entry_path("candidate-1.0", "../escape")
        ),
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path"}},
            safe_entry_path("candidate-1.0", "evil/../../../tmp/replacement")
        ),
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path"}},
            safe_entry_path("candidate-1.0", "/abs/path")
        ),
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path", reason => resolves_to_install_root}},
            safe_entry_path("candidate-1.0", "dir/..")
        ),
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path", reason => resolves_to_install_root}},
            safe_entry_path("candidate-1.0", ".")
        ),
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path", reason => resolves_to_install_root}},
            safe_entry_path("candidate-1.0", "")
        ),
        %% an entry which resolves back to the package root would overwrite it
        ?_assertEqual(
            {error, #{msg => "unsafe_tar_entry_path", reason => resolves_to_package_root}},
            safe_entry_path("candidate-1.0", "candidate-1.0/dir/..")
        )
    ].
-endif.
