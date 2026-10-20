%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_fs).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("kernel/include/file.hrl").

%% Tarfile operations
-export([
    get_tar/1,
    write_tar/2,
    write_tar/3,
    delete_tar/1,
    is_tar_present/1,
    backup_package/1,
    restore_package/2,
    restore_package_or_log/2
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
    %% To tell which plugin a loaded application comes from
    name_vsn_of_path/1,
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
    max_extraction_time_ms/0,
    %% Where a package is unpacked before it is published.  Exposed so that the
    %% attempts which are in progress can be enumerated at a stable path; the
    %% location is load bearing, see the comment at its definition.
    staging_root/0,
    %% Called by the plugins application when it starts.
    cleanup_stale_staging/0
]).

%% The installation lock, for the callers which have to run a whole sequence of
%% steps which read or write an installation
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
        case read_tar_if_size_ok(TarGz) of
            {error, enoent} ->
                case create_tar(NameVsn, TarGz) of
                    ok ->
                        read_tar_if_size_ok(TarGz);
                    Err ->
                        Err
                end;
            Result ->
                Result
        end
    end).

%% Read the tarball only after verifying its on-disk size is within
%% `max_package_size'. Checking after `file:read_file/1' would have already
%% loaded an oversized package into memory, defeating the limit.
read_tar_if_size_ok(TarGz) ->
    case file:read_file_info(TarGz) of
        {ok, #file_info{size = Size}} ->
            Limit = max_package_size(),
            case Size > Limit of
                true ->
                    {error, #{
                        msg => "package_too_large",
                        reason => package_size_limit_exceeded,
                        size => Size,
                        limit => Limit
                    }};
                false ->
                    file:read_file(TarGz)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

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
    write_tar(NameVsn, Content, none).

%% @doc Write the package file, reusing a snapshot the caller already took.
%%
%% When `Backup' is `none' the file which is replaced is snapshotted here, so
%% that it can be put back when writing the new files fails.  The caller must
%% hold the installation lock: the snapshot and the write are one critical
%% section.
-spec write_tar(name_vsn(), iodata(), none | package_backup()) -> ok | {error, map()}.
write_tar(NameVsn, Content, Backup) ->
    emqx_plugins_utils:with_valid_name(NameVsn, fun() ->
        case iolist_size(Content) > max_package_size() of
            true ->
                {error, #{
                    msg => "package_too_large",
                    reason => package_size_limit_exceeded,
                    size => iolist_size(Content),
                    limit => max_package_size()
                }};
            false ->
                write_tar_replacing(NameVsn, Content, Backup)
        end
    end).

write_tar_replacing(NameVsn, Content, none) ->
    maybe
        {ok, PreviousPackage} ?= backup_package(NameVsn),
        write_tar_replacing(NameVsn, Content, PreviousPackage)
    end;
write_tar_replacing(NameVsn, Content, PreviousPackage) ->
    case write_tar_files(NameVsn, Content) of
        ok ->
            ok;
        {error, _} = Error ->
            restore_package_or_log(NameVsn, PreviousPackage),
            Error
    end.

%% @doc Put the package file back, logging a failure.
%%
%% Best effort: the result of the installation attempt must not be replaced by
%% the failure of the restore, but the operator has to see it.
-spec restore_package_or_log(name_vsn(), package_backup()) -> ok.
restore_package_or_log(NameVsn, PreviousPackage) ->
    case restore_package(NameVsn, PreviousPackage) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_restore_plugin_package",
                name_vsn => NameVsn,
                reason => Reason
            }),
            ok
    end.

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
        {ok, []} ->
            %% `erl_tar' with `memory' returns regular files only, so a package
            %% which holds none of them has no plugin to publish.
            Error = #{msg => "bad_plugin_package", reason => empty_plugin_package},
            ?SLOG(warning, Error),
            {error, Error};
        {ok, SafeEntries} ->
            case make_staging_dir() of
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
                    Error = Reason#{name_vsn => NameVsn},
                    ?SLOG(warning, Error),
                    {error, Error}
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

%% Read the package file of `NameVsn' and enforce the package limits before it
%% is unpacked.
read_tar_content(NameVsn) ->
    TarGz = tar_file_path(NameVsn),
    maybe
        ok ?= validate_package_file_size(TarGz),
        ok ?= validate_decompressed_storage_limits(TarGz),
        {ok, TarContent} ?= extract_tarball(TarGz),
        ok ?= validate_decompressed_size(TarContent),
        {ok, TarContent}
    end.

validate_package_file_size(TarGz) ->
    case file:read_file_info(TarGz) of
        {ok, #file_info{size = Size}} ->
            Limit = max_package_size(),
            case Size > Limit of
                true ->
                    {error, #{
                        msg => "package_too_large",
                        path => TarGz,
                        reason => package_size_limit_exceeded,
                        size => Size,
                        limit => Limit
                    }};
                false ->
                    ok
            end;
        {error, enoent} ->
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

%% Read the tar entry table (names and metadata only, no content) and reject
%% packages with too many entries, too-deep paths, or a total declared size
%% beyond the limit before extraction. The table read itself runs in a
%% time-bounded worker because reading a crafted archive can be slow.
validate_decompressed_storage_limits(TarGz) ->
    Timeout = max_extraction_time_ms(),
    %% verbose mode returns {Name, Type, Size, Mtime, Mode, Uid, Gid} tuples,
    %% giving us the declared sizes for a pre-extraction memory check
    case run_with_timeout(fun() -> erl_tar:table(TarGz, [compressed, verbose]) end, Timeout) of
        {ok, {ok, Entries}} ->
            maybe
                ok ?= validate_max_file_count(Entries),
                ok ?= validate_max_path_deps(Entries),
                ok ?= validate_max_decompressed_size(Entries)
            end;
        {ok, {error, Reason}} ->
            {error, #{
                msg => "bad_plugin_package",
                path => TarGz,
                reason => Reason
            }};
        {error, timeout} ->
            {error, #{
                msg => "tar_table_timeout",
                path => TarGz,
                reason => tar_table_timeout,
                limit_ms => Timeout
            }};
        {error, {worker_down, Reason}} ->
            {error, #{
                msg => "bad_plugin_package",
                path => TarGz,
                reason => {tar_table_crashed, Reason}
            }}
    end.

validate_max_file_count(Entries) ->
    Count = length(Entries),
    Limit = max_file_count(),
    case Count > Limit of
        true ->
            {error, #{
                msg => "too_many_files_in_package",
                reason => file_count_limit_exceeded,
                count => Count,
                limit => Limit
            }};
        false ->
            ok
    end.

validate_max_path_deps(Entries) ->
    Limit = max_path_depth(),
    case lists:any(fun(Entry) -> path_depth(entry_name(Entry)) > Limit end, Entries) of
        true ->
            {error, #{
                msg => "tar_entry_path_too_deep",
                reason => path_depth_limit_exceeded,
                limit => Limit
            }};
        false ->
            ok
    end.

path_depth(Name) ->
    length(filename:split(Name)).

%% Reject packages whose entries declare (in the tar table) a total
%% decompressed size beyond the limit, before allocating memory for them.
%% The post-extraction check remains as a backstop for entries whose actual
%% size differs from the declared one.
validate_max_decompressed_size(Entries) ->
    Limit = max_decompressed_size(),
    Declared = lists:sum([entry_size(E) || E <- Entries]),
    case Declared > Limit of
        true ->
            {error, #{
                msg => "decompressed_content_too_large",
                reason => decompressed_size_limit_exceeded,
                size => Declared,
                limit => Limit
            }};
        false ->
            ok
    end.

entry_name({Name, _, _, _, _, _, _}) ->
    Name;
entry_name(Name) ->
    Name.

entry_size({_, _, Size, _, _, _, _}) ->
    Size;
entry_size(_) ->
    0.

%% Run `Fun' in a monitored worker process so that an over-long operation
%% can be aborted. Returns {ok, Result} | {error, timeout | {worker_down, Reason}}.
run_with_timeout(Fun, Timeout) ->
    Parent = self(),
    {Pid, MRef} = spawn_monitor(fun() ->
        Parent ! {run_result, self(), Fun()}
    end),
    receive
        {run_result, Pid, Result} ->
            erlang:demonitor(MRef, [flush]),
            {ok, Result};
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, {worker_down, Reason}}
    after Timeout ->
        exit(Pid, kill),
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% Extract in a monitored worker process so that an over-long extraction can
%% be aborted. Returns the same error shapes as the previous direct call.
extract_tarball(TarGz) ->
    Timeout = max_extraction_time_ms(),
    case run_with_timeout(fun() -> erl_tar:extract(TarGz, [compressed, memory]) end, Timeout) of
        {ok, Result} ->
            map_extract_result(TarGz, Result);
        {error, timeout} ->
            {error, #{
                msg => "package_extraction_timeout",
                path => TarGz,
                reason => extraction_time_limit_exceeded,
                limit_ms => Timeout
            }};
        {error, {worker_down, Reason}} ->
            {error, #{
                msg => "bad_plugin_package",
                path => TarGz,
                reason => {extract_crashed, Reason}
            }}
    end.

map_extract_result(_TarGz, {ok, TarContent}) ->
    {ok, TarContent};
map_extract_result(TarGz, {error, {_, enoent}}) ->
    {error, #{
        msg => "failed_to_extract_plugin_package",
        path => TarGz,
        reason => plugin_tarball_not_found
    }};
map_extract_result(TarGz, {error, Reason}) ->
    {error, #{
        msg => "bad_plugin_package",
        path => TarGz,
        reason => Reason
    }}.

validate_decompressed_size(TarContent) ->
    Limit = max_decompressed_size(),
    Total = lists:foldl(
        fun({_Name, Bin}, Acc) -> Acc + byte_size(Bin) end,
        0,
        TarContent
    ),
    case Total > Limit of
        true ->
            {error, #{
                msg => "decompressed_content_too_large",
                reason => decompressed_size_limit_exceeded,
                size => Total,
                limit => Limit
            }};
        false ->
            ok
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

-doc """
The name-vsn of the plugin whose install directory holds `Path`.
Return `error` when `Path` is not inside the install directory of a plugin.
""".
-spec name_vsn_of_path(file:filename_all()) -> {ok, binary()} | error.
name_vsn_of_path(Path) ->
    InstallDirParts = filename:split(filename:absname(wrap_to_list(install_dir()))),
    PathParts = filename:split(filename:absname(wrap_to_list(Path))),
    case lists:prefix(InstallDirParts, PathParts) of
        true ->
            case lists:nthtail(length(InstallDirParts), PathParts) of
                [NameVsn, _ | _] -> {ok, iolist_to_binary(NameVsn)};
                _ -> error
            end;
        false ->
            error
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

install_dir() ->
    emqx_config:get([?CONF_ROOT, install_dir], "").

%% Defaults must mirror emqx_plugins_schema. The two-arg form is used so
%% that a hot-upgraded node whose config map predates `package_limits'
%% still gets bounded extraction instead of a config_not_found crash.
max_package_size() ->
    emqx_config:get([?CONF_ROOT, package_limits, max_package_size], 10 * 1024 * 1024).

max_decompressed_size() ->
    emqx_config:get([?CONF_ROOT, package_limits, max_decompressed_size], 50 * 1024 * 1024).

max_file_count() ->
    emqx_config:get([?CONF_ROOT, package_limits, max_file_count], 10000).

max_path_depth() ->
    emqx_config:get([?CONF_ROOT, package_limits, max_path_depth], 32).

max_extraction_time_ms() ->
    emqx_config:get([?CONF_ROOT, package_limits, max_extraction_time_ms], 60_000).

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
    case plugin_data_dir_name(NameVsn) of
        {ok, Name} ->
            wrap_to_list(filename:join([emqx:data_dir(), "plugins", Name]));
        error ->
            {error, Reason} = bad_package_name(NameVsn),
            error(Reason)
    end.

plugin_data_dir_name(NameVsn) ->
    case
        {emqx_plugins_utils:split_name_vsn(NameVsn), emqx_plugins_utils:validate_name_vsn(NameVsn)}
    of
        {{Name, _Vsn}, ok} -> {ok, Name};
        _ -> error
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
%% so `"."' and `".."' have to be excluded by hand, and `?PLUGIN_STAGING_DIR' is the
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
            [Root] when Root =/= ".", Root =/= "..", Root =/= ?PLUGIN_STAGING_DIR ->
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
    case emqx_plugins_utils:safe_relative_path(Name) of
        unsafe ->
            {error, #{msg => "unsafe_tar_entry_path"}};
        [] ->
            {error, #{msg => "unsafe_tar_entry_path", reason => resolves_to_install_root}};
        {ok, Safe} ->
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
%% Two properties of this path are load bearing:
%%
%% * it is below the install directory, so the `file:rename/2' which publishes a
%%   staged tree stays within one file system and is atomic;
%% * a staged tree is `<attempt>/<name-vsn>/...', so its `release.json' is never
%%   at the `<install_dir>/*/release.json' depth which `list_name_vsn/0' globs
%%   and it is never mistaken for an installation.  The depth does that, not the
%%   leading dot: `filelib:wildcard/1' does match dot prefixed names.
-spec staging_root() -> file:filename().
staging_root() ->
    wrap_to_list(filename:join(install_dir(), ?PLUGIN_STAGING_DIR)).

%% The staging directory of one installation attempt.  Attempts which were left
%% behind by a crashed process are swept when the application starts; the id
%% counter restarts with the node, so a directory which the sweep could not
%% remove is skipped by trying the next id.
make_staging_dir() ->
    make_staging_dir(10).

make_staging_dir(Retries) ->
    StagingDir = filename:join(staging_root(), unique()),
    %% `filelib:ensure_dir/1' creates the staging root, `file:make_dir/1'
    %% creates the attempt directory itself: an attempt directory which is
    %% already there is a leftover, not a directory two installations share.
    case filelib:ensure_dir(StagingDir) of
        ok ->
            case file:make_dir(StagingDir) of
                ok ->
                    {ok, StagingDir};
                {error, eexist} when Retries > 0 ->
                    make_staging_dir(Retries - 1);
                {error, Reason} ->
                    staging_dir_failed(StagingDir, Reason)
            end;
        {error, Reason} ->
            staging_dir_failed(StagingDir, Reason)
    end.

staging_dir_failed(StagingDir, Reason) ->
    {error, #{
        msg => "failed_to_create_plugin_staging_dir",
        dir => StagingDir,
        reason => Reason
    }}.

%% Remove this attempt's staging directory.  Cleanup is best effort: it never
%% changes the result of the installation.
%%
%% Only this attempt's directory is removed: the staging root is shared with
%% the other attempts, so a concurrent installation can not lose its attempt
%% directory to this cleanup.  What is left of it is swept when the application
%% starts.
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
            Failed = [Dir || Dir <- Dirs, file:del_dir_r(Dir) =/= ok],
            _ = file:del_dir(staging_root()),
            Log = #{
                msg => "cleaned_stale_plugin_staging",
                dir => staging_root(),
                count => length(Dirs) - length(Failed)
            },
            case Failed of
                [] -> ?SLOG(warning, Log);
                _ -> ?SLOG(warning, Log#{failed => Failed})
            end,
            ok
    end.

%% A node-unique, short attempt id: the staging path stays short, and an id
%% which a leftover directory still occupies is skipped by `make_staging_dir/0'.
unique() ->
    integer_to_list(erlang:unique_integer([positive, monotonic])).

%%--------------------------------------------------------------------
%% The install lock, and publishing a staged package
%%--------------------------------------------------------------------

%% Run `Fun' while holding the installation lock of `NameVsn''s plugin.
%%
%% The lock covers the state check (a tree which a concurrent attempt has
%% published but not validated yet is not an installation) and the installation
%% which follows: move the installed tree away, rename the staged tree into
%% place, validate it, roll back or discard what was replaced.  A caller which
%% also writes or snapshots the package file takes the lock around that too, so
%% the bytes the unpack reads are not replaced under it.
%%
%% The lock is per plugin and per node (`emqx_plugins_install_serializer'), and
%% an occupied one refuses the operation instead of queueing it.
with_installation_lock(NameVsn, Fun) ->
    emqx_plugins_install_serializer:run(NameVsn, Fun).

do_publish_validated_package(NameVsn, StagingDir, InstallValidator) ->
    Target = plugin_dir(NameVsn),
    Staged = filename:join(StagingDir, NameVsn),
    %% `file:rename/2' can not atomically replace a non-empty directory, so the
    %% installed tree is moved away before the staged one is renamed into place:
    %% the plugin directory does not exist for a short moment, and a reader
    %% which does not hold the installation lock can see the plugin as missing.
    case move_away(Target) of
        {ok, Previous} ->
            case file:rename(Staged, Target) of
                ok ->
                    validate_published_package(NameVsn, Previous, InstallValidator);
                {error, Reason} ->
                    _ = restore_previous(NameVsn, Target, Previous),
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
%% Whatever the validator does (it may raise, it talks to the config schema
%% store), the installation which was there before is put back when the tree is
%% refused.  The side effects of the validator itself are not restored.
%%
%% A rollback which does not happen leaves the refused tree published: the
%% error then carries `rollback => not_restored' so that the caller keeps the
%% package it wrote.  When the validator raises, this has to be reported
%% together with the original exception: the exception is raised again as
%% `{tree_not_restored, Class, Reason, RollbackError}' after this function's
%% rollback attempt, which the upload path understands.
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
            ?SLOG(warning, #{
                msg => "rolled_back_plugin_install",
                name_vsn => NameVsn,
                reason => Reason
            }),
            rollback_staged_package(NameVsn, Previous, Reason)
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "rolled_back_plugin_install_after_validator_crash",
                name_vsn => NameVsn,
                class => Class,
                reason => Reason
            }),
            case rollback_staged_package(NameVsn, Previous, Reason) of
                {error, #{rollback := not_restored} = RollbackError} ->
                    erlang:raise(
                        error, {tree_not_restored, Class, Reason, RollbackError}, Stacktrace
                    );
                _RolledBack ->
                    erlang:raise(Class, Reason, Stacktrace)
            end
    end.

%% Put the previous installation back, and report a rollback which did not
%% happen: the refused tree is still published then, and `install_state/1`
%% describes something other than what the caller was told.
rollback_staged_package(NameVsn, Previous, Reason) ->
    case unpublish_staged_package(NameVsn, Previous) of
        ok ->
            {error, Reason};
        {error, RollbackReason} ->
            ?SLOG(error, #{
                msg => "failed_to_rollback_plugin_install",
                name_vsn => NameVsn,
                reason => Reason,
                rollback_reason => RollbackReason
            }),
            {error, #{
                msg => "failed_to_rollback_plugin_install",
                name_vsn => NameVsn,
                reason => Reason,
                rollback_reason => RollbackReason,
                rollback => not_restored
            }}
    end.

%% Move the installed tree of the plugin (if any) into the staging directory.
%%
%% A target which is not a directory (a leftover file, a broken symlink) is
%% moved away just the same: the target has to be gone before the new tree is
%% renamed into place.
move_away(Target) ->
    case file:read_link_info(Target) of
        {error, enoent} ->
            {ok, none};
        {error, Reason} ->
            {error, Reason};
        {ok, _} ->
            Replaced = filename:join(staging_root(), "replaced-" ++ unique()),
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
    case file:del_dir_r(Target) of
        ok ->
            rename_previous(NameVsn, Target, Path);
        {error, enoent} ->
            rename_previous(NameVsn, Target, Path);
        {error, Reason} ->
            {error, Reason}
    end.

rename_previous(NameVsn, Target, Path) ->
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
            {error, Reason}
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
    Target = plugin_dir(NameVsn),
    case file:del_dir_r(Target) of
        ok ->
            restore_previous(NameVsn, Target, Previous);
        {error, enoent} ->
            restore_previous(NameVsn, Target, Previous);
        {error, _} = Error ->
            Error
    end.

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
