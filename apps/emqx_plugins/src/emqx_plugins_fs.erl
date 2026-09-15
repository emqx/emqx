%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_fs).

-feature(maybe_expr, enable).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

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
    plugin_dir/1
]).

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
    end.

-spec is_tar_present(name_vsn()) ->
    false | {true, [file:filename()]}.
is_tar_present(NameVsn) ->
    TarGz = tar_file_path(NameVsn),
    case filelib:is_regular(TarGz) of
        true -> {true, [TarGz]};
        false -> false
    end.

-spec write_tar(name_vsn(), iodata()) -> ok.
write_tar(NameVsn, Content) ->
    TarFilePath = tar_file_path(NameVsn),
    ok = filelib:ensure_dir(TarFilePath),
    ok = file:write_file(TarFilePath, Content),
    MD5 = emqx_utils:bin_to_hexstr(crypto:hash(md5, Content), lower),
    ok = file:write_file(md5sum_file_path(NameVsn), MD5).

%% @doc Snapshot the package file (and its checksum) as it is on disk, so that
%% it can be put back when a replacement of the installation fails.
%%
%% A package file which is not there is part of the snapshot as `none' (the
%% restore deletes it again); a file which is there but can not be read is a
%% failure instead, see `read_package_file/1'.
-spec backup_package(name_vsn()) -> {ok, package_backup()} | {error, map()}.
backup_package(NameVsn) ->
    maybe
        {ok, Tar} ?= read_package_file(tar_file_path(NameVsn)),
        {ok, Md5sum} ?= read_package_file(md5sum_file_path(NameVsn)),
        {ok, #{tar => Tar, md5sum => Md5sum}}
    end.

%% @doc Put the package file (and its checksum) back as it was before a failed
%% installation attempt: a file which was not there is deleted.
-spec restore_package(name_vsn(), package_backup()) -> ok | {error, term()}.
restore_package(NameVsn, #{tar := Tar, md5sum := Md5sum}) ->
    maybe
        ok ?= restore_file(tar_file_path(NameVsn), Tar),
        ok ?= restore_file(md5sum_file_path(NameVsn), Md5sum)
    end.

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

install_from_local_tar(NameVsn, InstallValidator) ->
    TarGz = tar_file_path(NameVsn),
    case erl_tar:extract(TarGz, [compressed, memory]) of
        {ok, TarContent} ->
            case write_tar_file_content(install_dir(), TarContent) of
                ok ->
                    case validate_unpacked(NameVsn, InstallValidator) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            ?SLOG(warning, #{
                                msg => "failed_to_read_after_install", reason => Reason
                            }),
                            ok = delete_tar_file_content(install_dir(), TarContent),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
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
    end.

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

-spec delete_tar(name_vsn()) -> ok.
delete_tar(NameVsn) ->
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
    end.

-spec purge_installed(name_vsn()) -> ok | {error, term()}.
purge_installed(NameVsn) ->
    Dir = plugin_dir(NameVsn),
    purge_plugin_dir(Dir).

-spec ensure_config_dir(name_vsn()) -> ok | {error, term()}.
ensure_config_dir(NameVsn) ->
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
    end.

-spec lib_dir(name_vsn()) -> string().
lib_dir(NameVsn) ->
    wrap_to_list(filename:join([install_dir(), NameVsn])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

install_dir() ->
    emqx_config:get([?CONF_ROOT, install_dir], "").

plugin_dir(NameVsn) ->
    wrap_to_list(filename:join([install_dir(), NameVsn])).

tar_file_path(NameVsn) ->
    wrap_to_list(
        filename:join([install_dir(), unicode:characters_to_binary([NameVsn, ".tar.gz"])])
    ).

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
                    _ -> wrap_to_list(filename:join([install_dir(), NameVsn, "priv"]))
                end
        end
    else
        %% Otherwise assume the priv directory is under the plugin root directory
        _ -> wrap_to_list(filename:join([install_dir(), NameVsn, "priv"]))
    end.

plugin_data_dir(NameVsn) ->
    {NameAtom, _Vsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    wrap_to_list(filename:join([emqx:data_dir(), "plugins", atom_to_list(NameAtom)])).

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

write_tar_file_content(BaseDir, TarContent) ->
    %% Validate every entry up front. A single zip-slip entry must not
    %% leave half-written legitimate entries behind on disk.
    case unsafe_entries(BaseDir, TarContent) of
        [] ->
            lists:foreach(
                fun({Name, Bin}) ->
                    Filename = filename:join(BaseDir, Name),
                    ok = filelib:ensure_dir(Filename),
                    ok = file:write_file(Filename, Bin)
                end,
                TarContent
            );
        [_ | _] = Unsafe ->
            {error, #{
                msg => "unsafe_tar_entry_path",
                hint => "tar entries must stay under the install dir",
                entries => Unsafe
            }}
    end.

delete_tar_file_content(BaseDir, TarContent) ->
    %% Defense in depth: never follow a tar entry path that escapes
    %% BaseDir, even on the cleanup path.
    SafeEntries = [E || {Name, _} = E <- TarContent, is_safe_entry(BaseDir, Name)],
    lists:foreach(
        fun({Name, _}) ->
            Filename = filename:join(BaseDir, Name),
            maybe
                true ?= filelib:is_file(Filename),
                {ok, TopDirOrFile} ?= top_dir(BaseDir, Filename),
                ok ?= file:del_dir_r(TopDirOrFile)
            end
        end,
        SafeEntries
    ).

unsafe_entries(BaseDir, TarContent) ->
    [Name || {Name, _} <- TarContent, not is_safe_entry(BaseDir, Name)].

is_safe_entry(BaseDir, Name) ->
    case filelib:safe_relative_path(Name, BaseDir) of
        unsafe -> false;
        _ -> true
    end.

top_dir(BaseDir0, DirOrFile) ->
    BaseDir = normalize_dir(BaseDir0),
    case filename:dirname(DirOrFile) of
        RockBottom when RockBottom =:= "/" orelse RockBottom =:= "." ->
            {error, {out_of_bounds, DirOrFile}};
        BaseDir ->
            {ok, DirOrFile};
        Parent ->
            top_dir(BaseDir, Parent)
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

normalize_dir(Dir) ->
    %% Get rid of possible trailing slash
    filename:join([Dir, ""]).

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
normalize_dir_test_() ->
    [
        ?_assertEqual("foo", normalize_dir("foo")),
        ?_assertEqual("foo", normalize_dir("foo/")),
        ?_assertEqual("/foo", normalize_dir("/foo")),
        ?_assertEqual("/foo", normalize_dir("/foo/"))
    ].

top_dir_test_() ->
    [
        ?_assertEqual({ok, "base/foo"}, top_dir("base", filename:join(["base", "foo", "bar"]))),
        ?_assertEqual(
            {ok, "/base/foo"}, top_dir("/base", filename:join(["/", "base", "foo", "bar"]))
        ),
        ?_assertEqual(
            {ok, "/base/foo"}, top_dir("/base/", filename:join(["/", "base", "foo", "bar"]))
        ),
        ?_assertMatch({error, {out_of_bounds, _}}, top_dir("/base", filename:join(["/", "base"]))),
        ?_assertMatch(
            {error, {out_of_bounds, _}}, top_dir("/base", filename:join(["/", "foo", "bar"]))
        )
    ].

is_safe_entry_test_() ->
    %% Use cwd as a real existing directory; safe_relative_path/2 needs that.
    {ok, Cwd} = file:get_cwd(),
    [
        ?_assert(is_safe_entry(Cwd, "evil-1.0.0/release.json")),
        ?_assert(is_safe_entry(Cwd, "deep/nested/path.txt")),
        ?_assertNot(is_safe_entry(Cwd, "../escape")),
        ?_assertNot(is_safe_entry(Cwd, "../../../tmp/pwned")),
        ?_assertNot(is_safe_entry(Cwd, "evil/../../../tmp/pwned")),
        ?_assertNot(is_safe_entry(Cwd, "/abs/path"))
    ].
-endif.
