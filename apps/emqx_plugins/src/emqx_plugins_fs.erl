%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_fs).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("kernel/include/file.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([normalize_dir/1, top_dir/2, is_safe_entry/2]).
-endif.

%% Tarfile operations
-export([
    get_tar/1,
    write_tar/2,
    delete_tar/1,
    is_tar_present/1
]).

%% Unpack plugin tar/delete unpacked content
-export([
    ensure_installed_from_tar/2,
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
    max_extraction_time_ms/0
]).

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
    end.

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
    case iolist_size(Content) > max_package_size() of
        true ->
            {error, #{
                msg => "package_too_large",
                reason => package_size_limit_exceeded,
                size => iolist_size(Content),
                limit => max_package_size()
            }};
        false ->
            TarFilePath = tar_file_path(NameVsn),
            ok = filelib:ensure_dir(TarFilePath),
            ok = file:write_file(TarFilePath, Content),
            MD5 = emqx_utils:bin_to_hexstr(crypto:hash(md5, Content), lower),
            ok = file:write_file(md5sum_file_path(NameVsn), MD5)
    end.

%%--------------------------------------------------------------------
%% Plugin package extraction
%%--------------------------------------------------------------------

install_from_local_tar(NameVsn, InstallValidator) ->
    TarGz = tar_file_path(NameVsn),
    maybe
        ok ?= validate_package_file_size(TarGz),
        ok ?= validate_decompressed_storage_limits(TarGz),
        {ok, TarContent} ?= extract_tarball(TarGz),
        ok ?= validate_decompressed_size(TarContent),
        ok ?= write_tar_file_content(install_dir(), TarContent),
        case InstallValidator() of
            ok ->
                ok;
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "failed_to_read_after_install", reason => Reason
                }),
                ok = delete_tar_file_content(install_dir(), TarContent),
                {error, Reason}
        end
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

-spec ensure_installed_from_tar(name_vsn(), fun(() -> ok | {error, term()})) -> ok | {error, map()}.
ensure_installed_from_tar(NameVsn, InstallValidator) ->
    case is_installed(NameVsn) of
        true ->
            InstallValidator();
        false ->
            install_from_local_tar(NameVsn, InstallValidator)
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
