%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    read_avsc_bin_all/0,
    read_i18n/1,
    read_hocon/1
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
    {AppName, _Vsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    Wildcard = tar_file_path([bin(AppName), "-*"]),
    case filelib:wildcard(Wildcard) of
        [] -> false;
        TarGzs -> {true, TarGzs}
    end.

-spec write_tar(name_vsn(), iodata()) -> ok.
write_tar(NameVsn, Content) ->
    TarFilePath = tar_file_path(NameVsn),
    ok = filelib:ensure_dir(TarFilePath),
    ok = file:write_file(TarFilePath, Content),
    MD5 = emqx_utils:bin_to_hexstr(crypto:hash(md5, Content), lower),
    ok = file:write_file(md5sum_file_path(NameVsn), MD5).

%%--------------------------------------------------------------------
%% Plugin package extraction
%%--------------------------------------------------------------------

install_from_local_tar(NameVsn, InstallValidator) ->
    TarGz = tar_file_path(NameVsn),
    case erl_tar:extract(TarGz, [compressed, memory]) of
        {ok, TarContent} ->
            ok = write_tar_file_content(install_dir(), TarContent),
            case InstallValidator() of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{msg => "failed_to_read_after_install", reason => Reason}),
                    ok = delete_tar_file_content(install_dir(), TarContent),
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

-spec ensure_installed_from_tar(name_vsn(), fun(() -> ok | {error, term()})) -> ok | {error, map()}.
ensure_installed_from_tar(NameVsn, InstallValidator) ->
    case is_installed(NameVsn) of
        true ->
            ok;
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

plugin_dir(NameVsn) ->
    wrap_to_list(filename:join([install_dir(), NameVsn])).

tar_file_path(NameVsn) ->
    wrap_to_list(filename:join([install_dir(), bin([NameVsn, ".tar.gz"])])).

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
        {ok, AppDir} ?= app_dir(Name, Apps),
        wrap_to_list(filename:join([plugin_dir(NameVsn), AppDir, "priv"]))
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
    lists:foreach(
        fun({Name, Bin}) ->
            Filename = filename:join(BaseDir, Name),
            ok = filelib:ensure_dir(Filename),
            ok = file:write_file(Filename, Bin)
        end,
        TarContent
    ).

delete_tar_file_content(BaseDir, TarContent) ->
    lists:foreach(
        fun({Name, _}) ->
            Filename = filename:join(BaseDir, Name),
            maybe
                true ?= filelib:is_file(Filename),
                {ok, TopDirOrFile} ?= top_dir(BaseDir, Filename),
                ok ?= file:del_dir_r(TopDirOrFile)
            end
        end,
        TarContent
    ).

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

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8).

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
-endif.
