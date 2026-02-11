%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(PACKAGE_SUFFIX, ".tar.gz").
-define(DOWNLOAD_RETRIES, 3).
-define(HTTP_TIMEOUT, 30_000).

get_demo_plugin_package(
    #{
        release_name := ReleaseName,
        git_url := GitUrl,
        vsn := PluginVsn,
        tag := ReleaseTag,
        shdir := WorkDir
    } = Opts
) ->
    TargetName = lists:flatten([ReleaseName, "-", PluginVsn, ?PACKAGE_SUFFIX]),
    FileURI = lists:flatten(lists:join("/", [GitUrl, ReleaseTag, TargetName])),
    PluginBin = download_plugin_package(FileURI, ?DOWNLOAD_RETRIES),
    Pkg = filename:join([
        WorkDir,
        TargetName
    ]),
    ok = file:write_file(Pkg, PluginBin),
    Opts#{
        package => Pkg,
        name_vsn => bin([ReleaseName, "-", PluginVsn])
    }.

download_plugin_package(_FileURI, 0) ->
    error({failed_to_download_plugin_package, retries_exhausted});
download_plugin_package(FileURI, RetriesLeft) ->
    case httpc:request(get, {FileURI, []}, [{timeout, ?HTTP_TIMEOUT}], [{body_format, binary}]) of
        {ok, {{_HTTP, 200, _Reason}, _Headers, PluginBin}} when is_binary(PluginBin) ->
            case is_valid_tar_gz(PluginBin) of
                true ->
                    PluginBin;
                false ->
                    download_plugin_package(FileURI, RetriesLeft - 1)
            end;
        _ ->
            download_plugin_package(FileURI, RetriesLeft - 1)
    end.

is_valid_tar_gz(PluginBin) ->
    case erl_tar:extract({binary, PluginBin}, [compressed, memory]) of
        {ok, _} -> true;
        {error, _} -> false
    end.

purge_plugins() ->
    emqx_plugins:put_configured([]),
    lists:foreach(
        fun(#{name := Name, rel_vsn := Vsn}) ->
            emqx_plugins:purge(bin([Name, "-", Vsn]))
        end,
        emqx_plugins:list()
    ).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.
