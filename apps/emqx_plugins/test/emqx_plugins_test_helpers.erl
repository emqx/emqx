%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(PACKAGE_SUFFIX, ".tar.gz").

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
    {ok, {_, _, PluginBin}} = httpc:request(FileURI),
    Pkg = filename:join([
        WorkDir,
        TargetName
    ]),
    ok = file:write_file(Pkg, PluginBin),
    Opts#{
        package => Pkg,
        name_vsn => bin([ReleaseName, "-", PluginVsn])
    }.

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
