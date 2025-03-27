%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
        fun(#{<<"name">> := Name, <<"rel_vsn">> := Vsn}) ->
            emqx_plugins:purge(bin([Name, "-", Vsn]))
        end,
        emqx_plugins:list()
    ).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.
