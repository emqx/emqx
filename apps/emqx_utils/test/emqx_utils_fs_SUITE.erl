%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_fs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Root = ?config(data_dir, Config),
    D1 = filename:join([Root, "nonempty", "d1/"]),
    D2 = filename:join([Root, "nonempty", "d2/"]),
    F1 = filename:join([D1, "1"]),
    F2 = filename:join([D1, "2"]),
    DeepDir = filename:join([Root, "nonempty", "d2", "deep", "down/"]),
    DeepFile = filename:join([DeepDir, "here"]),
    ok = filelib:ensure_dir(F1),
    ok = filelib:ensure_dir(F2),
    ok = filelib:ensure_dir(DeepFile),
    D1Mutrec = filename:join([D1, "mutrec"]),
    D2Mutrec = filename:join([D2, "deep", "mutrec"]),
    ok = file:write_file(F1, <<"">>, [write]),
    ok = file:write_file(F2, <<"">>, [write]),
    ok = file:write_file(DeepFile, <<"">>, [write]),
    {ok, D1FileInfo} = file:read_file_info(D1),
    ok = file:write_file_info(D1, D1FileInfo#file_info{mode = 8#00777}),
    _ = file:delete(D1Mutrec),
    _ = file:delete(D2Mutrec),
    ok = file:make_symlink(DeepDir, D1Mutrec),
    %% can't file:make_link("../../d1", D2Mutrec) on mac, it return {error, eperm}
    ok = file:make_symlink("../../d1", D2Mutrec),
    {ok, DeepFileInfo} = file:read_file_info(DeepFile),
    ok = file:write_file_info(DeepFile, DeepFileInfo#file_info{mode = 8#00600}),
    {ok, D2MutrecInfo} = file:read_link_info(D2Mutrec),
    ct:pal("~p~n", [D2MutrecInfo]),
    %ok = file:write_link_info(D2Mutrec, D2MutrecInfo#file_info{mode = 8#00777}),
    Config.

end_per_suite(Config) ->
    Root = ?config(data_dir, Config),
    %ok = file:del_dir_r(filename:join([Root, "nonempty"])),
    ok.

%%

t_traverse_dir(Config) ->
    Dir = ?config(data_dir, Config),
    Traversal = lists:sort(emqx_utils_fs:traverse_dir(fun cons_fileinfo/3, [], Dir)),
    ?assertMatch(
        [
            {"nonempty/d1/1", #file_info{type = regular}},
            {"nonempty/d1/2", #file_info{type = regular}},
            {"nonempty/d1/mutrec", #file_info{type = symlink, mode = ARWX}},
            {"nonempty/d2/deep/down/here", #file_info{type = regular, mode = ORW}},
            {"nonempty/d2/deep/mutrec", #file_info{type = symlink, mode = ARWX}}
        ] when
            ((ORW band 8#00600 =:= 8#00600) and
                (ARWX band 8#00777 =:= 8#00777)),

        [{string:prefix(Filename, Dir), Info} || {Filename, Info} <- Traversal]
    ).

t_traverse_symlink(Config) ->
    Dir = filename:join([?config(data_dir, Config), "nonempty", "d1", "mutrec"]),
    ?assertMatch(
        [{Dir, #file_info{type = symlink}}],
        emqx_utils_fs:traverse_dir(fun cons_fileinfo/3, [], Dir)
    ).

t_traverse_symlink_subdir(Config) ->
    Dir = filename:join([?config(data_dir, Config), "nonempty", "d2", "deep", "mutrec", "."]),
    Traversal = lists:sort(emqx_utils_fs:traverse_dir(fun cons_fileinfo/3, [], Dir)),
    ?assertMatch(
        [
            {"nonempty/d2/deep/mutrec/1", #file_info{type = regular}},
            {"nonempty/d2/deep/mutrec/2", #file_info{type = regular}},
            {"nonempty/d2/deep/mutrec/mutrec", #file_info{type = symlink}}
        ],
        [
            {string:prefix(Filename, ?config(data_dir, Config)), Info}
         || {Filename, Info} <- Traversal
        ]
    ).

t_traverse_empty(Config) ->
    Dir = filename:join(?config(data_dir, Config), "empty"),
    _ = file:make_dir(Dir),
    ?assertEqual(
        [],
        emqx_utils_fs:traverse_dir(fun cons_fileinfo/3, [], Dir)
    ).

t_traverse_nonexisting(_) ->
    ?assertEqual(
        [{"this/should/not/exist", {error, enoent}}],
        emqx_utils_fs:traverse_dir(fun cons_fileinfo/3, [], "this/should/not/exist")
    ).

cons_fileinfo(Filename, Info, Acc) ->
    [{Filename, Info} | Acc].

%%

t_canonicalize_empty(_) ->
    ?assertEqual(
        "",
        emqx_utils_fs:canonicalize(<<>>)
    ).

t_canonicalize_relative(_) ->
    ?assertEqual(
        "rel",
        emqx_utils_fs:canonicalize(<<"rel/">>)
    ).

t_canonicalize_trailing_slash(_) ->
    ?assertEqual(
        "/usr/local",
        emqx_utils_fs:canonicalize("/usr/local/")
    ).

t_canonicalize_double_slashes(_) ->
    ?assertEqual(
        "/usr/local/.",
        emqx_utils_fs:canonicalize("//usr//local//.//")
    ).

t_canonicalize_non_utf8(_) ->
    ?assertError(
        badarg,
        emqx_utils_fs:canonicalize(<<128, 128, 128>>)
    ).
