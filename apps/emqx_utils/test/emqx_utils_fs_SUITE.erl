%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Files = [F1, F2, DeepFile],
    lists:foreach(fun filelib:ensure_dir/1, Files),
    D1LinkMutrec = filename:join([D1, "mutrec"]),
    D2LinkMutrec = filename:join([D2, "deep", "mutrec"]),
    lists:foreach(fun(File) -> file:write_file(File, <<"">>, [write]) end, Files),
    chmod_file(D1, 8#00777),
    chmod_file(DeepFile, 8#00600),
    make_symlink(DeepDir, D1LinkMutrec),
    %% can't file:make_link("../../d1", D2Mutrec) on mac, return {error, eperm}
    make_symlink("../../d1", D2LinkMutrec),
    {ok, D2MutrecInfo} = file:read_link_info(D2LinkMutrec),
    ct:pal("~ts 's file_info is ~p~n", [D2LinkMutrec, D2MutrecInfo]),
    Config.

end_per_suite(Config) ->
    Root = ?config(data_dir, Config),
    ok = file:del_dir_r(filename:join([Root, "nonempty"])),
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
            (((ORW band 8#00600 =:= 8#00600) and
                (ARWX band 8#00777 =:= 8#00777))),

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

%%

t_find_relpath(_) ->
    ?assertEqual(
        "d1/1",
        emqx_utils_fs:find_relpath("/usr/local/nonempty/d1/1", "/usr/local/nonempty")
    ).

t_find_relpath_same(_) ->
    ?assertEqual(
        ".",
        emqx_utils_fs:find_relpath("/usr/local/bin", "/usr/local/bin/")
    ),
    ?assertEqual(
        ".",
        emqx_utils_fs:find_relpath("/usr/local/bin/.", "/usr/local/bin")
    ).

t_find_relpath_no_prefix(_) ->
    ?assertEqual(
        "/usr/lib/erlang/lib",
        emqx_utils_fs:find_relpath("/usr/lib/erlang/lib", "/usr/local/bin")
    ).

t_find_relpath_both_relative(_) ->
    ?assertEqual(
        "1/2/3",
        emqx_utils_fs:find_relpath("local/nonempty/1/2/3", "local/nonempty")
    ).

t_find_relpath_different_types(_) ->
    ?assertEqual(
        "local/nonempty/1/2/3",
        emqx_utils_fs:find_relpath("local/nonempty/1/2/3", "/usr/local/nonempty")
    ).

%%

chmod_file(File, Mode) ->
    {ok, FileInfo} = file:read_file_info(File),
    ok = file:write_file_info(File, FileInfo#file_info{mode = Mode}).

make_symlink(FileOrDir, NewLink) ->
    _ = file:delete(NewLink),
    ok = file:make_symlink(FileOrDir, NewLink).
