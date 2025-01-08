%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_fs_util_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/file.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%├── a
%%│   ├── b
%%│   │  └── foo
%%│   │     ├── 42
%%│   │     └── Я
%%│   └── link -> ../c
%%├── c
%%│   ├── bar
%%│   │  └── 中文
%%│   └── link -> ../a
%%└── d
%%    ├── e
%%    │  └── baz
%%    │      └── needle
%%    └── haystack

init_per_suite(Config) ->
    Root = ?config(data_dir, Config),
    A = filename:join([Root, "a", "b", "foo"]),
    C = filename:join([Root, "c", "bar"]),
    D = filename:join([Root, "d", "e", "baz"]),

    F42 = filename:join([A, "42"]),
    F42_1 = filename:join([A, "Я"]),
    FBar = filename:join([C, "中文"]),
    FNeedle = filename:join([D, "needle"]),
    FHayStack = filename:join([Root, "d", "haystack"]),
    Files = [F42, F42_1, FBar, FNeedle, FHayStack],
    lists:foreach(fun filelib:ensure_dir/1, Files),
    %% create files
    lists:foreach(fun(File) -> file:write_file(File, <<"">>, [write]) end, Files),
    %% create links
    ALink = filename:join([Root, "a", "link"]),
    CLink = filename:join([Root, "c", "link"]),
    make_symlink("../c", ALink),
    make_symlink("../a", CLink),
    Config.

end_per_suite(Config) ->
    Root = ?config(data_dir, Config),
    ok = file:del_dir_r(filename:join([Root, "a"])),
    ok = file:del_dir_r(filename:join([Root, "c"])),
    ok = file:del_dir_r(filename:join([Root, "d"])),
    ok.

t_fold_single_level(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [
            {"a", #file_info{type = directory}, ["a"]},
            {"c", #file_info{type = directory}, ["c"]},
            {"d", #file_info{type = directory}, ["d"]}
        ],
        sort(fold(fun cons/4, [], Root, ['*']))
    ).

t_fold_multi_level(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [
            {"a/b/foo/42", #file_info{type = regular}, ["42", "foo", "b", "a"]},
            {"a/b/foo/Я", #file_info{type = regular}, ["Я", "foo", "b", "a"]},
            {"d/e/baz/needle", #file_info{type = regular}, ["needle", "baz", "e", "d"]}
        ],
        sort(fold(fun cons/4, [], Root, ['*', '*', '*', '*']))
    ),
    ?assertMatch(
        [
            {"a/b/foo", #file_info{type = directory}, ["foo", "b", "a"]},
            {"c/bar/中文", #file_info{type = regular}, ["中文", "bar", "c"]},
            {"d/e/baz", #file_info{type = directory}, ["baz", "e", "d"]}
        ],
        sort(fold(fun cons/4, [], Root, ['*', '*', '*']))
    ).

t_fold_no_glob(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [{"", #file_info{type = directory}, []}],
        sort(fold(fun cons/4, [], Root, []))
    ).

t_fold_glob_too_deep(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [],
        sort(fold(fun cons/4, [], Root, ['*', '*', '*', '*', '*']))
    ).

t_fold_invalid_root(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [],
        sort(fold(fun cons/4, [], filename:join([Root, "a", "link"]), ['*']))
    ),
    ?assertMatch(
        [],
        sort(fold(fun cons/4, [], filename:join([Root, "d", "haystack"]), ['*']))
    ).

t_fold_filter_unicode(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [
            {"a/b/foo/42", #file_info{type = regular}, ["42", "foo", "b", "a"]},
            {"d/e/baz/needle", #file_info{type = regular}, ["needle", "baz", "e", "d"]}
        ],
        sort(fold(fun cons/4, [], Root, ['*', '*', '*', fun is_latin1/1]))
    ),
    ?assertMatch(
        [
            {"a/b/foo/Я", #file_info{type = regular}, ["Я", "foo", "b", "a"]}
        ],
        sort(fold(fun cons/4, [], Root, ['*', '*', '*', is_not(fun is_latin1/1)]))
    ).

t_fold_filter_levels(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [
            {"a/b/foo", #file_info{type = directory}, ["foo", "b", "a"]},
            {"d/e/baz", #file_info{type = directory}, ["baz", "e", "d"]}
        ],
        sort(fold(fun cons/4, [], Root, [fun is_letter/1, fun is_letter/1, '*']))
    ).

t_fold_errors(Config) ->
    Root = ?config(data_dir, Config),
    ok = meck:new(emqx_ft_fs_util, [passthrough]),
    ok = meck:expect(emqx_ft_fs_util, read_info, fun(AbsFilepath) ->
        ct:pal("read_info(~p)", [AbsFilepath]),
        Filename = filename:basename(AbsFilepath),
        case Filename of
            "b" -> {error, eacces};
            "link" -> {error, enotsup};
            "bar" -> {error, enotdir};
            "needle" -> {error, ebusy};
            _ -> meck:passthrough([AbsFilepath])
        end
    end),
    ?assertMatch(
        [
            {"a/b", {error, eacces}, ["b", "a"]},
            {"a/link", {error, enotsup}, ["link", "a"]},
            {"c/link", {error, enotsup}, ["link", "c"]},
            {"d/e/baz/needle", {error, ebusy}, ["needle", "baz", "e", "d"]}
        ],
        sort(fold(fun cons/4, [], Root, ['*', '*', '*', '*']))
    ).

t_seek_fold(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [
            {leaf, "a/b/foo/42", #file_info{type = regular}, ["42", "foo", "b", "a"]},
            {leaf, "a/b/foo/Я", #file_info{type = regular}, ["Я", "foo", "b", "a"]},
            {leaf, "d/e/baz/needle", #file_info{type = regular}, ["needle", "baz", "e", "d"]}
            | _Nodes
        ],
        sort(
            emqx_ft_fs_iterator:fold(
                fun cons/2,
                [],
                emqx_ft_fs_iterator:seek(["a", "a"], Root, ['*', '*', '*', '*'])
            )
        )
    ),
    ?assertMatch(
        [
            {leaf, "a/b/foo/Я", #file_info{type = regular}, ["Я", "foo", "b", "a"]},
            {leaf, "d/e/baz/needle", #file_info{type = regular}, ["needle", "baz", "e", "d"]}
            | _Nodes
        ],
        sort(
            emqx_ft_fs_iterator:fold(
                fun cons/2,
                [],
                emqx_ft_fs_iterator:seek(["a", "b", "foo", "42"], Root, ['*', '*', '*', '*'])
            )
        )
    ),
    ?assertMatch(
        [
            {leaf, "d/e/baz/needle", #file_info{type = regular}, ["needle", "baz", "e", "d"]}
            | _Nodes
        ],
        sort(
            emqx_ft_fs_iterator:fold(
                fun cons/2,
                [],
                emqx_ft_fs_iterator:seek(["c", "d", "e", "f"], Root, ['*', '*', '*', '*'])
            )
        )
    ).

t_seek_empty(Config) ->
    Root = ?config(data_dir, Config),
    ?assertEqual(
        emqx_ft_fs_iterator:fold(
            fun cons/2,
            [],
            emqx_ft_fs_iterator:new(Root, ['*', '*', '*', '*'])
        ),
        emqx_ft_fs_iterator:fold(
            fun cons/2,
            [],
            emqx_ft_fs_iterator:seek([], Root, ['*', '*', '*', '*'])
        )
    ).

t_seek_past_end(Config) ->
    Root = ?config(data_dir, Config),
    ?assertEqual(
        none,
        emqx_ft_fs_iterator:next(
            emqx_ft_fs_iterator:seek(["g", "h"], Root, ['*', '*', '*', '*'])
        )
    ).

t_seek_with_filter(Config) ->
    Root = ?config(data_dir, Config),
    ?assertMatch(
        [
            {leaf, "d/e/baz", #file_info{type = directory}, ["baz", "e", "d"]}
            | _Nodes
        ],
        sort(
            emqx_ft_fs_iterator:fold(
                fun cons/2,
                [],
                emqx_ft_fs_iterator:seek(["a", "link"], Root, ['*', fun is_letter/1, '*'])
            )
        )
    ).

%%

fold(FoldFun, Acc, Root, Glob) ->
    emqx_ft_fs_util:fold(FoldFun, Acc, Root, Glob).

is_not(F) ->
    fun(X) -> not F(X) end.

is_latin1(Filename) ->
    case unicode:characters_to_binary(Filename, unicode, latin1) of
        {error, _, _} ->
            false;
        _ ->
            true
    end.

is_letter(Filename) ->
    case Filename of
        [_] ->
            true;
        _ ->
            false
    end.

cons(Path, Info, Stack, Acc) ->
    [{Path, Info, Stack} | Acc].

cons(Entry, Acc) ->
    [Entry | Acc].

sort(L) when is_list(L) ->
    lists:sort(L).

make_symlink(FileOrDir, NewLink) ->
    _ = file:delete(NewLink),
    ok = file:make_symlink(FileOrDir, NewLink).
