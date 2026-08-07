%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_fs_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

normalize_dir_test_() ->
    [
        ?_assertEqual("foo", emqx_plugins_fs:normalize_dir("foo")),
        ?_assertEqual("foo", emqx_plugins_fs:normalize_dir("foo/")),
        ?_assertEqual("/foo", emqx_plugins_fs:normalize_dir("/foo")),
        ?_assertEqual("/foo", emqx_plugins_fs:normalize_dir("/foo/"))
    ].

top_dir_test_() ->
    [
        ?_assertEqual(
            {ok, "base/foo"}, emqx_plugins_fs:top_dir("base", filename:join(["base", "foo", "bar"]))
        ),
        ?_assertEqual(
            {ok, "/base/foo"},
            emqx_plugins_fs:top_dir("/base", filename:join(["/", "base", "foo", "bar"]))
        ),
        ?_assertEqual(
            {ok, "/base/foo"},
            emqx_plugins_fs:top_dir("/base/", filename:join(["/", "base", "foo", "bar"]))
        ),
        ?_assertMatch(
            {error, {out_of_bounds, _}},
            emqx_plugins_fs:top_dir("/base", filename:join(["/", "base"]))
        ),
        ?_assertMatch(
            {error, {out_of_bounds, _}},
            emqx_plugins_fs:top_dir("/base", filename:join(["/", "foo", "bar"]))
        )
    ].

is_safe_entry_test_() ->
    %% Use cwd as a real existing directory; safe_relative_path/2 needs that.
    {ok, Cwd} = file:get_cwd(),
    [
        ?_assert(emqx_plugins_fs:is_safe_entry(Cwd, "evil-1.0.0/release.json")),
        ?_assert(emqx_plugins_fs:is_safe_entry(Cwd, "deep/nested/path.txt")),
        ?_assertNot(emqx_plugins_fs:is_safe_entry(Cwd, "../escape")),
        ?_assertNot(emqx_plugins_fs:is_safe_entry(Cwd, "../../../tmp/pwned")),
        ?_assertNot(emqx_plugins_fs:is_safe_entry(Cwd, "evil/../../../tmp/pwned")),
        ?_assertNot(emqx_plugins_fs:is_safe_entry(Cwd, "/abs/path"))
    ].
