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

-module(emqx_ft_fs_util_tests).

-include_lib("eunit/include/eunit.hrl").

filename_safe_test_() ->
    [
        ?_assertEqual(ok, emqx_ft_fs_util:is_filename_safe("im.safe")),
        ?_assertEqual(ok, emqx_ft_fs_util:is_filename_safe(<<"im.safe">>)),
        ?_assertEqual(ok, emqx_ft_fs_util:is_filename_safe(<<".safe.100%">>)),
        ?_assertEqual(ok, emqx_ft_fs_util:is_filename_safe(<<"safe.as.🦺"/utf8>>))
    ].

filename_unsafe_test_() ->
    [
        ?_assertEqual({error, empty}, emqx_ft_fs_util:is_filename_safe("")),
        ?_assertEqual({error, special}, emqx_ft_fs_util:is_filename_safe(".")),
        ?_assertEqual({error, special}, emqx_ft_fs_util:is_filename_safe("..")),
        ?_assertEqual({error, special}, emqx_ft_fs_util:is_filename_safe(<<"..">>)),
        ?_assertEqual({error, unsafe}, emqx_ft_fs_util:is_filename_safe(<<".././..">>)),
        ?_assertEqual({error, unsafe}, emqx_ft_fs_util:is_filename_safe("/etc/passwd")),
        ?_assertEqual({error, unsafe}, emqx_ft_fs_util:is_filename_safe("../cookie")),
        ?_assertEqual({error, unsafe}, emqx_ft_fs_util:is_filename_safe("C:$cookie")),
        ?_assertEqual({error, nonprintable}, emqx_ft_fs_util:is_filename_safe([1, 2, 3])),
        ?_assertEqual({error, nonprintable}, emqx_ft_fs_util:is_filename_safe(<<4, 5, 6>>)),
        ?_assertEqual({error, nonprintable}, emqx_ft_fs_util:is_filename_safe([$a, 16#7F, $z]))
    ].

-define(NAMES, [
    {"just.file", <<"just.file">>},
    {".hidden", <<".hidden">>},
    {".~what", <<".~what">>},
    {"100%25.file", <<"100%.file">>},
    {"%2E%2E", <<"..">>},
    {"...", <<"...">>},
    {"%2Fetc%2Fpasswd", <<"/etc/passwd">>},
    {"%01%02%0A ", <<1, 2, 10, 32>>}
]).

escape_filename_test_() ->
    [
        ?_assertEqual(Filename, emqx_ft_fs_util:escape_filename(Input))
     || {Filename, Input} <- ?NAMES
    ].

unescape_filename_test_() ->
    [
        ?_assertEqual(Input, emqx_ft_fs_util:unescape_filename(Filename))
     || {Filename, Input} <- ?NAMES
    ].

mk_temp_filename_test_() ->
    [
        ?_assertMatch(
            "." ++ Suffix when length(Suffix) == 16,
            emqx_ft_fs_util:mk_temp_filename(<<>>)
        ),
        ?_assertMatch(
            "file.name." ++ Suffix when length(Suffix) == 16,
            emqx_ft_fs_util:mk_temp_filename("file.name")
        ),
        ?_assertMatch(
            "safe.🦺." ++ Suffix when length(Suffix) == 16,
            emqx_ft_fs_util:mk_temp_filename(<<"safe.🦺"/utf8>>)
        ),
        ?_assertEqual(
            % FilenameSlice + Dot + Suffix
            200 + 1 + 16,
            length(emqx_ft_fs_util:mk_temp_filename(lists:duplicate(63, "LONG")))
        )
    ].
