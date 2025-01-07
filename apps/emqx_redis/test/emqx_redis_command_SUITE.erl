%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_redis_command_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_split_ok(_Config) ->
    ?assertEqual(
        {ok, [<<"ab">>, <<"cd">>, <<"ef">>]},
        emqx_redis_command:split(<<" \"ab\" 'cd' ef ">>)
    ),
    ?assertEqual(
        {ok, [<<"ab">>, <<"cd">>, <<"ef">>]},
        emqx_redis_command:split(<<" ab\tcd ef">>)
    ),
    ?assertEqual(
        {ok, [<<"abc'd">>, <<"ef">>]},
        emqx_redis_command:split(<<"ab\"c'd\" ef">>)
    ),
    ?assertEqual(
        {ok, [<<"abc\"d">>, <<"ef">>]},
        emqx_redis_command:split(<<"ab'c\"d' ef">>)
    ),
    ?assertEqual(
        {ok, [<<"IJK">>, <<"\\x49\\x4a\\x4B">>]},
        emqx_redis_command:split(<<"\"\\x49\\x4a\\x4B\" \\x49\\x4a\\x4B">>)
    ),
    ?assertEqual(
        {ok, [<<"x\t\n\r\b\v">>]},
        emqx_redis_command:split(<<"\"\\x\\t\\n\\r\\b\\a\"">>)
    ),
    ?assertEqual(
        {ok, [<<"abc\'d">>, <<"ef">>]},
        emqx_redis_command:split(<<"'abc\\'d' ef">>)
    ),
    ?assertEqual(
        {ok, [<<>>, <<>>]},
        emqx_redis_command:split(<<" '' \"\" ">>)
    ).

t_split_error(_Config) ->
    ?assertEqual(
        {error, trailing_after_quote},
        emqx_redis_command:split(<<"\"a\"b">>)
    ),
    ?assertEqual(
        {error, unterminated_quote},
        emqx_redis_command:split(<<"\"ab">>)
    ),
    ?assertEqual(
        {error, trailing_after_single_quote},
        emqx_redis_command:split(<<"'a'b'c">>)
    ),
    ?assertEqual(
        {error, unterminated_single_quote},
        emqx_redis_command:split(<<"'ab">>)
    ).
