%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
