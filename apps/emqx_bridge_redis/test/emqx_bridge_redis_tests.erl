%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redis_tests).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Verifies that lists of results are returned as lists, so that each original query has its
individual result matched, and other results are left alone.
""".
map_batch_result_test_() ->
    [
        ?_assertMatch(
            [ok, {ok, _}, {error, _}, {error, _}],
            emqx_bridge_redis_connector:map_batch_result(
                {error, [
                    ok, {ok, 1}, {error, {recoverable_error, a}}, {error, {unrecoverable_error, b}}
                ]}
            )
        ),
        ?_assertMatch(
            [ok, {ok, _}],
            emqx_bridge_redis_connector:map_batch_result(
                {ok, [ok, {ok, 1}]}
            )
        ),
        ?_assertMatch(
            {error, {recoverable_error, no_connection}},
            emqx_bridge_redis_connector:map_batch_result(
                {error, {recoverable_error, no_connection}}
            )
        )
    ].
