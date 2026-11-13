%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_redis_tests).

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
Checks whether several query errors are classified as recoverable or unrecoverable.

Those reportedly require complicated timings and killing/restarting/resharding servers
whiles queries are executed, so we resort to mocking them here, by just asserting that we
classify them accordingly.
""".
map_result_test_() ->
    Cases = [
        #{error => tcp_closed, class => recoverable},
        #{error => ssl_closed, class => recoverable},
        #{error => no_connection, class => recoverable},
        #{error => ~"CLUSTERDOWN mocked", class => recoverable},
        #{error => ~"TRYAGAIN mocked", class => recoverable},
        #{error => ~"MOVED mocked", class => recoverable},
        #{error => ~"ASK mocked", class => recoverable},
        #{error => ~"LOADING mocked", class => recoverable},
        #{error => ~"MASTERDOWN mocked", class => recoverable},
        %% should this really be recoverable?
        #{error => ~"READONLY mocked", class => recoverable},
        #{error => ~"OOM mocked", class => recoverable},
        #{error => ~"BUSY mocked", class => recoverable},
        #{error => ~"WRONGTYPE mocked", class => unrecoverable},
        #{error => ~"NOPERM mocked", class => unrecoverable},
        #{error => ~"ERR mocked", class => unrecoverable}
    ],
    MkSingleTest = fun
        (#{error := Error, class := unrecoverable}) ->
            ?_assertMatch(
                {error, {unrecoverable_error, _}},
                emqx_redis:map_result({error, Error})
            );
        (#{error := Error, class := recoverable}) ->
            ?_assertMatch(
                {error, {recoverable_error, _}},
                emqx_redis:map_result({error, Error})
            )
    end,
    MkBatchTest = fun
        (#{error := Error, class := unrecoverable}) ->
            ?_assertMatch(
                {error, [{error, {unrecoverable_error, _}}]},
                emqx_redis:map_result([{error, Error}])
            );
        (#{error := Error, class := recoverable}) ->
            ?_assertMatch(
                {error, [{error, {recoverable_error, _}}]},
                emqx_redis:map_result([{error, Error}])
            )
    end,
    MkMixedBatchTest = fun(#{error := Error}) ->
        ?_assertMatch(
            {error, [ok, {ok, _}, {error, _}, ok]},
            emqx_redis:map_result([ok, {ok, 1}, {error, Error}, ok])
        )
    end,
    [
        ?_assertMatch(
            {ok, [ok, {ok, _}]},
            emqx_redis:map_result([ok, {ok, 1}])
        )
    ] ++
        [Mk(Case) || Case <- Cases, Mk <- [MkSingleTest, MkBatchTest, MkMixedBatchTest]].
