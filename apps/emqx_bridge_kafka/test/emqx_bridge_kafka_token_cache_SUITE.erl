%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_token_cache_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(client_id, <<"some:client:id">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [emqx_bridge_kafka],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    emqx_bridge_kafka_testlib:wait_until_kafka_is_up(),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    emqx_bridge_kafka_token_cache:clear_cache(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

mk_fetch_success() ->
    mk_fetch_success(_Opts = #{}).

mk_fetch_success(Opts) ->
    TestPid = self(),
    ExpiryMS = maps:get(expiry_ms, Opts, timer:hours(1)),
    ExtraContext = maps:get(extra_context, Opts, #{}),
    Prefix = maps:get(prefix, Opts, <<"">>),
    SuccessAgent = emqx_utils_maps:get_lazy(success_agent, Opts, fun() ->
        {ok, SuccessAgent0} = emqx_utils_agent:start_link(true),
        SuccessAgent0
    end),
    {ok, CountAgent} = emqx_utils_agent:start_link(0),
    fun() ->
        TimesCalled = emqx_utils_agent:get_and_update(CountAgent, fun(N) -> {N + 1, N + 1} end),
        ShouldSucceed = emqx_utils_agent:get(SuccessAgent),
        NBin = integer_to_binary(TimesCalled),
        Token = <<Prefix/binary, NBin/binary>>,
        TestPid !
            {fetched, ExtraContext#{
                succeeded => ShouldSucceed,
                times_called => TimesCalled,
                token => Token
            }},
        case ShouldSucceed of
            true ->
                {ok, ExpiryMS, Token};
            false ->
                {error, #{reason => mocked_error, called => TimesCalled}}
        end
    end.

get_or_refresh(RefreshFn) ->
    get_or_refresh(?client_id, RefreshFn).

get_or_refresh(ClientId, RefreshFn) ->
    on_exit(fun() -> emqx_bridge_kafka_token_cache:unregister(ClientId) end),
    emqx_bridge_kafka_token_cache:get_or_refresh(ClientId, RefreshFn).

get_or_refresh(ClientId, RefreshFn, Opts) ->
    on_exit(fun() -> emqx_bridge_kafka_token_cache:unregister(ClientId) end),
    emqx_bridge_kafka_token_cache:get_or_refresh(ClientId, RefreshFn, Opts).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Smoke test for cache hit (successful response).
""".
t_smoke_cache_hit_success(_TCConfig) ->
    RefreshFn = mk_fetch_success(),
    FirstResponse = get_or_refresh(RefreshFn),
    ?assertMatch({ok, <<"1">>}, FirstResponse),
    ?assertReceive({fetched, #{times_called := 1}}),
    %% Already cached; shouldn't fetch again
    ?assertMatch(FirstResponse, get_or_refresh(RefreshFn)),
    ?assertNotReceive({fetched, _}),
    ok.

-doc """
Smoke test for cache concurrency (if not cached, linearizes refresh calls to process).
""".
t_smoke_cache_concurrency(_TCConfig) ->
    RefreshFn = mk_fetch_success(),
    Responses =
        [OneResponse | _] =
        emqx_utils:pmap(fun(_) -> get_or_refresh(RefreshFn) end, lists:seq(1, 10)),
    ?assertMatch([OneResponse], lists:usort(Responses)),
    ?assertNotReceive({fetched, #{times_called := N}} when N > 1),
    ok.

-doc """
Smoke test for cache isolation (different client ids are independent).
""".
t_smoke_cache_isolation(_TCConfig) ->
    ExpiryMS = 500,
    ClientIdA = <<"a">>,
    RefreshFnA = mk_fetch_success(#{
        extra_context => #{client_id => ClientIdA},
        prefix => ClientIdA,
        expiry_ms => ExpiryMS
    }),
    ClientIdB = <<"b">>,
    RefreshFnB = mk_fetch_success(#{
        extra_context => #{client_id => ClientIdB},
        prefix => ClientIdB,
        expiry_ms => ExpiryMS
    }),
    ?assertMatch({ok, <<"a1">>}, get_or_refresh(ClientIdA, RefreshFnA)),
    ?assertMatch({ok, <<"b1">>}, get_or_refresh(ClientIdB, RefreshFnB)),
    ?assertReceive({fetched, #{times_called := 1, client_id := ClientIdA}}),
    ?assertReceive({fetched, #{times_called := 1, client_id := ClientIdB}}),
    %% Wait for expiration.
    ct:sleep(ExpiryMS * 2),
    ?assertReceive({fetched, #{times_called := N, client_id := ClientIdA}} when N > 1),
    ?assertReceive({fetched, #{times_called := N, client_id := ClientIdB}} when N > 1),
    ?assertNotMatch({ok, <<"a1">>}, get_or_refresh(ClientIdA, RefreshFnA)),
    ?assertNotMatch({ok, <<"b1">>}, get_or_refresh(ClientIdB, RefreshFnB)),
    ?assertMatch({ok, <<"a", _/binary>>}, get_or_refresh(ClientIdA, RefreshFnA)),
    ?assertMatch({ok, <<"b", _/binary>>}, get_or_refresh(ClientIdB, RefreshFnB)),
    %% Second refresh
    ?assertReceive({fetched, #{times_called := N, client_id := ClientIdA}} when N > 2),
    ?assertReceive({fetched, #{times_called := N, client_id := ClientIdB}} when N > 2),
    ok.

-doc """
Smoke test for cache expiration.
""".
t_smoke_cache_expiration(_TCConfig) ->
    ExpiryMS = 500,
    RefreshFn = mk_fetch_success(#{expiry_ms => ExpiryMS}),
    FirstResponse = get_or_refresh(RefreshFn),
    ?assertMatch({ok, <<"1">>}, FirstResponse),
    ?assertReceive({fetched, #{times_called := 1}}),
    %% Already cached; shouldn't fetch again
    ?assertMatch(FirstResponse, get_or_refresh(RefreshFn), #{first => FirstResponse}),
    ?assertNotReceive({fetched, _}),
    %% Wait for it to expire.  Should refresh automatically
    ct:sleep(ExpiryMS * 2),
    ?assertNotEqual(FirstResponse, get_or_refresh(RefreshFn)),
    ?assertReceive({fetched, #{times_called := N}} when N > 1),
    ok.

-doc """
Verifies clearing cache for one client.
""".
t_smoke_clear_one_cache(_TCConfig) ->
    ClientIdA = <<"a">>,
    RefreshFnA = mk_fetch_success(#{
        extra_context => #{client_id => ClientIdA},
        prefix => ClientIdA
    }),
    ClientIdB = <<"b">>,
    RefreshFnB = mk_fetch_success(#{
        extra_context => #{client_id => ClientIdB},
        prefix => ClientIdB
    }),

    ?assertMatch({ok, <<"a1">>}, get_or_refresh(ClientIdA, RefreshFnA)),
    ?assertMatch({ok, <<"b1">>}, get_or_refresh(ClientIdB, RefreshFnB)),
    ?assertReceive({fetched, #{times_called := 1, client_id := ClientIdA}}),
    ?assertReceive({fetched, #{times_called := 1, client_id := ClientIdB}}),

    ?assertMatch(ok, emqx_bridge_kafka_token_cache:clear_cache(ClientIdA)),

    ?assertMatch({ok, <<"a2">>}, get_or_refresh(ClientIdA, RefreshFnA)),
    ?assertMatch({ok, <<"b1">>}, get_or_refresh(ClientIdB, RefreshFnB)),
    ?assertReceive({fetched, #{times_called := 2, client_id := ClientIdA}}),
    ?assertNotReceive({fetched, #{times_called := _, client_id := ClientIdB}}),

    ok.

-doc """
Verifies that if we fail to fetch a token _when refreshing_, a retry is scheduled.

We don't cache the failure.
""".
t_error_fetching_during_refresh(_TCConfig) ->
    %% Succeed initially
    {ok, SuccessAgent} = emqx_utils_agent:start_link(true),
    ExpiryMS = 500,
    RefreshFn = mk_fetch_success(#{expiry_ms => ExpiryMS, success_agent => SuccessAgent}),
    ?assertMatch({ok, <<"1">>}, get_or_refresh(?client_id, RefreshFn, #{retry_interval => 300})),
    ?assertReceive({fetched, #{succeeded := true, times_called := 1}}),
    %% Now set it to fail when refreshing
    ok = emqx_utils_agent:set(SuccessAgent, false),
    ?assertReceive({fetched, #{succeeded := false, times_called := 2}}),
    %% Let it succeed
    ok = emqx_utils_agent:set(SuccessAgent, true),
    {fetched, #{token := NewToken}} =
        ?assertReceive({fetched, #{succeeded := true, times_called := N}} when N > 2),
    ?assertEqual({ok, NewToken}, get_or_refresh(?client_id, RefreshFn)),
    ok.

-doc """
Verifies that if we fail to fetch a token _during the first call_, no retry is scheduled.

We also cache the error response for a short time, for concurrent callers.
""".
t_error_fetching_first_time(_TCConfig) ->
    %% Fail initially
    {ok, SuccessAgent} = emqx_utils_agent:start_link(false),
    ExpiryMS = 500,
    RefreshFn = mk_fetch_success(#{expiry_ms => ExpiryMS, success_agent => SuccessAgent}),
    ?assertMatch(
        {error, #{
            reason := mocked_error,
            called := 1
        }},
        get_or_refresh(?client_id, RefreshFn, #{
            retry_interval => 100,
            cache_failures_for => 500
        })
    ),
    ?assertReceive({fetched, #{succeeded := false, times_called := 1}}),
    %% Failure should be cached for a while
    ?assertMatch(
        {error, #{
            reason := mocked_error,
            called := 1
        }},
        get_or_refresh(?client_id, RefreshFn, #{
            retry_interval => 100,
            cache_failures_for => 500
        })
    ),
    ?assertNotReceive({fetched, #{times_called := 2}}),

    %% Should not schedule retry
    ct:sleep(ExpiryMS * 2),
    ?assertNotReceive({fetched, #{times_called := 2}}),
    ?assertMatch(
        {error, #{
            reason := mocked_error,
            called := 2
        }},
        get_or_refresh(?client_id, RefreshFn, #{
            retry_interval => 100,
            cache_failures_for => 500
        })
    ),
    ?assertReceive({fetched, #{succeeded := false, times_called := 2}}),

    ok.

-doc """
Verifies that if we catch exceptions from refresh function.
""".
t_exception_fetching(_TCConfig) ->
    RefreshFn = fun() -> throw(mocked_exception) end,
    ?assertMatch({error, {throw, mocked_exception}}, get_or_refresh(RefreshFn)),
    ok.
