%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cth_ct_hook_flaky).

-moduledoc """
This hooks is necessary work around to handle the cases when a test case fails due to the
way CT framework tallies failure counts from timetraps.

See also: https://github.com/emqx/otp/pull/56
""".

%% Required callbacks
-export([init/2]).

%% Optional callbacks
-export([pre_init_per_testcase/4]).
-export([post_end_per_testcase/5]).

init(_Id, _Opts) ->
    {ok, #{attempts => #{}}}.

pre_init_per_testcase(Suite, TestCase, TCConfig, State0) ->
    FlakyTests = emqx_common_test_helpers:flaky_tests(Suite),
    case maps:find(TestCase, FlakyTests) of
        {ok, Attempts} ->
            State = set_remaining_attempts(Suite, TestCase, Attempts, State0),
            {TCConfig, State};
        error ->
            {TCConfig, State0}
    end.

post_end_per_testcase(_Suite, _TestCase, _TCConfig, ok = Return, State) ->
    {Return, State};
post_end_per_testcase(Suite, TestCase, _TCConfig, Return, State0) ->
    case has_remaining_attempts(Suite, TestCase, State0) of
        false ->
            {Return, State0};
        true ->
            State = inc_attempt(Suite, TestCase, State0),
            {{failed, keep_going}, State}
    end.

has_remaining_attempts(Suite, TestCase, State) ->
    Remaining = get_remaining_attempts(Suite, TestCase, State),
    Remaining > 0.

set_remaining_attempts(Suite, TestCase, Remaining, State0) ->
    emqx_utils_maps:deep_put([attempts, Suite, TestCase], State0, Remaining).

inc_attempt(Suite, TestCase, State0) ->
    Remaining0 = get_remaining_attempts(Suite, TestCase, State0),
    Remaining = Remaining0 + 1,
    set_remaining_attempts(Suite, TestCase, Remaining, State0).

get_remaining_attempts(Suite, TestCase, State) ->
    emqx_utils_maps:deep_get([attempts, Suite, TestCase], State, 0).
