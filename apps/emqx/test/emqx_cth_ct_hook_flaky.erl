%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
            case is_already_repeating(Suite, TestCase, State0) of
                false ->
                    %% New case or new run (different group)
                    State = set_remaining_attempts(Suite, TestCase, max(0, Attempts - 1), State0),
                    {TCConfig, State};
                true ->
                    %% Already repeating
                    {TCConfig, State0}
            end;
        error ->
            {TCConfig, State0}
    end.

post_end_per_testcase(Suite, TestCase, _TCConfig, ok = Return, State0) ->
    State = ensure_no_attempts(Suite, TestCase, State0),
    {Return, State};
post_end_per_testcase(Suite, TestCase, _TCConfig, Return, State0) ->
    case has_remaining_attempts(Suite, TestCase, State0) of
        false ->
            State = ensure_no_attempts(Suite, TestCase, State0),
            {Return, State};
        true ->
            State = dec_attempts(Suite, TestCase, State0),
            {{failed, keep_going}, State}
    end.

has_remaining_attempts(Suite, TestCase, State) ->
    Remaining = get_remaining_attempts(Suite, TestCase, State),
    Remaining > 0.

set_remaining_attempts(Suite, TestCase, Remaining, State0) ->
    emqx_utils_maps:deep_put([attempts, Suite, TestCase], State0, Remaining).

dec_attempts(Suite, TestCase, State0) ->
    Remaining0 = get_remaining_attempts(Suite, TestCase, State0),
    Remaining = max(0, Remaining0 - 1),
    set_remaining_attempts(Suite, TestCase, Remaining, State0).

get_remaining_attempts(Suite, TestCase, State) ->
    emqx_utils_maps:deep_get([attempts, Suite, TestCase], State, 0).

ensure_no_attempts(Suite, TestCase, State) ->
    emqx_utils_maps:deep_remove([attempts, Suite, TestCase], State).

is_already_repeating(Suite, TestCase, State) ->
    Attempts = emqx_utils_maps:deep_get([attempts, Suite, TestCase], State, undefined),
    is_integer(Attempts).
