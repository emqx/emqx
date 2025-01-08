%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This file contains common macros for testing.
%% It must not be used anywhere except in test suites.

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(assertWaitEvent(Code, EventMatch, Timeout),
    ?assertMatch(
        {_, {ok, EventMatch}},
        ?wait_async_action(
            Code,
            EventMatch,
            Timeout
        )
    )
).

-define(drainMailbox(), ?drainMailbox(0)).
-define(drainMailbox(TIMEOUT),
    (fun F__Flush_() ->
        receive
            X__Msg_ -> [X__Msg_ | F__Flush_()]
        after TIMEOUT -> []
        end
    end)()
).

-define(assertReceive(PATTERN),
    ?assertReceive(PATTERN, 1000)
).

-define(assertReceive(PATTERN, TIMEOUT),
    ?assertReceive(PATTERN, TIMEOUT, #{})
).

-define(assertReceive(PATTERN, TIMEOUT, EXTRA),
    (fun() ->
        receive
            X__V = PATTERN -> X__V
        after TIMEOUT ->
            erlang:error(
                {assertReceive, [
                    {module, ?MODULE},
                    {line, ?LINE},
                    {expression, (??PATTERN)},
                    {mailbox, ?drainMailbox()},
                    {extra_info, EXTRA}
                ]}
            )
        end
    end)()
).

-define(assertNotReceive(PATTERN),
    ?assertNotReceive(PATTERN, 300)
).

-define(assertNotReceive(PATTERN, TIMEOUT),
    (fun() ->
        receive
            X__V = PATTERN ->
                erlang:error(
                    {assertNotReceive, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expression, (??PATTERN)},
                        {message, X__V}
                    ]}
                )
        after TIMEOUT ->
            ok
        end
    end)()
).

-define(assertExceptionOneOf(CT1, CT2, EXPR),
    (fun() ->
        X__Attrs = [
            {module, ?MODULE},
            {line, ?LINE},
            {expression, (??EXPR)},
            {pattern, "[ " ++ (??CT1) ++ ", " ++ (??CT2) ++ " ]"}
        ],
        X__Exc =
            try (EXPR) of
                X__V -> erlang:error({assertException, [{unexpected_success, X__V} | X__Attrs]})
            catch
                X__C:X__T:X__S -> {X__C, X__T, X__S}
            end,
        case {element(1, X__Exc), element(2, X__Exc)} of
            CT1 -> ok;
            CT2 -> ok;
            _ -> erlang:error({assertException, [{unexpected_exception, X__Exc} | X__Attrs]})
        end
    end)()
).

-define(retrying(CONFIG, NUM_RETRIES, TEST_BODY_FN), begin
    __TEST_CASE = ?FUNCTION_NAME,
    (fun
        __GO(__CONFIG, __N) when __N >= NUM_RETRIES ->
            TEST_BODY_FN(__CONFIG);
        __GO(__CONFIG, __N) ->
            try
                TEST_BODY_FN(__CONFIG)
            catch
                __KIND:__REASON:__STACKTRACE ->
                    ct:pal("test errored; will retry\n  ~p", [
                        #{kind => __KIND, reason => __REASON, stacktrace => __STACKTRACE}
                    ]),
                    end_per_testcase(__TEST_CASE, __CONFIG),
                    garbage_collect(),
                    timer:sleep(1000),
                    __CONFIG1 = init_per_testcase(__TEST_CASE, __CONFIG),
                    __GO(__CONFIG1, __N + 1)
            end
    end)(
        CONFIG, 0
    )
end).
