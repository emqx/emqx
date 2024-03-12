%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_utils_tests).

-include_lib("eunit/include/eunit.hrl").

is_redacted_test_() ->
    [
        ?_assertNot(emqx_utils:is_redacted(password, <<"secretpass">>)),
        ?_assertNot(emqx_utils:is_redacted(password, <<>>)),
        ?_assertNot(emqx_utils:is_redacted(password, undefined)),
        ?_assert(emqx_utils:is_redacted(password, <<"******">>)),
        ?_assertNot(emqx_utils:is_redacted(password, fun() -> <<"secretpass">> end)),
        ?_assertNot(emqx_utils:is_redacted(password, emqx_secret:wrap(<<"secretpass">>))),
        ?_assert(emqx_utils:is_redacted(password, fun() -> <<"******">> end)),
        ?_assert(emqx_utils:is_redacted(password, emqx_secret:wrap(<<"******">>)))
    ].

foldl_while_test_() ->
    [
        ?_assertEqual(
            [3, 2, 1],
            emqx_utils:foldl_while(fun(X, Acc) -> {cont, [X | Acc]} end, [], [1, 2, 3])
        ),
        ?_assertEqual(
            [1],
            emqx_utils:foldl_while(
                fun
                    (X, Acc) when X == 2 ->
                        {halt, Acc};
                    (X, Acc) ->
                        {cont, [X | Acc]}
                end,
                [],
                [1, 2, 3]
            )
        ),
        ?_assertEqual(
            finished,
            emqx_utils:foldl_while(
                fun
                    (X, _Acc) when X == 3 ->
                        {halt, finished};
                    (X, Acc) ->
                        {cont, [X | Acc]}
                end,
                [],
                [1, 2, 3]
            )
        )
    ].
