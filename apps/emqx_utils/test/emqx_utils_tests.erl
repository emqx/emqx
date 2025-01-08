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
-module(emqx_utils_tests).

-include_lib("eunit/include/eunit.hrl").

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

to_binary_representation(Bin) when is_binary(Bin) ->
    Bytes = binary_to_list(Bin),
    iolist_to_binary([
        [$<, $<],
        lists:join($,, [
            integer_to_binary(B)
         || B <- Bytes
        ]),
        [$>, $>]
    ]).

readable_error_msg_test_() ->
    [
        {"binary in nested structure with non-latin1 characters",
            ?_assert(begin
                %% An unexpected error that could occur and be returned via HTTP API.
                Text = <<"test中文"/utf8>>,
                Error = {badmatch, #{description => Text}},
                %% Output shouldn't contain the "exploded" bytes
                Exploded = to_binary_representation(Text),
                Formatted = emqx_utils:readable_error_msg(Error),
                %% Precondition: `+pc unicode' must be set on the VM.
                ?assertEqual(unicode, io:printable_range()),
                nomatch =:= re:run(Formatted, Exploded, [{capture, all, binary}])
            end)}
    ].
