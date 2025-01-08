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

-module(emqx_trie_search_tests).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_trie_search, [filter/1]).

filter_test_() ->
    [
        ?_assertEqual(
            [<<"sensor">>, '+', <<"metric">>, <<>>, '#'],
            filter(<<"sensor/+/metric//#">>)
        ),
        ?_assertEqual(
            false,
            filter(<<"sensor/1/metric//42">>)
        )
    ].

topic_validation_test_() ->
    NextF = fun(_) -> '$end_of_table' end,
    Call = fun(Topic) ->
        emqx_trie_search:match(Topic, NextF)
    end,
    [
        ?_assertError(badarg, Call(<<"+">>)),
        ?_assertError(badarg, Call(<<"#">>)),
        ?_assertError(badarg, Call(<<"a/+/b">>)),
        ?_assertError(badarg, Call(<<"a/b/#">>)),
        ?_assertEqual(false, Call(<<"a/b/b+">>)),
        ?_assertEqual(false, Call(<<"a/b/c#">>))
    ].
