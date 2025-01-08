%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_conv_tests).

-import(emqx_utils_conv, [bin/1, str/1]).

-include_lib("eunit/include/eunit.hrl").

bin_test_() ->
    [
        ?_assertEqual(<<"abc">>, bin("abc")),
        ?_assertEqual(<<"abc">>, bin(abc)),
        ?_assertEqual(<<"{\"a\":1}">>, bin(#{a => 1})),
        ?_assertEqual(<<"[{\"a\":1}]">>, bin([#{a => 1}])),
        ?_assertEqual(<<"1">>, bin(1)),
        ?_assertEqual(<<"2.0">>, bin(2.0)),
        ?_assertEqual(<<"true">>, bin(true)),
        ?_assertError(_, bin({a, v}))
    ].

str_test_() ->
    [
        ?_assertEqual("abc", str("abc")),
        ?_assertEqual("abc", str(abc)),
        ?_assertEqual("{\"a\":1}", str(#{a => 1})),
        ?_assertEqual("1", str(1)),
        ?_assertEqual("2.0", str(2.0)),
        ?_assertEqual("true", str(true)),
        ?_assertError(_, str({a, v}))
    ].
