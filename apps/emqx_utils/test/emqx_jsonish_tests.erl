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

-module(emqx_jsonish_tests).

-include_lib("eunit/include/eunit.hrl").

prop_prio_test_() ->
    [
        ?_assertEqual(
            {ok, 42},
            emqx_jsonish:lookup([<<"foo">>], #{<<"foo">> => 42, foo => 1337})
        ),
        ?_assertEqual(
            {ok, 1337},
            emqx_jsonish:lookup([<<"foo">>], #{foo => 1337})
        )
    ].

undefined_test() ->
    ?assertEqual(
        {error, undefined},
        emqx_jsonish:lookup([<<"foo">>], #{})
    ).

undefined_deep_test() ->
    ?assertEqual(
        {error, undefined},
        emqx_jsonish:lookup([<<"foo">>, <<"bar">>], #{})
    ).

undefined_deep_json_test() ->
    ?assertEqual(
        {error, undefined},
        emqx_jsonish:lookup(
            [<<"foo">>, <<"bar">>, <<"baz">>],
            <<"{\"foo\":{\"bar\":{\"no\":{}}}}">>
        )
    ).

invalid_type_test() ->
    ?assertEqual(
        {error, {0, number}},
        emqx_jsonish:lookup([<<"foo">>], <<"42">>)
    ).

invalid_type_deep_test() ->
    ?assertEqual(
        {error, {2, atom}},
        emqx_jsonish:lookup([<<"foo">>, <<"bar">>, <<"tuple">>], #{foo => #{bar => baz}})
    ).

decode_json_test() ->
    ?assertEqual(
        {ok, 42},
        emqx_jsonish:lookup([<<"foo">>, <<"bar">>], <<"{\"foo\":{\"bar\":42}}">>)
    ).

decode_json_deep_test() ->
    ?assertEqual(
        {ok, 42},
        emqx_jsonish:lookup([<<"foo">>, <<"bar">>], #{<<"foo">> => <<"{\"bar\": 42}">>})
    ).

decode_json_invalid_type_test() ->
    ?assertEqual(
        {error, {1, list}},
        emqx_jsonish:lookup([<<"foo">>, <<"bar">>], #{<<"foo">> => <<"[1,2,3]">>})
    ).

decode_no_json_test() ->
    ?assertEqual(
        {error, {1, binary}},
        emqx_jsonish:lookup([<<"foo">>, <<"bar">>], #{<<"foo">> => <<0, 1, 2, 3>>})
    ).

decode_json_no_nested_test() ->
    ?assertEqual(
        {error, {2, binary}},
        emqx_jsonish:lookup(
            [<<"foo">>, <<"bar">>, <<"baz">>],
            #{<<"foo">> => <<"{\"bar\":\"{\\\"baz\\\":42}\"}">>}
        )
    ).
