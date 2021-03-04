%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_http_lib_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

uri_encode_decode_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 10,
     fun() -> ?assert(proper:quickcheck(prop_run(), Opts)) end}.

prop_run() ->
    ?FORALL(Generated, prop_uri(), test_prop_uri(iolist_to_binary(Generated))).

prop_uri() ->
    proper_types:non_empty(proper_types:list(proper_types:union([prop_char(), prop_reserved()]))).

prop_char() -> proper_types:integer(32, 126).

prop_reserved() ->
    proper_types:oneof([$;, $:, $@, $&, $=, $+, $,, $/, $?,
        $#, $[, $], $<, $>, $\", ${, $}, $|,
        $\\, $', $^, $%, $ ]).

test_prop_uri(URI) ->
    Encoded = emqx_http_lib:uri_encode(URI),
    Decoded1 = emqx_http_lib:uri_decode(Encoded),
    ?assertEqual(URI, Decoded1),
    Decoded2 =  uri_string:percent_decode(Encoded),
    ?assertEqual(URI, Decoded2),
    true.
