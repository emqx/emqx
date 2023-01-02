%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

uri_parse_test_() ->
    [ {"default port http",
       fun() -> ?assertMatch({ok, #{port := 80, scheme := http, host := "localhost"}},
                             emqx_http_lib:uri_parse("localhost"))
       end
      }
    , {"default port https",
       fun() -> ?assertMatch({ok, #{port := 443, scheme := https}},
                             emqx_http_lib:uri_parse("https://localhost"))
       end
      }
    , {"bad url",
       fun() -> ?assertMatch({error, {invalid_uri, _}},
                             emqx_http_lib:uri_parse("https://localhost:notnumber"))
       end
      }
    , {"normalise",
       fun() -> ?assertMatch({ok, #{scheme := https, host := {127, 0, 0, 1}}},
                             emqx_http_lib:uri_parse("HTTPS://127.0.0.1"))
       end
      }
    , {"coap default port",
       fun() -> ?assertMatch({ok, #{scheme := coap, port := 5683}},
                             emqx_http_lib:uri_parse("coap://127.0.0.1"))
       end
      }
    , {"coaps default port",
       fun() -> ?assertMatch({ok, #{scheme := coaps, port := 5684}},
                             emqx_http_lib:uri_parse("coaps://127.0.0.1"))
       end
      }
    , {"unsupported_scheme",
       fun() -> ?assertEqual({error, {unsupported_scheme, <<"wss">>}},
                             emqx_http_lib:uri_parse("wss://127.0.0.1"))
       end
      }
    , {"ipv6 host",
       fun() -> ?assertMatch({ok, #{scheme := http, host := T}} when size(T) =:= 8,
                             emqx_http_lib:uri_parse("http://[::1]:80"))
       end
      }
    ].

normalise_headers_test() ->
    ?assertEqual([{<<"content-type">>, "applicaiton/binary"}],
                 emqx_http_lib:normalise_headers([{"Content_Type", "applicaiton/binary"},
                                                  {"content-type", "applicaiton/json"}])).
