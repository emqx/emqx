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

-module(emqx_tls_lib_tests).

-include_lib("eunit/include/eunit.hrl").

%% one of the cipher suite from tlsv1.2 and tlsv1.3 each
-define(TLS_12_CIPHER, "ECDHE-ECDSA-AES256-GCM-SHA384").
-define(TLS_13_CIPHER, "TLS_AES_256_GCM_SHA384").

ensure_tls13_ciphers_added_test() ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.3'], [?TLS_12_CIPHER]),
    ?assert(lists:member(?TLS_12_CIPHER, Ciphers)),
    ?assert(lists:member(?TLS_13_CIPHER, Ciphers)).

legacy_cipher_suites_test() ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.2'], [?TLS_12_CIPHER]),
    ?assertEqual([?TLS_12_CIPHER], Ciphers).

use_default_ciphers_test() ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.3', 'tlsv1.2'], ""),
    ?assert(lists:member(?TLS_12_CIPHER, Ciphers)),
    ?assert(lists:member(?TLS_13_CIPHER, Ciphers)).

ciphers_format_test_() ->
    String = ?TLS_13_CIPHER ++ "," ++ ?TLS_12_CIPHER,
    Binary = iolist_to_binary(String),
    List = [?TLS_13_CIPHER, ?TLS_12_CIPHER],
    [ {"string", fun() -> test_cipher_format(String) end}
    , {"binary", fun() -> test_cipher_format(Binary) end}
    , {"string-list", fun() -> test_cipher_format(List) end}
    ].

test_cipher_format(Input) ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.3', 'tlsv1.2'], Input),
    ?assertEqual([?TLS_13_CIPHER, ?TLS_12_CIPHER], Ciphers).

tls_versions_test() ->
    ?assert(lists:member('tlsv1.3', emqx_tls_lib:default_versions())).

tls_version_unknown_test() ->
    ?assertEqual(emqx_tls_lib:default_versions(),
                 emqx_tls_lib:integral_versions([])),
    ?assertEqual(emqx_tls_lib:default_versions(),
                 emqx_tls_lib:integral_versions(<<>>)),
    ?assertEqual(emqx_tls_lib:default_versions(),
                 emqx_tls_lib:integral_versions("foo")),
    ?assertError(#{reason := no_available_tls_version},
                 emqx_tls_lib:integral_versions([foo])).

cipher_suites_no_duplication_test() ->
    AllCiphers = emqx_tls_lib:default_ciphers(),
    ?assertEqual(length(AllCiphers), length(lists:usort(AllCiphers))).

