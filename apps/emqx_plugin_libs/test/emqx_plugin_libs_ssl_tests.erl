%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugin_libs_ssl_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

no_crash_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 60,
     fun() -> ?assert(proper:quickcheck(prop_run(), Opts)) end}.

prop_run() ->
    ?FORALL(Generated, prop_opts_input(), test_opts_input(Generated)).

%% proper type to generate input value.
prop_opts_input() ->
    [{keyfile, prop_file_or_content()},
     {certfile, prop_file_or_content()},
     {cacertfile, prop_file_or_content()},
     {verify, proper_types:boolean()},
     {versions, prop_tls_versions()},
     {ciphers, prop_tls_ciphers()},
     {other, proper_types:binary()}].

prop_file_or_content() ->
    proper_types:oneof([prop_cert_file_name(),
                        {prop_cert_file_name(), proper_types:binary()}]).

prop_cert_file_name() ->
    File = code:which(?MODULE), %% existing
    proper_types:oneof(["", <<>>, undefined, File]).

prop_tls_versions() ->
    proper_types:oneof(["tlsv1.3",
                        <<"tlsv1.3,tlsv1.2">>,
                        "tlsv1.2 , tlsv1.1",
                        "1.2",
                        "v1.3",
                        "",
                        <<>>,
                        undefined]).

prop_tls_ciphers() ->
    proper_types:oneof(["TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256",
                        <<>>,
                        "",
                        undefined]).

test_opts_input(Inputs) ->
    KF = fun(K) -> {_, V} = lists:keyfind(K, 1, Inputs), V end,
    Generated = #{<<"keyfile">> => file_or_content(KF(keyfile)),
                  <<"certfile">> => file_or_content(KF(certfile)),
                  <<"cafile">> => file_or_content(KF(cacertfile)),
                  <<"verify">> => file_or_content(KF(verify)),
                  <<"tls_versions">> => KF(versions),
                  <<"ciphers">> => KF(ciphers),
                  <<"other">> => KF(other)},
    _ = emqx_plugin_libs_ssl:save_files_return_opts(Generated, "test-data"),
    true.

file_or_content({Name, Content}) ->
    #{<<"file">> => Content, <<"filename">> => Name};
file_or_content(Name) ->
    Name.

bad_cert_file_test() ->
    Input = #{<<"keyfile">> =>
                #{<<"filename">> => "notafile",
                  <<"file">> => ""}},
    ?assertThrow({bad_cert_file, _},
                 emqx_plugin_libs_ssl:save_files_return_opts(Input, "test-data")).
