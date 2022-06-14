%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_schema_tests).

-include_lib("eunit/include/eunit.hrl").

ssl_opts_dtls_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(
        #{
            versions => dtls_all_available,
            ciphers => dtls_all_available
        },
        false
    ),
    Checked = validate(Sc, #{<<"versions">> => [<<"dtlsv1.2">>, <<"dtlsv1">>]}),
    ?assertMatch(
        #{
            versions := ['dtlsv1.2', 'dtlsv1'],
            ciphers := ["ECDHE-ECDSA-AES256-GCM-SHA384" | _]
        },
        Checked
    ).

ssl_opts_tls_1_3_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{<<"versions">> => [<<"tlsv1.3">>]}),
    ?assertNot(maps:is_key(handshake_timeout, Checked)),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := [_ | _]
        },
        Checked
    ).

ssl_opts_tls_for_ranch_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, true),
    Checked = validate(Sc, #{<<"versions">> => [<<"tlsv1.3">>]}),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := [_ | _],
            handshake_timeout := _
        },
        Checked
    ).

ssl_opts_cipher_array_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{
        <<"versions">> => [<<"tlsv1.3">>],
        <<"ciphers">> => [
            <<"TLS_AES_256_GCM_SHA384">>,
            <<"ECDHE-ECDSA-AES256-GCM-SHA384">>
        ]
    }),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := ["TLS_AES_256_GCM_SHA384", "ECDHE-ECDSA-AES256-GCM-SHA384"]
        },
        Checked
    ).

ssl_opts_cipher_comma_separated_string_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{
        <<"versions">> => [<<"tlsv1.3">>],
        <<"ciphers">> => <<"TLS_AES_256_GCM_SHA384,ECDHE-ECDSA-AES256-GCM-SHA384">>
    }),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := ["TLS_AES_256_GCM_SHA384", "ECDHE-ECDSA-AES256-GCM-SHA384"]
        },
        Checked
    ).

ssl_opts_tls_psk_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{<<"versions">> => [<<"tlsv1.2">>]}),
    ?assertMatch(#{versions := ['tlsv1.2']}, Checked).

bad_cipher_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Reason = {bad_ciphers, ["foo"]},
    ?assertThrow(
        {_Sc, [#{kind := validation_error, reason := Reason}]},
        validate(Sc, #{
            <<"versions">> => [<<"tlsv1.2">>],
            <<"ciphers">> => [<<"foo">>]
        })
    ),
    ok.

validate(Schema, Data0) ->
    Sc = #{
        roots => [ssl_opts],
        fields => #{ssl_opts => Schema}
    },
    Data = Data0#{
        cacertfile => <<"cacertfile">>,
        certfile => <<"certfile">>,
        keyfile => <<"keyfile">>
    },
    #{ssl_opts := Checked} =
        hocon_tconf:check_plain(
            Sc,
            #{<<"ssl_opts">> => Data},
            #{atom_key => true}
        ),
    Checked.

ciperhs_schema_test() ->
    Sc = emqx_schema:ciphers_schema(undefined),
    WSc = #{roots => [{ciphers, Sc}]},
    ?assertThrow(
        {_, [#{kind := validation_error}]},
        hocon_tconf:check_plain(WSc, #{<<"ciphers">> => <<"foo,bar">>})
    ).

bad_tls_version_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Reason = {unsupported_ssl_versions, [foo]},
    ?assertThrow(
        {_Sc, [#{kind := validation_error, reason := Reason}]},
        validate(Sc, #{<<"versions">> => [<<"foo">>]})
    ),
    ok.
