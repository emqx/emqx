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

-module(emqx_authn_jwt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(AUTHN_ID, <<"mechanism:jwt">>).

-define(JWKS_PORT, 31333).
-define(JWKS_PATH, "/jwks.json").

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_jwt], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_hmac_based(_) ->
    Secret = <<"abcdef">>,
    Config = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        algorithm => 'hmac-based',
        secret => Secret,
        secret_base64_encoded => false,
        verify_claims => [{<<"username">>, <<"${username}">>}],
        disconnect_after_expire => false
    },
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"username">> => <<"myuser">>, <<"exp">> => erlang:system_time(second) + 60},
    JWS = generate_jws('hmac-based', Payload, Secret),
    Credential = #{
        username => <<"myuser">>,
        password => JWS
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential, State)),

    Payload1 = #{
        <<"username">> => <<"myuser">>,
        <<"is_superuser">> => true,
        <<"exp">> => erlang:system_time(second) + 60
    },
    JWS1 = generate_jws('hmac-based', Payload1, Secret),
    Credential1 = #{
        username => <<"myuser">>,
        password => JWS1
    },
    ?assertMatch({ok, #{is_superuser := true}}, emqx_authn_jwt:authenticate(Credential1, State)),

    BadJWS = generate_jws('hmac-based', Payload, <<"bad_secret">>),
    Credential2 = Credential#{password => BadJWS},
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential2, State)),

    %% secret_base64_encoded
    Config2 = Config#{
        secret => base64:encode(Secret),
        secret_base64_encoded => true
    },
    {ok, State2} = emqx_authn_jwt:update(Config2, State),
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential, State2)),

    %% invalid secret
    BadConfig = Config#{
        secret => <<"emqxsecret">>,
        secret_base64_encoded => true
    },
    {error, {invalid_parameter, secret}} = emqx_authn_jwt:create(?AUTHN_ID, BadConfig),

    Config3 = Config#{verify_claims => [{<<"username">>, <<"${username}">>}]},
    {ok, State3} = emqx_authn_jwt:update(Config3, State2),
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential, State3)),
    ?assertEqual(
        {error, bad_username_or_password},
        emqx_authn_jwt:authenticate(Credential#{username => <<"otheruser">>}, State3)
    ),

    %% Expiration
    Payload3 = #{
        <<"username">> => <<"myuser">>,
        <<"exp">> => erlang:system_time(second) - 60
    },
    JWS3 = generate_jws('hmac-based', Payload3, Secret),
    Credential3 = Credential#{password => JWS3},
    ?assertEqual(
        {error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential3, State3)
    ),

    Payload4 = #{
        <<"username">> => <<"myuser">>,
        <<"exp">> => erlang:system_time(second) + 60
    },
    JWS4 = generate_jws('hmac-based', Payload4, Secret),
    Credential4 = Credential#{password => JWS4},
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential4, State3)),

    %% Issued At (iat) should not matter
    Payload5 = #{
        <<"username">> => <<"myuser">>,
        <<"iat">> => erlang:system_time(second) - 60,
        <<"exp">> => erlang:system_time(second) + 60
    },
    JWS5 = generate_jws('hmac-based', Payload5, Secret),
    Credential5 = Credential#{password => JWS5},
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential5, State3)),

    Payload6 = #{
        <<"username">> => <<"myuser">>,
        <<"iat">> => erlang:system_time(second) + 60
    },
    JWS6 = generate_jws('hmac-based', Payload6, Secret),
    Credential6 = Credential#{password => JWS6},
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential6, State3)),

    %% Not Before
    Payload7 = #{
        <<"username">> => <<"myuser">>,
        <<"nbf">> => erlang:system_time(second) - 60,
        <<"exp">> => erlang:system_time(second) + 60
    },
    JWS7 = generate_jws('hmac-based', Payload7, Secret),
    Credential7 = Credential6#{password => JWS7},
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential7, State3)),

    Payload8 = #{
        <<"username">> => <<"myuser">>,
        <<"nbf">> => erlang:system_time(second) + 60,
        <<"exp">> => erlang:system_time(second) + 60
    },
    JWS8 = generate_jws('hmac-based', Payload8, Secret),
    Credential8 = Credential#{password => JWS8},
    ?assertEqual(
        {error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential8, State3)
    ),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State3)),
    ok.

t_public_key(_) ->
    PublicKey = test_rsa_key(public),
    PrivateKey = test_rsa_key(private),
    Config = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        enable => true,
        algorithm => 'public-key',
        public_key => PublicKey,
        verify_claims => [],
        disconnect_after_expire => false
    },
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"username">> => <<"myuser">>, <<"exp">> => erlang:system_time(second) + 60},
    JWS = generate_jws('public-key', Payload, PrivateKey),
    Credential = #{
        username => <<"myuser">>,
        password => JWS
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential, State)),
    ?assertEqual(
        ignore, emqx_authn_jwt:authenticate(Credential#{password => <<"badpassword">>}, State)
    ),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State)),
    ok.

t_bad_public_keys(_) ->
    BaseConfig = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        algorithm => 'public-key',
        verify_claims => [],
        disconnect_after_expire => false
    },

    %% try create with invalid public key
    ?assertMatch(
        {error, invalid_public_key},
        emqx_authn_jwt:create(?AUTHN_ID, BaseConfig#{
            enable => true,
            public_key => <<"bad_public_key">>
        })
    ),

    %% no such file
    ?assertMatch(
        {error, invalid_public_key},
        emqx_authn_jwt:create(?AUTHN_ID, BaseConfig#{
            enable => true,
            public_key => data_file("bad_flie_path.pem")
        })
    ),

    %% bad public key file content
    ?assertMatch(
        {error, invalid_public_key},
        emqx_authn_jwt:create(?AUTHN_ID, BaseConfig#{
            enable => true,
            public_key => data_file("bad_public_key_file.pem")
        })
    ),

    %% assume jwk authenticator is disabled
    {ok, State} =
        emqx_authn_jwt:create(?AUTHN_ID, BaseConfig#{public_key => <<"bad_public_key">>}),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State)),
    ok.

t_jwt_in_username(_) ->
    Secret = <<"abcdef">>,
    Config = #{
        mechanism => jwt,
        from => username,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        algorithm => 'hmac-based',
        secret => Secret,
        secret_base64_encoded => false,
        verify_claims => [],
        disconnect_after_expire => false
    },
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"exp">> => erlang:system_time(second) + 60},
    JWS = generate_jws('hmac-based', Payload, Secret),
    Credential = #{
        username => JWS,
        password => <<"pass">>
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential, State)).

t_complex_template(_) ->
    Secret = <<"abcdef">>,
    Config = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        algorithm => 'hmac-based',
        secret => Secret,
        secret_base64_encoded => false,
        verify_claims => [{<<"id">>, <<"${username}-${clientid}">>}],
        disconnect_after_expire => false
    },
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload0 = #{<<"id">> => <<"myuser-myclient">>, <<"exp">> => erlang:system_time(second) + 60},
    JWS0 = generate_jws('hmac-based', Payload0, Secret),
    Credential0 = #{
        clientid => <<"myclient">>,
        username => <<"myuser">>,
        password => JWS0
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential0, State)),

    Payload1 = #{<<"id">> => <<"-myclient">>, <<"exp">> => erlang:system_time(second) + 60},
    JWS1 = generate_jws('hmac-based', Payload1, Secret),
    Credential1 = #{
        clientid => <<"myclient">>,
        password => JWS1
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential1, State)).

t_jwks_renewal(_Config) ->
    {ok, _} = emqx_authn_http_test_server:start_link(?JWKS_PORT, ?JWKS_PATH, server_ssl_opts()),
    ok = emqx_authn_http_test_server:set_handler(fun jwks_handler/2),

    PrivateKey = test_rsa_key(private),
    Payload0 = #{<<"username">> => <<"myuser">>},
    JWS0 = generate_jws('public-key', Payload0, PrivateKey),
    Credential0 = #{
        username => <<"myuser">>,
        password => JWS0
    },

    BadConfig0 = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        algorithm => 'public-key',
        ssl => #{enable => false},
        verify_claims => [],
        disconnect_after_expire => false,
        use_jwks => true,
        endpoint => "https://127.0.0.1:" ++ integer_to_list(?JWKS_PORT + 1) ++ ?JWKS_PATH,
        headers => #{<<"Accept">> => <<"application/json">>},
        refresh_interval => 1000,
        pool_size => 1
    },

    ok = snabbkaffe:start_trace(),

    {{ok, State0}, _} = ?wait_async_action(
        emqx_authn_jwt:create(?AUTHN_ID, BadConfig0),
        #{?snk_kind := jwks_endpoint_response},
        10000
    ),

    ok = snabbkaffe:stop(),

    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential0, State0)),
    ?assertEqual(
        ignore, emqx_authn_jwt:authenticate(Credential0#{password => <<"badpassword">>}, State0)
    ),

    ClientSSLOpts = client_ssl_opts(),
    BadClientSSLOpts = ClientSSLOpts#{server_name_indication => "authn-server-unknown-host"},

    BadConfig1 = BadConfig0#{
        endpoint =>
            "https://127.0.0.1:" ++ integer_to_list(?JWKS_PORT) ++ ?JWKS_PATH,
        ssl => BadClientSSLOpts
    },

    ok = snabbkaffe:start_trace(),

    {{ok, State1}, _} = ?wait_async_action(
        emqx_authn_jwt:create(?AUTHN_ID, BadConfig1),
        #{?snk_kind := jwks_endpoint_response},
        10000
    ),

    ok = snabbkaffe:stop(),

    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential0, State1)),
    ?assertEqual(
        ignore, emqx_authn_jwt:authenticate(Credential0#{password => <<"badpassword">>}, State0)
    ),

    GoodConfig = BadConfig1#{
        ssl => ClientSSLOpts,
        verify_claims => [{<<"foo">>, <<"${username}">>}]
    },

    Payload1 = #{
        <<"username">> => <<"myuser">>,
        <<"foo">> => <<"myuser">>,
        <<"exp">> => erlang:system_time(second) + 10
    },
    Payload2 = #{
        <<"username">> => <<"myuser">>,
        <<"foo">> => <<"notmyuser">>,
        <<"exp">> => erlang:system_time(second) + 10
    },
    JWS1 = generate_jws('public-key', Payload1, PrivateKey),
    JWS2 = generate_jws('public-key', Payload2, PrivateKey),
    Credential1 = #{
        username => <<"myuser">>,
        password => JWS1
    },

    ok = snabbkaffe:start_trace(),

    {{ok, State2}, _} = ?wait_async_action(
        emqx_authn_jwt:update(GoodConfig, State1),
        #{?snk_kind := jwks_endpoint_response},
        10000
    ),

    ok = snabbkaffe:stop(),

    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential1, State2)),
    ?assertEqual(
        {error, bad_username_or_password},
        emqx_authn_jwt:authenticate(Credential1#{password => JWS2}, State2)
    ),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State2)),
    ok = emqx_authn_http_test_server:stop().

t_jwks_custom_headers(_Config) ->
    {ok, _} = emqx_authn_http_test_server:start_link(?JWKS_PORT, ?JWKS_PATH, server_ssl_opts()),
    on_exit(fun() -> ok = emqx_authn_http_test_server:stop() end),
    ok = emqx_authn_http_test_server:set_handler(jwks_handler_spy()),

    Endpoint = iolist_to_binary("https://127.0.0.1:" ++ integer_to_list(?JWKS_PORT) ++ ?JWKS_PATH),
    Config0 = #{
        <<"mechanism">> => <<"jwt">>,
        <<"use_jwks">> => true,
        <<"from">> => <<"password">>,
        <<"endpoint">> => Endpoint,
        <<"headers">> => #{
            <<"Accept">> => <<"application/json">>,
            <<"Content-Type">> => <<>>,
            <<"foo">> => <<"bar">>
        },
        <<"pool_size">> => 1,
        <<"refresh_interval">> => 1_000,
        <<"ssl">> => #{
            <<"keyfile">> => cert_file("client.key"),
            <<"certfile">> => cert_file("client.crt"),
            <<"cacertfile">> => cert_file("ca.crt"),
            <<"enable">> => true,
            <<"verify">> => <<"verify_peer">>,
            <<"server_name_indication">> => <<"authn-server">>
        },
        <<"verify_claims">> => #{<<"foo">> => <<"${username}">>}
    },
    {ok, Config} = hocon:binary(hocon_pp:do(Config0, #{})),
    ChainName = 'mqtt:global',
    AuthenticatorId = <<"jwt">>,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            %% bad header keys
            BadConfig1 = emqx_utils_maps:deep_put(
                [<<"headers">>, <<"ça-va"/utf8>>], Config, <<"bien">>
            ),
            ?assertMatch(
                {error, #{
                    kind := validation_error,
                    reason := <<"headers should contain only characters matching ", _/binary>>
                }},
                emqx_authn_api:update_config(
                    [authentication],
                    {create_authenticator, ChainName, BadConfig1}
                )
            ),
            BadConfig2 = emqx_utils_maps:deep_put(
                [<<"headers">>, <<"test_哈哈"/utf8>>],
                Config,
                <<"test_haha">>
            ),
            ?assertMatch(
                {error, #{
                    kind := validation_error,
                    reason := <<"headers should contain only characters matching ", _/binary>>
                }},
                emqx_authn_api:update_config(
                    [authentication],
                    {create_authenticator, ChainName, BadConfig2}
                )
            ),
            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    emqx_authn_api:update_config(
                        [authentication],
                        {create_authenticator, ChainName, Config}
                    ),
                    #{?snk_kind := jwks_endpoint_response},
                    5_000
                ),
            ?assertReceive(
                {http_request, #{
                    headers := #{
                        <<"accept">> := <<"application/json">>,
                        <<"foo">> := <<"bar">>
                    }
                }}
            ),
            {ok, _} = emqx_authn_api:update_config(
                [authentication],
                {delete_authenticator, ChainName, AuthenticatorId}
            ),
            ok
        end,
        []
    ),
    ok.

t_verify_claims(_) ->
    Secret = <<"abcdef">>,
    Config0 = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        algorithm => 'hmac-based',
        secret => Secret,
        secret_base64_encoded => false,
        verify_claims => [{<<"foo">>, <<"bar">>}],
        disconnect_after_expire => false
    },
    {ok, State0} = emqx_authn_jwt:create(?AUTHN_ID, Config0),

    Payload0 = #{
        <<"username">> => <<"myuser">>,
        <<"foo">> => <<"bar">>,
        <<"exp">> => erlang:system_time(second) + 10
    },
    JWS0 = generate_jws('hmac-based', Payload0, Secret),
    Credential0 = #{
        username => <<"myuser">>,
        password => JWS0
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential0, State0)),

    Config1 = Config0#{
        verify_claims => [{<<"foo">>, <<"${username}">>}]
    },
    {ok, State1} = emqx_authn_jwt:update(Config1, State0),

    Payload1 = #{<<"username">> => <<"myuser">>, <<"exp">> => erlang:system_time(second) + 10},
    JWS1 = generate_jws('hmac-based', Payload1, Secret),
    Credential1 = #{
        username => <<"myuser">>,
        password => JWS1
    },
    ?assertEqual(
        {error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential1, State1)
    ),

    Payload2 = #{
        <<"username">> => <<"myuser">>,
        <<"foo">> => <<"notmyuser">>,
        <<"exp">> => erlang:system_time(second) + 10
    },
    JWS2 = generate_jws('hmac-based', Payload2, Secret),
    Credential2 = #{
        username => <<"myuser">>,
        password => JWS2
    },
    ?assertEqual(
        {error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential2, State1)
    ),

    Payload3 = #{
        <<"username">> => <<"myuser">>,
        <<"foo">> => <<"myuser">>,
        <<"exp">> => erlang:system_time(second) + 10
    },
    JWS3 = generate_jws('hmac-based', Payload3, Secret),
    Credential3 = #{
        username => <<"myuser">>,
        password => JWS3
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential3, State1)),

    %% No exp treated as unexpired
    Payload4 = #{<<"username">> => <<"myuser">>, <<"foo">> => <<"myuser">>},
    JWS4 = generate_jws('hmac-based', Payload4, Secret),
    Credential4 = #{
        username => <<"myuser">>,
        password => JWS4
    },
    ?assertMatch(
        {ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential4, State1)
    ),

    Payload5 = #{
        <<"username">> => <<"myuser">>,
        <<"foo">> => <<"myuser">>,
        <<"exp">> => erlang:system_time(second) + 10.5
    },
    JWS5 = generate_jws('hmac-based', Payload5, Secret),
    Credential5 = #{
        username => <<"myuser">>,
        password => JWS5
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential5, State1)).

t_verify_claim_clientid(_) ->
    Secret = <<"abcdef">>,
    Config = #{
        mechanism => jwt,
        from => password,
        acl_claim_name => <<"acl">>,
        use_jwks => false,
        algorithm => 'hmac-based',
        secret => Secret,
        secret_base64_encoded => false,
        verify_claims => [{<<"cl">>, <<"${clientid}">>}],
        disconnect_after_expire => false
    },
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload0 = #{<<"cl">> => <<"mycl">>, <<"exp">> => erlang:system_time(second) + 60},
    JWS0 = generate_jws('hmac-based', Payload0, Secret),
    Credential0 = #{
        username => <<"myuser">>,
        clientid => <<"mycl">>,
        password => JWS0
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential0, State)),

    Credential1 = #{
        username => <<"myuser">>,
        clientid => <<"mycl-invalid">>,
        password => JWS0
    },
    ?assertMatch(
        {error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential1, State)
    ).

t_jwt_not_allow_empty_claim_name(_) ->
    Request = #{
        <<"use_jwks">> => false,
        <<"algorithm">> => <<"hmac-based">>,
        <<"acl_claim_name">> => <<"acl">>,
        <<"secret">> => <<"secret">>,
        <<"mechanism">> => <<"jwt">>
    },
    ?assertMatch(
        {200, _},
        emqx_authn_api:authenticators(
            post, #{body => Request}
        )
    ),

    ?assertMatch(
        {400, _},
        emqx_authn_api:authenticator(
            put, #{
                bindings => #{id => <<"jwt">>},
                body => Request#{<<"verify_claims">> => #{<<>> => <<>>}}
            }
        )
    ),

    ?assertMatch(
        {204},
        emqx_authn_api:authenticator(
            put, #{
                bindings => #{id => <<"jwt">>},
                body => Request#{<<"verify_claims">> => #{<<"key">> => <<>>}}
            }
        )
    ).

t_schema(_Config) ->
    RawClaims0 = [
        #{<<"name">> => <<"a">>, <<"value">> => <<"v">>},
        #{<<"name">> => <<"b">>, <<"value">> => <<"${username}">>},
        #{<<"name">> => <<"c">>, <<"value">> => <<"${clientid}">>}
    ],
    ?assertMatch(
        {ok, [
            {<<"a">>, <<"v">>},
            {<<"b">>, <<"${username}">>},
            {<<"c">>, <<"${clientid}">>}
        ]},
        check_schema(RawClaims0)
    ),

    RawClaims1 = [#{<<"key">> => <<"a">>, <<"value">> => <<"v">>}],
    ?assertMatch(
        {error, _},
        check_schema(RawClaims1)
    ),
    RawClaims2 = #{
        <<"a">> => <<"v">>,
        <<"b">> => <<"${username}">>,
        <<"c">> => <<"${clientid}">>
    },
    ?assertMatch(
        {ok, [
            {<<"a">>, <<"v">>},
            {<<"b">>, <<"${username}">>},
            {<<"c">>, <<"${clientid}">>}
        ]},
        check_schema(RawClaims2)
    ),
    ?assertMatch(
        {ok, [{<<"x">>, <<"${foo}">>}]},
        check_schema(#{<<"x">> => <<"${foo}">>})
    ),
    ?assertMatch(
        {error, _},
        check_schema([<<"foo">>])
    ),
    ?assertMatch(
        {error, _},
        check_schema([#{}])
    ),
    ?assertMatch(
        {error, _},
        check_schema([[]])
    ),
    ?assertMatch(
        {error, _},
        check_schema(<<"val">>)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

check_schema(RawClaims) ->
    Config = #{
        <<"conf">> =>
            #{
                <<"use_jwks">> => false,
                <<"algorithm">> => <<"hmac-based">>,
                <<"acl_claim_name">> => <<"acl">>,
                <<"secret">> => <<"secret">>,
                <<"mechanism">> => <<"jwt">>,
                <<"verify_claims">> => RawClaims
            }
    },
    UnionMemberSelector =
        fun
            (all_union_members) -> emqx_authn_jwt_schema:refs();
            ({value, Value}) -> emqx_authn_jwt_schema:select_union_member(Value)
        end,
    Schema = #{roots => [{conf, hoconsc:union(UnionMemberSelector)}]},
    case emqx_hocon:check(Schema, Config) of
        {ok, #{conf := #{verify_claims := VerifyClaims}}} ->
            {ok, VerifyClaims};
        Error ->
            Error
    end.

jwks_handler(Req0, State) ->
    JWK = jose_jwk:from_pem_file(test_rsa_key(public)),
    JWKS = jose_jwk_set:to_map([JWK], #{}),
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(JWKS),
        Req0
    ),
    {ok, Req, State}.

jwks_handler_spy() ->
    TestPid = self(),
    fun(Req, State) ->
        ReqHeaders = cowboy_req:headers(Req),
        ReqMap = #{headers => ReqHeaders},
        ct:pal("jwks request:\n  ~p", [ReqMap]),
        TestPid ! {http_request, ReqMap},
        jwks_handler(Req, State)
    end.

test_rsa_key(public) ->
    data_file("public_key.pem");
test_rsa_key(private) ->
    data_file("private_key.pem").

data_file(Name) ->
    Dir = code:lib_dir(emqx_auth),
    list_to_binary(filename:join([Dir, "test", "data", Name])).

cert_file(Name) ->
    data_file(filename:join(["certs", Name])).

generate_jws('hmac-based', Payload, Secret) ->
    JWK = jose_jwk:from_oct(Secret),
    Header = #{
        <<"alg">> => <<"HS256">>,
        <<"typ">> => <<"JWT">>
    },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS;
generate_jws('public-key', Payload, PrivateKey) ->
    JWK = jose_jwk:from_pem_file(PrivateKey),
    Header = #{
        <<"alg">> => <<"RS256">>,
        <<"typ">> => <<"JWT">>
    },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS.

client_ssl_opts() ->
    #{
        keyfile => cert_file("client.key"),
        certfile => cert_file("client.crt"),
        cacertfile => cert_file("ca.crt"),
        enable => true,
        verify => verify_peer,
        server_name_indication => "authn-server"
    }.

server_ssl_opts() ->
    [
        {keyfile, cert_file("server.key")},
        {certfile, cert_file("server.crt")},
        {cacertfile, cert_file("ca.crt")},
        {verify, verify_none}
    ].
