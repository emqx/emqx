%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(AUTHN_ID, <<"mechanism:jwt">>).

-define(JWKS_PORT, 31333).
-define(JWKS_PATH, "/jwks.json").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_authn]),
    application:ensure_all_started(emqx_resource),
    application:ensure_all_started(emqx_connector),
    Config.

end_per_suite(_) ->
    application:stop(emqx_connector),
    application:stop(emqx_resource),
    emqx_common_test_helpers:stop_apps([emqx_authn]),
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
        verify_claims => [{<<"username">>, <<"${username}">>}]
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

    %% Issued At
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
    ?assertEqual(
        {error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential6, State3)
    ),

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
        algorithm => 'public-key',
        public_key => PublicKey,
        verify_claims => []
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
        verify_claims => []
    },
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"exp">> => erlang:system_time(second) + 60},
    JWS = generate_jws('hmac-based', Payload, Secret),
    Credential = #{
        username => JWS,
        password => <<"pass">>
    },
    ?assertMatch({ok, #{is_superuser := false}}, emqx_authn_jwt:authenticate(Credential, State)).

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

        use_jwks => true,
        endpoint => "https://127.0.0.1:" ++ integer_to_list(?JWKS_PORT + 1) ++ ?JWKS_PATH,
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
        verify_claims => [{<<"foo">>, <<"bar">>}]
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
        {200, _},
        emqx_authn_api:authenticator(
            put, #{
                bindings => #{id => <<"jwt">>},
                body => Request#{<<"verify_claims">> => #{<<"key">> => <<>>}}
            }
        )
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

jwks_handler(Req0, State) ->
    JWK = jose_jwk:from_pem_file(test_rsa_key(public)),
    JWKS = jose_jwk_set:to_map([JWK], #{}),
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        jiffy:encode(JWKS),
        Req0
    ),
    {ok, Req, State}.

test_rsa_key(public) ->
    data_file("public_key.pem");
test_rsa_key(private) ->
    data_file("private_key.pem").

data_file(Name) ->
    Dir = code:lib_dir(emqx_authn, test),
    list_to_binary(filename:join([Dir, "data", Name])).

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
