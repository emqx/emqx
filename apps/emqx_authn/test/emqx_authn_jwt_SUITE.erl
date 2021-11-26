%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_authn.hrl").

-define(AUTHN_ID, <<"mechanism:jwt">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    ok.

t_jwt_authenticator(_) ->
    Secret = <<"abcdef">>,
    Config = #{mechanism => jwt,
               use_jwks => false,
               algorithm => 'hmac-based',
               secret => Secret,
               secret_base64_encoded => false,
               verify_claims => []},
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"username">> => <<"myuser">>},
    JWS = generate_jws('hmac-based', Payload, Secret),
    Credential = #{username => <<"myuser">>,
			       password => JWS},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State)),

    Payload1 = #{<<"username">> => <<"myuser">>, <<"is_superuser">> => true},
    JWS1 = generate_jws('hmac-based', Payload1, Secret),
    Credential1 = #{username => <<"myuser">>,
			        password => JWS1},
    ?assertEqual({ok, #{is_superuser => true}}, emqx_authn_jwt:authenticate(Credential1, State)),

    BadJWS = generate_jws('hmac-based', Payload, <<"bad_secret">>),
    Credential2 = Credential#{password => BadJWS},
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential2, State)),

    %% secret_base64_encoded
    Config2 = Config#{secret => base64:encode(Secret),
                      secret_base64_encoded => true},
    {ok, State2} = emqx_authn_jwt:update(Config2, State),
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State2)),

    %% invalid secret
    BadConfig = Config#{secret => <<"emqxsecret">>,
                        secret_base64_encoded => true},
    {error, {invalid_parameter, secret}} = emqx_authn_jwt:create(?AUTHN_ID, BadConfig),

    Config3 = Config#{verify_claims => [{<<"username">>, <<"${username}">>}]},
    {ok, State3} = emqx_authn_jwt:update(Config3, State2),
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State3)),
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential#{username => <<"otheruser">>}, State3)),

    %% Expiration
    Payload3 = #{ <<"username">> => <<"myuser">>
                , <<"exp">> => erlang:system_time(second) - 60},
    JWS3 = generate_jws('hmac-based', Payload3, Secret),
    Credential3 = Credential#{password => JWS3},
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential3, State3)),

    Payload4 = #{ <<"username">> => <<"myuser">>
                , <<"exp">> => erlang:system_time(second) + 60},
    JWS4 = generate_jws('hmac-based', Payload4, Secret),
    Credential4 = Credential#{password => JWS4},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential4, State3)),

    %% Issued At
    Payload5 = #{ <<"username">> => <<"myuser">>
                , <<"iat">> => erlang:system_time(second) - 60},
    JWS5 = generate_jws('hmac-based', Payload5, Secret),
    Credential5 = Credential#{password => JWS5},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential5, State3)),

    Payload6 = #{ <<"username">> => <<"myuser">>
                , <<"iat">> => erlang:system_time(second) + 60},
    JWS6 = generate_jws('hmac-based', Payload6, Secret),
    Credential6 = Credential#{password => JWS6},
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential6, State3)),

    %% Not Before
    Payload7 = #{ <<"username">> => <<"myuser">>
                , <<"nbf">> => erlang:system_time(second) - 60},
    JWS7 = generate_jws('hmac-based', Payload7, Secret),
    Credential7 = Credential6#{password => JWS7},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential7, State3)),

    Payload8 = #{ <<"username">> => <<"myuser">>
                , <<"nbf">> => erlang:system_time(second) + 60},
    JWS8 = generate_jws('hmac-based', Payload8, Secret),
    Credential8 = Credential#{password => JWS8},
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential8, State3)),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State3)),
    ok.

t_jwt_authenticator2(_) ->
    Dir = code:lib_dir(emqx_authn, test),
    PublicKey = list_to_binary(filename:join([Dir, "data/public_key.pem"])),
    PrivateKey = list_to_binary(filename:join([Dir, "data/private_key.pem"])),
    Config = #{mechanism => jwt,
               use_jwks => false,
               algorithm => 'public-key',
               certificate => PublicKey,
               verify_claims => []},
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"username">> => <<"myuser">>},
    JWS = generate_jws('public-key', Payload, PrivateKey),
    Credential = #{username => <<"myuser">>,
			       password => JWS},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State)),
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential#{password => <<"badpassword">>}, State)),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State)),
    ok.

generate_jws('hmac-based', Payload, Secret) ->
    JWK = jose_jwk:from_oct(Secret),
    Header = #{ <<"alg">> => <<"HS256">>
              , <<"typ">> => <<"JWT">>
              },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS;
generate_jws('public-key', Payload, PrivateKey) ->
    JWK = jose_jwk:from_pem_file(PrivateKey),
    Header = #{ <<"alg">> => <<"RS256">>
              , <<"typ">> => <<"JWT">>
              },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS.
