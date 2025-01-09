%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_authn_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").

-define(PATH, [authentication]).
-define(DEVICE_ID, <<"test-device">>).
-define(PROJECT, <<"iot-export">>).
-define(LOCATION, <<"europe-west1">>).
-define(REGISTRY, <<"my-registry">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config0) ->
    ok = snabbkaffe:start_trace(),
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_gcp_device], #{
        work_dir => ?config(priv_dir, Config0)
    }),
    ValidExpirationTime = erlang:system_time(second) + 3600,
    ValidJWT = generate_jws(ValidExpirationTime),
    ExpiredJWT = generate_jws(0),
    ValidClient = generate_client(ValidExpirationTime),
    ExpiredClient = generate_client(0),
    [
        {device_id, ?DEVICE_ID},
        {client_id, client_id()},
        {valid_jwt, ValidJWT},
        {expired_jwt, ExpiredJWT},
        {valid_client, ValidClient},
        {expired_client, ExpiredClient},
        {apps, Apps}
        | Config0
    ].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_Case, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_config(),
    {ok, _} = emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}),
    ?assertMatch(
        {ok, [#{provider := emqx_gcp_device_authn}]},
        emqx_authn_chains:list_authenticators(?GLOBAL)
    ).

t_destroy(Config) ->
    ClientId = ?config(client_id, Config),
    JWT = ?config(valid_jwt, Config),
    Credential = credential(ClientId, JWT),
    Client = ?config(valid_client, Config),
    AuthConfig = raw_config(),
    {ok, _} = emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}),
    ok = emqx_gcp_device:put_device(Client),
    ?assertMatch(
        {ok, _},
        emqx_access_control:authenticate(Credential)
    ),
    emqx_authn_test_lib:delete_authenticators([authentication], ?GLOBAL),
    ?assertMatch(
        ignore,
        emqx_gcp_device_authn:authenticate(Credential, #{})
    ).

t_expired_client(Config) ->
    ClientId = ?config(client_id, Config),
    JWT = ?config(expired_jwt, Config),
    Credential = credential(ClientId, JWT),
    Client = ?config(expired_client, Config),
    AuthConfig = raw_config(),
    {ok, _} = emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}),
    ?assertMatch(
        {ok, [#{provider := emqx_gcp_device_authn}]},
        emqx_authn_chains:list_authenticators(?GLOBAL)
    ),
    ok = emqx_gcp_device:put_device(Client),
    ?assertMatch(
        {error, not_authorized},
        emqx_access_control:authenticate(Credential)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_config() ->
    #{
        <<"mechanism">> => <<"gcp_device">>,
        <<"enable">> => <<"true">>
    }.

generate_client(ExpirationTime) ->
    generate_client(?DEVICE_ID, ExpirationTime).

generate_client(ClientId, ExpirationTime) ->
    #{
        deviceid => ClientId,
        project => ?PROJECT,
        location => ?LOCATION,
        registry => ?REGISTRY,
        config => <<>>,
        keys =>
            [
                #{
                    key_type => <<"RSA_PEM">>,
                    key => public_key(),
                    expires_at => ExpirationTime
                }
            ]
    }.

client_id() ->
    client_id(?DEVICE_ID).

client_id(DeviceId) ->
    <<"projects/", ?PROJECT/binary, "/locations/", ?LOCATION/binary, "/registries/",
        ?REGISTRY/binary, "/devices/", DeviceId/binary>>.

generate_jws(ExpirationTime) ->
    Payload = #{<<"exp">> => ExpirationTime},
    JWK = jose_jwk:from_pem_file(test_rsa_key(private)),
    Header = #{<<"alg">> => <<"RS256">>, <<"typ">> => <<"JWT">>},
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS.

public_key() ->
    {ok, Data} = file:read_file(test_rsa_key(public)),
    Data.

private_key() ->
    {ok, Data} = file:read_file(test_rsa_key(private)),
    Data.

test_rsa_key(public) ->
    data_file("public_key.pem");
test_rsa_key(private) ->
    data_file("private_key.pem").

data_file(Name) ->
    Dir = code:lib_dir(emqx_auth),
    list_to_binary(filename:join([Dir, "test", "data", Name])).

credential(ClientId, JWT) ->
    #{
        listener => 'tcp:default',
        protocol => mqtt,
        clientid => ClientId,
        password => JWT
    }.

check(Module, HoconConf) ->
    emqx_hocon:check(Module, ["authentication= ", HoconConf]).
