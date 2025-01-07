%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-define(KEYS, [
    {<<"c1-ec">>, <<"ES256_PEM">>, <<"c1_ec_private.pem">>, <<"c1_ec_public.pem">>},
    {<<"c2-ec-x509">>, <<"ES256_X509_PEM">>, <<"c2_ec_private.pem">>, <<"c2_ec_cert.pem">>},
    {<<"c3-rsa">>, <<"RSA_PEM">>, <<"c3_rsa_private.pem">>, <<"c3_rsa_public.pem">>},
    {<<"c4-rsa-x509">>, <<"RSA_X509_PEM">>, <<"c4_rsa_private.pem">>, <<"c4_rsa_cert.pem">>}
]).

exported_data() ->
    FileName =
        filename:join([code:lib_dir(emqx_gcp_device), "test", "data", "gcp-data.json"]),
    {ok, Data} = file:read_file(FileName),
    jiffy:decode(Data, [return_maps]).

key(Name) ->
    {ok, Data} = file:read_file(key_path(Name)),
    Data.

key_path(Name) ->
    filename:join([code:lib_dir(emqx_gcp_device), "test", "data", "keys", Name]).

clear_data() ->
    {atomic, ok} = mria:clear_table(emqx_gcp_device),
    ok = emqx_retainer:clean(),
    ok.

keys() ->
    ?KEYS.

client_id(DeviceId) ->
    <<"projects/iot-export/locations/europe-west1/registries/my-registry/devices/",
        DeviceId/binary>>.

generate_jws(Payload, KeyType, PrivateKeyName) ->
    JWK = jose_jwk:from_pem_file(
        emqx_gcp_device_test_helpers:key_path(PrivateKeyName)
    ),
    Header = #{<<"alg">> => alg(KeyType), <<"typ">> => <<"JWT">>},
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS.

alg(<<"ES256_PEM">>) ->
    <<"ES256">>;
alg(<<"ES256_X509_PEM">>) ->
    <<"ES256">>;
alg(<<"RSA_PEM">>) ->
    <<"RS256">>;
alg(<<"RSA_X509_PEM">>) ->
    <<"RS256">>.

client_info(ClientId, JWT) ->
    #{
        listener => 'tcp:default',
        protocol => mqtt,
        clientid => ClientId,
        password => JWT
    }.
