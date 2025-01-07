%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_utils).

-compile(nowarn_export_all).
-compile(export_all).

generate_service_account_json() ->
    PrivateKeyPEM = generate_private_key_pem(),
    service_account_json(PrivateKeyPEM).

generate_private_key_pem() ->
    PublicExponent = 65537,
    Size = 2048,
    Key = public_key:generate_key({rsa, Size, PublicExponent}),
    DERKey = public_key:der_encode('PrivateKeyInfo', Key),
    public_key:pem_encode([{'PrivateKeyInfo', DERKey, not_encrypted}]).

service_account_json(PrivateKeyPEM) ->
    #{
        <<"type">> => <<"service_account">>,
        <<"project_id">> => <<"myproject">>,
        <<"private_key_id">> => <<"kid">>,
        <<"private_key">> => PrivateKeyPEM,
        <<"client_email">> => <<"test@myproject.iam.gserviceaccount.com">>,
        <<"client_id">> => <<"123812831923812319190">>,
        <<"auth_uri">> => <<"https://accounts.google.com/o/oauth2/auth">>,
        <<"token_uri">> => <<"https://oauth2.googleapis.com/token">>,
        <<"auth_provider_x509_cert_url">> => <<"https://www.googleapis.com/oauth2/v1/certs">>,
        <<"client_x509_cert_url">> =>
            <<"https://www.googleapis.com/robot/v1/metadata/x509/test%40myproject.iam.gserviceaccount.com">>
    }.
