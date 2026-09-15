%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_utils).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

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

%% Reads the connector via the HTTP API, then sends the redacted body back via update and
%% probe.  Both must succeed, and the stored service account JSON must stay unchanged.
%% Expects the connector to be already created.
assert_redacted_service_account_json_round_trip(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    ServiceAccountJSON = proplists:get_value(service_account_json, TCConfig),
    ?assertEqual(ServiceAccountJSON, persisted_service_account_json(Type, Name)),
    {200, #{<<"service_account_json">> := <<"******">>} = RedactedParams0} =
        emqx_bridge_v2_testlib:simplify_result(
            emqx_bridge_v2_testlib:get_connector_api(Type, Name)
        ),
    RedactedParams = maps:without(
        [
            <<"actions">>,
            <<"sources">>,
            <<"name">>,
            <<"type">>,
            <<"status">>,
            <<"status_reason">>,
            <<"node_status">>
        ],
        RedactedParams0
    ),
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>, <<"service_account_json">> := <<"******">>}},
        emqx_bridge_v2_testlib:simplify_result(
            emqx_bridge_v2_testlib:update_connector_api(Name, Type, RedactedParams)
        )
    ),
    ?assertEqual(ServiceAccountJSON, persisted_service_account_json(Type, Name)),
    ?assertMatch(
        {200, #{<<"service_account_json">> := <<"******">>}},
        emqx_bridge_v2_testlib:simplify_result(
            emqx_bridge_v2_testlib:get_connector_api(Type, Name)
        )
    ),
    ?assertMatch(
        {204, _},
        emqx_bridge_v2_testlib:probe_connector_api2(TCConfig, RedactedParams)
    ),
    %% A probe for a connector that does not exist has no stored value to restore.
    ?assertMatch(
        {400, _},
        emqx_bridge_v2_testlib:probe_connector_api2(
            [{connector_name, <<Name/binary, "_new">>} | TCConfig],
            RedactedParams
        )
    ),
    ok.

%% Returns the service account JSON stored in cluster.hocon, decoded to a map.
persisted_service_account_json(Type, Name) ->
    {ok, Hocon} = hocon:files([application:get_env(emqx, cluster_hocon_file, undefined)]),
    case
        emqx_utils_maps:deep_get(
            [<<"connectors">>, Type, Name, <<"service_account_json">>],
            Hocon
        )
    of
        Bin when is_binary(Bin) ->
            emqx_utils_json:decode(Bin, [return_maps]);
        Map when is_map(Map) ->
            Map
    end.
