%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_authn/include/emqx_authn.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(PATH, [authentication]).
-define(BASE_CONF, <<
    ""
    "\n"
    "retainer {\n"
    "    enable = true\n"
    "}"
    ""
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_retainer_schema, ?BASE_CONF),
    ok = emqx_common_test_helpers:start_apps([emqx_gcp_device, emqx_authn, emqx_conf, emqx_retainer]),
    emqx_dashboard_api_test_helpers:set_default_config(),
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    _ = emqx_common_test_helpers:stop_apps([emqx_authn, emqx_retainer, emqx_gcp_device]),
    Config.

init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    clear_data(),
    Config.

end_per_testcase(_TestCase, Config) ->
    clear_data(),
    Config.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_import(_Config) ->
    ?assertMatch(
        {ok, #{<<"errors">> := 0, <<"imported">> := 14}},
        api(post, ["gcp_devices"], emqx_gcp_device_test_helpers:exported_data())
    ),

    InvalidData =
        [
            #{<<"deviceid">> => <<"device1">>, <<"device_numid">> => <<"device1">>},
            #{<<"name">> => []}
        ],
    ?assertMatch({error, {_, 400, _}}, api(post, ["gcp_devices"], InvalidData)),

    ?assertMatch(
        {ok, #{<<"meta">> := #{<<"count">> := 14}}},
        api(get, ["gcp_devices"])
    ),

    ?assertMatch(
        {ok, #{
            <<"meta">> :=
                #{
                    <<"count">> := 14,
                    <<"page">> := 2,
                    <<"limit">> := 3
                }
        }},
        api(get, ["gcp_devices"], [{"limit", "3"}, {"page", "2"}])
    ).

t_device_crud_ok(_Config) ->
    AuthConfig = raw_config(),
    DeviceId = <<"my device">>,
    DeviceIdReq = emqx_http_lib:uri_encode(DeviceId),
    ConfigTopic = emqx_gcp_device:config_topic(DeviceId),
    DeviceConfig = <<"myconfig">>,
    EncodedConfig = base64:encode(DeviceConfig),
    {ok, _} = emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}),

    Payload = #{<<"exp">> => erlang:system_time(second) + 3600},
    JWT = generate_jws(Payload, <<"ES256_PEM">>, "c1_ec_private.pem"),
    ClientInfo = client_info(client_id(DeviceId), JWT),
    ?assertMatch(
        {error, _},
        emqx_access_control:authenticate(ClientInfo)
    ),
    Device0 =
        #{
            <<"project">> => <<"iot-export">>,
            <<"location">> => <<"europe-west1">>,
            <<"registry">> => <<"my-registry">>,
            <<"keys">> =>
                [
                    #{
                        <<"key">> => emqx_gcp_device_test_helpers:key("c1_ec_public.pem"),
                        <<"key_type">> => <<"ES256_PEM">>,
                        <<"expires_at">> => 0
                    },
                    #{
                        <<"key">> => emqx_gcp_device_test_helpers:key("c1_ec_public.pem"),
                        <<"key_type">> => <<"ES256_PEM">>,
                        <<"expires_at">> => 0
                    }
                ],
            <<"config">> => EncodedConfig
        },
    ?assertMatch(
        {ok, #{<<"deviceid">> := DeviceId}},
        api(put, ["gcp_devices", DeviceIdReq], Device0)
    ),
    ?assertMatch(
        {ok, _},
        emqx_access_control:authenticate(ClientInfo)
    ),

    ?retry(
        _Sleep = 100,
        _Attempts = 10,
        ?assertMatch(
            {ok, [#message{payload = DeviceConfig}]},
            emqx_retainer:read_message(ConfigTopic)
        )
    ),
    ?assertMatch(
        {ok, #{
            <<"project">> := <<"iot-export">>,
            <<"location">> := <<"europe-west1">>,
            <<"registry">> := <<"my-registry">>,
            <<"keys">> :=
                [
                    #{
                        <<"key">> := _,
                        <<"key_type">> := <<"ES256_PEM">>,
                        <<"expires_at">> := 0
                    },
                    #{
                        <<"key">> := _,
                        <<"key_type">> := <<"ES256_PEM">>,
                        <<"expires_at">> := 0
                    }
                ],
            <<"config">> := EncodedConfig
        }},
        api(get, ["gcp_devices", DeviceIdReq])
    ),

    Device1 = maps:without([<<"project">>, <<"location">>, <<"registry">>], Device0),
    ?assertMatch(
        {ok, #{<<"deviceid">> := DeviceId}},
        api(put, ["gcp_devices", DeviceIdReq], Device1)
    ),

    ?assertMatch(
        {ok, #{
            <<"project">> := <<>>,
            <<"location">> := <<>>,
            <<"registry">> := <<>>
        }},
        api(get, ["gcp_devices", DeviceIdReq])
    ),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, api(delete, ["gcp_devices", DeviceIdReq])),

    ?retry(
        _Sleep = 100,
        _Attempts = 10,
        ?assertNotMatch(
            {ok, [#message{payload = DeviceConfig}]},
            emqx_retainer:read_message(ConfigTopic)
        )
    ),
    ?assertMatch({error, {_, 404, _}}, api(get, ["gcp_devices", DeviceIdReq])).

t_device_crud_nok(_Config) ->
    DeviceId = <<"my device">>,
    DeviceIdReq = emqx_http_lib:uri_encode(DeviceId),
    Config = <<"myconfig">>,
    EncodedConfig = base64:encode(Config),

    BadDevices =
        [
            #{
                <<"project">> => 5,
                <<"keys">> => [],
                <<"config">> => EncodedConfig
            },
            #{
                <<"keys">> => <<"keys">>,
                <<"config">> => EncodedConfig
            },
            #{
                <<"keys">> => [<<"key">>],
                <<"config">> => EncodedConfig
            },
            #{
                <<"keys">> => [#{<<"key">> => <<"key">>}],
                <<"config">> => EncodedConfig
            },
            #{
                <<"keys">> => [#{<<"key_type">> => <<"ES256_PEM">>}],
                <<"config">> => EncodedConfig
            },
            #{
                <<"keys">> =>
                    [
                        #{
                            <<"key">> => <<"key">>,
                            <<"key_type">> => <<"ES256_PEM">>,
                            <<"expires_at">> => <<"123">>
                        }
                    ],
                <<"config">> => EncodedConfig
            }
        ],

    lists:foreach(
        fun(BadDevice) ->
            ?assertMatch(
                {error, {_, 400, _}},
                api(put, ["gcp_devices", DeviceIdReq], BadDevice)
            )
        end,
        BadDevices
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

assert_no_retained(ConfigTopic) ->
    {ok, Pid} = emqtt:start_link(),
    {ok, _} = emqtt:connect(Pid),
    {ok, _, _} = emqtt:subscribe(Pid, ConfigTopic, 0),

    receive
        {publish, #{payload := Config}} ->
            ct:fail("Unexpected config received: ~p", [Config])
    after 100 ->
        ok
    end,

    _ = emqtt:stop(Pid).

api(get, Path) ->
    api(get, Path, "");
api(delete, Path) ->
    api(delete, Path, []).

api(get, Path, Query) ->
    maybe_decode_response(
        emqx_mgmt_api_test_util:request_api(
            get,
            emqx_mgmt_api_test_util:api_path(Path),
            uri_string:compose_query(Query),
            emqx_mgmt_api_test_util:auth_header_()
        )
    );
api(delete, Path, Query) ->
    emqx_mgmt_api_test_util:request_api(
        delete,
        emqx_mgmt_api_test_util:api_path(Path),
        uri_string:compose_query(Query),
        emqx_mgmt_api_test_util:auth_header_(),
        [],
        #{return_all => true}
    );
api(Method, Path, Data) when
    Method =:= put orelse Method =:= post
->
    api(Method, Path, [], Data).

api(Method, Path, Query, Data) when
    Method =:= put orelse Method =:= post
->
    maybe_decode_response(
        emqx_mgmt_api_test_util:request_api(
            Method,
            emqx_mgmt_api_test_util:api_path(Path),
            uri_string:compose_query(Query),
            emqx_mgmt_api_test_util:auth_header_(),
            Data
        )
    ).

maybe_decode_response({ok, ResponseBody}) ->
    {ok, jiffy:decode(list_to_binary(ResponseBody), [return_maps])};
maybe_decode_response({error, _} = Error) ->
    Error.

generate_jws(Payload, KeyType, PrivateKeyName) ->
    emqx_gcp_device_test_helpers:generate_jws(Payload, KeyType, PrivateKeyName).

client_info(ClientId, Password) ->
    emqx_gcp_device_test_helpers:client_info(ClientId, Password).

client_id(DeviceId) ->
    emqx_gcp_device_test_helpers:client_id(DeviceId).

raw_config() ->
    #{
        <<"mechanism">> => <<"gcp_device">>,
        <<"enable">> => <<"true">>
    }.

clear_data() ->
    emqx_gcp_device_test_helpers:clear_data(),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok.
