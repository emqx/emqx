%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/emqx.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_auth, emqx_gcp_device, {emqx_retainer, "retainer {enable = true}"}],
        #{
            work_dir => ?config(priv_dir, Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
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

t_ignore_non_jwt(_Config) ->
    ClientId = gcp_client_id(<<"clientid">>),
    ClientInfo = client_info(ClientId, <<"non_jwt_password">>),
    ?check_trace(
        ?assertEqual(
            ignore,
            emqx_gcp_device_authn:authenticate(ClientInfo, #{})
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{result := ignore, reason := "not a JWT"}],
                ?of_kind(authn_gcp_device_check, Trace)
            )
        end
    ),
    ok.

t_ignore_non_gcp_clientid(_Config) ->
    % GCP Client pattern:
    % projects/<project>/locations/<location>/registries/<registry>/devices/<deviceid>
    NonGCPClientIdList = [
        <<"non_gcp_clientid">>,
        <<"projects/non_gcp_client">>,
        <<"projects/proj/locations/non_gcp_client">>,
        <<"projects/proj/locations/loc/registries/non_gcp_client">>,
        <<"projects/proj/locations/loc/registries/reg/device/non_gcp_client">>
    ],
    [{_DeviceId, KeyType, PrivateKeyName, _PublicKey} | _] = keys(),
    Payload = #{<<"exp">> => 0},
    JWT = generate_jws(Payload, KeyType, PrivateKeyName),
    lists:foreach(
        fun(ClientId) ->
            ClientInfo = client_info(ClientId, JWT),
            ?check_trace(
                ?assertEqual(
                    ignore,
                    emqx_gcp_device_authn:authenticate(ClientInfo, #{}),
                    ClientId
                ),
                fun(Trace) ->
                    ?assertMatch(
                        [#{result := ignore, reason := "not a GCP ClientId"}],
                        ?of_kind(authn_gcp_device_check, Trace),
                        ClientId
                    )
                end
            )
        end,
        NonGCPClientIdList
    ),
    ok.

t_deny_expired_jwt(_Config) ->
    lists:foreach(
        fun({DeviceId, KeyType, PrivateKeyName, _PublicKey}) ->
            ClientId = gcp_client_id(DeviceId),
            Payload = #{<<"exp">> => 0},
            JWT = generate_jws(Payload, KeyType, PrivateKeyName),
            ClientInfo = client_info(ClientId, JWT),
            ?check_trace(
                ?assertMatch(
                    {error, _},
                    emqx_gcp_device_authn:authenticate(ClientInfo, #{}),
                    DeviceId
                ),
                fun(Trace) ->
                    ?assertMatch(
                        [#{result := not_authorized, reason := "expired JWT"}],
                        ?of_kind(authn_gcp_device_check, Trace),
                        DeviceId
                    )
                end
            )
        end,
        keys()
    ),
    ok.

t_no_keys(_Config) ->
    lists:foreach(
        fun({DeviceId, KeyType, PrivateKeyName, _PublicKey}) ->
            ClientId = gcp_client_id(DeviceId),
            Payload = #{<<"exp">> => erlang:system_time(second) + 3600},
            JWT = generate_jws(Payload, KeyType, PrivateKeyName),
            ClientInfo = client_info(ClientId, JWT),
            ?check_trace(
                ?assertMatch(
                    ignore,
                    emqx_gcp_device_authn:authenticate(ClientInfo, #{}),
                    DeviceId
                ),
                fun(Trace) ->
                    ?assertMatch(
                        [#{result := ignore, reason := "key not found"}],
                        ?of_kind(authn_gcp_device_check, Trace),
                        DeviceId
                    )
                end
            )
        end,
        keys()
    ),
    ok.

t_expired_keys(_Config) ->
    lists:foreach(
        fun({DeviceId, KeyType, PrivateKeyName, PublicKey}) ->
            ClientId = gcp_client_id(DeviceId),
            Device = #{
                deviceid => DeviceId,
                config => <<>>,
                keys =>
                    [
                        #{
                            key_type => KeyType,
                            key => key_data(PublicKey),
                            expires_at => erlang:system_time(second) - 3600
                        }
                    ]
            },
            ok = emqx_gcp_device:put_device(Device),
            Payload = #{<<"exp">> => erlang:system_time(second) + 3600},
            JWT = generate_jws(Payload, KeyType, PrivateKeyName),
            ClientInfo = client_info(ClientId, JWT),
            ?check_trace(
                ?assertMatch(
                    {error, _},
                    emqx_gcp_device_authn:authenticate(ClientInfo, #{}),
                    DeviceId
                ),
                fun(Trace) ->
                    ?assertMatch(
                        [
                            #{
                                result := {error, bad_username_or_password},
                                reason := "no matching or valid keys"
                            }
                        ],
                        ?of_kind(authn_gcp_device_check, Trace),
                        DeviceId
                    )
                end
            )
        end,
        keys()
    ),
    ok.

t_valid_keys(_Config) ->
    [
        {DeviceId, KeyType0, PrivateKeyName0, PublicKey0},
        {_DeviceId1, KeyType1, PrivateKeyName1, PublicKey1},
        {_DeviceId2, KeyType2, PrivateKeyName2, _PublicKey}
        | _
    ] = keys(),
    Device = #{
        deviceid => DeviceId,
        config => <<>>,
        keys =>
            [
                #{
                    key_type => KeyType0,
                    key => key_data(PublicKey0),
                    expires_at => erlang:system_time(second) + 3600
                },
                #{
                    key_type => KeyType1,
                    key => key_data(PublicKey1),
                    expires_at => erlang:system_time(second) + 3600
                }
            ]
    },
    ok = emqx_gcp_device:put_device(Device),
    Payload = #{<<"exp">> => erlang:system_time(second) + 3600},
    JWT0 = generate_jws(Payload, KeyType0, PrivateKeyName0),
    JWT1 = generate_jws(Payload, KeyType1, PrivateKeyName1),
    JWT2 = generate_jws(Payload, KeyType2, PrivateKeyName2),
    ClientId = gcp_client_id(DeviceId),
    lists:foreach(
        fun(JWT) ->
            ?check_trace(
                begin
                    ClientInfo = client_info(ClientId, JWT),
                    ?assertMatch(
                        ok,
                        emqx_gcp_device_authn:authenticate(ClientInfo, #{})
                    )
                end,
                fun(Trace) ->
                    ?assertMatch(
                        [#{result := ok, reason := "auth success"}],
                        ?of_kind(authn_gcp_device_check, Trace)
                    )
                end
            )
        end,
        [JWT0, JWT1]
    ),
    ?check_trace(
        begin
            ClientInfo = client_info(ClientId, JWT2),
            ?assertMatch(
                {error, bad_username_or_password},
                emqx_gcp_device_authn:authenticate(ClientInfo, #{})
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        result := {error, bad_username_or_password},
                        reason := "no matching or valid keys"
                    }
                ],
                ?of_kind(authn_gcp_device_check, Trace)
            )
        end
    ),
    ok.

t_all_key_types(_Config) ->
    lists:foreach(
        fun({DeviceId, KeyType, _PrivateKeyName, PublicKey}) ->
            Device = #{
                deviceid => DeviceId,
                config => <<>>,
                keys =>
                    [
                        #{
                            key_type => KeyType,
                            key => key_data(PublicKey),
                            expires_at => 0
                        }
                    ]
            },
            ok = emqx_gcp_device:put_device(Device)
        end,
        keys()
    ),
    Payload = #{<<"exp">> => erlang:system_time(second) + 3600},
    lists:foreach(
        fun({DeviceId, KeyType, PrivateKeyName, _PublicKey}) ->
            ClientId = gcp_client_id(DeviceId),
            JWT = generate_jws(Payload, KeyType, PrivateKeyName),
            ClientInfo = client_info(ClientId, JWT),
            ?check_trace(
                ?assertMatch(
                    ok,
                    emqx_gcp_device_authn:authenticate(ClientInfo, #{})
                ),
                fun(Trace) ->
                    ?assertMatch(
                        [#{result := ok, reason := "auth success"}],
                        ?of_kind(authn_gcp_device_check, Trace)
                    )
                end
            )
        end,
        keys()
    ),
    ok.

t_config(_Config) ->
    Device = #{
        deviceid => <<"t">>,
        config => base64:encode(<<"myconf">>),
        keys => []
    },
    ok = emqx_gcp_device:put_device(Device),

    {ok, Pid} = emqtt:start_link(),
    {ok, _} = emqtt:connect(Pid),
    {ok, _, _} = emqtt:subscribe(Pid, <<"/devices/t/config">>, 0),

    receive
        {publish, #{payload := <<"myconf">>}} ->
            ok
    after 1000 ->
        ct:fail("No config received")
    end,
    emqtt:stop(Pid),
    ok.

t_wrong_device(_Config) ->
    Device = #{wrong_field => wrong_value},
    ?assertMatch(
        {error, {function_clause, _}},
        emqx_gcp_device:put_device(Device)
    ),
    ok.

t_import_wrong_devices(_Config) ->
    InvalidDevices = [
        #{wrong_field => wrong_value},
        #{another_wrong_field => another_wrong_value},
        #{yet_another_wrong_field => yet_another_wrong_value}
    ],
    ValidDevices = [
        #{
            deviceid => gcp_client_id(<<"valid_device_1">>),
            config => <<>>,
            keys => []
        },
        #{
            deviceid => gcp_client_id(<<"valid_device_2">>),
            config => <<>>,
            keys => []
        }
    ],
    Devices = InvalidDevices ++ ValidDevices,
    InvalidDevicesLength = length(InvalidDevices),
    ValidDevicesLength = length(ValidDevices),
    ?assertMatch(
        {ValidDevicesLength, InvalidDevicesLength},
        emqx_gcp_device:import_devices(Devices)
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(ClientId, Password) ->
    emqx_gcp_device_test_helpers:client_info(ClientId, Password).

device_loc(DeviceId) ->
    {<<"iot-export">>, <<"europe-west1">>, <<"my-registry">>, DeviceId}.

gcp_client_id(DeviceId) ->
    emqx_gcp_device_test_helpers:client_id(DeviceId).

keys() ->
    emqx_gcp_device_test_helpers:keys().

key_data(Filename) ->
    emqx_gcp_device_test_helpers:key(Filename).

generate_jws(Payload, KeyType, PrivateKeyName) ->
    emqx_gcp_device_test_helpers:generate_jws(Payload, KeyType, PrivateKeyName).

clear_data() ->
    emqx_gcp_device_test_helpers:clear_data(),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok.
