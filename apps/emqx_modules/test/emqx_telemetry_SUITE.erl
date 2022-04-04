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

-module(emqx_telemetry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(proplists, [get_value/2]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = meck:new(emqx_authz, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_authz,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_authz, "etc/acl.conf")
        end
    ),
    emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authn, emqx_authz, emqx_modules],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_authn, emqx_authz, emqx_modules]),
    meck:unload(emqx_authz),
    ok.

init_per_testcase(t_get_telemetry, Config) ->
    DataDir = ?config(data_dir, Config),
    mock_httpc(),
    ok = meck:new(emqx_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_telemetry,
        read_raw_build_info,
        fun() ->
            Path = filename:join([DataDir, "BUILD_INFO"]),
            {ok, Template} = file:read_file(Path),
            Vars0 = [
                {build_info_arch, "arch"},
                {build_info_wordsize, "64"},
                {build_info_os, "os"},
                {build_info_erlang, "erlang"},
                {build_info_elixir, "elixir"},
                {build_info_relform, "relform"}
            ],
            Vars = [
                {atom_to_list(K), iolist_to_binary(V)}
             || {K, V} <- Vars0
            ],
            Rendered = bbmustache:render(Template, Vars),
            {ok, Rendered}
        end
    ),
    Config;
init_per_testcase(t_advanced_mqtt_features, Config) ->
    OldValues = emqx_modules:get_advanced_mqtt_features_in_use(),
    emqx_modules:set_advanced_mqtt_features_in_use(#{
        delayed => false,
        topic_rewrite => false,
        retained => false,
        auto_subscribe => false
    }),
    [{old_values, OldValues} | Config];
init_per_testcase(t_authn_authz_info, Config) ->
    mock_httpc(),
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    create_authn('mqtt:global', built_in_database),
    create_authn('tcp:default', redis),
    create_authn('ws:default', redis),
    create_authz(postgresql),
    Config;
init_per_testcase(_Testcase, Config) ->
    TestPID = self(),
    ok = meck:new(httpc, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(httpc, request, fun(
        Method, {URL, Headers, _ContentType, Body}, _HTTPOpts, _Opts
    ) ->
        TestPID ! {request, Method, URL, Headers, Body}
    end),
    Config.

end_per_testcase(t_get_telemetry, _Config) ->
    meck:unload([httpc, emqx_telemetry]),
    ok;
end_per_testcase(t_advanced_mqtt_features, Config) ->
    OldValues = ?config(old_values, Config),
    emqx_modules:set_advanced_mqtt_features_in_use(OldValues);
end_per_testcase(t_authn_authz_info, _Config) ->
    meck:unload([httpc]),
    emqx_authz:update({delete, postgresql}, #{}),
    lists:foreach(
        fun(ChainName) ->
            catch emqx_authn_test_lib:delete_authenticators(
                [authentication],
                ChainName
            )
        end,
        ['mqtt:global', 'tcp:default', 'ws:default']
    ),
    ok;
end_per_testcase(_Testcase, _Config) ->
    meck:unload([httpc]),
    ok.

t_uuid(_) ->
    UUID = emqx_telemetry:generate_uuid(),
    Parts = binary:split(UUID, <<"-">>, [global, trim]),
    ?assertEqual(5, length(Parts)),
    {ok, UUID2} = emqx_telemetry:get_uuid(),
    emqx_telemetry:disable(),
    emqx_telemetry:enable(),
    emqx_modules_conf:set_telemetry_status(false),
    emqx_modules_conf:set_telemetry_status(true),
    {ok, UUID3} = emqx_telemetry:get_uuid(),
    {ok, UUID4} = emqx_telemetry_proto_v1:get_uuid(node()),
    ?assertEqual(UUID2, UUID3),
    ?assertEqual(UUID3, UUID4),
    ?assertMatch({badrpc, nodedown}, emqx_telemetry_proto_v1:get_uuid('fake@node')).

t_official_version(_) ->
    true = emqx_telemetry:official_version("0.0.0"),
    true = emqx_telemetry:official_version("1.1.1"),
    true = emqx_telemetry:official_version("10.10.10"),
    false = emqx_telemetry:official_version("0.0.0.0"),
    false = emqx_telemetry:official_version("1.1.a"),
    true = emqx_telemetry:official_version("0.0-alpha.1"),
    true = emqx_telemetry:official_version("1.1-alpha.1"),
    true = emqx_telemetry:official_version("10.10-alpha.10"),
    false = emqx_telemetry:official_version("1.1-alpha.0"),
    true = emqx_telemetry:official_version("1.1-beta.1"),
    true = emqx_telemetry:official_version("1.1-rc.1"),
    false = emqx_telemetry:official_version("1.1-alpha.a").

t_get_telemetry(_Config) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    OTPVersion = bin(erlang:system_info(otp_release)),
    ?assertEqual(OTPVersion, get_value(otp_version, TelemetryData)),
    {ok, UUID} = emqx_telemetry:get_uuid(),
    ?assertEqual(UUID, get_value(uuid, TelemetryData)),
    ?assertEqual(0, get_value(num_clients, TelemetryData)),
    BuildInfo = get_value(build_info, TelemetryData),
    ?assertMatch(
        #{
            <<"arch">> := <<_/binary>>,
            <<"elixir">> := <<_/binary>>,
            <<"erlang">> := <<_/binary>>,
            <<"os">> := <<_/binary>>,
            <<"relform">> := <<_/binary>>,
            <<"wordsize">> := Wordsize
        } when is_integer(Wordsize),
        BuildInfo
    ),
    VMSpecs = get_value(vm_specs, TelemetryData),
    ?assert(is_integer(get_value(num_cpus, VMSpecs))),
    ?assert(0 =< get_value(num_cpus, VMSpecs)),
    ?assert(is_integer(get_value(total_memory, VMSpecs))),
    ?assert(0 =< get_value(total_memory, VMSpecs)),
    MQTTRTInsights = get_value(mqtt_runtime_insights, TelemetryData),
    ?assert(is_number(maps:get(messages_sent_rate, MQTTRTInsights))),
    ?assert(is_number(maps:get(messages_received_rate, MQTTRTInsights))),
    ?assert(is_integer(maps:get(num_topics, MQTTRTInsights))),
    ?assert(is_map(get_value(authn_authz, TelemetryData))),
    ok.

t_advanced_mqtt_features(_) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    AdvFeats = get_value(advanced_mqtt_features, TelemetryData),
    ?assertEqual(
        #{
            retained => 0,
            topic_rewrite => 0,
            auto_subscribe => 0,
            delayed => 0
        },
        AdvFeats
    ),
    lists:foreach(
        fun(TelemetryKey) ->
            EnabledFeats = emqx_modules:get_advanced_mqtt_features_in_use(),
            emqx_modules:set_advanced_mqtt_features_in_use(EnabledFeats#{TelemetryKey => true}),
            {ok, Data} = emqx_telemetry:get_telemetry(),
            #{TelemetryKey := Value} = get_value(advanced_mqtt_features, Data),
            ?assertEqual(1, Value, #{key => TelemetryKey})
        end,
        [
            retained,
            topic_rewrite,
            auto_subscribe,
            delayed
        ]
    ),
    ok.

t_authn_authz_info(_) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    AuthnAuthzInfo = get_value(authn_authz, TelemetryData),
    ?assertEqual(
        #{
            authn =>
                [
                    <<"password_based:built_in_database">>,
                    <<"password_based:redis">>
                ],
            authn_listener => #{<<"password_based:redis">> => 2},
            authz => [postgresql]
        },
        AuthnAuthzInfo
    ).

t_enable(_) ->
    ok = meck:new(emqx_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_telemetry, official_version, fun(_) -> true end),
    ok = emqx_telemetry:enable(),
    ok = emqx_telemetry:disable(),
    meck:unload([emqx_telemetry]).

t_send_after_enable(_) ->
    ok = meck:new(emqx_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_telemetry, official_version, fun(_) -> true end),
    ok = emqx_telemetry:disable(),
    ok = snabbkaffe:start_trace(),
    try
        ok = emqx_telemetry:enable(),
        ?assertMatch({ok, _}, ?block_until(#{?snk_kind := telemetry_data_reported}, 2000, 100)),
        receive
            {request, post, _URL, _Headers, Body} ->
                {ok, Decoded} = emqx_json:safe_decode(Body, [return_maps]),
                ?assertMatch(
                    #{
                        <<"uuid">> := _,
                        <<"messages_received">> := _,
                        <<"messages_sent">> := _,
                        <<"build_info">> := #{},
                        <<"vm_specs">> :=
                            #{
                                <<"num_cpus">> := _,
                                <<"total_memory">> := _
                            },
                        <<"mqtt_runtime_insights">> :=
                            #{
                                <<"messages_received_rate">> := _,
                                <<"messages_sent_rate">> := _,
                                <<"num_topics">> := _
                            },
                        <<"advanced_mqtt_features">> :=
                            #{
                                <<"retained">> := _,
                                <<"topic_rewrite">> := _,
                                <<"auto_subscribe">> := _,
                                <<"delayed">> := _
                            }
                    },
                    Decoded
                )
        after 2100 ->
            exit(telemetry_not_reported)
        end
    after
        ok = snabbkaffe:stop(),
        meck:unload([emqx_telemetry])
    end.

t_mqtt_runtime_insights(_) ->
    State0 = emqx_telemetry:empty_state(),
    {MQTTRTInsights1, State1} = emqx_telemetry:mqtt_runtime_insights(State0),
    ?assertEqual(
        #{
            messages_sent_rate => 0.0,
            messages_received_rate => 0.0,
            num_topics => 0
        },
        MQTTRTInsights1
    ),
    %% add some fake stats
    emqx_metrics:set('messages.sent', 10_000_000_000),
    emqx_metrics:set('messages.received', 20_000_000_000),
    emqx_stats:setstat('topics.count', 30_000),
    {MQTTRTInsights2, _State2} = emqx_telemetry:mqtt_runtime_insights(State1),
    assert_approximate(MQTTRTInsights2, messages_sent_rate, "16.53"),
    assert_approximate(MQTTRTInsights2, messages_received_rate, "33.07"),
    ?assertEqual(30_000, maps:get(num_topics, MQTTRTInsights2)),
    ok.

assert_approximate(Map, Key, Expected) ->
    Value = maps:get(Key, Map),
    ?assertEqual(Expected, float_to_list(Value, [{decimals, 2}])).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

mock_httpc() ->
    TestPID = self(),
    ok = meck:new(httpc, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(httpc, request, fun(
        Method, {URL, Headers, _ContentType, Body}, _HTTPOpts, _Opts
    ) ->
        TestPID ! {request, Method, URL, Headers, Body}
    end).

create_authn(ChainName, built_in_database) ->
    emqx_authentication:initialize_authentication(
        ChainName,
        [
            #{
                mechanism => password_based,
                backend => built_in_database,
                enable => true,
                user_id_type => username,
                password_hash_algorithm => #{
                    name => plain,
                    salt_position => suffix
                }
            }
        ]
    );
create_authn(ChainName, redis) ->
    emqx_authentication:initialize_authentication(
        ChainName,
        [
            #{
                mechanism => password_based,
                backend => redis,
                enable => true,
                user_id_type => username,
                cmd => "HMGET mqtt_user:${username} password_hash salt is_superuser",
                password_hash_algorithm => #{
                    name => plain,
                    salt_position => suffix
                }
            }
        ]
    ).

create_authz(postgresql) ->
    emqx_authz:update(
        append,
        #{
            <<"type">> => <<"postgresql">>,
            <<"enable">> => true,
            <<"server">> => <<"127.0.0.1:27017">>,
            <<"pool_size">> => 1,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"xx">>,
            <<"password">> => <<"ee">>,
            <<"auto_reconnect">> => true,
            <<"ssl">> => #{<<"enable">> => false},
            <<"query">> => <<"abcb">>
        }
    ).

set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.
