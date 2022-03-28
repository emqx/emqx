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
    snabbkaffe:fix_ct_logging(),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_modules]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_modules]).

init_per_testcase(t_get_telemetry, Config) ->
    DataDir = ?config(data_dir, Config),
    TestPID = self(),
    ok = meck:new(httpc, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(httpc, request, fun(Method, URL, Headers, Body) ->
        TestPID ! {request, Method, URL, Headers, Body}
    end),
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
    emqx_telemetry_proto_v1:disable_telemetry(node()),
    emqx_telemetry_proto_v1:enable_telemetry(node()),
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
