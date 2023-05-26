%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_config_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_fill_default_values(_) ->
    Conf = #{
        <<"broker">> => #{
            <<"perf">> => #{},
            <<"route_batch_clean">> => false
        }
    },
    WithDefaults = emqx_config:fill_defaults(Conf),
    ?assertMatch(
        #{
            <<"broker">> :=
                #{
                    <<"enable_session_registry">> := true,
                    <<"perf">> :=
                        #{
                            <<"route_lock_type">> := key,
                            <<"trie_compaction">> := true
                        },
                    <<"route_batch_clean">> := false,
                    <<"session_locking_strategy">> := quorum,
                    <<"shared_subscription_strategy">> := round_robin
                }
        },
        WithDefaults
    ),
    %% ensure JSON compatible
    _ = emqx_utils_json:encode(WithDefaults),
    ok.

t_init_load(_Config) ->
    ConfFile = "./test_emqx.conf",
    ok = file:write_file(ConfFile, <<"">>),
    ExpectRootNames = lists:sort(hocon_schema:root_names(emqx_schema)),
    emqx_config:erase_all(),
    {ok, DeprecatedFile} = application:get_env(emqx, cluster_override_conf_file),
    ?assertEqual(false, filelib:is_regular(DeprecatedFile), DeprecatedFile),
    %% Don't has deprecated file
    ok = emqx_config:init_load(emqx_schema, [ConfFile]),
    ?assertEqual(ExpectRootNames, lists:sort(emqx_config:get_root_names())),
    ?assertMatch({ok, #{raw_config := 256}}, emqx:update_config([mqtt, max_topic_levels], 256)),
    emqx_config:erase_all(),
    %% Has deprecated file
    ok = file:write_file(DeprecatedFile, <<"{}">>),
    ok = emqx_config:init_load(emqx_schema, [ConfFile]),
    ?assertEqual(ExpectRootNames, lists:sort(emqx_config:get_root_names())),
    ?assertMatch({ok, #{raw_config := 128}}, emqx:update_config([mqtt, max_topic_levels], 128)),
    ok = file:delete(DeprecatedFile).

t_unknown_rook_keys(_) ->
    ?check_trace(
        #{timetrap => 1000},
        begin
            ok = emqx_config:init_load(
                emqx_schema, <<"test_1 {}\n test_2 {sub = 100}\n listeners {}">>
            ),
            ?block_until(#{?snk_kind := unknown_config_keys})
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{unknown_config_keys := "test_1,test_2"}],
                ?of_kind(unknown_config_keys, Trace)
            )
        end
    ),
    ok.

t_init_load_emqx_schema(Config) ->
    emqx_config:erase_all(),
    %% Given empty config file
    ConfFile = prepare_conf_file(?FUNCTION_NAME, <<"">>, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    %% When load emqx_schema
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    %% Then default zone is injected with all global defaults
    Default = emqx_config:get([zones, default]),
    ?assertMatch(
        #{
            mqtt := _,
            stats := _,
            flapping_detect := _,
            force_shutdown := _,
            conn_congestion := _,
            force_gc := _,
            overload_protection := _
        },
        Default
    ).

t_init_zones_load_emqx_schema_no_default(Config) ->
    emqx_config:erase_all(),
    %% Given empty config file
    ConfFile = prepare_conf_file(?FUNCTION_NAME, <<"">>, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    %% When load emqx_schema
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    %% Then read for none existing zone should throw error
    ?assertError(
        {config_not_found, [zones, no_exists]},
        emqx_config:get([zones, no_exists])
    ).

t_init_zones_load_other_schema(Config) ->
    emqx_config:erase_all(),
    %% Given empty config file
    ConfFile = prepare_conf_file(?FUNCTION_NAME, <<"">>, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    %% When load schema other than emqx_schema
    %% Then load should success
    ?assertEqual(ok, emqx_config:init_load(emqx_limiter_schema)),
    %% Then no default zone is loaded.
    ?assertError(
        {config_not_found, [zones, default]},
        emqx_config:get([zones, default])
    ).

t_init_zones_with_user_defined_default_zone(Config) ->
    emqx_config:erase_all(),
    %% Given user defined config for default zone
    ConfFile = prepare_conf_file(
        ?FUNCTION_NAME, <<"zones.default.mqtt.max_topic_alias=1024">>, Config
    ),
    application:set_env(emqx, config_files, [ConfFile]),
    %% When load schema
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    %% Then user defined value is set and others are defaults

    ?assertMatch(
        #{
            conn_congestion :=
                #{enable_alarm := true, min_alarm_sustain_duration := 60000},
            flapping_detect :=
                #{ban_time := 300000, max_count := 15, window_time := disabled},
            force_gc :=
                #{bytes := 16777216, count := 16000, enable := true},
            force_shutdown :=
                #{
                    enable := true,
                    max_heap_size := 4194304,
                    max_mailbox_size := 1000
                },
            mqtt :=
                #{
                    await_rel_timeout := 300000,
                    exclusive_subscription := false,
                    idle_timeout := 15000,
                    ignore_loop_deliver := false,
                    keepalive_backoff := 0.75,
                    keepalive_multiplier := 1.5,
                    max_awaiting_rel := 100,
                    max_clientid_len := 65535,
                    max_inflight := 32,
                    max_mqueue_len := 1000,
                    max_packet_size := 1048576,
                    max_qos_allowed := 2,
                    max_subscriptions := infinity,
                    %% <=== here!
                    max_topic_alias := 1024,
                    max_topic_levels := 128,
                    mqueue_default_priority := lowest,
                    mqueue_priorities := disabled,
                    mqueue_store_qos0 := true,
                    peer_cert_as_clientid := disabled,
                    peer_cert_as_username := disabled,
                    response_information := [],
                    retain_available := true,
                    retry_interval := 30000,
                    server_keepalive := disabled,
                    session_expiry_interval := 7200000,
                    shared_subscription := true,
                    strict_mode := false,
                    upgrade_qos := false,
                    use_username_as_clientid := false,
                    wildcard_subscription := true
                },
            overload_protection :=
                #{
                    backoff_delay := 1,
                    backoff_gc := false,
                    backoff_hibernation := true,
                    backoff_new_conn := true,
                    enable := false
                },
            stats := #{enable := true}
        },
        emqx_config:get([zones, default])
    ).

t_init_zones_with_user_defined_other_zone(Config) ->
    emqx_config:erase_all(),
    %% Given user defined config for default zone
    ConfFile = prepare_conf_file(
        ?FUNCTION_NAME, <<"zones.myzone.mqtt.max_topic_alias=1024">>, Config
    ),
    application:set_env(emqx, config_files, [ConfFile]),
    %% When load schema
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    %% Then user defined value is set and others are defaults
    ?assertMatch(
        #{
            conn_congestion :=
                #{enable_alarm := true, min_alarm_sustain_duration := 60000},
            flapping_detect :=
                #{ban_time := 300000, max_count := 15, window_time := disabled},
            force_gc :=
                #{bytes := 16777216, count := 16000, enable := true},
            force_shutdown :=
                #{
                    enable := true,
                    max_heap_size := 4194304,
                    max_mailbox_size := 1000
                },
            mqtt :=
                #{
                    await_rel_timeout := 300000,
                    exclusive_subscription := false,
                    idle_timeout := 15000,
                    ignore_loop_deliver := false,
                    keepalive_backoff := 0.75,
                    keepalive_multiplier := 1.5,
                    max_awaiting_rel := 100,
                    max_clientid_len := 65535,
                    max_inflight := 32,
                    max_mqueue_len := 1000,
                    max_packet_size := 1048576,
                    max_qos_allowed := 2,
                    max_subscriptions := infinity,
                    %% <=== here!
                    max_topic_alias := 1024,
                    max_topic_levels := 128,
                    mqueue_default_priority := lowest,
                    mqueue_priorities := disabled,
                    mqueue_store_qos0 := true,
                    peer_cert_as_clientid := disabled,
                    peer_cert_as_username := disabled,
                    response_information := [],
                    retain_available := true,
                    retry_interval := 30000,
                    server_keepalive := disabled,
                    session_expiry_interval := 7200000,
                    shared_subscription := true,
                    strict_mode := false,
                    upgrade_qos := false,
                    use_username_as_clientid := false,
                    wildcard_subscription := true
                },
            overload_protection :=
                #{
                    backoff_delay := 1,
                    backoff_gc := false,
                    backoff_hibernation := true,
                    backoff_new_conn := true,
                    enable := false
                },
            stats := #{enable := true}
        },
        emqx_config:get([zones, myzone])
    ),

    %% Then default zone still have the defaults
    ?assertMatch(
        #{
            conn_congestion :=
                #{enable_alarm := true, min_alarm_sustain_duration := 60000},
            flapping_detect :=
                #{ban_time := 300000, max_count := 15, window_time := disabled},
            force_gc :=
                #{bytes := 16777216, count := 16000, enable := true},
            force_shutdown :=
                #{
                    enable := true,
                    max_heap_size := 4194304,
                    max_mailbox_size := 1000
                },
            mqtt :=
                #{
                    await_rel_timeout := 300000,
                    exclusive_subscription := false,
                    idle_timeout := 15000,
                    ignore_loop_deliver := false,
                    keepalive_backoff := 0.75,
                    keepalive_multiplier := 1.5,
                    max_awaiting_rel := 100,
                    max_clientid_len := 65535,
                    max_inflight := 32,
                    max_mqueue_len := 1000,
                    max_packet_size := 1048576,
                    max_qos_allowed := 2,
                    max_subscriptions := infinity,
                    max_topic_alias := 65535,
                    max_topic_levels := 128,
                    mqueue_default_priority := lowest,
                    mqueue_priorities := disabled,
                    mqueue_store_qos0 := true,
                    peer_cert_as_clientid := disabled,
                    peer_cert_as_username := disabled,
                    response_information := [],
                    retain_available := true,
                    retry_interval := 30000,
                    server_keepalive := disabled,
                    session_expiry_interval := 7200000,
                    shared_subscription := true,
                    strict_mode := false,
                    upgrade_qos := false,
                    use_username_as_clientid := false,
                    wildcard_subscription := true
                },
            overload_protection :=
                #{
                    backoff_delay := 1,
                    backoff_gc := false,
                    backoff_hibernation := true,
                    backoff_new_conn := true,
                    enable := false
                },
            stats := #{enable := true}
        },
        emqx_config:get([zones, default])
    ).

t_init_zones_with_cust_root_mqtt(Config) ->
    emqx_config:erase_all(),
    %% Given user defined non default mqtt schema in config file
    ConfFile = prepare_conf_file(?FUNCTION_NAME, <<"mqtt.retry_interval=600000">>, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    %% When emqx_schema is loaded
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    %% Then the value is reflected in default `zone' and other fields under mqtt are default.
    ?assertMatch(
        #{
            await_rel_timeout := 300000,
            exclusive_subscription := false,
            idle_timeout := 15000,
            ignore_loop_deliver := false,
            keepalive_backoff := 0.75,
            keepalive_multiplier := 1.5,
            max_awaiting_rel := 100,
            max_clientid_len := 65535,
            max_inflight := 32,
            max_mqueue_len := 1000,
            max_packet_size := 1048576,
            max_qos_allowed := 2,
            max_subscriptions := infinity,
            max_topic_alias := 65535,
            max_topic_levels := 128,
            mqueue_default_priority := lowest,
            mqueue_priorities := disabled,
            mqueue_store_qos0 := true,
            peer_cert_as_clientid := disabled,
            peer_cert_as_username := disabled,
            response_information := [],
            retain_available := true,
            %% <=== here
            retry_interval := 600000,
            server_keepalive := disabled,
            session_expiry_interval := 7200000,
            shared_subscription := true,
            strict_mode := false,
            upgrade_qos := false,
            use_username_as_clientid := false,
            wildcard_subscription := true
        },
        emqx_config:get([zones, default, mqtt])
    ).

t_default_zone_is_updated_after_global_defaults_updated(Config) ->
    emqx_config:erase_all(),
    %% Given user defined non default mqtt schema in config file
    ConfFile = prepare_conf_file(?FUNCTION_NAME, <<"">>, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    ?assertNotEqual(900000, emqx_config:get([zones, default, mqtt, retry_interval])),
    %% When emqx_schema is loaded
    emqx_config:put([mqtt, retry_interval], 900000),
    %% Then the value is reflected in default `zone' and other fields under mqtt are default.
    ?assertMatch(
        #{
            await_rel_timeout := 300000,
            exclusive_subscription := false,
            idle_timeout := 15000,
            ignore_loop_deliver := false,
            keepalive_backoff := 0.75,
            keepalive_multiplier := 1.5,
            max_awaiting_rel := 100,
            max_clientid_len := 65535,
            max_inflight := 32,
            max_mqueue_len := 1000,
            max_packet_size := 1048576,
            max_qos_allowed := 2,
            max_subscriptions := infinity,
            max_topic_alias := 65535,
            max_topic_levels := 128,
            mqueue_default_priority := lowest,
            mqueue_priorities := disabled,
            mqueue_store_qos0 := true,
            peer_cert_as_clientid := disabled,
            peer_cert_as_username := disabled,
            response_information := [],
            retain_available := true,
            %% <=== here
            retry_interval := 900000,
            server_keepalive := disabled,
            session_expiry_interval := 7200000,
            shared_subscription := true,
            strict_mode := false,
            upgrade_qos := false,
            use_username_as_clientid := false,
            wildcard_subscription := true
        },
        emqx_config:get([zones, default, mqtt])
    ).

t_other_zone_is_updated_after_global_defaults_updated(Config) ->
    emqx_config:erase_all(),
    %% Given user defined non default mqtt schema in config file
    ConfFile = prepare_conf_file(?FUNCTION_NAME, <<"zones.myzone.mqtt.max_inflight=32">>, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    ?assertEqual(ok, emqx_config:init_load(emqx_schema)),
    ?assertNotEqual(900000, emqx_config:get([zones, myzone, mqtt, retry_interval])),
    %% When emqx_schema is loaded
    emqx_config:put([mqtt, retry_interval], 900000),
    %% Then the value is reflected in default `zone' and other fields under mqtt are default.
    ?assertMatch(
        #{
            await_rel_timeout := 300000,
            exclusive_subscription := false,
            idle_timeout := 15000,
            ignore_loop_deliver := false,
            keepalive_backoff := 0.75,
            keepalive_multiplier := 1.5,
            max_awaiting_rel := 100,
            max_clientid_len := 65535,
            max_inflight := 32,
            max_mqueue_len := 1000,
            max_packet_size := 1048576,
            max_qos_allowed := 2,
            max_subscriptions := infinity,
            max_topic_alias := 65535,
            max_topic_levels := 128,
            mqueue_default_priority := lowest,
            mqueue_priorities := disabled,
            mqueue_store_qos0 := true,
            peer_cert_as_clientid := disabled,
            peer_cert_as_username := disabled,
            response_information := [],
            retain_available := true,
            %% <=== here
            retry_interval := 900000,
            server_keepalive := disabled,
            session_expiry_interval := 7200000,
            shared_subscription := true,
            strict_mode := false,
            upgrade_qos := false,
            use_username_as_clientid := false,
            wildcard_subscription := true
        },
        emqx_config:get([zones, myzone, mqtt])
    ).

%%%
%%% Helpers
%%%
prepare_conf_file(Name, Content, CTConfig) ->
    Filename = tc_conf_file(Name, CTConfig),
    filelib:ensure_dir(Filename),
    ok = file:write_file(Filename, Content),
    Filename.

tc_conf_file(TC, Config) ->
    DataDir = ?config(data_dir, Config),
    filename:join([DataDir, TC, 'emqx.conf']).
