%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-include("emqx_authentication.hrl").
-include("emqx_access_control.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-type duration() :: integer().
-type duration_s() :: integer().
-type duration_ms() :: integer().
-type bytesize() :: integer().
-type wordsize() :: bytesize().
-type percent() :: float().
-type file() :: string().
-type comma_separated_list() :: list().
-type comma_separated_binary() :: [binary()].
-type comma_separated_atoms() :: [atom()].
-type bar_separated_list() :: list().
-type ip_port() :: tuple().
-type cipher() :: map().

-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({duration_ms/0, emqx_schema, to_duration_ms}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({wordsize/0, emqx_schema, to_wordsize}).
-typerefl_from_string({percent/0, emqx_schema, to_percent}).
-typerefl_from_string({comma_separated_list/0, emqx_schema, to_comma_separated_list}).
-typerefl_from_string({comma_separated_binary/0, emqx_schema, to_comma_separated_binary}).
-typerefl_from_string({bar_separated_list/0, emqx_schema, to_bar_separated_list}).
-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({cipher/0, emqx_schema, to_erl_cipher_suite}).
-typerefl_from_string({comma_separated_atoms/0, emqx_schema, to_comma_separated_atoms}).

-export([
    validate_heap_size/1,
    parse_user_lookup_fun/1,
    validate_alarm_actions/1,
    non_empty_string/1,
    validations/0
]).

-export([qos/0]).

% workaround: prevent being recognized as unused functions
-export([
    to_duration/1,
    to_duration_s/1,
    to_duration_ms/1,
    mk_duration/2,
    to_bytesize/1,
    to_wordsize/1,
    to_percent/1,
    to_comma_separated_list/1,
    to_comma_separated_binary/1,
    to_bar_separated_list/1,
    to_ip_port/1,
    to_erl_cipher_suite/1,
    to_comma_separated_atoms/1
]).

-behaviour(hocon_schema).

-reflect_type([
    duration/0,
    duration_s/0,
    duration_ms/0,
    bytesize/0,
    wordsize/0,
    percent/0,
    file/0,
    comma_separated_list/0,
    comma_separated_binary/0,
    bar_separated_list/0,
    ip_port/0,
    cipher/0,
    comma_separated_atoms/0
]).

-export([namespace/0, roots/0, roots/1, fields/1, desc/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([server_ssl_opts_schema/2, client_ssl_opts_schema/1, ciphers_schema/1, default_ciphers/1]).
-export([sc/2, map/2]).

-elvis([{elvis_style, god_modules, disable}]).

namespace() -> broker.

roots() ->
    %% TODO change config importance to a field metadata
    roots(high) ++ roots(medium) ++ roots(low).

roots(high) ->
    [
        {"listeners",
            sc(
                ref("listeners"),
                #{}
            )},
        {"zones",
            sc(
                map("name", ref("zone")),
                #{desc => ?DESC(zones)}
            )},
        {"mqtt",
            sc(
                ref("mqtt"),
                #{desc => ?DESC(mqtt)}
            )},
        {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME, authentication(global)},
        %% NOTE: authorization schema here is only to keep emqx app prue
        %% the full schema for EMQX node is injected in emqx_conf_schema.
        {?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME,
            sc(
                ref(?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME),
                #{}
            )}
    ];
roots(medium) ->
    [
        {"broker",
            sc(
                ref("broker"),
                #{desc => ?DESC(broker)}
            )},
        {"sys_topics",
            sc(
                ref("sys_topics"),
                #{desc => ?DESC(sys_topics)}
            )},
        {"force_shutdown",
            sc(
                ref("force_shutdown"),
                #{}
            )},
        {"overload_protection",
            sc(
                ref("overload_protection"),
                #{}
            )}
    ];
roots(low) ->
    [
        {"force_gc",
            sc(
                ref("force_gc"),
                #{}
            )},
        {"conn_congestion",
            sc(
                ref("conn_congestion"),
                #{}
            )},
        {"stats",
            sc(
                ref("stats"),
                #{}
            )},
        {"sysmon",
            sc(
                ref("sysmon"),
                #{}
            )},
        {"alarm",
            sc(
                ref("alarm"),
                #{}
            )},
        {"flapping_detect",
            sc(
                ref("flapping_detect"),
                #{}
            )},
        {"persistent_session_store",
            sc(
                ref("persistent_session_store"),
                #{}
            )},
        {"trace",
            sc(
                ref("trace"),
                #{}
            )}
    ].

fields("persistent_session_store") ->
    [
        {"enabled",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(persistent_session_store_enabled)
                }
            )},
        {"on_disc",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(persistent_store_on_disc)
                }
            )},
        {"ram_cache",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(persistent_store_ram_cache)
                }
            )},
        {"backend",
            sc(
                hoconsc:union([ref("persistent_session_builtin")]),
                #{
                    default => #{
                        <<"type">> => <<"builtin">>,
                        <<"session">> =>
                            #{<<"ram_cache">> => <<"true">>},
                        <<"session_messages">> =>
                            #{<<"ram_cache">> => <<"true">>},
                        <<"messages">> =>
                            #{<<"ram_cache">> => <<"false">>}
                    },
                    desc => ?DESC(persistent_session_store_backend)
                }
            )},
        {"max_retain_undelivered",
            sc(
                duration(),
                #{
                    default => "1h",
                    desc => ?DESC(persistent_session_store_max_retain_undelivered)
                }
            )},
        {"message_gc_interval",
            sc(
                duration(),
                #{
                    default => "1h",
                    desc => ?DESC(persistent_session_store_message_gc_interval)
                }
            )},
        {"session_message_gc_interval",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc => ?DESC(persistent_session_store_session_message_gc_interval)
                }
            )}
    ];
fields("persistent_table_mria_opts") ->
    [
        {"ram_cache",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(persistent_store_ram_cache)
                }
            )}
    ];
fields("persistent_session_builtin") ->
    [
        {"type", sc(hoconsc:enum([builtin]), #{default => builtin, desc => ""})},
        {"session",
            sc(ref("persistent_table_mria_opts"), #{
                desc => ?DESC(persistent_session_builtin_session_table)
            })},
        {"session_messages",
            sc(ref("persistent_table_mria_opts"), #{
                desc => ?DESC(persistent_session_builtin_sess_msg_table)
            })},
        {"messages",
            sc(ref("persistent_table_mria_opts"), #{
                desc => ?DESC(persistent_session_builtin_messages_table)
            })}
    ];
fields("stats") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(stats_enable)
                }
            )}
    ];
fields("authorization") ->
    [
        {"no_match",
            sc(
                hoconsc:enum([allow, deny]),
                #{
                    default => allow,
                    required => true,
                    desc => ?DESC(fields_authorization_no_match)
                }
            )},
        {"deny_action",
            sc(
                hoconsc:enum([ignore, disconnect]),
                #{
                    default => ignore,
                    required => true,
                    desc => ?DESC(fields_authorization_deny_action)
                }
            )},
        {"cache",
            sc(
                ref(?MODULE, "cache"),
                #{}
            )}
    ];
fields("cache") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    required => true,
                    desc => ?DESC(fields_cache_enable)
                }
            )},
        {"max_size",
            sc(
                range(1, 1048576),
                #{
                    default => 32,
                    desc => ?DESC(fields_cache_max_size)
                }
            )},
        {"ttl",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc => ?DESC(fields_cache_ttl)
                }
            )}
    ];
fields("mqtt") ->
    [
        {"idle_timeout",
            sc(
                hoconsc:union([infinity, duration()]),
                #{
                    default => "15s",
                    desc => ?DESC(mqtt_idle_timeout)
                }
            )},
        {"max_packet_size",
            sc(
                bytesize(),
                #{
                    default => "1MB",
                    desc => ?DESC(mqtt_max_packet_size)
                }
            )},
        {"max_clientid_len",
            sc(
                range(23, 65535),
                #{
                    default => 65535,
                    desc => ?DESC(mqtt_max_clientid_len)
                }
            )},
        {"max_topic_levels",
            sc(
                range(1, 65535),
                #{
                    default => 65535,
                    desc => ?DESC(mqtt_max_topic_levels)
                }
            )},
        {"max_qos_allowed",
            sc(
                qos(),
                #{
                    default => 2,
                    desc => ?DESC(mqtt_max_qos_allowed)
                }
            )},
        {"max_topic_alias",
            sc(
                range(0, 65535),
                #{
                    default => 65535,
                    desc => ?DESC(mqtt_max_topic_alias)
                }
            )},
        {"retain_available",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_retain_available)
                }
            )},
        {"wildcard_subscription",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_wildcard_subscription)
                }
            )},
        {"shared_subscription",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_shared_subscription)
                }
            )},
        {"exclusive_subscription",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_exclusive_subscription)
                }
            )},
        {"ignore_loop_deliver",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_ignore_loop_deliver)
                }
            )},
        {"strict_mode",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_strict_mode)
                }
            )},
        {"response_information",
            sc(
                string(),
                #{
                    default => <<"">>,
                    desc => ?DESC(mqtt_response_information)
                }
            )},
        {"server_keepalive",
            sc(
                hoconsc:union([integer(), disabled]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_server_keepalive)
                }
            )},
        {"keepalive_backoff",
            sc(
                number(),
                #{
                    default => 0.75,
                    desc => ?DESC(mqtt_keepalive_backoff)
                }
            )},
        {"max_subscriptions",
            sc(
                hoconsc:union([range(1, inf), infinity]),
                #{
                    default => infinity,
                    desc => ?DESC(mqtt_max_subscriptions)
                }
            )},
        {"upgrade_qos",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_upgrade_qos)
                }
            )},
        {"max_inflight",
            sc(
                range(1, 65535),
                #{
                    default => 32,
                    desc => ?DESC(mqtt_max_inflight)
                }
            )},
        {"retry_interval",
            sc(
                duration(),
                #{
                    default => "30s",
                    desc => ?DESC(mqtt_retry_interval)
                }
            )},
        {"max_awaiting_rel",
            sc(
                hoconsc:union([integer(), infinity]),
                #{
                    default => 100,
                    desc => ?DESC(mqtt_max_awaiting_rel)
                }
            )},
        {"await_rel_timeout",
            sc(
                duration(),
                #{
                    default => "300s",
                    desc => ?DESC(mqtt_await_rel_timeout)
                }
            )},
        {"session_expiry_interval",
            sc(
                duration(),
                #{
                    default => "2h",
                    desc => ?DESC(mqtt_session_expiry_interval)
                }
            )},
        {"max_mqueue_len",
            sc(
                hoconsc:union([non_neg_integer(), infinity]),
                #{
                    default => 1000,
                    desc => ?DESC(mqtt_max_mqueue_len)
                }
            )},
        {"mqueue_priorities",
            sc(
                hoconsc:union([map(), disabled]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_mqueue_priorities)
                }
            )},
        {"mqueue_default_priority",
            sc(
                hoconsc:enum([highest, lowest]),
                #{
                    default => lowest,
                    desc => ?DESC(mqtt_mqueue_default_priority)
                }
            )},
        {"mqueue_store_qos0",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_mqueue_store_qos0)
                }
            )},
        {"use_username_as_clientid",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_use_username_as_clientid)
                }
            )},
        {"peer_cert_as_username",
            sc(
                hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_peer_cert_as_username)
                }
            )},
        {"peer_cert_as_clientid",
            sc(
                hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_peer_cert_as_clientid)
                }
            )}
    ];
fields("zone") ->
    Fields = emqx_zone_schema:roots(),
    [{F, ref(emqx_zone_schema, F)} || F <- Fields];
fields("flapping_detect") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(flapping_detect_enable)
                }
            )},
        {"max_count",
            sc(
                integer(),
                #{
                    default => 15,
                    desc => ?DESC(flapping_detect_max_count)
                }
            )},
        {"window_time",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc => ?DESC(flapping_detect_window_time)
                }
            )},
        {"ban_time",
            sc(
                duration(),
                #{
                    default => "5m",
                    desc => ?DESC(flapping_detect_ban_time)
                }
            )}
    ];
fields("force_shutdown") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(force_shutdown_enable)
                }
            )},
        {"max_message_queue_len",
            sc(
                range(0, inf),
                #{
                    default => 1000,
                    desc => ?DESC(force_shutdown_max_message_queue_len)
                }
            )},
        {"max_heap_size",
            sc(
                wordsize(),
                #{
                    default => "32MB",
                    desc => ?DESC(force_shutdown_max_heap_size),
                    validator => fun ?MODULE:validate_heap_size/1
                }
            )}
    ];
fields("overload_protection") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_enable),
                    default => false
                }
            )},
        {"backoff_delay",
            sc(
                range(0, inf),
                #{
                    desc => ?DESC(overload_protection_backoff_delay),
                    default => 1
                }
            )},
        {"backoff_gc",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_backoff_gc),
                    default => false
                }
            )},
        {"backoff_hibernation",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_backoff_hibernation),
                    default => true
                }
            )},
        {"backoff_new_conn",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_backoff_new_conn),
                    default => true
                }
            )}
    ];
fields("conn_congestion") ->
    [
        {"enable_alarm",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(conn_congestion_enable_alarm)
                }
            )},
        {"min_alarm_sustain_duration",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc => ?DESC(conn_congestion_min_alarm_sustain_duration)
                }
            )}
    ];
fields("force_gc") ->
    [
        {"enable",
            sc(
                boolean(),
                #{default => true, desc => ?DESC(force_gc_enable)}
            )},
        {"count",
            sc(
                range(0, inf),
                #{
                    default => 16000,
                    desc => ?DESC(force_gc_count)
                }
            )},
        {"bytes",
            sc(
                bytesize(),
                #{
                    default => "16MB",
                    desc => ?DESC(force_gc_bytes)
                }
            )}
    ];
fields("listeners") ->
    [
        {"tcp",
            sc(
                map(name, ref("mqtt_tcp_listener")),
                #{
                    desc => ?DESC(fields_listeners_tcp),
                    required => {false, recursively}
                }
            )},
        {"ssl",
            sc(
                map(name, ref("mqtt_ssl_listener")),
                #{
                    desc => ?DESC(fields_listeners_ssl),
                    required => {false, recursively}
                }
            )},
        {"ws",
            sc(
                map(name, ref("mqtt_ws_listener")),
                #{
                    desc => ?DESC(fields_listeners_ws),
                    required => {false, recursively}
                }
            )},
        {"wss",
            sc(
                map(name, ref("mqtt_wss_listener")),
                #{
                    desc => ?DESC(fields_listeners_wss),
                    required => {false, recursively}
                }
            )},
        {"quic",
            sc(
                map(name, ref("mqtt_quic_listener")),
                #{
                    desc => ?DESC(fields_listeners_quic),
                    required => {false, recursively}
                }
            )}
    ];
fields("mqtt_tcp_listener") ->
    mqtt_listener(1883) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )}
        ];
fields("mqtt_ssl_listener") ->
    mqtt_listener(8883) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )},
            {"ssl_options",
                sc(
                    ref("listener_ssl_opts"),
                    #{}
                )}
        ];
fields("mqtt_ws_listener") ->
    mqtt_listener(8083) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )},
            {"websocket",
                sc(
                    ref("ws_opts"),
                    #{}
                )}
        ];
fields("mqtt_wss_listener") ->
    mqtt_listener(8084) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )},
            {"ssl_options",
                sc(
                    ref("listener_wss_opts"),
                    #{}
                )},
            {"websocket",
                sc(
                    ref("ws_opts"),
                    #{}
                )}
        ];
fields("mqtt_quic_listener") ->
    [
        %% TODO: ensure cacertfile is configurable
        {"certfile",
            sc(
                string(),
                #{desc => ?DESC(fields_mqtt_quic_listener_certfile)}
            )},
        {"keyfile",
            sc(
                string(),
                #{desc => ?DESC(fields_mqtt_quic_listener_keyfile)}
            )},
        {"ciphers", ciphers_schema(quic)},
        {"idle_timeout",
            sc(
                duration(),
                #{
                    default => "15s",
                    desc => ?DESC(fields_mqtt_quic_listener_idle_timeout)
                }
            )}
    ] ++ base_listener(14567);
fields("ws_opts") ->
    [
        {"mqtt_path",
            sc(
                string(),
                #{
                    default => "/mqtt",
                    desc => ?DESC(fields_ws_opts_mqtt_path)
                }
            )},
        {"mqtt_piggyback",
            sc(
                hoconsc:enum([single, multiple]),
                #{
                    default => multiple,
                    desc => ?DESC(fields_ws_opts_mqtt_piggyback)
                }
            )},
        {"compress",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_ws_opts_compress)
                }
            )},
        {"idle_timeout",
            sc(
                duration(),
                #{
                    default => "7200s",
                    desc => ?DESC(fields_mqtt_quic_listener_idle_timeout)
                }
            )},
        {"max_frame_size",
            sc(
                hoconsc:union([infinity, integer()]),
                #{
                    default => infinity,
                    desc => ?DESC(fields_ws_opts_max_frame_size)
                }
            )},
        {"fail_if_no_subprotocol",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_fail_if_no_subprotocol)
                }
            )},
        {"supported_subprotocols",
            sc(
                comma_separated_list(),
                #{
                    default => "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5",
                    desc => ?DESC(fields_ws_opts_supported_subprotocols)
                }
            )},
        {"check_origin_enable",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_ws_opts_check_origin_enable)
                }
            )},
        {"allow_origin_absence",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_allow_origin_absence)
                }
            )},
        {"check_origins",
            sc(
                comma_separated_binary(),
                #{
                    default => <<"http://localhost:18083, http://127.0.0.1:18083">>,
                    desc => ?DESC(fields_ws_opts_check_origins)
                }
            )},
        {"proxy_address_header",
            sc(
                string(),
                #{
                    default => "x-forwarded-for",
                    desc => ?DESC(fields_ws_opts_proxy_address_header)
                }
            )},
        {"proxy_port_header",
            sc(
                string(),
                #{
                    default => "x-forwarded-port",
                    desc => ?DESC(fields_ws_opts_proxy_port_header)
                }
            )},
        {"deflate_opts",
            sc(
                ref("deflate_opts"),
                #{}
            )}
    ];
fields("tcp_opts") ->
    [
        {"active_n",
            sc(
                integer(),
                #{
                    default => 100,
                    desc => ?DESC(fields_tcp_opts_active_n)
                }
            )},
        {"backlog",
            sc(
                pos_integer(),
                #{
                    default => 1024,
                    desc => ?DESC(fields_tcp_opts_backlog)
                }
            )},
        {"send_timeout",
            sc(
                duration(),
                #{
                    default => "15s",
                    desc => ?DESC(fields_tcp_opts_send_timeout)
                }
            )},
        {"send_timeout_close",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_tcp_opts_send_timeout_close)
                }
            )},
        {"recbuf",
            sc(
                bytesize(),
                #{
                    example => <<"2KB">>,
                    desc => ?DESC(fields_tcp_opts_recbuf)
                }
            )},
        {"sndbuf",
            sc(
                bytesize(),
                #{
                    example => <<"4KB">>,
                    desc => ?DESC(fields_tcp_opts_sndbuf)
                }
            )},
        {"buffer",
            sc(
                bytesize(),
                #{
                    default => <<"4KB">>,
                    example => <<"4KB">>,
                    desc => ?DESC(fields_tcp_opts_buffer)
                }
            )},
        {"high_watermark",
            sc(
                bytesize(),
                #{
                    default => "1MB",
                    desc => ?DESC(fields_tcp_opts_high_watermark)
                }
            )},
        {"nodelay",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_tcp_opts_nodelay)
                }
            )},
        {"reuseaddr",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_tcp_opts_reuseaddr)
                }
            )}
    ];
fields("listener_ssl_opts") ->
    server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available,
            ciphers => tls_all_available
        },
        false
    );
fields("listener_wss_opts") ->
    server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available,
            ciphers => tls_all_available
        },
        true
    );
fields("ssl_client_opts") ->
    client_ssl_opts_schema(#{});
fields("deflate_opts") ->
    [
        {"level",
            sc(
                hoconsc:enum([none, default, best_compression, best_speed]),
                #{desc => ?DESC(fields_deflate_opts_level)}
            )},
        {"mem_level",
            sc(
                range(1, 9),
                #{
                    default => 8,
                    desc => ?DESC(fields_deflate_opts_mem_level)
                }
            )},
        {"strategy",
            sc(
                hoconsc:enum([default, filtered, huffman_only, rle]),
                #{
                    default => default,
                    desc => ?DESC(fields_deflate_opts_strategy)
                }
            )},
        {"server_context_takeover",
            sc(
                hoconsc:enum([takeover, no_takeover]),
                #{
                    default => takeover,
                    desc => ?DESC(fields_deflate_opts_server_context_takeover)
                }
            )},
        {"client_context_takeover",
            sc(
                hoconsc:enum([takeover, no_takeover]),
                #{
                    default => takeover,
                    desc => ?DESC(fields_deflate_opts_client_context_takeover)
                }
            )},
        {"server_max_window_bits",
            sc(
                range(8, 15),
                #{
                    default => 15,
                    desc => ?DESC(fields_deflate_opts_server_max_window_bits)
                }
            )},
        {"client_max_window_bits",
            sc(
                range(8, 15),
                #{
                    default => 15,
                    desc => ?DESC(fields_deflate_opts_client_max_window_bits)
                }
            )}
    ];
fields("broker") ->
    [
        {"enable_session_registry",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(broker_enable_session_registry)
                }
            )},
        {"session_locking_strategy",
            sc(
                hoconsc:enum([local, leader, quorum, all]),
                #{
                    default => quorum,
                    desc => ?DESC(broker_session_locking_strategy)
                }
            )},
        {"shared_subscription_strategy",
            sc(
                hoconsc:enum([random, round_robin, sticky, local, hash_topic, hash_clientid]),
                #{
                    default => round_robin,
                    desc => ?DESC(broker_shared_subscription_strategy)
                }
            )},
        {"shared_dispatch_ack_enabled",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(broker_shared_dispatch_ack_enabled)
                }
            )},
        {"route_batch_clean",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(broker_route_batch_clean)
                }
            )},
        {"perf",
            sc(
                ref("broker_perf"),
                #{}
            )},
        {"shared_subscription_group",
            sc(
                map(name, ref("shared_subscription_group")),
                #{
                    example => #{<<"example_group">> => #{<<"strategy">> => <<"random">>}},
                    desc => ?DESC(shared_subscription_group_strategy)
                }
            )}
    ];
fields("shared_subscription_group") ->
    [
        {"strategy",
            sc(
                hoconsc:enum([random, round_robin, sticky, local, hash_topic, hash_clientid]),
                #{
                    default => random,
                    desc => ?DESC(shared_subscription_strategy_enum)
                }
            )}
    ];
fields("broker_perf") ->
    [
        {"route_lock_type",
            sc(
                hoconsc:enum([key, tab, global]),
                #{
                    default => key,
                    desc => ?DESC(broker_perf_route_lock_type)
                }
            )},
        {"trie_compaction",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(broker_perf_trie_compaction)
                }
            )}
    ];
fields("sys_topics") ->
    [
        {"sys_msg_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => "1m",
                    desc => ?DESC(sys_msg_interval)
                }
            )},
        {"sys_heartbeat_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => "30s",
                    desc => ?DESC(sys_heartbeat_interval)
                }
            )},
        {"sys_event_messages",
            sc(
                ref("event_names"),
                #{desc => ?DESC(sys_event_messages)}
            )}
    ];
fields("event_names") ->
    [
        {"client_connected",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sys_event_client_connected)
                }
            )},
        {"client_disconnected",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sys_event_client_disconnected)
                }
            )},
        {"client_subscribed",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(sys_event_client_subscribed)
                }
            )},
        {"client_unsubscribed",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(sys_event_client_unsubscribed)
                }
            )}
    ];
fields("sysmon") ->
    [
        {"vm",
            sc(
                ref("sysmon_vm"),
                #{}
            )},
        {"os",
            sc(
                ref("sysmon_os"),
                #{}
            )},
        {"top",
            sc(
                ref("sysmon_top"),
                #{}
            )}
    ];
fields("sysmon_vm") ->
    [
        {"process_check_interval",
            sc(
                duration(),
                #{
                    default => "30s",
                    desc => ?DESC(sysmon_vm_process_check_interval)
                }
            )},
        {"process_high_watermark",
            sc(
                percent(),
                #{
                    default => "80%",
                    desc => ?DESC(sysmon_vm_process_high_watermark)
                }
            )},
        {"process_low_watermark",
            sc(
                percent(),
                #{
                    default => "60%",
                    desc => ?DESC(sysmon_vm_process_low_watermark)
                }
            )},
        {"long_gc",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => disabled,
                    desc => ?DESC(sysmon_vm_long_gc)
                }
            )},
        {"long_schedule",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => "240ms",
                    desc => ?DESC(sysmon_vm_long_schedule)
                }
            )},
        {"large_heap",
            sc(
                hoconsc:union([disabled, bytesize()]),
                #{
                    default => "32MB",
                    desc => ?DESC(sysmon_vm_large_heap)
                }
            )},
        {"busy_dist_port",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sysmon_vm_busy_dist_port)
                }
            )},
        {"busy_port",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sysmon_vm_busy_port)
                }
            )}
    ];
fields("sysmon_os") ->
    [
        {"cpu_check_interval",
            sc(
                duration(),
                #{
                    default => "60s",
                    desc => ?DESC(sysmon_os_cpu_check_interval)
                }
            )},
        {"cpu_high_watermark",
            sc(
                percent(),
                #{
                    default => "80%",
                    desc => ?DESC(sysmon_os_cpu_high_watermark)
                }
            )},
        {"cpu_low_watermark",
            sc(
                percent(),
                #{
                    default => "60%",
                    desc => ?DESC(sysmon_os_cpu_low_watermark)
                }
            )},
        {"mem_check_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => "60s",
                    desc => ?DESC(sysmon_os_mem_check_interval)
                }
            )},
        {"sysmem_high_watermark",
            sc(
                percent(),
                #{
                    default => "70%",
                    desc => ?DESC(sysmon_os_sysmem_high_watermark)
                }
            )},
        {"procmem_high_watermark",
            sc(
                percent(),
                #{
                    default => "5%",
                    desc => ?DESC(sysmon_os_procmem_high_watermark)
                }
            )}
    ];
fields("sysmon_top") ->
    [
        {"num_items",
            sc(
                non_neg_integer(),
                #{
                    mapping => "system_monitor.top_num_items",
                    default => 10,
                    desc => ?DESC(sysmon_top_num_items)
                }
            )},
        {"sample_interval",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "system_monitor.top_sample_interval",
                    default => "2s",
                    desc => ?DESC(sysmon_top_sample_interval)
                }
            )},
        {"max_procs",
            sc(
                non_neg_integer(),
                #{
                    mapping => "system_monitor.top_max_procs",
                    default => 1_000_000,
                    desc => ?DESC(sysmon_top_max_procs)
                }
            )},
        {"db_hostname",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_hostname",
                    desc => ?DESC(sysmon_top_db_hostname),
                    default => ""
                }
            )},
        {"db_port",
            sc(
                integer(),
                #{
                    mapping => "system_monitor.db_port",
                    default => 5432,
                    desc => ?DESC(sysmon_top_db_port)
                }
            )},
        {"db_username",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_username",
                    default => "system_monitor",
                    desc => ?DESC(sysmon_top_db_username)
                }
            )},
        {"db_password",
            sc(
                binary(),
                #{
                    mapping => "system_monitor.db_password",
                    default => "system_monitor_password",
                    desc => ?DESC(sysmon_top_db_password)
                }
            )},
        {"db_name",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_name",
                    default => "postgres",
                    desc => ?DESC(sysmon_top_db_name)
                }
            )}
    ];
fields("alarm") ->
    [
        {"actions",
            sc(
                hoconsc:array(atom()),
                #{
                    default => [log, publish],
                    validator => fun ?MODULE:validate_alarm_actions/1,
                    example => [log, publish],
                    desc => ?DESC(alarm_actions)
                }
            )},
        {"size_limit",
            sc(
                range(1, 3000),
                #{
                    default => 1000,
                    example => 1000,
                    desc => ?DESC(alarm_size_limit)
                }
            )},
        {"validity_period",
            sc(
                duration(),
                #{
                    default => "24h",
                    example => "24h",
                    desc => ?DESC(alarm_validity_period)
                }
            )}
    ];
fields("trace") ->
    [
        {"payload_encode",
            sc(hoconsc:enum([hex, text, hidden]), #{
                default => text,
                desc => ?DESC(fields_trace_payload_encode)
            })}
    ].

mqtt_listener(Bind) ->
    base_listener(Bind) ++
        [
            {"access_rules",
                sc(
                    hoconsc:array(string()),
                    #{
                        desc => ?DESC(mqtt_listener_access_rules),
                        default => [<<"allow all">>]
                    }
                )},
            {"proxy_protocol",
                sc(
                    boolean(),
                    #{
                        desc => ?DESC(mqtt_listener_proxy_protocol),
                        default => false
                    }
                )},
            {"proxy_protocol_timeout",
                sc(
                    duration(),
                    #{
                        desc => ?DESC(mqtt_listener_proxy_protocol_timeout),
                        default => "3s"
                    }
                )},
            {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME, authentication(listener)}
        ].

base_listener(Bind) ->
    [
        {"enabled",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_listener_enabled)
                }
            )},
        {"bind",
            sc(
                hoconsc:union([ip_port(), integer()]),
                #{
                    default => Bind,
                    required => true,
                    desc => ?DESC(base_listener_bind)
                }
            )},
        {"acceptors",
            sc(
                pos_integer(),
                #{
                    default => 16,
                    desc => ?DESC(base_listener_acceptors)
                }
            )},
        {"max_connections",
            sc(
                hoconsc:union([infinity, pos_integer()]),
                #{
                    default => infinity,
                    desc => ?DESC(base_listener_max_connections)
                }
            )},
        {"mountpoint",
            sc(
                binary(),
                #{
                    default => <<>>,
                    desc => ?DESC(base_listener_mountpoint)
                }
            )},
        {"zone",
            sc(
                atom(),
                #{
                    desc => ?DESC(base_listener_zone),
                    default => 'default'
                }
            )},
        {"limiter",
            sc(
                map("ratelimit_name", emqx_limiter_schema:bucket_name()),
                #{
                    desc => ?DESC(base_listener_limiter),
                    default => #{<<"connection">> => <<"default">>}
                }
            )},
        {"enable_authn",
            sc(
                boolean(),
                #{
                    desc => ?DESC(base_listener_enable_authn),
                    default => true
                }
            )}
    ].

desc("persistent_session_store") ->
    "Settings for message persistence.";
desc("persistent_session_builtin") ->
    "Settings for the built-in storage engine of persistent messages.";
desc("persistent_table_mria_opts") ->
    "Tuning options for the mria table.";
desc("stats") ->
    "Enable/disable statistic data collection.\n"
    "Statistic data such as message receive/send count/rate etc. "
    "It provides insights of system performance and helps to diagnose issues. "
    "You can find statistic data from the dashboard, or from the '/stats' API.";
desc("authorization") ->
    "Settings for client authorization.";
desc("mqtt") ->
    "Global MQTT configuration.</br>\n"
    "The configs here work as default values which can be overridden\n"
    "in <code>zone</code> configs";
desc("cache") ->
    "Settings for the authorization cache.";
desc("zone") ->
    "A `Zone` defines a set of configuration items (such as the maximum number of connections)"
    " that can be shared between multiple listeners.\n\n"
    "`Listener` can refer to a `Zone` through the configuration item"
    " <code>listener.\\<Protocol>.\\<Listener Name>.zone</code>.\n\n"
    "The configs defined in the zones will override the global configs with the same key.\n\n"
    "For example, given the following config:\n"
    "```\n"
    "a {\n"
    "    b: 1, c: 1\n"
    "}\n"
    "zone.my_zone {\n"
    "  a {\n"
    "    b:2\n"
    "  }\n"
    "}\n"
    "```\n\n"
    "The global config `a` is overridden by the configs `a` inside the zone `my_zone`.\n\n"
    "If there is a listener using the zone `my_zone`, the value of config `a` will be: "
    "`{b:2, c: 1}`.\n"
    "Note that although the default value of `a.c` is `0`, the global value is used,"
    " i.e. configs in the zone have no default values. To override `a.c` one must configure"
    " it explicitly in the zone.\n\n"
    "All the global configs that can be overridden in zones are:\n"
    " - `stats.*`\n"
    " - `mqtt.*`\n"
    " - `authorization.*`\n"
    " - `flapping_detect.*`\n"
    " - `force_shutdown.*`\n"
    " - `conn_congestion.*`\n"
    " - `force_gc.*`\n\n";
desc("flapping_detect") ->
    "This config controls the allowed maximum number of `CONNECT` packets received\n"
    "from the same clientid in a time frame defined by `window_time`.\n"
    "After the limit is reached, successive `CONNECT` requests are forbidden\n"
    "(banned) until the end of the time period defined by `ban_time`.";
desc("force_shutdown") ->
    "When the process message queue length, or the memory bytes\n"
    "reaches a certain value, the process is forced to close.\n\n"
    "Note: \"message queue\" here refers to the \"message mailbox\"\n"
    "of the Erlang process, not the `mqueue` of QoS 1 and QoS 2.";
desc("overload_protection") ->
    "Overload protection mechanism monitors the load of the system and temporarily\n"
    "disables some features (such as accepting new connections) when the load is high.";
desc("conn_congestion") ->
    "Settings for `conn_congestion` alarm.\n\n"
    "Sometimes the MQTT connection (usually an MQTT subscriber) may\n"
    "get \"congested\", because there are too many packets to be sent.\n"
    "The socket tries to buffer the packets until the buffer is\n"
    "full. If more packets arrive after that, the packets will be\n"
    "\"pending\" in the queue, and we consider the connection\n"
    "congested.\n\n"
    "Note: `sndbuf` can be set to larger value if the\n"
    "alarm is triggered too often.\n"
    "The name of the alarm is of format `conn_congestion/<ClientID>/<Username>`,\n"
    "where the `<ClientID>` is the client ID of the congested MQTT connection,\n"
    "and `<Username>` is the username or `unknown_user`.";
desc("force_gc") ->
    "Force garbage collection in MQTT connection process after\n"
    " they process certain number of messages or bytes of data.";
desc("listeners") ->
    "MQTT listeners identified by their protocol type and assigned names";
desc("mqtt_tcp_listener") ->
    "Settings for the MQTT over TCP listener.";
desc("mqtt_ssl_listener") ->
    "Settings for the MQTT over SSL listener.";
desc("mqtt_ws_listener") ->
    "Settings for the MQTT over WebSocket listener.";
desc("mqtt_wss_listener") ->
    "Settings for the MQTT over WebSocket/SSL listener.";
desc("mqtt_quic_listener") ->
    "Settings for the MQTT over QUIC listener.";
desc("ws_opts") ->
    "WebSocket listener options.";
desc("tcp_opts") ->
    "TCP listener options.";
desc("listener_ssl_opts") ->
    "Socket options for SSL connections.";
desc("listener_wss_opts") ->
    "Socket options for WebSocket/SSL connections.";
desc("ssl_client_opts") ->
    "Socket options for SSL clients.";
desc("deflate_opts") ->
    "Compression options.";
desc("broker") ->
    "Message broker options.";
desc("broker_perf") ->
    "Broker performance tuning parameters.";
desc("sys_topics") ->
    "The EMQX Broker periodically publishes its own status, message statistics,\n"
    "client online and offline events to the system topic starting with `$SYS/`.\n\n"
    "The following options control the behavior of `$SYS` topics.";
desc("event_names") ->
    "Enable or disable client lifecycle event publishing.\n\n"
    "The following options affect MQTT clients as well as\n"
    "gateway clients. The types of the clients\n"
    "are distinguished by the topic prefix:\n\n"
    "- For the MQTT clients, the format is:\n"
    "`$SYS/broker/<node>/clients/<clientid>/<event>`\n"
    "- For the Gateway clients, it is\n"
    "`$SYS/broker/<node>/gateway/<gateway-name>/clients/<clientid>/<event>`\n";
desc("sysmon") ->
    "Features related to system monitoring and introspection.";
desc("sysmon_vm") ->
    "This part of the configuration is responsible for collecting\n"
    " BEAM VM events, such as long garbage collection, traffic congestion in the inter-broker\n"
    " communication, etc.";
desc("sysmon_os") ->
    "This part of the configuration is responsible for monitoring\n"
    " the host OS health, such as free memory, disk space, CPU load, etc.";
desc("sysmon_top") ->
    "This part of the configuration is responsible for monitoring\n"
    " the Erlang processes in the VM. This information can be sent to an external\n"
    " PostgreSQL database. This feature is inactive unless the PostgreSQL sink is configured.";
desc("alarm") ->
    "Settings for the alarms.";
desc("trace") ->
    "Real-time filtering logs for the ClientID or Topic or IP for debugging.";
desc("shared_subscription_group") ->
    "Per group dispatch strategy for shared subscription";
desc(_) ->
    undefined.

%% utils
-spec conf_get(string() | [string()], hocon:config()) -> term().
conf_get(Key, Conf) ->
    V = hocon_maps:get(Key, Conf),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

conf_get(Key, Conf, Default) ->
    V = hocon_maps:get(Key, Conf, Default),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private This function defines the SSL opts which are commonly used by
%% SSL listener and client.
-spec common_ssl_opts_schema(map()) -> hocon_schema:field_schema().
common_ssl_opts_schema(Defaults) ->
    D = fun(Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    Df = fun(Field, Default) -> maps:get(to_atom(Field), Defaults, Default) end,
    [
        {"cacertfile",
            sc(
                binary(),
                #{
                    default => D("cacertfile"),
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_cacertfile)
                }
            )},
        {"certfile",
            sc(
                binary(),
                #{
                    default => D("certfile"),
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_certfile)
                }
            )},
        {"keyfile",
            sc(
                binary(),
                #{
                    default => D("keyfile"),
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_keyfile)
                }
            )},
        {"verify",
            sc(
                hoconsc:enum([verify_peer, verify_none]),
                #{
                    default => Df("verify", verify_none),
                    desc => ?DESC(common_ssl_opts_schema_verify)
                }
            )},
        {"reuse_sessions",
            sc(
                boolean(),
                #{
                    default => Df("reuse_sessions", true),
                    desc => ?DESC(common_ssl_opts_schema_reuse_sessions)
                }
            )},
        {"depth",
            sc(
                integer(),
                #{
                    default => Df("depth", 10),
                    desc => ?DESC(common_ssl_opts_schema_depth)
                }
            )},
        {"password",
            sc(
                string(),
                #{
                    sensitive => true,
                    required => false,
                    example => <<"">>,
                    desc => ?DESC(common_ssl_opts_schema_password)
                }
            )},
        {"versions",
            sc(
                hoconsc:array(typerefl:atom()),
                #{
                    default => default_tls_vsns(maps:get(versions, Defaults, tls_all_available)),
                    desc => ?DESC(common_ssl_opts_schema_versions),
                    validator => fun validate_tls_versions/1
                }
            )},
        {"ciphers", ciphers_schema(D("ciphers"))},
        {"user_lookup_fun",
            sc(
                typerefl:alias("string", any()),
                #{
                    default => <<"emqx_tls_psk:lookup">>,
                    converter => fun ?MODULE:parse_user_lookup_fun/1,
                    desc => ?DESC(common_ssl_opts_schema_user_lookup_fun)
                }
            )},
        {"secure_renegotiate",
            sc(
                boolean(),
                #{
                    default => Df("secure_renegotiate", true),
                    desc => ?DESC(common_ssl_opts_schema_secure_renegotiate)
                }
            )}
    ].

%% @doc Make schema for SSL listener options.
%% When it's for ranch listener, an extra field `handshake_timeout' is added.
-spec server_ssl_opts_schema(map(), boolean()) -> hocon_schema:field_schema().
server_ssl_opts_schema(Defaults, IsRanchListener) ->
    D = fun(Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    Df = fun(Field, Default) -> maps:get(to_atom(Field), Defaults, Default) end,
    common_ssl_opts_schema(Defaults) ++
        [
            {"dhfile",
                sc(
                    string(),
                    #{
                        default => D("dhfile"),
                        required => false,
                        desc => ?DESC(server_ssl_opts_schema_dhfile)
                    }
                )},
            {"fail_if_no_peer_cert",
                sc(
                    boolean(),
                    #{
                        default => Df("fail_if_no_peer_cert", false),
                        desc => ?DESC(server_ssl_opts_schema_fail_if_no_peer_cert)
                    }
                )},
            {"honor_cipher_order",
                sc(
                    boolean(),
                    #{
                        default => Df("honor_cipher_order", true),
                        desc => ?DESC(server_ssl_opts_schema_honor_cipher_order)
                    }
                )},
            {"client_renegotiation",
                sc(
                    boolean(),
                    #{
                        default => Df("client_renegotiation", true),
                        desc => ?DESC(server_ssl_opts_schema_client_renegotiation)
                    }
                )}
            | [
                {"handshake_timeout",
                    sc(
                        duration(),
                        #{
                            default => Df("handshake_timeout", "15s"),
                            desc => ?DESC(server_ssl_opts_schema_handshake_timeout)
                        }
                    )}
             || IsRanchListener
            ]
        ].

%% @doc Make schema for SSL client.
-spec client_ssl_opts_schema(map()) -> hocon_schema:field_schema().
client_ssl_opts_schema(Defaults) ->
    common_ssl_opts_schema(Defaults) ++
        [
            {"enable",
                sc(
                    boolean(),
                    #{
                        default => false,
                        desc => ?DESC(client_ssl_opts_schema_enable)
                    }
                )},
            {"server_name_indication",
                sc(
                    hoconsc:union([disable, string()]),
                    #{
                        required => false,
                        example => disable,
                        validator => fun emqx_schema:non_empty_string/1,
                        desc => ?DESC(client_ssl_opts_schema_server_name_indication)
                    }
                )}
        ].

default_tls_vsns(dtls_all_available) ->
    proplists:get_value(available_dtls, ssl:versions());
default_tls_vsns(tls_all_available) ->
    emqx_tls_lib:default_versions().

-spec ciphers_schema(quic | dtls_all_available | tls_all_available | undefined) ->
    hocon_schema:field_schema().
ciphers_schema(Default) ->
    Desc =
        case Default of
            quic -> ?DESC(ciphers_schema_quic);
            _ -> ?DESC(ciphers_schema_common)
        end,
    sc(
        hoconsc:array(string()),
        #{
            default => default_ciphers(Default),
            converter => fun
                (Ciphers) when is_binary(Ciphers) ->
                    binary:split(Ciphers, <<",">>, [global]);
                (Ciphers) when is_list(Ciphers) ->
                    Ciphers
            end,
            validator =>
                case Default =:= quic of
                    %% quic has openssl statically linked
                    true -> undefined;
                    false -> fun validate_ciphers/1
                end,
            desc => Desc
        }
    ).

default_ciphers(Which) ->
    lists:map(
        fun erlang:iolist_to_binary/1,
        do_default_ciphers(Which)
    ).

do_default_ciphers(undefined) ->
    do_default_ciphers(tls_all_available);
do_default_ciphers(quic) ->
    [
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256",
        "TLS_CHACHA20_POLY1305_SHA256"
    ];
do_default_ciphers(dtls_all_available) ->
    %% as of now, dtls does not support tlsv1.3 ciphers
    emqx_tls_lib:selected_ciphers(['dtlsv1.2', 'dtlsv1']);
do_default_ciphers(tls_all_available) ->
    emqx_tls_lib:default_ciphers().

%% @private return a list of keys in a parent field
-spec keys(string(), hocon:config()) -> [string()].
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

-spec ceiling(number()) -> integer().
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

%% types

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

map(Name, Type) -> hoconsc:map(Name, Type).

ref(Field) -> hoconsc:ref(?MODULE, Field).

ref(Module, Field) -> hoconsc:ref(Module, Field).

mk_duration(Desc, OverrideMeta) ->
    DefaultMeta = #{
        desc => Desc ++
            " Time interval is a string that contains a number followed by time unit:</br>\n"
            "- `ms` for milliseconds,\n"
            "- `s` for seconds,\n"
            "- `m` for minutes,\n"
            "- `h` for hours;\n</br>"
            "or combination of whereof: `1h5m0s`"
    },
    hoconsc:mk(typerefl:alias("string", duration()), maps:merge(DefaultMeta, OverrideMeta)).

to_duration(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_duration_s(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_number(I) -> {ok, ceiling(I / 1000)};
        _ -> {error, Str}
    end.

-spec to_duration_ms(Input) -> {ok, integer()} | {error, Input} when
    Input :: string() | binary().
to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_number(I) -> {ok, ceiling(I)};
        _ -> {error, Str}
    end.

to_bytesize(Str) ->
    case hocon_postprocess:bytesize(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_wordsize(Str) ->
    WordSize = erlang:system_info(wordsize),
    case to_bytesize(Str) of
        {ok, Bytes} -> {ok, Bytes div WordSize};
        Error -> Error
    end.

to_percent(Str) ->
    {ok, hocon_postprocess:percent(Str)}.

to_comma_separated_list(Str) ->
    {ok, string:tokens(Str, ", ")}.

to_comma_separated_binary(Str) ->
    {ok, lists:map(fun erlang:list_to_binary/1, string:tokens(Str, ", "))}.

to_comma_separated_atoms(Str) ->
    {ok, lists:map(fun to_atom/1, string:tokens(Str, ", "))}.

to_bar_separated_list(Str) ->
    {ok, string:tokens(Str, "| ")}.

to_ip_port(Str) ->
    case string:tokens(Str, ": ") of
        [Ip, Port] ->
            PortVal = list_to_integer(Port),
            case inet:parse_address(Ip) of
                {ok, R} ->
                    {ok, {R, PortVal}};
                _ ->
                    %% check is a rfc1035's hostname
                    case inet_parse:domain(Ip) of
                        true ->
                            {ok, {Ip, PortVal}};
                        _ ->
                            {error, Str}
                    end
            end;
        _ ->
            {error, Str}
    end.

to_erl_cipher_suite(Str) ->
    case ssl:str_to_suite(Str) of
        {error, Reason} -> error({invalid_cipher, Reason});
        Cipher -> Cipher
    end.

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).

validate_heap_size(Siz) ->
    MaxSiz =
        case erlang:system_info(wordsize) of
            % arch_64
            8 ->
                (1 bsl 59) - 1;
            % arch_32
            4 ->
                (1 bsl 27) - 1
        end,
    case Siz > MaxSiz of
        true -> error(io_lib:format("force_shutdown_policy: heap-size ~ts is too large", [Siz]));
        false -> ok
    end.

validate_alarm_actions(Actions) ->
    UnSupported = lists:filter(
        fun(Action) -> Action =/= log andalso Action =/= publish end, Actions
    ),
    case UnSupported of
        [] -> ok;
        Error -> {error, Error}
    end.

parse_user_lookup_fun(StrConf) ->
    [ModStr, FunStr] = string:tokens(str(StrConf), ": "),
    Mod = list_to_atom(ModStr),
    Fun = list_to_atom(FunStr),
    {fun Mod:Fun/3, undefined}.

validate_ciphers(Ciphers) ->
    All = emqx_tls_lib:all_ciphers(),
    case lists:filter(fun(Cipher) -> not lists:member(Cipher, All) end, Ciphers) of
        [] -> ok;
        Bad -> {error, {bad_ciphers, Bad}}
    end.

validate_tls_versions(Versions) ->
    AvailableVersions =
        proplists:get_value(available, ssl:versions()) ++
            proplists:get_value(available_dtls, ssl:versions()),
    case lists:filter(fun(V) -> not lists:member(V, AvailableVersions) end, Versions) of
        [] -> ok;
        Vs -> {error, {unsupported_ssl_versions, Vs}}
    end.

validations() ->
    [
        {check_process_watermark, fun check_process_watermark/1},
        {check_cpu_watermark, fun check_cpu_watermark/1}
    ].

%% validations from emqx_conf_schema, we must filter other *_schema by undefined.
check_process_watermark(Conf) ->
    check_watermark("sysmon.vm.process_low_watermark", "sysmon.vm.process_high_watermark", Conf).

check_cpu_watermark(Conf) ->
    check_watermark("sysmon.os.cpu_low_watermark", "sysmon.os.cpu_high_watermark", Conf).

check_watermark(LowKey, HighKey, Conf) ->
    case hocon_maps:get(LowKey, Conf) of
        undefined ->
            true;
        Low ->
            High = hocon_maps:get(HighKey, Conf),
            case Low < High of
                true -> true;
                false -> {bad_watermark, #{LowKey => Low, HighKey => High}}
            end
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.

authentication(Which) ->
    Desc =
        case Which of
            global -> ?DESC(global_authentication);
            listener -> ?DESC(listener_authentication)
        end,
    %% The runtime module injection
    %% from EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY
    %% is for now only affecting document generation.
    %% maybe in the future, we can find a more straightforward way to support
    %% * document generation (at compile time)
    %% * type checks before boot (in bin/emqx config generation)
    %% * type checks at runtime (when changing configs via management API)
    Type0 =
        case persistent_term:get(?EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY, undefined) of
            undefined -> hoconsc:array(typerefl:map());
            Module -> Module:root_type()
        end,
    %% It is a lazy type because when handing runtime update requests
    %% the config is not checked by emqx_schema, but by the injected schema
    Type = hoconsc:lazy(Type0),
    #{
        type => Type,
        desc => Desc
    }.

-spec qos() -> typerefl:type().
qos() ->
    typerefl:alias("qos", typerefl:union([0, 1, 2])).

non_empty_string(<<>>) -> {error, empty_string_not_allowed};
non_empty_string("") -> {error, empty_string_not_allowed};
non_empty_string(S) when is_binary(S); is_list(S) -> ok;
non_empty_string(_) -> {error, invalid_string}.
