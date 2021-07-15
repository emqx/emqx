-module(emqx_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").

-type log_level() :: debug | info | notice | warning | error | critical | alert | emergency | all.
-type duration() :: integer().
-type duration_s() :: integer().
-type duration_ms() :: integer().
-type bytesize() :: integer().
-type wordsize() :: bytesize().
-type percent() :: float().
-type file() :: string().
-type comma_separated_list() :: list().
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
-typerefl_from_string({bar_separated_list/0, emqx_schema, to_bar_separated_list}).
-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({cipher/0, emqx_schema, to_erl_cipher_suite}).
-typerefl_from_string({comma_separated_atoms/0, emqx_schema, to_comma_separated_atoms}).

% workaround: prevent being recognized as unused functions
-export([to_duration/1, to_duration_s/1, to_duration_ms/1,
         to_bytesize/1, to_wordsize/1,
         to_percent/1, to_comma_separated_list/1,
         to_bar_separated_list/1, to_ip_port/1,
         to_erl_cipher_suite/1,
         to_comma_separated_atoms/1]).

-behaviour(hocon_schema).

-reflect_type([ log_level/0, duration/0, duration_s/0, duration_ms/0,
                bytesize/0, wordsize/0, percent/0, file/0,
                comma_separated_list/0, bar_separated_list/0, ip_port/0,
                cipher/0,
                comma_separated_atoms/0]).

-export([structs/0, fields/1, translations/0, translation/1]).
-export([t/1, t/3, t/4, ref/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([ssl/1]).

%% will be used by emqx_ct_helper to find the dependent apps
-export([includes/0]).

structs() -> ["cluster", "node", "rpc", "log", "lager",
              "zones", "listeners", "broker",
              "plugins", "sysmon", "alarm"]
             ++ includes().

-ifdef(TEST).
includes() ->[].
-else.
includes() ->
    [ "emqx_data_bridge"
    , "emqx_telemetry"
    , "emqx_retainer"
    , "emqx_statsd"
    , "emqx_authn"
    , "emqx_authz"
    , "emqx_bridge_mqtt"
    , "emqx_modules"
    , "emqx_management"
    , "emqx_gateway"
    ].
-endif.

fields("cluster") ->
    [ {"name", t(atom(), "ekka.cluster_name", emqxcl)}
    , {"discovery_strategy", t(union([manual, static, mcast, dns, etcd, k8s]),
        undefined, manual)}
    , {"autoclean", t(duration(), "ekka.cluster_autoclean", undefined)}
    , {"autoheal", t(boolean(), "ekka.cluster_autoheal", false)}
    , {"static", ref("static")}
    , {"mcast", ref("mcast")}
    , {"proto_dist", t(union([inet_tcp, inet6_tcp, inet_tls]), "ekka.proto_dist", inet_tcp)}
    , {"dns", ref("dns")}
    , {"etcd", ref("etcd")}
    , {"k8s", ref("k8s")}
    , {"db_backend", t(union([mnesia, rlog]), "ekka.db_backend", mnesia)}
    , {"rlog", ref("rlog")}
    ];

fields("static") ->
    [ {"seeds", t(hoconsc:array(string()))}];

fields("mcast") ->
    [ {"addr", t(string(), undefined, "239.192.0.1")}
    , {"ports", t(comma_separated_list(), undefined, "4369")}
    , {"iface", t(string(), undefined, "0.0.0.0")}
    , {"ttl", t(integer(), undefined, 255)}
    , {"loop", t(boolean(), undefined, true)}
    , {"sndbuf", t(bytesize(), undefined, "16KB")}
    , {"recbuf", t(bytesize(), undefined, "16KB")}
    , {"buffer", t(bytesize(), undefined, "32KB")}
    ];

fields("dns") ->
    [ {"name", t(string())}
    , {"app", t(string())}];

fields("etcd") ->
    [ {"server", t(comma_separated_list())}
    , {"prefix", t(string())}
    , {"node_ttl", t(duration(), undefined, "1m")}
    , {"ssl", ref("etcd_ssl")}
    ];

fields("etcd_ssl") ->
    ssl(#{});

fields("k8s") ->
    [ {"apiserver", t(string())}
    , {"service_name", t(string())}
    , {"address_type", t(union([ip, dns, hostname]))}
    , {"app_name", t(string())}
    , {"namespace", t(string())}
    , {"suffix", t(string(), undefined, "")}
    ];

fields("rlog") ->
    [ {"role", t(union([core, replicant]), "ekka.node_role", core)}
    , {"core_nodes", t(comma_separated_atoms(), "ekka.core_nodes", [])}
    ];

fields("node") ->
    [ {"name", t(string(), "vm_args.-name", "emqx@127.0.0.1", "EMQX_NODE_NAME")}
    , {"cookie", hoconsc:t(string(), #{mapping => "vm_args.-setcookie",
                                       default => "emqxsecretcookie",
                                       sensitive => true,
                                       override_env => "EMQX_NODE_COOKIE"
                                      })}
    , {"data_dir", t(string(), "emqx.data_dir", undefined)}
    , {"config_files", t(list(string()), "emqx.config_files",
        [ filename:join([os:getenv("RUNNER_ETC_DIR"), "emqx.conf"])
        ])}
    , {"global_gc_interval", t(duration_s(), "emqx.global_gc_interval", undefined)}
    , {"crash_dump_dir", t(file(), "vm_args.-env ERL_CRASH_DUMP", undefined)}
    , {"dist_net_ticktime", t(integer(), "vm_args.-kernel net_ticktime", undefined)}
    , {"dist_listen_min", t(integer(), "kernel.inet_dist_listen_min", undefined)}
    , {"dist_listen_max", t(integer(), "kernel.inet_dist_listen_max", undefined)}
    , {"backtrace_depth", t(integer(), "emqx.backtrace_depth", 16)}
    ];

fields("rpc") ->
    [ {"mode", t(union(sync, async), "emqx.rpc_mode", async)}
    , {"async_batch_size", t(integer(), "gen_rpc.max_batch_size", 256)}
    , {"port_discovery",t(union(manual, stateless), "gen_rpc.port_discovery", stateless)}
    , {"tcp_server_port", t(integer(), "gen_rpc.tcp_server_port", 5369)}
    , {"tcp_client_num", t(range(0, 255), undefined, 0)}
    , {"connect_timeout", t(duration(), "gen_rpc.connect_timeout", "5s")}
    , {"send_timeout", t(duration(), "gen_rpc.send_timeout", "5s")}
    , {"authentication_timeout", t(duration(), "gen_rpc.authentication_timeout", "5s")}
    , {"call_receive_timeout", t(duration(), "gen_rpc.call_receive_timeout", "15s")}
    , {"socket_keepalive_idle", t(duration_s(), "gen_rpc.socket_keepalive_idle", "7200s")}
    , {"socket_keepalive_interval", t(duration_s(), "gen_rpc.socket_keepalive_interval", "75s")}
    , {"socket_keepalive_count", t(integer(), "gen_rpc.socket_keepalive_count", 9)}
    , {"socket_sndbuf", t(bytesize(), "gen_rpc.socket_sndbuf", "1MB")}
    , {"socket_recbuf", t(bytesize(), "gen_rpc.socket_recbuf", "1MB")}
    , {"socket_buffer", t(bytesize(), "gen_rpc.socket_buffer", "1MB")}
    ];

fields("log") ->
    [ {"primary_level", t(log_level(), undefined, warning)}
    , {"console_handler", ref("console_handler")}
    , {"file_handlers", ref("file_handlers")}
    , {"time_offset", t(string(), undefined, "system")}
    , {"chars_limit", maybe_infinity(integer())}
    , {"supervisor_reports", t(union([error, progress]), undefined, error)}
    , {"max_depth", t(union([infinity, integer()]),
                      "kernel.error_logger_format_depth", 80)}
    , {"formatter", t(union([text, json]), undefined, text)}
    , {"single_line", t(boolean(), undefined, true)}
    , {"sync_mode_qlen", t(integer(), undefined, 100)}
    , {"drop_mode_qlen", t(integer(), undefined, 3000)}
    , {"flush_qlen", t(integer(), undefined, 8000)}
    , {"overload_kill", ref("log_overload_kill")}
    , {"burst_limit", ref("log_burst_limit")}
    , {"error_logger", t(atom(), "kernel.error_logger", silent)}
    ];

fields("console_handler") ->
    [ {"enable", t(boolean(), undefined, false)}
    , {"level", t(log_level(), undefined, warning)}
    ];

fields("file_handlers") ->
    [ {"$name", ref("log_file_handler")}
    ];

fields("log_file_handler") ->
    [ {"level", t(log_level(), undefined, warning)}
    , {"file", t(file(), undefined, undefined)}
    , {"rotation", ref("log_rotation")}
    , {"max_size", maybe_infinity(bytesize(), "10MB")}
    ];

fields("log_rotation") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"count", t(range(1, 2048), undefined, 10)}
    ];

fields("log_overload_kill") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"mem_size", t(bytesize(), undefined, "30MB")}
    , {"qlen", t(integer(), undefined, 20000)}
    , {"restart_after", t(union(duration(), infinity), undefined, "5s")}
    ];

fields("log_burst_limit") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_count", t(integer(), undefined, 10000)}
    , {"window_time", t(duration(), undefined, "1s")}
    ];

fields("lager") ->
    [ {"handlers", t(string(), "lager.handlers", "")}
    , {"crash_log", t(boolean(), "lager.crash_log", false)}
    ];

fields("stats") ->
    [ {"enable", t(boolean(), undefined, true)}
    ];

fields("auth") ->
    [ {"enable", t(boolean(), undefined, false)}
    ];

fields("acl") ->
    [ {"enable", t(boolean(), undefined, false)}
    , {"cache", ref("acl_cache")}
    , {"deny_action", t(union(ignore, disconnect), undefined, ignore)}
    ];

fields("acl_cache") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_size", maybe_infinity(range(1, 1048576), 32)}
    , {"ttl", t(duration(), undefined, "1m")}
    ];

fields("mqtt") ->
    [ {"mountpoint", t(binary(), undefined, <<>>)}
    , {"idle_timeout", maybe_infinity(duration(), "15s")}
    , {"max_packet_size", t(bytesize(), undefined, "1MB")}
    , {"max_clientid_len", t(integer(), undefined, 65535)}
    , {"max_topic_levels", t(integer(), undefined, 65535)}
    , {"max_qos_allowed", t(range(0, 2), undefined, 2)}
    , {"max_topic_alias", t(integer(), undefined, 65535)}
    , {"retain_available", t(boolean(), undefined, true)}
    , {"wildcard_subscription", t(boolean(), undefined, true)}
    , {"shared_subscription", t(boolean(), undefined, true)}
    , {"ignore_loop_deliver", t(boolean())}
    , {"strict_mode", t(boolean(), undefined, false)}
    , {"response_information", t(string(), undefined, "")}
    , {"server_keepalive", maybe_disabled(integer())}
    , {"keepalive_backoff", t(float(), undefined, 0.75)}
    , {"max_subscriptions", maybe_infinity(integer())}
    , {"upgrade_qos", t(boolean(), undefined, false)}
    , {"max_inflight", t(range(1, 65535))}
    , {"retry_interval", t(duration_s(), undefined, "30s")}
    , {"max_awaiting_rel", maybe_infinity(duration())}
    , {"await_rel_timeout", t(duration_s(), undefined, "300s")}
    , {"session_expiry_interval", t(duration_s(), undefined, "2h")}
    , {"max_mqueue_len", maybe_infinity(integer(), 1000)}
    , {"mqueue_priorities", maybe_disabled(map())}
    , {"mqueue_default_priority", t(union(highest, lowest), undefined, lowest)}
    , {"mqueue_store_qos0", t(boolean(), undefined, true)}
    , {"use_username_as_clientid", t(boolean(), undefined, false)}
    , {"peer_cert_as_username", maybe_disabled(union([cn, dn, crt, pem, md5]))}
    , {"peer_cert_as_clientid", maybe_disabled(union([cn, dn, crt, pem, md5]))}
    ];

fields("zones") ->
    [ {"$name", ref("zone_settings")}];

fields("zone_settings") ->
    [ {"mqtt", ref("mqtt")}
    , {"acl", ref("acl")}
    , {"auth", ref("auth")}
    , {"stats", ref("stats")}
    , {"flapping_detect", ref("flapping_detect")}
    , {"force_shutdown", ref("force_shutdown")}
    , {"conn_congestion", ref("conn_congestion")}
    , {"force_gc", ref("force_gc")}
    , {"overall_max_connections", maybe_infinity(integer())}
    , {"listeners", t("listeners")}
    ];

fields("rate_limit") ->
    [ {"max_conn_rate", maybe_infinity(integer(), 1000)}
    , {"conn_messages_in", maybe_infinity(comma_separated_list())}
    , {"conn_bytes_in", maybe_infinity(comma_separated_list())}
    , {"quota", ref("rate_limit_quota")}
    ];

fields("rate_limit_quota") ->
    [ {"conn_messages_routing", maybe_infinity(comma_separated_list())}
    , {"overall_messages_routing", maybe_infinity(comma_separated_list())}
    ];

fields("flapping_detect") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_count", t(integer(), undefined, 15)}
    , {"window_time", t(duration(), undefined, "1m")}
    , {"ban_time", t(duration(), undefined, "5m")}
    ];

fields("force_shutdown") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_message_queue_len", t(range(0, inf), undefined, 1000)}
    , {"max_heap_size", t(wordsize(), undefined, "32MB", undefined,
        fun(Siz) ->
            MaxSiz = case erlang:system_info(wordsize) of
                8 -> % arch_64
                    (1 bsl 59) - 1;
                4 -> % arch_32
                    (1 bsl 27) - 1
            end,
            case Siz > MaxSiz of
                true ->
                    error(io_lib:format("force_shutdown_policy: heap-size ~s is too large", [Siz]));
                false ->
                    ok
            end
        end)}
    ];

fields("conn_congestion") ->
    [ {"enable_alarm", t(boolean(), undefined, false)}
    , {"min_alarm_sustain_duration", t(duration(), undefined, "1m")}
    ];

fields("force_gc") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"count", t(range(0, inf), undefined, 16000)}
    , {"bytes", t(bytesize(), undefined, "16MB")}
    ];

fields("listeners") ->
    [ {"$name", hoconsc:union(
        [ hoconsc:ref("mqtt_tcp_listener")
        , hoconsc:ref("mqtt_ws_listener")
        , hoconsc:ref("mqtt_quic_listener")
        ])}
    ];

fields("mqtt_tcp_listener") ->
    [ {"type", t(tcp)}
    , {"tcp", ref("tcp_opts")}
    , {"ssl", ref("ssl_opts")}
    ] ++ mqtt_listener();

fields("mqtt_ws_listener") ->
    [ {"type", t(ws)}
    , {"tcp", ref("tcp_opts")}
    , {"ssl", ref("ssl_opts")}
    , {"websocket", ref("ws_opts")}
    ] ++ mqtt_listener();

fields("mqtt_quic_listener") ->
    [ {"type", t(quic)}
    , {"certfile", t(string(), undefined, undefined)}
    , {"keyfile", t(string(), undefined, undefined)}
    , {"ciphers", t(comma_separated_list(), undefined, "TLS_AES_256_GCM_SHA384,"
                    "TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256")}
    , {"idle_timeout", t(duration(), undefined, 60000)}
    ] ++ base_listener();

fields("ws_opts") ->
    [ {"mqtt_path", t(string(), undefined, "/mqtt")}
    , {"mqtt_piggyback", t(union(single, multiple), undefined, multiple)}
    , {"compress", t(boolean(), undefined, false)}
    , {"idle_timeout", t(duration(), undefined, "15s")}
    , {"max_frame_size", maybe_infinity(integer())}
    , {"fail_if_no_subprotocol", t(boolean(), undefined, true)}
    , {"supported_subprotocols", t(comma_separated_list(), undefined,
        "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5")}
    , {"check_origin_enable", t(boolean(), undefined, false)}
    , {"allow_origin_absence", t(boolean(), undefined, true)}
    , {"check_origins", t(hoconsc:array(binary()), undefined, [])}
    , {"proxy_address_header", t(string(), undefined, "x-forwarded-for")}
    , {"proxy_port_header", t(string(), undefined, "x-forwarded-port")}
    , {"deflate_opts", ref("deflate_opts")}
    ];

fields("tcp_opts") ->
    [ {"active_n", t(integer(), undefined, 100)}
    , {"backlog", t(integer(), undefined, 1024)}
    , {"send_timeout", t(duration(), undefined, "15s")}
    , {"send_timeout_close", t(boolean(), undefined, true)}
    , {"recbuf", t(bytesize())}
    , {"sndbuf", t(bytesize())}
    , {"buffer", t(bytesize())}
    , {"tune_buffer", t(boolean())}
    , {"high_watermark", t(bytesize(), undefined, "1MB")}
    , {"nodelay", t(boolean())}
    , {"reuseaddr", t(boolean())}
    ];

fields("ssl_opts") ->
    ssl(#{handshake_timeout => "15s"
        , depth => 10
        , reuse_sessions => true
        , versions => default_tls_vsns()
        , ciphers => default_ciphers()
        });

fields("deflate_opts") ->
    [ {"level", t(union([none, default, best_compression, best_speed]))}
    , {"mem_level", t(range(1, 9), undefined, 8)}
    , {"strategy", t(union([default, filtered, huffman_only, rle]))}
    , {"server_context_takeover", t(union(takeover, no_takeover))}
    , {"client_context_takeover", t(union(takeover, no_takeover))}
    , {"server_max_window_bits", t(range(8, 15), undefined, 15)}
    , {"client_max_window_bits", t(range(8, 15), undefined, 15)}
    ];


fields("presence") ->
    [ {"qos", t(range(0, 2), undefined, 1)}];

fields("subscription") ->
    [ {"$id", ref("subscription_settings")}];

fields("subscription_settings") ->
    [ {"topic", t(string())}
    , {"qos", t(range(0, 2), undefined, 1)}
    , {"nl", t(range(0, 1), undefined, 0)}
    , {"rap", t(range(0, 1), undefined, 0)}
    , {"rh", t(range(0, 2), undefined, 0)}
    ];

fields("rewrite") ->
    [ {"rule", ref("rule")}
    , {"pub_rule", ref("rule")}
    , {"sub_rule", ref("rule")}
    ];

fields("rule") ->
    [ {"$id", t(string())}];

fields("plugins") ->
    [ {"expand_plugins_dir", t(string(), "emqx.expand_plugins_dir", undefined)}
    ];

fields("broker") ->
    [ {"sys_msg_interval", maybe_disabled(duration(), "1m")}
    , {"sys_heartbeat_interval", maybe_disabled(duration(), "30s")}
    , {"enable_session_registry", t(boolean(), undefined, true)}
    , {"session_locking_strategy", t(union([local, leader, quorum, all]), undefined, quorum)}
    , {"shared_subscription_strategy", t(union(random, round_robin), undefined, round_robin)}
    , {"shared_dispatch_ack_enabled", t(boolean(), undefined, false)}
    , {"route_batch_clean", t(boolean(), undefined, true)}
    , {"perf", ref("perf")}
    ];

fields("perf") ->
    [ {"route_lock_type", t(union([key, tab, global]), "emqx.route_lock_type", key)}
    , {"trie_compaction", t(boolean(), "emqx.trie_compaction", true)}
    ];

fields("sysmon") ->
    [ {"vm", ref("sysmon_vm")}
    , {"os", ref("sysmon_os")}
    ];

fields("sysmon_vm") ->
    [ {"process_check_interval", t(duration(), undefined, "30s")}
    , {"process_high_watermark", t(percent(), undefined, "80%")}
    , {"process_low_watermark", t(percent(), undefined, "60%")}
    , {"long_gc", maybe_disabled(duration())}
    , {"long_schedule", maybe_disabled(duration(), 240)}
    , {"large_heap", maybe_disabled(bytesize(), "8MB")}
    , {"busy_dist_port", t(boolean(), undefined, true)}
    , {"busy_port", t(boolean(), undefined, false)}
    ];

fields("sysmon_os") ->
    [ {"cpu_check_interval", t(duration_s(), undefined, 60)}
    , {"cpu_high_watermark", t(percent(), undefined, "80%")}
    , {"cpu_low_watermark", t(percent(), undefined, "60%")}
    , {"mem_check_interval", maybe_disabled(duration_s(), 60)}
    , {"sysmem_high_watermark", t(percent(), undefined, "70%")}
    , {"procmem_high_watermark", t(percent(), undefined, "5%")}
    ];

fields("alarm") ->
    [ {"actions", t(hoconsc:array(atom()), undefined, [log, publish])}
    , {"size_limit", t(integer(), undefined, 1000)}
    , {"validity_period", t(duration_s(), undefined, "24h")}
    ];

fields(ExtraField) ->
    Mod = list_to_atom(ExtraField++"_schema"),
    Mod:fields(ExtraField).

mqtt_listener() ->
    base_listener() ++
    [ {"access_rules", t(hoconsc:array(string()))}
    , {"proxy_protocol", t(boolean(), undefined, false)}
    , {"proxy_protocol_timeout", t(duration())}
    ].

base_listener() ->
    [ {"bind", t(union(ip_port(), integer()))}
    , {"acceptors", t(integer(), undefined, 16)}
    , {"max_connections", maybe_infinity(integer(), infinity)}
    , {"rate_limit", ref("rate_limit")}
    ].

translations() -> ["kernel"].

translation("kernel") ->
    [ {"logger_level", fun tr_logger_level/1}
    , {"logger", fun tr_logger/1}].

tr_logger_level(Conf) -> conf_get("log.primary_level", Conf).

tr_logger(Conf) ->
    CharsLimit = case conf_get("log.chars_limit", Conf) of
                     -1 -> unlimited;
                     V -> V
                 end,
    SingleLine = conf_get("log.single_line", Conf),
    FmtName = conf_get("log.formatter", Conf),
    Formatter = formatter(FmtName, CharsLimit, SingleLine),
    BasicConf = #{
        sync_mode_qlen => conf_get("log.sync_mode_qlen", Conf),
        drop_mode_qlen => conf_get("log.drop_mode_qlen", Conf),
        flush_qlen => conf_get("log.flush_qlen", Conf),
        overload_kill_enable => conf_get("log.overload_kill.enable", Conf),
        overload_kill_qlen => conf_get("log.overload_kill.qlen", Conf),
        overload_kill_mem_size => conf_get("log.overload_kill.mem_size", Conf),
        overload_kill_restart_after => conf_get("log.overload_kill.restart_after", Conf),
        burst_limit_enable => conf_get("log.burst_limit.enable", Conf),
        burst_limit_max_count => conf_get("log.burst_limit.max_count", Conf),
        burst_limit_window_time => conf_get("log.burst_limit.window_time", Conf)
    },
    Filters = case conf_get("log.supervisor_reports", Conf) of
                  error -> [{drop_progress_reports, {fun logger_filters:progress/2, stop}}];
                  progress -> []
              end,
    %% For the default logger that outputs to console
    ConsoleHandler =
        case conf_get("log.console_handler.enable", Conf) of
            true ->
                [{handler, console, logger_std_h, #{
                    level => conf_get("log.console_handler.level", Conf),
                    config => BasicConf#{type => standard_io},
                    formatter => Formatter,
                    filters => Filters
                }}];
            false -> []
        end,
    %% For the file logger
    FileHandlers =
        [{handler, binary_to_atom(HandlerName, latin1), logger_disk_log_h, #{
                level => conf_get("level", SubConf),
                config => BasicConf#{
                    type => case conf_get("rotation.enable", SubConf) of
                                true -> wrap;
                                _ -> halt
                            end,
                    file => conf_get("file", SubConf),
                    max_no_files => conf_get("rotation.count", SubConf),
                    max_no_bytes => conf_get("max_size", SubConf)
                },
                formatter => Formatter,
                filters => Filters,
                filesync_repeat_interval => no_repeat
            }}
        || {HandlerName, SubConf} <- maps:to_list(conf_get("log.file_handlers", Conf))],

    [{handler, default, undefined}] ++ ConsoleHandler ++ FileHandlers.

%% helpers
formatter(json, CharsLimit, SingleLine) ->
    {emqx_logger_jsonfmt,
        #{chars_limit => CharsLimit,
            single_line => SingleLine
        }};
formatter(text, CharsLimit, SingleLine) ->
    {emqx_logger_textfmt,
        #{template =>
        [time," [",level,"] ",
            {clientid,
                [{peername,
                    [clientid,"@",peername," "],
                    [clientid, " "]}],
                [{peername,
                    [peername," "],
                    []}]},
            msg,"\n"],
            chars_limit => CharsLimit,
            single_line => SingleLine
        }}.

%% utils
-spec(conf_get(string() | [string()], hocon:config()) -> term()).
conf_get(Key, Conf) ->
    V = hocon_schema:deep_get(Key, Conf, value),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

conf_get(Key, Conf, Default) ->
    V = hocon_schema:deep_get(Key, Conf, value, Default),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% generate a ssl field.
%% ssl(#{"verify" => verify_peer}) will return:
%%  [ {"cacertfile", t(string(), undefined, undefined)}
%%  , {"certfile", t(string(), undefined, undefined)}
%%  , {"keyfile", t(string(), undefined, undefined)}
%%  , {"verify", t(union(verify_peer, verify_none), undefined, verify_peer)}
%%  , {"server_name_indication", undefined, undefined)}
%%  ...]
ssl(Defaults) ->
    D = fun (Field) -> maps:get(list_to_atom(Field), Defaults, undefined) end,
    [ {"enable", t(boolean(), undefined, D("enable"))}
    , {"cacertfile", t(string(), undefined, D("cacertfile"))}
    , {"certfile", t(string(), undefined, D("certfile"))}
    , {"keyfile", t(string(), undefined, D("keyfile"))}
    , {"verify", t(union(verify_peer, verify_none), undefined, D("verify"))}
    , {"fail_if_no_peer_cert", t(boolean(), undefined, D("fail_if_no_peer_cert"))}
    , {"secure_renegotiate", t(boolean(), undefined, D("secure_renegotiate"))}
    , {"reuse_sessions", t(boolean(), undefined, D("reuse_sessions"))}
    , {"honor_cipher_order", t(boolean(), undefined, D("honor_cipher_order"))}
    , {"handshake_timeout", t(duration(), undefined, D("handshake_timeout"))}
    , {"depth", t(integer(), undefined, D("depth"))}
    , {"password", hoconsc:t(string(), #{default => D("key_password"),
                                         sensitive => true
                                        })}
    , {"dhfile", t(string(), undefined, D("dhfile"))}
    , {"server_name_indication", t(union(disable, string()), undefined,
                                   D("server_name_indication"))}
    , {"versions", #{ type => list(atom())
                    , default => maps:get(versions, Defaults, default_tls_vsns())
                    , converter => fun (Vsns) -> [tls_vsn(V) || V <- Vsns] end
                    }}
    , {"ciphers", t(hoconsc:array(string()), undefined, D("ciphers"))}
    , {"user_lookup_fun", t(any(), undefined, {fun emqx_psk:lookup/3, <<>>})}
    ].

default_tls_vsns() -> [<<"tlsv1.3">>, <<"tlsv1.2">>, <<"tlsv1.1">>, <<"tlsv1">>].
tls_vsn(<<"tlsv1.3">>) -> 'tlsv1.3';
tls_vsn(<<"tlsv1.2">>) -> 'tlsv1.2';
tls_vsn(<<"tlsv1.1">>) -> 'tlsv1.1';
tls_vsn(<<"tlsv1">>) -> 'tlsv1'.

default_ciphers() -> [
    "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_128_CCM_SHA256", "TLS_AES_128_CCM_8_SHA256", "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384", "ECDHE-ECDSA-AES256-SHA384", "ECDHE-RSA-AES256-SHA384",
    "ECDHE-ECDSA-DES-CBC3-SHA", "ECDH-ECDSA-AES256-GCM-SHA384", "ECDH-RSA-AES256-GCM-SHA384",
    "ECDH-ECDSA-AES256-SHA384", "ECDH-RSA-AES256-SHA384", "DHE-DSS-AES256-GCM-SHA384",
    "DHE-DSS-AES256-SHA256", "AES256-GCM-SHA384", "AES256-SHA256",
    "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-AES128-SHA256", "ECDHE-RSA-AES128-SHA256", "ECDH-ECDSA-AES128-GCM-SHA256",
    "ECDH-RSA-AES128-GCM-SHA256", "ECDH-ECDSA-AES128-SHA256", "ECDH-RSA-AES128-SHA256",
    "DHE-DSS-AES128-GCM-SHA256", "DHE-DSS-AES128-SHA256", "AES128-GCM-SHA256", "AES128-SHA256",
    "ECDHE-ECDSA-AES256-SHA", "ECDHE-RSA-AES256-SHA", "DHE-DSS-AES256-SHA",
    "ECDH-ECDSA-AES256-SHA", "ECDH-RSA-AES256-SHA", "AES256-SHA", "ECDHE-ECDSA-AES128-SHA",
    "ECDHE-RSA-AES128-SHA", "DHE-DSS-AES128-SHA", "ECDH-ECDSA-AES128-SHA",
    "ECDH-RSA-AES128-SHA", "AES128-SHA"
    ] ++ psk_ciphers().

psk_ciphers() -> [
        "PSK-AES128-CBC-SHA", "PSK-AES256-CBC-SHA", "PSK-3DES-EDE-CBC-SHA", "PSK-RC4-SHA"
    ].

%% @private return a list of keys in a parent field
-spec(keys(string(), hocon:config()) -> [string()]).
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

-spec ceiling(float()) -> integer().
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

%% types

t(Type) -> hoconsc:t(Type).

t(Type, Mapping, Default) ->
    hoconsc:t(Type, #{mapping => Mapping, default => Default}).

t(Type, Mapping, Default, OverrideEnv) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     }).

t(Type, Mapping, Default, OverrideEnv, Validator) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     , validator => Validator
                     }).

ref(Field) ->
    fun (type) -> Field; (_) -> undefined end.

maybe_disabled(T) ->
    maybe_sth(disabled, T, disabled).

maybe_disabled(T, Default) ->
    maybe_sth(disabled, T, Default).

maybe_infinity(T) ->
    maybe_sth(infinity, T, infinity).

maybe_infinity(T, Default) ->
    maybe_sth(infinity, T, Default).

maybe_sth(What, Type, Default) ->
    t(union([What, Type]), undefined, Default).

to_duration(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_duration_s(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, ceiling(I / 1000)};
        _ -> {error, Str}
    end.

to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, ceiling(I)};
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

to_comma_separated_atoms(Str) ->
    {ok, lists:map(fun list_to_atom/1, string:tokens(Str, ", "))}.

to_bar_separated_list(Str) ->
    {ok, string:tokens(Str, "| ")}.

to_ip_port(Str) ->
    case string:tokens(Str, ":") of
        [Ip, Port] ->
            case inet:parse_address(Ip) of
                {ok, R} -> {ok, {R, list_to_integer(Port)}};
                _ -> {error, Str}
            end;
        _ -> {error, Str}
    end.

to_erl_cipher_suite(Str) ->
    case ssl:str_to_suite(Str) of
        {error, Reason} -> error({invalid_cipher, Reason});
        Cipher -> Cipher
    end.
