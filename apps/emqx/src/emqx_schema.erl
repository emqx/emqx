-module(emqx_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").

-type log_level() :: debug | info | notice | warning | error | critical | alert | emergency | all.
-type flag() :: true | false.
-type duration() :: integer().
-type duration_s() :: integer().
-type bytesize() :: integer().
-type percent() :: float().
-type file() :: string().
-type comma_separated_list() :: list().
-type bar_separated_list() :: list().
-type ip_port() :: tuple().

-typerefl_from_string({flag/0, emqx_schema, to_flag}).
-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({percent/0, emqx_schema, to_percent}).
-typerefl_from_string({comma_separated_list/0, emqx_schema, to_comma_separated_list}).
-typerefl_from_string({bar_separated_list/0, emqx_schema, to_bar_separated_list}).
-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).

% workaround: prevent being recognized as unused functions
-export([to_duration/1, to_duration_s/1, to_bytesize/1,
         to_flag/1, to_percent/1, to_comma_separated_list/1,
         to_bar_separated_list/1, to_ip_port/1]).

-behaviour(hocon_schema).

-reflect_type([ log_level/0, flag/0, duration/0, duration_s/0,
                bytesize/0, percent/0, file/0,
                comma_separated_list/0, bar_separated_list/0, ip_port/0]).

-export([structs/0, fields/1, translations/0, translation/1]).
-export([t/1, t/3, t/4, ref/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([ssl/2, tr_ssl/2, tr_password_hash/2]).

structs() -> ["cluster", "node", "rpc", "log", "lager",
              "acl", "mqtt", "zone", "listener", "module", "broker",
              "plugins", "sysmon", "os_mon", "vm_mon", "alarm", "telemetry"].

fields("cluster") ->
    [ {"name", t(atom(), "ekka.cluster_name", emqxcl)}
    , {"discovery", t(atom(), undefined, manual)}
    , {"autoclean", t(duration(), "ekka.cluster_autoclean", undefined)}
    , {"autoheal", t(flag(), "ekka.cluster_autoheal", false)}
    , {"static", ref("static")}
    , {"mcast", ref("mcast")}
    , {"proto_dist", t(union([inet_tcp, inet6_tcp, inet_tls]), "ekka.proto_dist", inet_tcp)}
    , {"dns", ref("dns")}
    , {"etcd", ref("etcd")}
    , {"k8s", ref("k8s")}
    , {"rlog", ref("rlog")}
    ];

fields("static") ->
    [ {"seeds", t(comma_separated_list())}];

fields("mcast") ->
    [ {"addr", t(string(), undefined, "239.192.0.1")}
    , {"ports", t(comma_separated_list(), undefined, "4369")}
    , {"iface", t(string(), undefined, "0.0.0.0")}
    , {"ttl", t(integer(), undefined, 255)}
    , {"loop", t(flag(), undefined, true)}
    , {"sndbuf", t(bytesize(), undefined, "16KB")}
    , {"recbuf", t(bytesize(), undefined, "16KB")}
    , {"buffer", t(bytesize(), undefined, "32KB")}
    ];

fields("dns") ->
    [ {"app", t(string())}];

fields("etcd") ->
    [ {"server", t(comma_separated_list())}
    , {"prefix", t(string())}
    , {"node_ttl", t(duration(), undefined, "1m")}
    , {"ssl", ref("etcd_ssl")}
    ];

fields("etcd_ssl") ->
    ssl(undefined, #{});

fields("k8s") ->
    [ {"apiserver", t(string())}
    , {"service_name", t(string())}
    , {"address_type", t(union([ip, dns, hostname]))}
    , {"app_name", t(string())}
    , {"namespace", t(string())}
    , {"suffix", t(string(), undefined, "")}
    ];

fields("rlog") ->
    [ {"backend", t(union([mnesia, rlog]), "ekka.db_backend", mnesia)}
    , {"role", t(union([core, replicant]), "ekka.node_role", core)}
    , {"core_nodes", t(comma_separated_list(), "ekka.core_nodes", [])}
    ];

fields("node") ->
    [ {"name", t(string(), "vm_args.-name", "emqx@127.0.0.1", "EMQX_NODE_NAME")}
    , {"ssl_dist_optfile", t(string(), "vm_args.-ssl_dist_optfile", undefined)}
    , {"cookie", t(string(), "vm_args.-setcookie", "emqxsecretcookie", "EMQX_NODE_COOKIE")}
    , {"data_dir", t(string(), "emqx.data_dir", undefined)}
    , {"heartbeat", t(flag(), undefined, false)}
    , {"async_threads", t(range(1, 1024), "vm_args.+A", undefined)}
    , {"process_limit", t(integer(), "vm_args.+P", undefined)}
    , {"max_ports", t(range(1024, 134217727), "vm_args.+Q", undefined, "EMQX_MAX_PORTS")}
    , {"dist_buffer_size", fun node__dist_buffer_size/1}
    , {"global_gc_interval", t(duration_s(), "emqx.global_gc_interval", undefined)}
    , {"fullsweep_after", t(non_neg_integer(),
                            "vm_args.-env ERL_FULLSWEEP_AFTER", 1000)}
    , {"max_ets_tables", t(integer(), "vm_args.+e", 256000)}
    , {"crash_dump", t(file(), "vm_args.-env ERL_CRASH_DUMP", undefined)}
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
    [ {"to", t(union([file, console, both]), undefined, file)}
    , {"level", t(log_level(), undefined, warning)}
    , {"time_offset", t(string(), undefined, "system")}
    , {"primary_log_level", t(log_level(), undefined, warning)}
    , {"dir", t(string(), undefined, "log")}
    , {"file", t(file(), undefined, "emqx.log")}
    , {"chars_limit", t(integer(), undefined, -1)}
    , {"supervisor_reports", t(union([error, progress]), undefined, error)}
    , {"max_depth", t(union([infinity, integer()]),
                      "kernel.error_logger_format_depth", 20)}
    , {"formatter", t(union([text, json]), undefined, text)}
    , {"single_line", t(boolean(), undefined, true)}
    , {"rotation", ref("rotation")}
    , {"size", t(union(bytesize(), infinity), undefined, infinity)}
    , {"sync_mode_qlen", t(integer(), undefined, 100)}
    , {"drop_mode_qlen", t(integer(), undefined, 3000)}
    , {"flush_qlen", t(integer(), undefined, 8000)}
    , {"overload_kill", t(flag(), undefined, true)}
    , {"overload_kill_mem_size", t(bytesize(), undefined, "30MB")}
    , {"overload_kill_qlen", t(integer(), undefined, 20000)}
    , {"overload_kill_restart_after", t(union(duration(), infinity), undefined, "5s")}
    , {"burst_limit", t(comma_separated_list(), undefined, "disabled")}
    , {"error_logger", t(atom(), "kernel.error_logger", silent)}
    , {"debug", ref("additional_log_file")}
    , {"info", ref("additional_log_file")}
    , {"notice", ref("additional_log_file")}
    , {"warning", ref("additional_log_file")}
    , {"error", ref("additional_log_file")}
    , {"critical", ref("additional_log_file")}
    , {"alert", ref("additional_log_file")}
    , {"emergency", ref("additional_log_file")}
    ];

fields("additional_log_file") ->
    [ {"file", t(string())}];

fields("rotation") ->
    [ {"enable", t(flag(), undefined, true)}
    , {"size", t(bytesize(), undefined, "10MB")}
    , {"count", t(integer(), undefined, 5)}
    ];

fields("lager") ->
    [ {"handlers", t(string(), "lager.handlers", "")}
    , {"crash_log", t(flag(), "lager.crash_log", false)}
    ];

fields("acl") ->
    [ {"allow_anonymous", t(boolean(), "emqx.allow_anonymous", false)}
    , {"acl_nomatch", t(union(allow, deny), "emqx.acl_nomatch", deny)}
    , {"acl_file", t(string(), "emqx.acl_file", undefined)}
    , {"enable_acl_cache", t(flag(), "emqx.enable_acl_cache", true)}
    , {"acl_cache_ttl", t(duration(), "emqx.acl_cache_ttl", "1m")}
    , {"acl_cache_max_size", t(range(1, inf), "emqx.acl_cache_max_size", 32)}
    , {"acl_deny_action", t(union(ignore, disconnect), "emqx.acl_deny_action", ignore)}
    , {"flapping_detect_policy", t(comma_separated_list(), undefined, "30,1m,5m")}
    ];

fields("mqtt") ->
    [ {"max_packet_size", t(bytesize(), "emqx.max_packet_size", "1MB", "EMQX_MAX_PACKET_SIZE")}
    , {"max_clientid_len", t(integer(), "emqx.max_clientid_len", 65535)}
    , {"max_topic_levels", t(integer(), "emqx.max_topic_levels", 0)}
    , {"max_qos_allowed", t(range(0, 2), "emqx.max_qos_allowed", 2)}
    , {"max_topic_alias", t(integer(), "emqx.max_topic_alias", 65535)}
    , {"retain_available", t(boolean(), "emqx.retain_available", true)}
    , {"wildcard_subscription", t(boolean(), "emqx.wildcard_subscription", true)}
    , {"shared_subscription", t(boolean(), "emqx.shared_subscription", true)}
    , {"ignore_loop_deliver", t(boolean(), "emqx.ignore_loop_deliver", true)}
    , {"strict_mode", t(boolean(), "emqx.strict_mode", false)}
    , {"response_information", t(string(), "emqx.response_information", undefined)}
    ];

fields("zone") ->
    [ {"$name", ref("zone_settings")}];

fields("zone_settings") ->
    [ {"idle_timeout", t(duration(), undefined, "15s")}
    , {"allow_anonymous", t(boolean())}
    , {"acl_nomatch", t(union(allow, deny))}
    , {"enable_acl", t(flag(), undefined, false)}
    , {"acl_deny_action", t(union(ignore, disconnect), undefined, ignore)}
    , {"enable_ban", t(flag(), undefined, false)}
    , {"enable_stats", t(flag(), undefined, false)}
    , {"max_packet_size", t(bytesize())}
    , {"max_clientid_len", t(integer())}
    , {"max_topic_levels", t(integer())}
    , {"max_qos_allowed", t(range(0, 2))}
    , {"max_topic_alias", t(integer())}
    , {"retain_available", t(boolean())}
    , {"wildcard_subscription", t(boolean())}
    , {"shared_subscription", t(boolean())}
    , {"server_keepalive", t(integer())}
    , {"keepalive_backoff", t(float(), undefined, 0.75)}
    , {"max_subscriptions", t(integer(), undefined, 0)}
    , {"upgrade_qos", t(flag(), undefined, false)}
    , {"max_inflight", t(range(0, 65535))}
    , {"retry_interval", t(duration_s(), undefined, "30s")}
    , {"max_awaiting_rel", t(duration(), undefined, 0)}
    , {"await_rel_timeout", t(duration_s(), undefined, "300s")}
    , {"ignore_loop_deliver", t(boolean())}
    , {"session_expiry_interval", t(duration_s(), undefined, "2h")}
    , {"max_mqueue_len", t(integer(), undefined, 1000)}
    , {"mqueue_priorities", t(comma_separated_list(), undefined, "none")}
    , {"mqueue_default_priority", t(union(highest, lowest), undefined, lowest)}
    , {"mqueue_store_qos0", t(boolean(), undefined, true)}
    , {"enable_flapping_detect", t(flag(), undefined, false)}
    , {"rate_limit", ref("rate_limit")}
    , {"conn_congestion", ref("conn_congestion")}
    , {"quota", ref("quota")}
    , {"force_gc_policy", t(bar_separated_list())}
    , {"force_shutdown_policy", t(bar_separated_list(), undefined, "default")}
    , {"mountpoint", t(string())}
    , {"use_username_as_clientid", t(boolean(), undefined, false)}
    , {"strict_mode", t(boolean(), undefined, false)}
    , {"response_information", t(string())}
    , {"bypass_auth_plugins", t(boolean(), undefined, false)}
    ];

fields("rate_limit") ->
    [ {"conn_messages_in", t(comma_separated_list())}
    , {"conn_bytes_in", t(comma_separated_list())}
    ];

fields("conn_congestion") ->
    [ {"alarm", t(flag(), undefined, false)}
    , {"min_alarm_sustain_duration", t(duration(), undefined, "1m")}
    ];

fields("quota") ->
    [ {"conn_messages_routing", t(comma_separated_list())}
    , {"overall_messages_routing", t(comma_separated_list())}
    ];

fields("listener") ->
    [ {"tcp", ref("tcp_listener")}
    , {"ssl", ref("ssl_listener")}
    , {"ws", ref("ws_listener")}
    , {"wss", ref("wss_listener")}
    , {"quic", ref("quic_listener")}
    ];

fields("tcp_listener") ->
    [ {"$name", ref("tcp_listener_settings")}];

fields("ssl_listener") ->
    [ {"$name", ref("ssl_listener_settings")}];

fields("ws_listener") ->
    [ {"$name", ref("ws_listener_settings")}];

fields("wss_listener") ->
    [ {"$name", ref("wss_listener_settings")}];

fields("quic_listener") ->
    [ {"$name", ref("quic_listener_settings")}];

fields("listener_settings") ->
    [ {"endpoint", t(union(ip_port(), integer()))}
    , {"acceptors", t(integer(), undefined, 8)}
    , {"max_connections", t(integer(), undefined, 1024)}
    , {"max_conn_rate", t(integer())}
    , {"active_n", t(integer(), undefined, 100)}
    , {"zone", t(string())}
    , {"rate_limit", t(comma_separated_list())}
    , {"access", ref("access")}
    , {"proxy_protocol", t(flag())}
    , {"proxy_protocol_timeout", t(duration())}
    , {"backlog", t(integer(), undefined, 1024)}
    , {"send_timeout", t(duration(), undefined, "15s")}
    , {"send_timeout_close", t(flag(), undefined, true)}
    , {"recbuf", t(bytesize())}
    , {"sndbuf", t(bytesize())}
    , {"buffer", t(bytesize())}
    , {"high_watermark", t(bytesize(), undefined, "1MB")}
    , {"tune_buffer", t(flag())}
    , {"nodelay", t(boolean())}
    , {"reuseaddr", t(boolean())}
    ];

fields("tcp_listener_settings") ->
    [ {"peer_cert_as_username", t(cn)}
    , {"peer_cert_as_clientid", t(cn)}
    ] ++ fields("listener_settings");

fields("ssl_listener_settings") ->
    [ {"peer_cert_as_username", t(union([cn, dn, crt, pem, md5]))}
    , {"peer_cert_as_clientid", t(union([cn, dn, crt, pem, md5]))}
    ] ++
    ssl(undefined, #{handshake_timeout => "15s"
                   , depth => 10
                   , reuse_sessions => true}) ++ fields("listener_settings");

fields("ws_listener_settings") ->
    [ {"mqtt_path", t(string(), undefined, "/mqtt")}
    , {"fail_if_no_subprotocol", t(boolean(), undefined, true)}
    , {"supported_subprotocols", t(string(), undefined, "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5")}
    , {"proxy_address_header", t(string(), undefined, "x-forwarded-for")}
    , {"proxy_port_header", t(string(), undefined, "x-forwarded-port")}
    , {"compress", t(boolean())}
    , {"deflate_opts", ref("deflate_opts")}
    , {"idle_timeout", t(duration())}
    , {"max_frame_size", t(integer())}
    , {"mqtt_piggyback", t(union(single, multiple), undefined, multiple)}
    , {"check_origin_enable", t(boolean(), undefined, false)}
    , {"allow_origin_absence", t(boolean(), undefined, true)}
    , {"check_origins", t(comma_separated_list())}
        % @fixme
    ] ++ lists:keydelete("high_watermark", 1, fields("tcp_listener_settings"));

fields("wss_listener_settings") ->
    % @fixme
    Ssl = ssl(undefined, #{depth => 10
                         , reuse_sessions => true}) ++ fields("listener_settings"),
    Settings = lists:ukeymerge(1, Ssl, fields("ws_listener_settings")),
    lists:keydelete("high_watermark", 1, Settings);

fields("quic_listener_settings") ->
    Unsupported = [ "max_connections"
                  , "max_conn_rate"
                  , "active_n"
                  , "access"
                  , "proxy_protocol"
                  , "proxy_protocol_timeout"
                  , "backlog"
                  , "send_timeout"
                  , "send_timeout_close"
                  , "recvbuf"
                  , "sndbuf"
                  , "buffer"
                  , "high_watermark"
                  , "tune_buffer"
                  , "nodelay"
                  , "reuseaddr"
                  ],
    lists:foldl(fun(K, Acc) ->
                        lists:keydelete(K, 1, Acc)
                end,
                [ {"certfile", t(string(), "emqx.certfile", undefined)}
                , {"keyfile", t(string(), "emqx.keyfile", undefined)}
                | fields("listener_settings")],
                Unsupported);

fields("access") ->
    [ {"$id", t(string(), undefined, undefined)}];

fields("deflate_opts") ->
    [ {"level", t(union([none, default, best_compression, best_speed]))}
    , {"mem_level", t(range(1, 9))}
    , {"strategy", t(union([default, filtered, huffman_only, rle]))}
    , {"server_context_takeover", t(union(takeover, no_takeover))}
    , {"client_context_takeover", t(union(takeover, no_takeover))}
    , {"server_max_window_bits", t(integer())}
    , {"client_max_window_bits", t(integer())}
    ];

fields("module") ->
    [ {"loaded_file", t(string(), "emqx.modules_loaded_file", undefined)}
    , {"presence", ref("presence")}
    , {"subscription", ref("subscription")}
    , {"rewrite", ref("rewrite")}
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
    [ {"etc_dir", t(string(), "emqx.plugins_etc_dir", undefined)}
    , {"loaded_file", t(string(), "emqx.plugins_loaded_file", undefined)}
    , {"expand_plugins_dir", t(string(), "emqx.expand_plugins_dir", undefined)}
    ];

fields("broker") ->
    [ {"sys_interval", t(duration(), "emqx.broker_sys_interval", "1m")}
    , {"sys_heartbeat", t(duration(), "emqx.broker_sys_heartbeat", "30s")}
    , {"enable_session_registry", t(flag(), "emqx.enable_session_registry", true)}
    , {"session_locking_strategy", t(union([local, leader, quorum, all]),
                                     "emqx.session_locking_strategy", quorum)}
    , {"shared_subscription_strategy", t(union(random, round_robin),
                                         "emqx.shared_subscription_strategy", round_robin)}
    , {"shared_dispatch_ack_enabled", t(boolean(), "emqx.shared_dispatch_ack_enabled", false)}
    , {"route_batch_clean", t(flag(), "emqx.route_batch_clean", true)}
    , {"perf", ref("perf")}
    ];

fields("perf") ->
    [ {"route_lock_type", t(union([key, tab, global]), "emqx.route_lock_type", key)}
    , {"trie_compaction", t(boolean(), "emqx.trie_compaction", true)}
    ];

fields("sysmon") ->
    [ {"long_gc", t(duration(), undefined, 0)}
    , {"long_schedule", t(duration(), undefined, 240)}
    , {"large_heap", t(bytesize(), undefined, "8MB")}
    , {"busy_dist_port", t(boolean(), undefined, true)}
    , {"busy_port", t(boolean(), undefined, false)}
    ];

fields("os_mon") ->
    [ {"cpu_check_interval", t(duration_s(), undefined, 60)}
    , {"cpu_high_watermark", t(percent(), undefined, "80%")}
    , {"cpu_low_watermark", t(percent(), undefined, "60%")}
    , {"mem_check_interval", t(duration_s(), undefined, 60)}
    , {"sysmem_high_watermark", t(percent(), undefined, "70%")}
    , {"procmem_high_watermark", t(percent(), undefined, "5%")}
    ];

fields("vm_mon") ->
    [ {"check_interval", t(duration_s(), undefined, 30)}
    , {"process_high_watermark", t(percent(), undefined, "80%")}
    , {"process_low_watermark", t(percent(), undefined, "60%")}
    ];

fields("alarm") ->
    [ {"actions", t(comma_separated_list(), undefined, "log,publish")}
    , {"size_limit", t(integer(), undefined, 1000)}
    , {"validity_period", t(duration_s(), undefined, "24h")}
    ];

fields("telemetry") ->
    [ {"enabled", t(boolean(), undefined, false)}
    , {"url", t(string(), undefined, "https://telemetry-emqx-io.bigpar.vercel.app/api/telemetry")}
    , {"report_interval", t(duration_s(), undefined, "7d")}
    ].


translations() -> ["ekka", "vm_args", "gen_rpc", "kernel", "emqx"].

translation("ekka") ->
    [ {"cluster_discovery", fun tr_cluster__discovery/1}];

translation("vm_args") ->
    [ {"+zdbbl", fun tr_zdbbl/1}
    , {"-heart", fun tr_heart/1}];

translation("gen_rpc") ->
    [ {"tcp_client_num", fun tr_tcp_client_num/1}
    , {"tcp_client_port", fun tr_tcp_client_port/1}];

translation("kernel") ->
    [ {"logger_level", fun tr_logger_level/1}
    , {"logger", fun tr_logger/1}];

translation("emqx") ->
    [ {"flapping_detect_policy", fun tr_flapping_detect_policy/1}
    , {"zones", fun tr_zones/1}
    , {"listeners", fun tr_listeners/1}
    , {"modules", fun tr_modules/1}
    , {"sysmon", fun tr_sysmon/1}
    , {"os_mon", fun tr_os_mon/1}
    , {"vm_mon", fun tr_vm_mon/1}
    , {"alarm", fun tr_alarm/1}
    , {"telemetry", fun tr_telemetry/1}
    ].

tr_cluster__discovery(Conf) ->
    Strategy = conf_get("cluster.discovery", Conf),
    {Strategy, filter(options(Strategy, Conf))}.

tr_heart(Conf) ->
    case conf_get("node.heartbeat", Conf) of
        true  -> "";
        "on" -> "";
        _ -> undefined
    end.

%% @doc http://www.erlang.org/doc/man/erl.html#%2bzdbbl
node__dist_buffer_size(type) -> bytesize();
node__dist_buffer_size(validator) ->
    fun(ZDBBL) ->
        case ZDBBL >= 1024 andalso ZDBBL =< 2147482624 of
            true ->
                ok;
            false ->
                {error, "must be between 1KB and 2097151KB"}
        end
    end;
node__dist_buffer_size(_) -> undefined.

tr_zdbbl(Conf) ->
    case conf_get("node.dist_buffer_size", Conf) of
        undefined -> undefined;
        X when is_integer(X) -> ceiling(X / 1024); %% Bytes to Kilobytes;
        _ -> undefined
    end.

%% Force client to use server listening port, because we do no provide
%% per-node listening port manual mapping from configs.
%% i.e. all nodes in the cluster should agree to the same
%% listening port number.
tr_tcp_client_num(Conf) ->
    case conf_get("rpc.tcp_client_num", Conf) of
        0 -> max(1, erlang:system_info(schedulers) div 2);
        V -> V
    end.

tr_tcp_client_port(Conf) ->
    conf_get("rpc.tcp_server_port", Conf).

tr_logger_level(Conf) -> conf_get("log.level", Conf).

tr_logger(Conf) ->
    LogTo = conf_get("log.to", Conf),
    LogLevel = conf_get("log.level", Conf),
    LogType = case conf_get("log.rotation.enable", Conf) of
                  true -> wrap;
                  _ -> halt
              end,
    CharsLimit = case conf_get("log.chars_limit", Conf) of
                     -1 -> unlimited;
                     V -> V
                 end,
    SingleLine = conf_get("log.single_line", Conf),
    FmtName = conf_get("log.formatter", Conf),
    Formatter = formatter(FmtName, CharsLimit, SingleLine),
    BurstLimit = conf_get("log.burst_limit", Conf),
    {BustLimitOn, {MaxBurstCount, TimeWindow}} = burst_limit(BurstLimit),
    FileConf = fun (Filename) ->
        BasicConf =
            #{type => LogType,
                file => filename:join(conf_get("log.dir", Conf), Filename),
                max_no_files => conf_get("log.rotation.count", Conf),
                sync_mode_qlen => conf_get("log.sync_mode_qlen", Conf),
                drop_mode_qlen => conf_get("log.drop_mode_qlen", Conf),
                flush_qlen => conf_get("log.flush_qlen", Conf),
                overload_kill_enable => conf_get("log.overload_kill", Conf),
                overload_kill_qlen => conf_get("log.overload_kill_qlen", Conf),
                overload_kill_mem_size => conf_get("log.overload_kill_mem_size", Conf),
                overload_kill_restart_after => conf_get("log.overload_kill_restart_after", Conf),
                burst_limit_enable => BustLimitOn,
                burst_limit_max_count => MaxBurstCount,
                burst_limit_window_time => TimeWindow
            },
        MaxNoBytes = case LogType of
                         wrap -> conf_get("log.rotation.size", Conf);
                         halt -> conf_get("log.size", Conf)
                     end,
        BasicConf#{max_no_bytes => MaxNoBytes} end,

    Filters = case conf_get("log.supervisor_reports", Conf) of
                  error -> [{drop_progress_reports, {fun logger_filters:progress/2, stop}}];
                  progress -> []
              end,

    %% For the default logger that outputs to console
    DefaultHandler =
        if LogTo =:= console orelse LogTo =:= both ->
            [{handler, console, logger_std_h,
                #{level => LogLevel,
                    config => #{type => standard_io},
                    formatter => Formatter,
                    filters => Filters
                }
            }];
            true ->
                [{handler, default, undefined}]
        end,

    %% For the file logger
    FileHandler =
        if LogTo =:= file orelse LogTo =:= both ->
            [{handler, file, logger_disk_log_h,
                #{level => LogLevel,
                    config => FileConf(conf_get("log.file", Conf)),
                    formatter => Formatter,
                    filesync_repeat_interval => no_repeat,
                    filters => Filters
                }}];
            true -> []
        end,

    AdditionalLogFiles = additional_log_files(Conf),
    AdditionalHandlers =
        [{handler, list_to_atom("file_for_"++Level), logger_disk_log_h,
            #{level => list_to_atom(Level),
                config => FileConf(Filename),
                formatter => Formatter,
                filesync_repeat_interval => no_repeat}}
            || {Level, Filename} <- AdditionalLogFiles],

    DefaultHandler ++ FileHandler ++ AdditionalHandlers.

tr_flapping_detect_policy(Conf) ->
    [Threshold, Duration, Interval] = conf_get("acl.flapping_detect_policy", Conf),
    ParseDuration = fun(S, F) ->
        case F(S) of
            {ok, I} -> I;
            {error, Reason} -> error({duration, Reason})
        end end,
    #{threshold => list_to_integer(Threshold),
        duration  => ParseDuration(Duration, fun to_duration/1),
        banned_interval => ParseDuration(Interval, fun to_duration_s/1)
    }.

tr_zones(Conf) ->
    Names = lists:usort(keys("zone", Conf)),
        lists:foldl(
            fun(Name, Zones) ->
                Zone = keys("zone." ++ Name, Conf),
                Mapped = lists:flatten([map_zones(K, conf_get(["zone", Name, K], Conf)) || K <- Zone]),
                [{list_to_atom(Name), lists:filter(fun ({K, []}) when K =:= ratelimit; K =:= quota -> false;
                                                       ({_, undefined}) -> false;
                                                       (_) -> true end, Mapped)} | Zones]
            end, [], Names).

tr_listeners(Conf) ->
    Atom = fun(undefined) -> undefined;
              (B) when is_binary(B)-> binary_to_atom(B);
              (S) when is_list(S) -> list_to_atom(S) end,

    Access = fun(S) ->
        [A, CIDR] = string:tokens(S, " "),
        {list_to_atom(A), case CIDR of "all" -> all; _ -> CIDR end}
             end,

    AccOpts = fun(Prefix) ->
        case keys(Prefix ++ ".access", Conf) of
            [] -> [];
            Ids ->
                [{access_rules, [Access(conf_get(Prefix ++ ".access." ++ Id, Conf)) || Id <- Ids]}]
        end end,

    RateLimit = fun(undefined) ->
        undefined;
        ([L, D]) ->
            Limit = case to_bytesize(L) of
                        {ok, I0} -> I0;
                        {error, R0} -> error({bytesize, R0})
                    end,
            Duration = case to_duration_s(D) of
                           {ok, I1} -> I1;
                           {error, R1} -> error({duration, R1})
                       end,
            {Limit, Duration}
                end,

    CheckOrigin = fun(S) -> [ list_to_binary(string:trim(O)) || O <- S] end,

    WsOpts = fun(Prefix) ->
        case conf_get(Prefix ++ ".check_origins", Conf) of
            undefined -> undefined;
            Rules -> lists:flatten(CheckOrigin(Rules))
        end
             end,

    LisOpts = fun(Prefix) ->
        filter([{acceptors, conf_get(Prefix ++ ".acceptors", Conf)},
            {mqtt_path, conf_get(Prefix ++ ".mqtt_path", Conf)},
            {max_connections, conf_get(Prefix ++ ".max_connections", Conf)},
            {max_conn_rate, conf_get(Prefix ++ ".max_conn_rate", Conf)},
            {active_n, conf_get(Prefix ++ ".active_n", Conf)},
            {tune_buffer, conf_get(Prefix ++ ".tune_buffer", Conf)},
            {zone, Atom(conf_get(Prefix ++ ".zone", Conf))},
            {rate_limit, RateLimit(conf_get(Prefix ++ ".rate_limit", Conf))},
            {proxy_protocol, conf_get(Prefix ++ ".proxy_protocol", Conf)},
            {proxy_address_header, list_to_binary(string:lowercase(conf_get(Prefix ++ ".proxy_address_header", Conf, <<"">>)))},
            {proxy_port_header, list_to_binary(string:lowercase(conf_get(Prefix ++ ".proxy_port_header", Conf, <<"">>)))},
            {proxy_protocol_timeout, conf_get(Prefix ++ ".proxy_protocol_timeout", Conf)},
            {fail_if_no_subprotocol, conf_get(Prefix ++ ".fail_if_no_subprotocol", Conf)},
            {supported_subprotocols, string:tokens(conf_get(Prefix ++ ".supported_subprotocols", Conf, ""), ", ")},
            {peer_cert_as_username, conf_get(Prefix ++ ".peer_cert_as_username", Conf)},
            {peer_cert_as_clientid, conf_get(Prefix ++ ".peer_cert_as_clientid", Conf)},
            {compress, conf_get(Prefix ++ ".compress", Conf)},
            {idle_timeout, conf_get(Prefix ++ ".idle_timeout", Conf)},
            {max_frame_size, conf_get(Prefix ++ ".max_frame_size", Conf)},
            {mqtt_piggyback, conf_get(Prefix ++ ".mqtt_piggyback", Conf)},
            {check_origin_enable, conf_get(Prefix ++ ".check_origin_enable", Conf)},
            {allow_origin_absence, conf_get(Prefix ++ ".allow_origin_absence", Conf)},
            {check_origins, WsOpts(Prefix)} | AccOpts(Prefix)])
              end,
    DeflateOpts = fun(Prefix) ->
        filter([{level, conf_get(Prefix ++ ".deflate_opts.level", Conf)},
                {mem_level, conf_get(Prefix ++ ".deflate_opts.mem_level", Conf)},
                {strategy, conf_get(Prefix ++ ".deflate_opts.strategy", Conf)},
                {server_context_takeover, conf_get(Prefix ++ ".deflate_opts.server_context_takeover", Conf)},
                {client_context_takeover, conf_get(Prefix ++ ".deflate_opts.client_context_takeover", Conf)},
                {server_max_windows_bits, conf_get(Prefix ++ ".deflate_opts.server_max_window_bits", Conf)},
                {client_max_windows_bits, conf_get(Prefix ++ ".deflate_opts.client_max_window_bits", Conf)}])
                  end,
    TcpOpts = fun(Prefix) ->
        filter([{backlog, conf_get(Prefix ++ ".backlog", Conf)},
                {send_timeout, conf_get(Prefix ++ ".send_timeout", Conf)},
                {send_timeout_close, conf_get(Prefix ++ ".send_timeout_close", Conf)},
                {recbuf,  conf_get(Prefix ++ ".recbuf", Conf)},
                {sndbuf,  conf_get(Prefix ++ ".sndbuf", Conf)},
                {buffer,  conf_get(Prefix ++ ".buffer", Conf)},
                {high_watermark,  conf_get(Prefix ++ ".high_watermark", Conf)},
                {nodelay, conf_get(Prefix ++ ".nodelay", Conf, true)},
                {reuseaddr, conf_get(Prefix ++ ".reuseaddr", Conf)}])
              end,

    SslOpts = fun(Prefix) ->
        Opts = tr_ssl(Prefix, Conf),
        case lists:keyfind(ciphers, 1, Opts) of
            false ->
                error(Prefix ++ ".ciphers or " ++ Prefix ++ ".psk_ciphers is absent");
            _ ->
                Opts
        end end,

    TcpListeners = fun(Type, Name) ->
        Prefix = string:join(["listener", Type, Name], "."),
        ListenOnN = case conf_get(Prefix ++ ".endpoint", Conf) of
                        undefined -> [];
                        ListenOn  -> ListenOn
                    end,
        [#{ proto => Atom(Type)
            , name => Name
            , listen_on => ListenOnN
            , opts => [ {deflate_options, DeflateOpts(Prefix)}
                , {tcp_options, TcpOpts(Prefix)}
                | LisOpts(Prefix)
            ]
        }
        ]
                   end,
    SslListeners = fun(Type, Name) ->
        Prefix = string:join(["listener", Type, Name], "."),
        case conf_get(Prefix ++ ".endpoint", Conf) of
            undefined ->
                [];
            ListenOn ->
                [#{ proto => Atom(Type)
                    , name => Name
                    , listen_on => ListenOn
                    , opts => [ {deflate_options, DeflateOpts(Prefix)}
                        , {tcp_options, TcpOpts(Prefix)}
                        , {ssl_options, SslOpts(Prefix)}
                        | LisOpts(Prefix)
                    ]
                }
                ]
        end end,


    lists:flatten([TcpListeners("tcp", Name) || Name <- keys("listener.tcp", Conf)]
               ++ [TcpListeners("ws", Name) || Name <- keys("listener.ws", Conf)]
               ++ [SslListeners("ssl", Name) || Name <- keys("listener.ssl", Conf)]
               ++ [SslListeners("wss", Name) || Name <- keys("listener.wss", Conf)]
               ++ [SslListeners("quic", Name) || Name <- keys("listener.quic", Conf)]
                 ).

tr_modules(Conf) ->
    Subscriptions = fun() ->
        List = keys("module.subscription", Conf),
        TopicList = [{N, conf_get(["module", "subscription", N, "topic"], Conf)}|| N <- List],
        [{list_to_binary(T), #{ qos => conf_get("module.subscription." ++ N ++ ".qos", Conf, 0),
                                nl  => conf_get("module.subscription." ++ N ++ ".nl", Conf, 0),
                                rap => conf_get("module.subscription." ++ N ++ ".rap", Conf, 0),
                                rh  => conf_get("module.subscription." ++ N ++ ".rh", Conf, 0)
        }} || {N, T} <- TopicList]
                    end,
    Rewrites = fun() ->
        Rules = keys("module.rewrite.rule", Conf),
        PubRules = keys("module.rewrite.pub_rule", Conf),
        SubRules = keys("module.rewrite.sub_rule", Conf),
        TotalRules =
            [ {["module", "rewrite", "pub", "rule", R], conf_get(["module.rewrite.rule", R], Conf)} || R <- Rules] ++
            [ {["module", "rewrite", "pub", "rule", R], conf_get(["module.rewrite.pub_rule", R], Conf)} || R <- PubRules] ++
            [ {["module", "rewrite", "sub", "rule", R], conf_get(["module.rewrite.rule", R], Conf)} || R <- Rules] ++
            [ {["module", "rewrite", "sub", "rule", R], conf_get(["module.rewrite.sub_rule", R], Conf)} || R <- SubRules],
        lists:map(fun({[_, "rewrite", PubOrSub, "rule", _], Rule}) ->
            [Topic, Re, Dest] = string:tokens(Rule, " "),
            {rewrite, list_to_atom(PubOrSub), list_to_binary(Topic), list_to_binary(Re), list_to_binary(Dest)}
                  end, TotalRules)
               end,
    lists:append([
        [{emqx_mod_presence, [{qos, conf_get("module.presence.qos", Conf, 1)}]}],
        [{emqx_mod_subscription, Subscriptions()}],
        [{emqx_mod_rewrite, Rewrites()}],
        [{emqx_mod_topic_metrics, []}],
        [{emqx_mod_delayed, []}],
        [{emqx_mod_acl_internal, [{acl_file, conf_get("acl.acl_file", Conf)}]}]
    ]).

tr_sysmon(Conf) ->
    Keys = maps:to_list(conf_get("sysmon", Conf, #{})),
    [{binary_to_atom(K), maps:get(value, V)} || {K, V} <- Keys].

tr_os_mon(Conf) ->
    [{cpu_check_interval, conf_get("os_mon.cpu_check_interval", Conf)}
    , {cpu_high_watermark, conf_get("os_mon.cpu_high_watermark", Conf) * 100}
    , {cpu_low_watermark, conf_get("os_mon.cpu_low_watermark", Conf) * 100}
    , {mem_check_interval, conf_get("os_mon.mem_check_interval", Conf)}
    , {sysmem_high_watermark, conf_get("os_mon.sysmem_high_watermark", Conf) * 100}
    , {procmem_high_watermark, conf_get("os_mon.procmem_high_watermark", Conf) * 100}
    ].

tr_vm_mon(Conf) ->
    [ {check_interval, conf_get("vm_mon.check_interval", Conf)}
    , {process_high_watermark, conf_get("vm_mon.process_high_watermark", Conf) * 100}
    , {process_low_watermark, conf_get("vm_mon.process_low_watermark", Conf) * 100}
    ].

tr_alarm(Conf) ->
    [ {actions, [list_to_atom(Action) || Action <- conf_get("alarm.actions", Conf)]}
    , {size_limit, conf_get("alarm.size_limit", Conf)}
    , {validity_period, conf_get("alarm.validity_period", Conf)}
    ].

tr_telemetry(Conf) ->
    [ {enabled,         conf_get("telemetry.enabled", Conf)}
    , {url,             conf_get("telemetry.url", Conf)}
    , {report_interval, conf_get("telemetry.report_interval", Conf)}
    ].

%% helpers

options(static, Conf) ->
    [{seeds, [list_to_atom(S) || S <- conf_get("cluster.static.seeds", Conf, "")]}];
options(mcast, Conf) ->
    {ok, Addr} = inet:parse_address(conf_get("cluster.mcast.addr", Conf)),
    {ok, Iface} = inet:parse_address(conf_get("cluster.mcast.iface", Conf)),
    Ports = [list_to_integer(S) || S <- conf_get("cluster.mcast.ports", Conf)],
    [{addr, Addr}, {ports, Ports}, {iface, Iface},
     {ttl, conf_get("cluster.mcast.ttl", Conf, 1)},
     {loop, conf_get("cluster.mcast.loop", Conf, true)}];
options(dns, Conf) ->
    [{name, conf_get("cluster.dns.name", Conf)},
     {app, conf_get("cluster.dns.app", Conf)}];
options(etcd, Conf) ->
    Namespace = "cluster.etcd.ssl",
    SslOpts = fun(C) ->
        Options = keys(Namespace, C),
        lists:map(fun(Key) -> {list_to_atom(Key), conf_get([Namespace, Key], Conf)} end, Options) end,
    [{server, conf_get("cluster.etcd.server", Conf)},
     {prefix, conf_get("cluster.etcd.prefix", Conf, "emqxcl")},
     {node_ttl, conf_get("cluster.etcd.node_ttl", Conf, 60)},
     {ssl_options, filter(SslOpts(Conf))}];
options(k8s, Conf) ->
    [{apiserver, conf_get("cluster.k8s.apiserver", Conf)},
     {service_name, conf_get("cluster.k8s.service_name", Conf)},
     {address_type, conf_get("cluster.k8s.address_type", Conf, ip)},
     {app_name, conf_get("cluster.k8s.app_name", Conf)},
     {namespace, conf_get("cluster.k8s.namespace", Conf)},
     {suffix, conf_get("cluster.k8s.suffix", Conf, "")}];
options(manual, _Conf) ->
    [].

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

burst_limit(["disabled"]) ->
    {false, {20000, 1000}};
burst_limit([Count, Window]) ->
    {true, {list_to_integer(Count),
        case to_duration(Window) of
            {ok, I} -> I;
            {error, R} -> error({duration, R})
        end}}.

%% For creating additional log files for specific log levels.
additional_log_files(Conf) ->
    LogLevel = ["debug", "info", "notice", "warning",
                "error", "critical", "alert", "emergency"],
    additional_log_files(Conf, LogLevel, []).

additional_log_files(_Conf, [], Acc) ->
    Acc;
additional_log_files(Conf, [L | More], Acc) ->
    case conf_get(["log", L, "file"], Conf) of
        undefined -> additional_log_files(Conf, More, Acc);
        F -> additional_log_files(Conf, More, [{L, F} | Acc])
    end.

rate_limit_byte_dur([L, D]) ->
    Limit = case to_bytesize(L) of
                {ok, I0} -> I0;
                {error, R0} -> error({bytesize, R0})
            end,
    Duration = case to_duration_s(D) of
                   {ok, I1} -> I1;
                   {error, R1} -> error({duration, R1})
               end,
    {Limit, Duration}.

rate_limit_num_dur([L, D]) ->
    Limit = case string:to_integer(L) of
                {Int, []} when is_integer(Int) -> Int;
                _ -> error("failed to parse bytesize string")
            end,
    Duration = case to_duration_s(D) of
                   {ok, I} -> I;
                   {error, Reason} -> error(Reason)
               end,
    {Limit, Duration}.

map_zones(_, undefined) ->
    {undefined, undefined};
map_zones("force_gc_policy", [Count, Bytes]) ->
    GcPolicy = case to_bytesize(Bytes) of
                   {error, Reason} ->
                       error({bytesize, Reason});
                   {ok, Bytes1} ->
                       #{bytes => Bytes1,
                           count => list_to_integer(Count)}
               end,
    {force_gc_policy, GcPolicy};
map_zones("force_shutdown_policy", ["default"]) ->
    WordSize = erlang:system_info(wordsize),
    {DefaultLen, DefaultSize} =
        case WordSize of
            8 -> % arch_64
                {10000, hocon_postprocess:bytesize("64MB")};
            4 -> % arch_32
                {1000, hocon_postprocess:bytesize("32MB")}
        end,
    {force_shutdown_policy, #{message_queue_len => DefaultLen,
        max_heap_size => DefaultSize div WordSize
    }};
map_zones("force_shutdown_policy", [Len, Siz]) ->
    WordSize = erlang:system_info(wordsize),
    MaxSiz = case WordSize of
                 8 -> % arch_64
                     (1 bsl 59) - 1;
                 4 -> % arch_32
                     (1 bsl 27) - 1
             end,
    ShutdownPolicy =
        case to_bytesize(Siz) of
            {error, Reason} ->
                error(Reason);
            {ok, Siz1} when Siz1 > MaxSiz ->
                error(io_lib:format("force_shutdown_policy: heap-size ~s is too large", [Siz]));
            {ok, Siz1} ->
                #{message_queue_len => list_to_integer(Len),
                    max_heap_size => Siz1 div WordSize}
        end,
    {force_shutdown_policy, ShutdownPolicy};
map_zones("mqueue_priorities", Val) ->
    case Val of
        ["none"] -> {mqueue_priorities, none}; % NO_PRIORITY_TABLE
        _ ->
            MqueuePriorities = lists:foldl(fun(T, Acc) ->
                %% NOTE: space in "= " is intended
                [Topic, Prio] = string:tokens(T, "= "),
                P = list_to_integer(Prio),
                (P < 0 orelse P > 255) andalso error({bad_priority, Topic, Prio}),
                maps:put(iolist_to_binary(Topic), P, Acc)
                                           end, #{}, Val),
            {mqueue_priorities, MqueuePriorities}
    end;
map_zones("mountpoint", Val) ->
    {mountpoint, iolist_to_binary(Val)};
map_zones("response_information", Val) ->
    {response_information, iolist_to_binary(Val)};
map_zones("rate_limit", Conf) ->
    Messages = case conf_get("conn_messages_in", #{value => Conf}) of
        undefined ->
            [];
        M ->
            [{conn_messages_in, rate_limit_num_dur(M)}]
    end,
    Bytes = case conf_get("conn_bytes_in", #{value => Conf}) of
        undefined ->
            [];
        B ->
            [{conn_bytes_in, rate_limit_byte_dur(B)}]
            end,
    {ratelimit, Messages ++ Bytes};
map_zones("conn_congestion", Conf) ->
    Alarm = case conf_get("alarm", #{value => Conf}) of
               undefined ->
                   [];
               A ->
                   [{conn_congestion_alarm_enabled, A}]
           end,
    MinAlarm = case conf_get("min_alarm_sustain_duration", #{value => Conf}) of
                undefined ->
                    [];
                M ->
                    [{conn_congestion_min_alarm_sustain_duration, M}]
            end,
    Alarm ++ MinAlarm;
map_zones("quota", Conf) ->
    Conn = case conf_get("conn_messages_routing", #{value => Conf}) of
                   undefined ->
                       [];
                   C ->
                       [{conn_messages_routing, rate_limit_num_dur(C)}]
               end,
    Overall = case conf_get("overall_messages_routing", #{value => Conf}) of
                undefined ->
                    [];
                O ->
                    [{overall_messages_routing, rate_limit_num_dur(O)}]
            end,
    {quota, Conn ++ Overall};
map_zones(Opt, Val) ->
    {list_to_atom(Opt), Val}.


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
%% ssl("emqx", #{"verify" => verify_peer}) will return
%% [ {"cacertfile", t(string(), "emqx.cacertfile", undefined)}
%% , {"certfile", t(string(), "emqx.certfile", undefined)}
%% , {"keyfile", t(string(), "emqx.keyfile", undefined)}
%% , {"verify", t(union(verify_peer, verify_none), "emqx.verify", verify_peer)}
%% , {"server_name_indication", "emqx.server_name_indication", undefined)}
%% ...
ssl(Mapping, Defaults) ->
    M = fun (Field) ->
        case (Mapping) of
            undefined -> undefined;
            _ -> Mapping ++ "." ++ Field
        end end,
    D = fun (Field) -> maps:get(list_to_atom(Field), Defaults, undefined) end,
    [ {"enable", t(flag(), M("enable"), D("enable"))}
    , {"cacertfile", t(string(), M("cacertfile"), D("cacertfile"))}
    , {"certfile", t(string(), M("certfile"), D("certfile"))}
    , {"keyfile", t(string(), M("keyfile"), D("keyfile"))}
    , {"verify", t(union(verify_peer, verify_none), M("verify"), D("verify"))}
    , {"fail_if_no_peer_cert", t(boolean(), M("fail_if_no_peer_cert"), D("fail_if_no_peer_cert"))}
    , {"secure_renegotiate", t(flag(), M("secure_renegotiate"), D("secure_renegotiate"))}
    , {"reuse_sessions", t(flag(), M("reuse_sessions"), D("reuse_sessions"))}
    , {"honor_cipher_order", t(flag(), M("honor_cipher_order"), D("honor_cipher_order"))}
    , {"handshake_timeout", t(duration(), M("handshake_timeout"), D("handshake_timeout"))}
    , {"depth", t(integer(), M("depth"), D("depth"))}
    , {"password", t(string(), M("key_password"), D("key_password"))}
    , {"dhfile", t(string(), M("dhfile"), D("dhfile"))}
    , {"server_name_indication", t(union(disable, string()), M("server_name_indication"),
                                   D("server_name_indication"))}
    , {"tls_versions", t(comma_separated_list(), M("tls_versions"), D("tls_versions"))}
    , {"ciphers", t(comma_separated_list(), M("ciphers"), D("ciphers"))}
    , {"psk_ciphers", t(comma_separated_list(), M("ciphers"), D("ciphers"))}].

tr_ssl(Field, Conf) ->
    Versions = case conf_get([Field, "tls_versions"], Conf) of
                   undefined -> undefined;
                   Vs -> [list_to_existing_atom(V) || V <- Vs]
               end,
    TLSCiphers = conf_get([Field, "ciphers"], Conf),
    PSKCiphers = conf_get([Field, "psk_ciphers"], Conf),
    Ciphers = ciphers(TLSCiphers, PSKCiphers, Field),
    case emqx_schema:conf_get([Field, "enable"], Conf) of
        X when X =:= true orelse X =:= undefined ->
            filter([{versions, Versions},
                    {ciphers, Ciphers},
                    {user_lookup_fun, user_lookup_fun(PSKCiphers)},
                    {handshake_timeout, conf_get([Field, "handshake_timeout"], Conf)},
                    {depth, conf_get([Field, "depth"], Conf)},
                    {password, conf_get([Field, "key_password"], Conf)},
                    {dhfile, conf_get([Field, "dhfile"], Conf)},
                    {keyfile, emqx_schema:conf_get([Field, "keyfile"], Conf)},
                    {certfile, emqx_schema:conf_get([Field, "certfile"], Conf)},
                    {cacertfile, emqx_schema:conf_get([Field, "cacertfile"], Conf)},
                    {verify, emqx_schema:conf_get([Field, "verify"], Conf)},
                    {fail_if_no_peer_cert, conf_get([Field, "fail_if_no_peer_cert"], Conf)},
                    {secure_renegotiate, conf_get([Field, "secure_renegotiate"], Conf)},
                    {reuse_sessions, conf_get([Field, "reuse_sessions"], Conf)},
                    {honor_cipher_order, conf_get([Field, "honor_cipher_order"], Conf)},
                    {server_name_indication, emqx_schema:conf_get([Field, "server_name_indication"], Conf)}
                   ]);
        _ ->
            []
    end.

map_psk_ciphers(PSKCiphers) ->
    lists:map(
        fun("PSK-AES128-CBC-SHA") -> {psk, aes_128_cbc, sha};
           ("PSK-AES256-CBC-SHA") -> {psk, aes_256_cbc, sha};
           ("PSK-3DES-EDE-CBC-SHA") -> {psk, '3des_ede_cbc', sha};
           ("PSK-RC4-SHA") -> {psk, rc4_128, sha}
        end, PSKCiphers).

ciphers(undefined, undefined, _) ->
    undefined;
ciphers(TLSCiphers, undefined, _) ->
    TLSCiphers;
ciphers(undefined, PSKCiphers, _) ->
    map_psk_ciphers(PSKCiphers);
ciphers(_, _, Field) ->
    error(Field ++ ".ciphers and " ++ Field ++ ".psk_ciphers cannot be configured at the same time").

user_lookup_fun(undefined) ->
    undefined;
user_lookup_fun(_PSKCiphers) ->
    {fun emqx_psk:lookup/3, <<>>}.

tr_password_hash(Field, Conf) ->
    case emqx_schema:conf_get([Field, "password_hash"], Conf) of
        [Hash]           -> list_to_atom(Hash);
        [Prefix, Suffix] -> {list_to_atom(Prefix), list_to_atom(Suffix)};
        [Hash, MacFun, Iterations, Dklen] -> {list_to_atom(Hash), list_to_atom(MacFun),
                                              list_to_integer(Iterations), list_to_integer(Dklen)};
        _                -> plain
    end.


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

ref(Field) ->
    fun (type) -> Field; (_) -> undefined end.

to_flag(Str) ->
    {ok, hocon_postprocess:onoff(Str)}.

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

to_bytesize(Str) ->
    case hocon_postprocess:bytesize(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_percent(Str) ->
    {ok, hocon_postprocess:percent(Str)}.

to_comma_separated_list(Str) ->
    {ok, string:tokens(Str, ", ")}.

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
