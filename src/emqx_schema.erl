-module(emqx_schema).

-include_lib("typerefl/include/types.hrl").

-type rpc_mode() :: sync | async.
-type proto_dist() :: inet_tcp | inet6_tcp | inet_tls.
-type k8s_address_type() :: ip | dns | hostname.
-type port_discovery() :: manual | stateless.
-type acl_nomatch() :: allow | deny.
-type acl_deny_action() :: ignore | disconnect.
-type mqueue_default_priority() :: highest | lowest.
-type endpoint() :: integer() | string().
-type listener_peer_cert_tcp() :: cn | tmp. % @todo fix
-type listener_peer_cert_ssl() :: cn | dn | crt | pem | md5.
-type mqtt_piggyback() :: single | multiple.
-type deflate_opts_level() :: none | default | best_compression | best_speed.
-type deflate_opts_strategy() :: default | filtered | huffman_only | rle.
-type context_takeover() :: takeover | no_takeover.
-type verify() :: verify_peer | verify_none.
-type session_locking_strategy() :: local | one | quorum | all.
-type shared_subscription_strategy() :: random | round_robin | sticky | hash.
-type route_lock_type() :: key | tab | global.
-type log_format_depth() :: unlimited | integer().
-type log_size() :: atom() | bytesize().
-type overload_kill_restart_after() :: atom() | duration().
-type log_formatter() :: text | json.
-type supervisor_reports() :: error | progress.
-type log_to() :: file | console | both.
-type log_level() :: debug | info | notice | warning | error | critical | alert | emergency | all.
-type flag() :: true | false.
-type duration() :: integer().
-type duration_s() :: integer().
-type bytesize() :: integer().
-type percent() :: float().
-type file() :: string().

-typerefl_from_string({flag/0, emqx_schema, to_flag}).
-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({percent/0, emqx_schema, to_percent}).

% workaround: prevent being recognized as unused functions
-export([to_duration/1, to_duration_s/1, to_bytesize/1, to_flag/1, to_percent/1]).

-behaviour(hocon_schema).

-reflect_type([ rpc_mode/0, proto_dist/0, k8s_address_type/0, port_discovery/0
    , acl_nomatch/0, acl_deny_action/0, mqueue_default_priority/0
    , endpoint/0, listener_peer_cert_tcp/0, listener_peer_cert_ssl/0
    , mqtt_piggyback/0, deflate_opts_level/0, deflate_opts_strategy/0, context_takeover/0
    , verify/0, session_locking_strategy/0
    , shared_subscription_strategy/0, route_lock_type/0
    , log_format_depth/0, log_size/0, overload_kill_restart_after/0
    , log_formatter/0, supervisor_reports/0
    , log_to/0, log_level/0, flag/0, duration/0, duration_s/0
    , bytesize/0, percent/0, file/0
]).

-export([structs/0, fields/1, translations/0, translation/1]).

structs() -> ["cluster", "node", "rpc", "log", "lager",
    "acl", "mqtt", "zone", "listener", "module", "broker",
    "plugins", "sysmon", "os_mon", "vm_mon", "alarm", "telemetry"].

fields("cluster") ->
    [ {"name", fun cluster__name/1}
        , {"discovery", fun cluster__discovery/1}
        , {"autoclean", duration("ekka.cluster_autoclean", undefined)}
        , {"autoheal", flag("ekka.cluster_autoheal", false)}
        , {"static", ref("static")}
        , {"mcast", ref("mcast")}
        , {"proto_dist", fun cluster__proto_dist/1}
        , {"dns", ref("dns")}
        , {"etcd", ref("etcd")}
        , {"k8s", ref("k8s")}
    ];

fields("static") ->
    [ {"seeds", fun string/1}];

fields("mcast") ->
    [ {"addr", string(undefined, "239.192.0.1")}
        , {"ports", string(undefined, "4369")}
        , {"iface", string(undefined, "0.0.0.0")}
        , {"ttl", integer(undefined, 255)}
        , {"loop", flag(undefined, true)}
        , {"sndbuf", bytesize(undefined, "16KB")}
        , {"recbuf", bytesize(undefined, "16KB")}
        , {"buffer", bytesize(undefined, "32KB")}
    ];

fields("dns") ->
    [ {"app", fun string/1}];

fields("etcd") ->
    [ {"server", fun string/1}
        , {"prefix", fun string/1}
        , {"node_ttl", duration(undefined, "1m")}
        , {"ssl", ref("ssl")}
    ];

fields("ssl") ->
    [ {"keyfile", fun string/1}
        , {"certfile", fun string/1}
        , {"cacertfile", fun string/1}
    ];

fields("k8s") ->
    [ {"apiserver", fun string/1}
        , {"service_name", fun string/1}
        , {"address_type", fun cluster__k8s__address_type/1}
        , {"app_name", fun string/1}
        , {"namespace", fun string/1}
        , {"suffix", string(undefined, "")}
    ];

fields("node") ->
    [ {"name", fun node__name/1}
        , {"ssl_dist_optfile", string("vm_args.-ssl_dist_optfile", undefined)}
        , {"cookie", fun node__cookie/1}
        , {"data_dir", string("emqx.data_dir", undefined)}
        , {"heartbeat", flag(undefined, false)}
        , {"async_threads", fun node__async_threads/1}
        , {"process_limit", integer("vm_args.+P", undefined)}
        , {"max_ports", fun node__max_ports/1}
        , {"dist_buffer_size", fun node__dist_buffer_size/1}
        , {"global_gc_interval", duration_s("emqx.global_gc_interval", undefined)}
        , {"fullsweep_after", fun node__fullsweep_after/1}
        , {"max_ets_tables", duration("vm_args.+e", 256000)}
        , {"crash_dump", fun node__crash_dump/1}
        , {"dist_net_ticktime", integer("vm_args.-kernel net_ticktime", undefined)}
        , {"dist_listen_min", integer("kernel.inet_dist_listen_min", undefined)}
        , {"dist_listen_max", integer("kernel.inet_dist_listen_max", undefined)}
        , {"backtrace_depth", integer("emqx.backtrace_depth", 16)}
    ];

fields("rpc") ->
    [ {"mode", fun rpc__mode/1}
        , {"async_batch_size", integer("gen_rpc.max_batch_size", 256)}
        , {"port_discovery", fun rpc__port_discovery/1}
        , {"tcp_server_port", integer("gen_rpc.tcp_server_port", 5369)}
        , {"tcp_client_num", fun rpc__tcp_client_num/1}
        , {"connect_timeout", duration("gen_rpc.connect_timeout", "5s")}
        , {"send_timeout", duration("gen_rpc.send_timeout", "5s")}
        , {"authentication_timeout", duration("gen_rpc.authentication_timeout", "5s")}
        , {"call_receive_timeout", duration("gen_rpc.call_receive_timeout", "15s")}
        , {"socket_keepalive_idle", duration_s("gen_rpc.socket_keepalive_idle", "7200s")}
        , {"socket_keepalive_interval", duration_s("gen_rpc.socket_keepalive_interval", "75s")}
        , {"socket_keepalive_count", integer("gen_rpc.socket_keepalive_count", 9)}
        , {"socket_sndbuf", bytesize("gen_rpc.socket_sndbuf", "1MB")}
        , {"socket_recbuf", bytesize("gen_rpc.socket_recbuf", "1MB")}
        , {"socket_buffer", bytesize("gen_rpc.socket_buffer", "1MB")}
    ];

fields("log") ->
    [ {"to", fun log__to/1}
        , {"level", fun log__level/1}
        , {"time_offset", string(undefined, "system")}
        , {"primary_log_level", fun log__primary_log_level/1}
        , {"dir", string(undefined,"log")}
        , {"file", fun log__file/1}
        , {"chars_limit", integer(undefined, -1)}
        , {"supervisor_reports", fun log__supervisor_reports/1}
        , {"max_depth", fun log__max_depth/1}
        , {"formatter", fun log__formatter/1}
        , {"single_line", boolean(undefined, true)}
        , {"rotation", ref("rotation")}
        , {"size", fun log__size/1}
        , {"sync_mode_qlen", integer(undefined, 100)}
        , {"drop_mode_qlen", integer(undefined, 3000)}
        , {"flush_qlen", integer(undefined, 8000)}
        , {"overload_kill", flag(undefined, true)}
        , {"overload_kill_mem_size", bytesize(undefined, "30MB")}
        , {"overload_kill_qlen", integer(undefined, 20000)}
        , {"overload_kill_restart_after", fun log__overload_kill_restart_after/1}
        , {"burst_limit", string(undefined, "disabled")}
        , {"error_logger", fun log__error_logger/1}
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
    [ {"file", fun string/1}];

fields("rotation") ->
    [ {"enable", flag(undefined, true)}
        , {"size", bytesize(undefined, "10MB")}
        , {"count", integer(undefined, 5)}
    ];

fields("lager") ->
    [ {"handlers", string("lager.handlers", "")}
        , {"crash_log", flag("lager.crash_log", false)}
    ];

fields("acl") ->
    [ {"allow_anonymous", boolean("emqx.allow_anonymous", false)}
        , {"acl_nomatch", fun acl_nomatch/1}
        , {"acl_file", string("emqx.acl_file", undefined)}
        , {"enable_acl_cache", flag("emqx.enable_acl_cache", true)}
        , {"acl_cache_ttl", duration("emqx.acl_cache_ttl", "1m")}
        , {"acl_cache_max_size", fun acl_cache_max_size/1}
        , {"acl_deny_action", fun acl_deny_action/1}
        , {"flapping_detect_policy", string(undefined, "30,1m,5m")}
    ];

fields("mqtt") ->
    [ {"max_packet_size", fun mqtt__max_packet_size/1}
        , {"max_clientid_len", integer("emqx.max_clientid_len", 65535)}
        , {"max_topic_levels", integer("emqx.max_topic_levels", 0)}
        , {"max_qos_allowed", fun mqtt__max_qos_allowed/1}
        , {"max_topic_alias", integer("emqx.max_topic_alias", 65535)}
        , {"retain_available", boolean("emqx.retain_available", true)}
        , {"wildcard_subscription", boolean("emqx.wildcard_subscription", true)}
        , {"shared_subscription", boolean("emqx.shared_subscription", true)}
        , {"ignore_loop_deliver", boolean("emqx.ignore_loop_deliver", true)}
        , {"strict_mode", boolean("emqx.strict_mode", false)}
        , {"response_information", string("emqx.response_information", undefined)}
    ];

fields("zone") ->
    [ {"$name", ref("zone_settings")}];

fields("zone_settings") ->
    [ {"idle_timeout", duration(undefined, "15s")}
        , {"allow_anonymous", fun boolean/1}
        , {"acl_nomatch", fun zones_acl_nomatch/1}
        , {"enable_acl", flag(undefined, false)}
        , {"acl_deny_action", fun zones_acl_deny_action/1}
        , {"enable_ban", flag(undefined, false)}
        , {"enable_stats", flag(undefined, false)}
        , {"max_packet_size", fun bytesize/1}
        , {"max_clientid_len", fun integer/1}
        , {"max_topic_levels", fun integer/1}
        , {"max_qos_allowed", fun zones_max_qos_allowed/1}
        , {"max_topic_alias", fun integer/1}
        , {"retain_available", fun boolean/1}
        , {"wildcard_subscription", fun boolean/1}
        , {"shared_subscription", fun boolean/1}
        , {"server_keepalive", fun integer/1}
        , {"keepalive_backoff", fun zones_keepalive_backoff/1}
        , {"max_subscriptions", integer(undefined, 0)}
        , {"upgrade_qos", flag(undefined, false)}
        , {"max_inflight", fun zones_max_inflight/1}
        , {"retry_interval", duration_s(undefined, "30s")}
        , {"max_awaiting_rel", duration(undefined, 0)}
        , {"await_rel_timeout", duration_s(undefined, "300s")}
        , {"ignore_loop_deliver", fun boolean/1}
        , {"session_expiry_interval", duration_s(undefined, "2h")}
        , {"max_mqueue_len", integer(undefined, 1000)}
        , {"mqueue_priorities", string(undefined, "none")}
        , {"mqueue_default_priority", fun zones_mqueue_default_priority/1}
        , {"mqueue_store_qos0", boolean(undefined, true)}
        , {"enable_flapping_detect", flag(undefined, false)}
        , {"rate_limit", ref("rate_limit")}
        , {"conn_congestion", ref("conn_congestion")}
        , {"quota", ref("quota")}
        , {"force_gc_policy", fun string/1}
        , {"force_shutdown_policy", string(undefined, "default")}
        , {"mountpoint", fun string/1}
        , {"use_username_as_clientid", boolean(undefined, false)}
        , {"strict_mode", boolean(undefined, false)}
        , {"response_information", fun string/1}
        , {"bypass_auth_plugins", boolean(undefined, false)}
    ];

fields("rate_limit") ->
    [ {"conn_messages_in", fun string/1}
        , {"conn_bytes_in", fun string/1}
    ];

fields("conn_congestion") ->
    [ {"alarm", flag(undefined, false)}
        , {"min_alarm_sustain_duration", duration(undefined, "1m")}
    ];

fields("quota") ->
    [ {"conn_messages_routing", fun string/1}
        , {"overall_messages_routing", fun string/1}
    ];

fields("listener") ->
    [ {"tcp", ref("tcp_listener")}
        , {"ssl", ref("ssl_listener")}
        , {"ws", ref("ws_listener")}
        , {"wss", ref("wss_listener")}
    ];

fields("tcp_listener") ->
    [ {"$name", ref("tcp_listener_settings")}];

fields("ssl_listener") ->
    [ {"$name", ref("ssl_listener_settings")}];

fields("ws_listener") ->
    [ {"$name", ref("ws_listener_settings")}];

fields("wss_listener") ->
    [ {"$name", ref("wss_listener_settings")}];

fields("listener_settings") ->
    [ {"endpoint", fun listener_endpoint/1}
        , {"acceptors", integer(undefined, 8)}
        , {"max_connections", integer(undefined, 1024)}
        , {"max_conn_rate", fun integer/1}
        , {"active_n", integer(undefined, 100)}
        , {"zone", fun string/1}
        , {"rate_limit", fun string/1}
        , {"access", ref("access")}
        , {"proxy_protocol", fun flag/1}
        , {"proxy_protocol_timeout", fun duration/1}
        , {"backlog", integer(undefined, 1024)}
        , {"send_timeout", duration(undefined, "15s")}
        , {"send_timeout_close", flag(undefined, true)}
        , {"recbuf", fun bytesize/1}
        , {"sndbuf", fun bytesize/1}
        , {"buffer", fun bytesize/1}
        , {"high_watermark", bytesize(undefined, "1MB")}
        , {"tune_buffer", fun flag/1}
        , {"nodelay", fun boolean/1}
        , {"reuseaddr", fun boolean/1}
    ];

fields("tcp_listener_settings") ->
    [ {"peer_cert_as_username", fun listener_peer_cert_as_username_tcp/1}
        , {"peer_cert_as_clientid", fun listener_peer_cert_as_clientid_tcp/1}
    ] ++ fields("listener_settings");

fields("ssl_listener_settings") ->
    [ {"tls_versions", fun string/1}
        , {"ciphers", fun string/1}
        , {"psk_ciphers", fun string/1}
        , {"handshake_timeout", duration(undefined, "15s")}
        , {"depth", integer(undefined, 10)}
        , {"key_password", fun string/1}
        , {"dhfile", fun string/1}
        , {"keyfile", fun string/1}
        , {"certfile", fun string/1}
        , {"cacertfile", fun string/1}
        , {"verify", fun listener_verify/1}
        , {"fail_if_no_peer_cert", fun boolean/1}
        , {"secure_renegotiate", fun flag/1}
        , {"reuse_sessions", flag(undefined, true)}
        , {"honor_cipher_order", fun flag/1}
        , {"peer_cert_as_username", fun listener_peer_cert_as_username_ssl/1}
        , {"peer_cert_as_clientid", fun listener_peer_cert_as_clientid_ssl/1}
    ] ++ fields("listener_settings");

fields("ws_listener_settings") ->
    [ {"mqtt_path", string(undefined, "/mqtt")}
        , {"fail_if_no_subprotocol", boolean(undefined, true)}
        , {"supported_subprotocols", string(undefined, "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5")}
        , {"proxy_address_header", string(undefined, "X-Forwarded-For")}
        , {"proxy_port_header", string(undefined, "X-Forwarded-Port")}
        , {"compress", fun boolean/1}
        , {"deflate_opts", ref("deflate_opts")}
        , {"idle_timeout", fun duration/1}
        , {"max_frame_size", fun integer/1}
        , {"mqtt_piggyback", fun listener_mqtt_piggyback/1}
        , {"check_origin_enable", boolean(undefined, false)}
        , {"allow_origin_absence", boolean(undefined, true)}
        , {"check_origins", fun string/1}
        % @fixme
    ] ++ lists:keydelete("high_watermark", 1, fields("tcp_listener_settings"));

fields("wss_listener_settings") ->
    % @fixme
    Settings = lists:ukeymerge(1, fields("ssl_listener_settings"), fields("ws_listener_settings")),
    [{K, V} || {K, V} <- Settings,
        lists:all(fun(X) -> X =/= K end, ["high_watermark", "handshake_timeout", "dhfile"])];

fields("access") ->
    [ {"$id", fun string/1}];

fields("deflate_opts") ->
    [ {"level", fun deflate_opts_level/1}
        , {"mem_level", fun deflate_opts_mem_level/1}
        , {"strategy", fun deflate_opts_strategy/1}
        , {"server_context_takeover", fun deflate_opts_server_context_takeover/1}
        , {"client_context_takeover", fun deflate_opts_client_context_takeover/1}
        , {"server_max_window_bits", fun integer/1}
        , {"client_max_window_bits", fun integer/1}
    ];

fields("module") ->
    [ {"loaded_file", string("emqx.modules_loaded_file", undefined)}
        , {"presence", ref("presence")}
        , {"subscription", ref("subscription")}
        , {"rewrite", ref("rewrite")}
    ];

fields("presence") ->
    [ {"qos", fun module_presence__qos/1}];

fields("subscription") ->
    [ {"$id", ref("subscription_settings")}
    ];

fields("subscription_settings") ->
    [ {"topic", fun string/1}
        , {"qos", fun module_subscription_qos/1}
        , {"nl", fun module_subscription_nl/1}
        , {"rap", fun module_subscription_rap/1}
        , {"rh", fun module_subscription_rh/1}
    ];


fields("rewrite") ->
    [ {"rule", ref("rule")}
        , {"pub_rule", ref("rule")}
        , {"sub_rule", ref("rule")}
    ];

fields("rule") ->
    [ {"$id", fun string/1}];

fields("plugins") ->
    [ {"etc_dir", string("emqx.plugins_etc_dir", undefined)}
        , {"loaded_file", string("emqx.plugins_loaded_file", undefined)}
        , {"expand_plugins_dir", string("emqx.expand_plugins_dir", undefined)}
    ];

fields("broker") ->
    [ {"sys_interval", duration("emqx.broker_sys_interval", "1m")}
        , {"sys_heartbeat", duration("emqx.broker_sys_heartbeat", "30s")}
        , {"enable_session_registry", flag("emqx.enable_session_registry", true)}
        , {"session_locking_strategy", fun broker__session_locking_strategy/1}
        , {"shared_subscription_strategy", fun broker__shared_subscription_strategy/1}
        , {"shared_dispatch_ack_enabled", boolean("emqx.shared_dispatch_ack_enabled", false)}
        , {"route_batch_clean", flag("emqx.route_batch_clean", true)}
        , {"perf", ref("perf")}
    ];

fields("perf") ->
    [ {"route_lock_type", fun broker__perf__route_lock_type/1}
        , {"trie_compaction", boolean("emqx.trie_compaction", true)}
    ];

fields("sysmon") ->
    [ {"long_gc", duration(undefined, 0)}
        , {"long_schedule", duration(undefined, 240)}
        , {"large_heap", bytesize(undefined, "8MB")}
        , {"busy_dist_port", boolean(undefined, true)}
        , {"busy_port", boolean(undefined, false)}
    ];

fields("os_mon") ->
    [ {"cpu_check_interval", duration_s(undefined, 60)}
        , {"cpu_high_watermark", percent(undefined, "80%")}
        , {"cpu_low_watermark", percent(undefined, "60%")}
        , {"mem_check_interval", duration_s(undefined, 60)}
        , {"sysmem_high_watermark", percent(undefined, "70%")}
        , {"procmem_high_watermark", percent(undefined, "5%")}
    ];

fields("vm_mon") ->
    [ {"check_interval", duration_s(undefined, 30)}
        , {"process_high_watermark", percent(undefined, "80%")}
        , {"process_low_watermark", percent(undefined, "60%")}
    ];

fields("alarm") ->
    [ {"actions", string(undefined, "log,publish")}
        , {"size_limit", integer(undefined, 1000)}
        , {"validity_period", duration_s(undefined, "24h")}
    ];

fields("telemetry") ->
    [ {"enabled", boolean(undefined, false)}
        , {"url", string(undefined, "https://telemetry-emqx-io.bigpar.vercel.app/api/telemetry")}
        , {"report_interval", duration_s(undefined, "7d")}
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

cluster__name(mapping) -> "ekka.cluster_name";
cluster__name(default) -> emqxcl;
cluster__name(type) -> atom();
cluster__name(_) -> undefined.

cluster__discovery(default) -> manual;
cluster__discovery(type) -> atom();
cluster__discovery(_) -> undefined.

tr_cluster__discovery(Conf) ->
    Strategy = conf_get("cluster.discovery", Conf),
    Filter  = fun(Opts) -> [{K, V} || {K, V} <- Opts, V =/= undefined] end,
    {Strategy, Filter(options(Strategy, Conf))}.

%% @doc The erlang distributed protocol
cluster__proto_dist(mapping) -> "ekka.proto_dist";
cluster__proto_dist(type) -> proto_dist();
cluster__proto_dist(default) -> inet_tcp;
cluster__proto_dist(_) -> undefined.

cluster__k8s__address_type(type) -> k8s_address_type();
cluster__k8s__address_type(_) -> undefined.

%% @doc Node name
node__name(mapping) -> "vm_args.-name";
node__name(type) -> string();
node__name(default) -> "emqx@127.0.0.1";
node__name(override_env) -> "NODE_NAME";
node__name(_) -> undefined.

%% @doc Secret cookie for distributed erlang node
node__cookie(mapping) -> "vm_args.-setcookie";
node__cookie(default) -> "emqxsecretcookie";
node__cookie(override_env) -> "NODE_COOKIE";
node__cookie(X) -> string(X).

tr_heart(Conf) ->
    case conf_get("node.heartbeat", Conf) of
        true  -> "";
        "on" -> "";
        _ -> undefined
    end.

%% @doc More information at: http://erlang.org/doc/man/erl.html
node__async_threads(mapping) -> "vm_args.+A";
node__async_threads(type) -> range(1, 1024);
node__async_threads(_) -> undefined.

%% @doc The maximum number of concurrent ports/sockets.Valid range is 1024-134217727
node__max_ports(mapping) -> "vm_args.+Q";
node__max_ports(type) -> range(1024, 134217727);
node__max_ports(override_env) -> "MAX_PORTS";
node__max_ports(_) -> undefined.

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
        X when is_integer(X) -> cuttlefish_util:ceiling(X / 1024); %% Bytes to Kilobytes;
        _ -> undefined
    end.

%% @doc http://www.erlang.org/doc/man/erlang.html#system_flag-2
node__fullsweep_after(mapping) -> "vm_args.-env ERL_FULLSWEEP_AFTER";
node__fullsweep_after(type) -> non_neg_integer();
node__fullsweep_after(default) -> 1000;
node__fullsweep_after(_) -> undefined.

%% @doc Set the location of crash dumps
node__crash_dump(mapping) -> "vm_args.-env ERL_CRASH_DUMP";
node__crash_dump(type) -> file();
node__crash_dump(_) -> undefined.

rpc__mode(mapping) -> "emqx.rpc_mode";
rpc__mode(type) -> rpc_mode();
rpc__mode(default) -> async;
rpc__mode(_) -> undefined.

rpc__port_discovery(mapping) -> "gen_rpc.port_discovery";
rpc__port_discovery(type) -> port_discovery();
rpc__port_discovery(default) -> stateless;
rpc__port_discovery(_) -> undefined.

rpc__tcp_client_num(type) -> range(0, 255);
rpc__tcp_client_num(default) -> 0;
rpc__tcp_client_num(_) -> undefined.

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

log__to(type) -> log_to();
log__to(default) -> file;
log__to(_) -> undefined.

log__level(type) -> log_level();
log__level(default) -> warning;
log__level(_) -> undefined.

tr_logger_level(Conf) -> conf_get("log.level", Conf).

log__primary_log_level(mapping) -> "kernel.logger_level";
log__primary_log_level(type) -> log_level();
log__primary_log_level(default) -> warning;
log__primary_log_level(_) -> undefined.

log__file(type) -> file();
log__file(default) -> "emqx.log";
log__file(_) -> undefined.

log__supervisor_reports(type) -> supervisor_reports();
log__supervisor_reports(default) -> error;
log__supervisor_reports(_) -> undefined.

%% @doc Maximum depth in Erlang term log formattingand message queue inspection.
log__max_depth(mapping) -> "kernel.error_logger_format_depth";
log__max_depth(type) -> log_format_depth();
log__max_depth(default) -> 20;
log__max_depth(_) -> undefined.

%% @doc format logs as JSON objects
log__formatter(type) -> log_formatter();
log__formatter(default) -> text;
log__formatter(_) -> undefined.

log__size(type) -> log_size();
log__size(default) -> infinity;
log__size(_) -> undefined.

% @todo convert to union
log__overload_kill_restart_after(type) -> duration();
log__overload_kill_restart_after(default) -> "5s";
log__overload_kill_restart_after(_) -> undefined.

log__error_logger(mapping) -> "kernel.error_logger";
log__error_logger(type) -> atom();
log__error_logger(default) -> silent;
log__error_logger(_) -> undefined.

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
    BurstLimit = string:tokens(conf_get("log.burst_limit", Conf), ", "),
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

%% @doc ACL nomatch.
acl_nomatch(mapping) -> "emqx.acl_nomatch";
acl_nomatch(type) -> acl_nomatch();
acl_nomatch(default) -> deny;
acl_nomatch(_) -> undefined.

%% @doc ACL cache size.
acl_cache_max_size(mapping) -> "emqx.acl_cache_max_size";
acl_cache_max_size(type) -> range(1, inf);
acl_cache_max_size(default) -> 32;
acl_cache_max_size(_) -> undefined.

%% @doc Action when acl check reject current operation
acl_deny_action(mapping) -> "emqx.acl_deny_action";
acl_deny_action(type) -> acl_deny_action();
acl_deny_action(default) -> ignore;
acl_deny_action(_) -> undefined.

tr_flapping_detect_policy(Conf) ->
    Policy = conf_get("acl.flapping_detect_policy", Conf),
    [Threshold, Duration, Interval] = string:tokens(Policy, ", "),
    ParseDuration = fun(S, Dur) ->
        case cuttlefish_duration:parse(S, Dur) of
            I when is_integer(I) -> I;
            {error, Reason} -> error(Reason)
        end end,
    #{threshold => list_to_integer(Threshold),
        duration  => ParseDuration(Duration, ms),
        banned_interval => ParseDuration(Interval, s)
    }.

%% @doc Max Packet Size Allowed, 1MB by default.
mqtt__max_packet_size(mapping) -> "emqx.max_packet_size";
mqtt__max_packet_size(override_env) -> "MAX_PACKET_SIZE";
mqtt__max_packet_size(type) -> bytesize();
mqtt__max_packet_size(default) -> "1MB";
mqtt__max_packet_size(_) -> undefined.

%% @doc Set the Maximum QoS allowed.
mqtt__max_qos_allowed(mapping) -> "emqx.max_qos_allowed";
mqtt__max_qos_allowed(type) -> range(0, 2);
mqtt__max_qos_allowed(default) -> 2;
mqtt__max_qos_allowed(_) -> undefined.

zones_acl_nomatch(type) -> acl_nomatch();
zones_acl_nomatch(_) -> undefined.

%% @doc Action when acl check reject current operation
zones_acl_deny_action(type) -> acl_deny_action();
zones_acl_deny_action(default) -> ignore;
zones_acl_deny_action(_) -> undefined.

%% @doc Set the Maximum QoS allowed.
zones_max_qos_allowed(type) -> range(0, 2);
zones_max_qos_allowed(_) -> undefined.

%% @doc Keepalive backoff
zones_keepalive_backoff(type) -> float();
zones_keepalive_backoff(default) -> 0.75;
zones_keepalive_backoff(_) -> undefined.

%% @doc Max number of QoS 1 and 2 messages that can be “inflight” at one time.0 is equivalent to maximum allowed
zones_max_inflight(type) -> range(0, 65535);
zones_max_inflight(_) -> undefined.

%% @doc Default priority for topics not in priority table.
zones_mqueue_default_priority(type) -> mqueue_default_priority();
zones_mqueue_default_priority(default) -> lowest;
zones_mqueue_default_priority(_) -> undefined.

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

listener_endpoint(type) -> endpoint();
listener_endpoint(_) -> undefined.

listener_peer_cert_as_username_tcp(type) -> listener_peer_cert_tcp();
listener_peer_cert_as_username_tcp(_) -> undefined.

listener_peer_cert_as_clientid_tcp(type) -> listener_peer_cert_tcp();
listener_peer_cert_as_clientid_tcp(_) -> undefined.

listener_peer_cert_as_username_ssl(type) -> listener_peer_cert_ssl();
listener_peer_cert_as_username_ssl(_) -> undefined.

listener_peer_cert_as_clientid_ssl(type) -> listener_peer_cert_ssl();
listener_peer_cert_as_clientid_ssl(_) -> undefined.

listener_verify(type) -> verify();
listener_verify(_) -> undefined.

listener_mqtt_piggyback(type) -> mqtt_piggyback();
listener_mqtt_piggyback(default) -> multiple;
listener_mqtt_piggyback(_) -> undefined.

deflate_opts_level(type) -> deflate_opts_level();
deflate_opts_level(_) -> undefined.

deflate_opts_mem_level(type) -> range(1, 9);
deflate_opts_mem_level(_) -> undefined.

deflate_opts_strategy(type) -> deflate_opts_strategy();
deflate_opts_strategy(_) -> undefined.

deflate_opts_server_context_takeover(type) -> context_takeover();
deflate_opts_server_context_takeover(_) -> undefined.

deflate_opts_client_context_takeover(type) -> context_takeover();
deflate_opts_client_context_takeover(_) -> undefined.

tr_listeners(Conf) ->
    Filter  = fun(Opts) -> [{K, V} || {K, V} <- Opts, V =/= undefined] end,

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
        (Val) ->
            [L, D] = string:tokens(Val, ", "),
            Limit = case cuttlefish_bytesize:parse(L) of
                        Sz when is_integer(Sz) -> Sz;
                        {error, Reason} -> error(Reason)
                    end,
            Duration = case cuttlefish_duration:parse(D, s) of
                           Secs when is_integer(Secs) -> Secs;
                           {error, Reason1} -> error(Reason1)
                       end,
            {Limit, Duration}
                end,

    CheckOrigin = fun(S) ->
        Origins = string:tokens(S, ","),
        [ list_to_binary(string:trim(O)) || O <- Origins]
                  end,

    WsOpts = fun(Prefix) ->
        case conf_get(Prefix ++ ".check_origins", Conf) of
            undefined -> undefined;
            Rules -> lists:flatten(CheckOrigin(Rules))
        end
             end,

    LisOpts = fun(Prefix) ->
        Filter([{acceptors, conf_get(Prefix ++ ".acceptors", Conf)},
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
        Filter([{level, conf_get(Prefix ++ ".deflate_opts.level", Conf)},
            {mem_level, conf_get(Prefix ++ ".deflate_opts.mem_level", Conf)},
            {strategy, conf_get(Prefix ++ ".deflate_opts.strategy", Conf)},
            {server_context_takeover, conf_get(Prefix ++ ".deflate_opts.server_context_takeover", Conf)},
            {client_context_takeover, conf_get(Prefix ++ ".deflate_opts.client_context_takeover", Conf)},
            {server_max_windows_bits, conf_get(Prefix ++ ".deflate_opts.server_max_window_bits", Conf)},
            {client_max_windows_bits, conf_get(Prefix ++ ".deflate_opts.client_max_window_bits", Conf)}])
                  end,
    TcpOpts = fun(Prefix) ->
        Filter([{backlog, conf_get(Prefix ++ ".backlog", Conf)},
            {send_timeout, conf_get(Prefix ++ ".send_timeout", Conf)},
            {send_timeout_close, conf_get(Prefix ++ ".send_timeout_close", Conf)},
            {recbuf,  conf_get(Prefix ++ ".recbuf", Conf)},
            {sndbuf,  conf_get(Prefix ++ ".sndbuf", Conf)},
            {buffer,  conf_get(Prefix ++ ".buffer", Conf)},
            {high_watermark,  conf_get(Prefix ++ ".high_watermark", Conf)},
            {nodelay, conf_get(Prefix ++ ".nodelay", Conf, true)},
            {reuseaddr, conf_get(Prefix ++ ".reuseaddr", Conf)}])
              end,
    SplitFun = fun(undefined) -> undefined; (S) -> string:tokens(S, ",") end,
    MapPSKCiphers = fun(PSKCiphers) ->
        lists:map(
            fun("PSK-AES128-CBC-SHA") -> {psk, aes_128_cbc, sha};
                ("PSK-AES256-CBC-SHA") -> {psk, aes_256_cbc, sha};
                ("PSK-3DES-EDE-CBC-SHA") -> {psk, '3des_ede_cbc', sha};
                ("PSK-RC4-SHA") -> {psk, rc4_128, sha}
            end, PSKCiphers)
                    end,
    SslOpts = fun(Prefix) ->
        Versions = case SplitFun(conf_get(Prefix ++ ".tls_versions", Conf)) of
                       undefined -> undefined;
                       L -> [list_to_atom(V) || V <- L]
                   end,
        TLSCiphers = conf_get(Prefix++".ciphers", Conf),
        PSKCiphers = conf_get(Prefix++".psk_ciphers", Conf),
        Ciphers =
            case {TLSCiphers, PSKCiphers} of
                {undefined, undefined} ->
                    cuttlefish:invalid(Prefix++".ciphers or "++Prefix++".psk_ciphers is absent");
                {TLSCiphers, undefined} ->
                    SplitFun(TLSCiphers);
                {undefined, PSKCiphers} ->
                    MapPSKCiphers(SplitFun(PSKCiphers));
                {_TLSCiphers, _PSKCiphers} ->
                    cuttlefish:invalid(Prefix++".ciphers and "++Prefix++".psk_ciphers cannot be configured at the same time")
            end,
        UserLookupFun =
            case PSKCiphers of
                undefined -> undefined;
                _ -> {fun emqx_psk:lookup/3, <<>>}
            end,
        Filter([{versions, Versions},
            {ciphers, Ciphers},
            {user_lookup_fun, UserLookupFun},
            {handshake_timeout, conf_get(Prefix ++ ".handshake_timeout", Conf)},
            {depth, conf_get(Prefix ++ ".depth", Conf)},
            {password, conf_get(Prefix ++ ".key_password", Conf)},
            {dhfile, conf_get(Prefix ++ ".dhfile", Conf)},
            {keyfile,    conf_get(Prefix ++ ".keyfile", Conf)},
            {certfile,   conf_get(Prefix ++ ".certfile", Conf)},
            {cacertfile, conf_get(Prefix ++ ".cacertfile", Conf)},
            {verify,     conf_get(Prefix ++ ".verify", Conf)},
            {fail_if_no_peer_cert, conf_get(Prefix ++ ".fail_if_no_peer_cert", Conf)},
            {secure_renegotiate, conf_get(Prefix ++ ".secure_renegotiate", Conf)},
            {reuse_sessions, conf_get(Prefix ++ ".reuse_sessions", Conf)},
            {honor_cipher_order, conf_get(Prefix ++ ".honor_cipher_order", Conf)}])
              end,

    Listen_fix = fun(IpPort) when is_list(IpPort) ->
        [Ip, Port] = string:tokens(IpPort, ":"),
        case inet:parse_address(Ip) of
            {ok, R} -> {R, list_to_integer(Port)};
            _ -> error("failed to parse ip address")
        end;
        (Other) -> Other end,

    TcpListeners = fun(Type, Name) ->
        Prefix = string:join(["listener", Type, Name], "."),
        ListenOnN = case conf_get(Prefix ++ ".endpoint", Conf) of
                        undefined -> [];
                        ListenOn  -> Listen_fix(ListenOn)
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
                    , listen_on => Listen_fix(ListenOn)
                    , opts => [ {deflate_options, DeflateOpts(Prefix)}
                        , {tcp_options, TcpOpts(Prefix)}
                        , {ssl_options, SslOpts(Prefix)}
                        | LisOpts(Prefix)
                    ]
                }
                ]
        end
                   end,


    lists:flatten([TcpListeners("tcp", Name) || Name <- keys("listener.tcp", Conf)]
    ++ [TcpListeners("ws", Name) || Name <- keys("listener.ws", Conf)]
        ++ [SslListeners("ssl", Name) || Name <- keys("listener.ssl", Conf)]
        ++ [SslListeners("wss", Name) || Name <- keys("listener.wss", Conf)]).

module_presence__qos(type) -> range(0, 2);
module_presence__qos(default) -> 1;
module_presence__qos(_) -> undefined.

module_subscription_qos(type) -> range(0, 2);
module_subscription_qos(default) -> 1;
module_subscription_qos(_) -> undefined.

module_subscription_nl(type) -> range(0, 1);
module_subscription_nl(default) -> 0;
module_subscription_nl(_) -> undefined.

module_subscription_rap(type) -> range(0, 1);
module_subscription_rap(default) -> 0;
module_subscription_rap(_) -> undefined.

module_subscription_rh(type) -> range(0, 2);
module_subscription_rh(default) -> 0;
module_subscription_rh(_) -> undefined.

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

broker__session_locking_strategy(mapping) -> "emqx.session_locking_strategy";
broker__session_locking_strategy(type) -> session_locking_strategy();
broker__session_locking_strategy(default) -> quorum;
broker__session_locking_strategy(_) -> undefined.

%% @doc Shared Subscription Dispatch Strategy.randomly pick a subscriber
%% round robin alive subscribers one message after another
%% pick a random subscriber and stick to it
%% hash client ID to a group member
broker__shared_subscription_strategy(mapping) -> "emqx.shared_subscription_strategy";
broker__shared_subscription_strategy(type) -> shared_subscription_strategy();
broker__shared_subscription_strategy(default) -> round_robin;
broker__shared_subscription_strategy(_) -> undefined.

%% @doc Performance toggle for subscribe/unsubscribe wildcard topic.
%% Change this toggle only when there are many wildcard topics.
%% key:   mnesia translational updates with per-key locks. recommended for single node setup.
%% tab:   mnesia translational updates with table lock. recommended for multi-nodes setup.
%% global: global lock protected updates. recommended for larger cluster.
%% NOTE: when changing from/to 'global' lock, it requires all nodes in the cluster
broker__perf__route_lock_type(mapping) -> "emqx.route_lock_type";
broker__perf__route_lock_type(type) -> route_lock_type();
broker__perf__route_lock_type(default) -> key;
broker__perf__route_lock_type(_) -> undefined.

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
    [ {actions, [list_to_atom(Action) || Action <- string:tokens(conf_get("alarm.actions", Conf), ",")]}
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
    [{seeds, [list_to_atom(S) || S <- string:tokens(conf_get("cluster.static.seeds", Conf, ""), ",")]}];
options(mcast, Conf) ->
    {ok, Addr} = inet:parse_address(conf_get("cluster.mcast.addr", Conf)),
    {ok, Iface} = inet:parse_address(conf_get("cluster.mcast.iface", Conf)),
    Ports = [list_to_integer(S) || S <- string:tokens(conf_get("cluster.mcast.ports", Conf), ",")],
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
    [{server, string:tokens(conf_get("cluster.etcd.server", Conf), ",")},
        {prefix, conf_get("cluster.etcd.prefix", Conf, "emqxcl")},
        {node_ttl, conf_get("cluster.etcd.node_ttl", Conf, 60)},
        {ssl_options, SslOpts(Conf)}];
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
        case hocon_postprocess:duration(Window) of
            Secs when is_integer(Secs) -> Secs;
            _ -> error({duration, Window})
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

rate_limit(Val) ->
    [L, D] = string:tokens(Val, ", "),
    Limit = case cuttlefish_bytesize:parse(L) of
                Sz when is_integer(Sz) -> Sz;
                {error, Reason1} -> error(Reason1)
            end,
    Duration = case cuttlefish_duration:parse(D, s) of
                   Secs when is_integer(Secs) -> Secs;
                   {error, Reason} -> error(Reason)
               end,
    {Limit, Duration}.

map_zones(_, undefined) ->
    {undefined, undefined};
map_zones("force_gc_policy", Val) ->
    [Count, Bytes] = string:tokens(Val, "| "),
    GcPolicy = case cuttlefish_bytesize:parse(Bytes) of
                   {error, Reason} ->
                       error(Reason);
                   Bytes1 ->
                       #{bytes => Bytes1,
                           count => list_to_integer(Count)}
               end,
    {force_gc_policy, GcPolicy};
map_zones("force_shutdown_policy", "default") ->
    WordSize = erlang:system_info(wordsize),
    {DefaultLen, DefaultSize} =
        case WordSize of
            8 -> % arch_64
                {10000, cuttlefish_bytesize:parse("64MB")};
            4 -> % arch_32
                {1000, cuttlefish_bytesize:parse("32MB")}
        end,
    {force_shutdown_policy, #{message_queue_len => DefaultLen,
        max_heap_size => DefaultSize div WordSize
    }};
map_zones("force_shutdown_policy", Val) ->
    [Len, Siz] = string:tokens(Val, "| "),
    WordSize = erlang:system_info(wordsize),
    MaxSiz = case WordSize of
                 8 -> % arch_64
                     (1 bsl 59) - 1;
                 4 -> % arch_32
                     (1 bsl 27) - 1
             end,
    ShutdownPolicy =
        case cuttlefish_bytesize:parse(Siz) of
            {error, Reason} ->
                error(Reason);
            Siz1 when Siz1 > MaxSiz ->
                cuttlefish:invalid(io_lib:format("force_shutdown_policy: heap-size ~s is too large", [Siz]));
            Siz1 ->
                #{message_queue_len => list_to_integer(Len),
                    max_heap_size => Siz1 div WordSize}
        end,
    {force_shutdown_policy, ShutdownPolicy};
map_zones("mqueue_priorities", Val) ->
    case Val of
        "none" -> {mqueue_priorities, none}; % NO_PRIORITY_TABLE
        _ ->
            MqueuePriorities = lists:foldl(fun(T, Acc) ->
                %% NOTE: space in "= " is intended
                [Topic, Prio] = string:tokens(T, "= "),
                P = list_to_integer(Prio),
                (P < 0 orelse P > 255) andalso error({bad_priority, Topic, Prio}),
                maps:put(iolist_to_binary(Topic), P, Acc)
                                           end, #{}, string:tokens(Val, ",")),
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
                       [{conn_messages_in, rate_limit(M)}]
               end,
    Bytes = case conf_get("conn_bytes_in", #{value => Conf}) of
                undefined ->
                    [];
                B ->
                    [{conn_bytes_in, rate_limit(B)}]
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
                   [{conn_messages_routing, rate_limit(C)}]
           end,
    Overall = case conf_get("overall_messages_routing", #{value => Conf}) of
                  undefined ->
                      [];
                  O ->
                      [{overall_messages_routing, rate_limit(O)}]
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

%% @private return a list of keys in a parent field
-spec(keys(string(), hocon:config()) -> [string()]).
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

%% types

duration(type) -> duration();
duration(_) -> undefined.

flag(type) -> flag();
flag(_) -> undefined.

string(type) -> string();
string(_) -> undefined.

integer(type) -> integer();
integer(_) -> undefined.

bytesize(type) -> bytesize();
bytesize(_) -> undefined.

boolean(type) -> boolean();
boolean(_) -> undefined.

duration(M, D) ->
    fun (type) -> duration(); (mapping) -> M; (default) -> D; (_) -> undefined end.
duration_s(M, D) ->
    fun (type) -> duration_s(); (mapping) -> M; (default) -> D; (_) -> undefined end.
flag(M, D) ->
    fun (type) -> flag(); (mapping) -> M; (default) -> D; (_) -> undefined end.
string(M, D) ->
    fun (type) -> string(); (mapping) -> M; (default) -> D; (_) -> undefined end.
integer(M, D) ->
    fun (type) -> integer(); (mapping) -> M; (default) -> D; (_) -> undefined end.
bytesize(M, D) ->
    fun (type) -> bytesize(); (mapping) -> M; (default) -> D; (_) -> undefined end.
boolean(M, D) ->
    fun (type) -> boolean(); (mapping) -> M; (default) -> D; (_) -> undefined end.
percent(M, D) ->
    fun (type) -> percent(); (mapping) -> M; (default) -> D; (_) -> undefined end.

ref(Field) ->
    fun (type) -> Field; (_) -> undefined end.

to_flag(Str) ->
    {ok, hocon_postprocess:onoff(Str)}.

to_duration(Str) ->
    {ok, hocon_postprocess:duration(Str)}.

to_duration_s(Str) ->
    {ok, hocon_postprocess:duration(Str) div 1000}.

to_bytesize(Str) ->
    {ok, hocon_postprocess:bytesize(Str)}.

to_percent(Str) ->
    {ok, hocon_postprocess:percent(Str)}.
