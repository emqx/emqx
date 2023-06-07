%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx_authentication.hrl").

-type log_level() :: debug | info | notice | warning | error | critical | alert | emergency | all.
-type file() :: string().
-type cipher() :: map().

-behaviour(hocon_schema).

-reflect_type([
    log_level/0,
    file/0,
    cipher/0
]).

-export([
    namespace/0, roots/0, fields/1, translations/0, translation/1, validations/0, desc/1, tags/0
]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).

%% Static apps which merge their configs into the merged emqx.conf
%% The list can not be made a dynamic read at run-time as it is used
%% by nodetool to generate app.<time>.config before EMQX is started
-define(MERGED_CONFIGS, [
    emqx_bridge_schema,
    emqx_retainer_schema,
    emqx_authn_schema,
    emqx_authz_schema,
    emqx_auto_subscribe_schema,
    {emqx_telemetry_schema, ce},
    emqx_modules_schema,
    emqx_plugins_schema,
    emqx_dashboard_schema,
    emqx_gateway_schema,
    emqx_prometheus_schema,
    emqx_rule_engine_schema,
    emqx_exhook_schema,
    emqx_psk_schema,
    emqx_limiter_schema,
    emqx_slow_subs_schema,
    emqx_mgmt_api_key_schema
]).

%% root config should not have a namespace
namespace() -> undefined.

tags() ->
    [<<"EMQX">>].

roots() ->
    PtKey = ?EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY,
    case persistent_term:get(PtKey, undefined) of
        undefined -> persistent_term:put(PtKey, emqx_authn_schema);
        _ -> ok
    end,
    emqx_schema_high_prio_roots() ++
        [
            {"node",
                sc(
                    ?R_REF("node"),
                    #{translate_to => ["emqx"]}
                )},
            {"cluster",
                sc(
                    ?R_REF("cluster"),
                    #{translate_to => ["ekka"]}
                )},
            {"log",
                sc(
                    ?R_REF("log"),
                    #{
                        translate_to => ["kernel"],
                        importance => ?IMPORTANCE_HIGH
                    }
                )},
            {"rpc",
                sc(
                    ?R_REF("rpc"),
                    #{
                        translate_to => ["gen_rpc"],
                        importance => ?IMPORTANCE_LOW
                    }
                )}
        ] ++
        emqx_schema:roots(medium) ++
        emqx_schema:roots(low) ++
        lists:flatmap(fun roots/1, common_apps()).

validations() ->
    hocon_schema:validations(emqx_schema) ++
        lists:flatmap(fun hocon_schema:validations/1, common_apps()).

common_apps() ->
    Edition = emqx_release:edition(),
    lists:filtermap(
        fun
            ({N, E}) ->
                case E =:= Edition of
                    true -> {true, N};
                    false -> false
                end;
            (N) when is_atom(N) -> {true, N}
        end,
        ?MERGED_CONFIGS
    ).

fields("cluster") ->
    [
        {"name",
            sc(
                atom(),
                #{
                    mapping => "ekka.cluster_name",
                    default => emqxcl,
                    desc => ?DESC(cluster_name),
                    'readOnly' => true
                }
            )},
        {"discovery_strategy",
            sc(
                hoconsc:enum([manual, static, dns, etcd, k8s, mcast]),
                #{
                    default => manual,
                    desc => ?DESC(cluster_discovery_strategy),
                    'readOnly' => true
                }
            )},
        {"core_nodes",
            sc(
                node_array(),
                #{
                    mapping => "mria.core_nodes",
                    default => [],
                    'readOnly' => true,
                    desc => ?DESC(db_core_nodes)
                }
            )},
        {"autoclean",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "ekka.cluster_autoclean",
                    default => <<"5m">>,
                    desc => ?DESC(cluster_autoclean),
                    'readOnly' => true
                }
            )},
        {"autoheal",
            sc(
                boolean(),
                #{
                    mapping => "ekka.cluster_autoheal",
                    default => true,
                    desc => ?DESC(cluster_autoheal),
                    'readOnly' => true
                }
            )},
        {"proto_dist",
            sc(
                hoconsc:enum([inet_tcp, inet6_tcp, inet_tls]),
                #{
                    mapping => "ekka.proto_dist",
                    default => inet_tcp,
                    'readOnly' => true,
                    desc => ?DESC(cluster_proto_dist)
                }
            )},
        {"static",
            sc(
                ?R_REF(cluster_static),
                #{}
            )},
        {"mcast",
            sc(
                ?R_REF(cluster_mcast),
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        {"dns",
            sc(
                ?R_REF(cluster_dns),
                #{}
            )},
        {"etcd",
            sc(
                ?R_REF(cluster_etcd),
                #{}
            )},
        {"k8s",
            sc(
                ?R_REF(cluster_k8s),
                #{}
            )}
    ];
fields(cluster_static) ->
    [
        {"seeds",
            sc(
                node_array(),
                #{
                    default => [],
                    desc => ?DESC(cluster_static_seeds),
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_mcast) ->
    [
        {"addr",
            sc(
                string(),
                #{
                    default => <<"239.192.0.1">>,
                    desc => ?DESC(cluster_mcast_addr),
                    'readOnly' => true
                }
            )},
        {"ports",
            sc(
                hoconsc:array(integer()),
                #{
                    default => [4369, 4370],
                    'readOnly' => true,
                    desc => ?DESC(cluster_mcast_ports)
                }
            )},
        {"iface",
            sc(
                string(),
                #{
                    default => <<"0.0.0.0">>,
                    desc => ?DESC(cluster_mcast_iface),
                    'readOnly' => true
                }
            )},
        {"ttl",
            sc(
                range(0, 255),
                #{
                    default => 255,
                    desc => ?DESC(cluster_mcast_ttl),
                    'readOnly' => true
                }
            )},
        {"loop",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(cluster_mcast_loop),
                    'readOnly' => true
                }
            )},
        {"sndbuf",
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"16KB">>,
                    desc => ?DESC(cluster_mcast_sndbuf),
                    'readOnly' => true
                }
            )},
        {"recbuf",
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"16KB">>,
                    desc => ?DESC(cluster_mcast_recbuf),
                    'readOnly' => true
                }
            )},
        {"buffer",
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"32KB">>,
                    desc => ?DESC(cluster_mcast_buffer),
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_dns) ->
    [
        {"name",
            sc(
                string(),
                #{
                    default => <<"localhost">>,
                    desc => ?DESC(cluster_dns_name),
                    'readOnly' => true
                }
            )},
        {"record_type",
            sc(
                hoconsc:enum([a, srv]),
                #{
                    default => a,
                    desc => ?DESC(cluster_dns_record_type),
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_etcd) ->
    [
        {"server",
            sc(
                emqx_schema:comma_separated_list(),
                #{
                    desc => ?DESC(cluster_etcd_server),
                    'readOnly' => true
                }
            )},
        {"prefix",
            sc(
                string(),
                #{
                    default => <<"emqxcl">>,
                    desc => ?DESC(cluster_etcd_prefix),
                    'readOnly' => true
                }
            )},
        {"node_ttl",
            sc(
                emqx_schema:duration(),
                #{
                    default => <<"1m">>,
                    'readOnly' => true,
                    desc => ?DESC(cluster_etcd_node_ttl)
                }
            )},
        {"ssl_options",
            sc(
                ?R_REF(emqx_schema, "ssl_client_opts"),
                #{
                    desc => ?DESC(cluster_etcd_ssl),
                    aliases => [ssl],
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_k8s) ->
    [
        {"apiserver",
            sc(
                string(),
                #{
                    default => <<"http://10.110.111.204:8080">>,
                    desc => ?DESC(cluster_k8s_apiserver),
                    'readOnly' => true
                }
            )},
        {"service_name",
            sc(
                string(),
                #{
                    default => <<"emqx">>,
                    desc => ?DESC(cluster_k8s_service_name),
                    'readOnly' => true
                }
            )},
        {"address_type",
            sc(
                hoconsc:enum([ip, dns, hostname]),
                #{
                    default => ip,
                    desc => ?DESC(cluster_k8s_address_type),
                    'readOnly' => true
                }
            )},
        {"namespace",
            sc(
                string(),
                #{
                    default => <<"default">>,
                    desc => ?DESC(cluster_k8s_namespace),
                    'readOnly' => true
                }
            )},
        {"suffix",
            sc(
                string(),
                #{
                    default => <<"pod.local">>,
                    'readOnly' => true,
                    desc => ?DESC(cluster_k8s_suffix)
                }
            )}
    ];
fields("node") ->
    [
        {"name",
            sc(
                string(),
                #{
                    default => <<"emqx@127.0.0.1">>,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIGH,
                    desc => ?DESC(node_name)
                }
            )},
        {"cookie",
            sc(
                string(),
                #{
                    mapping => "vm_args.-setcookie",
                    required => true,
                    'readOnly' => true,
                    sensitive => true,
                    desc => ?DESC(node_cookie),
                    importance => ?IMPORTANCE_HIGH,
                    converter => fun emqx_schema:password_converter/2
                }
            )},
        {"process_limit",
            sc(
                range(1024, 134217727),
                #{
                    mapping => "vm_args.+P",
                    desc => ?DESC(process_limit),
                    default => 2097152,
                    importance => ?IMPORTANCE_MEDIUM,
                    'readOnly' => true
                }
            )},
        {"max_ports",
            sc(
                range(1024, 134217727),
                #{
                    mapping => "vm_args.+Q",
                    desc => ?DESC(max_ports),
                    default => 1048576,
                    importance => ?IMPORTANCE_HIGH,
                    'readOnly' => true
                }
            )},
        {"dist_buffer_size",
            sc(
                range(1, 2097151),
                #{
                    mapping => "vm_args.+zdbbl",
                    desc => ?DESC(dist_buffer_size),
                    default => 8192,
                    importance => ?IMPORTANCE_LOW,
                    'readOnly' => true
                }
            )},
        {"max_ets_tables",
            sc(
                pos_integer(),
                #{
                    mapping => "vm_args.+e",
                    desc => ?DESC(max_ets_tables),
                    default => 262144,
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"data_dir",
            sc(
                string(),
                #{
                    required => true,
                    'readOnly' => true,
                    mapping => "emqx.data_dir",
                    %% for now, it's tricky to use a different data_dir
                    %% otherwise data paths in cluster config may differ
                    %% TODO: change configurable data file paths to relative
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(node_data_dir)
                }
            )},
        {"config_files",
            sc(
                hoconsc:array(string()),
                #{
                    mapping => "emqx.config_files",
                    importance => ?IMPORTANCE_HIDDEN,
                    required => false,
                    'readOnly' => true
                }
            )},
        {"global_gc_interval",
            sc(
                hoconsc:union([disabled, emqx_schema:duration()]),
                #{
                    mapping => "emqx_machine.global_gc_interval",
                    default => <<"15m">>,
                    desc => ?DESC(node_global_gc_interval),
                    importance => ?IMPORTANCE_LOW,
                    'readOnly' => true
                }
            )},
        {"crash_dump_file",
            sc(
                file(),
                #{
                    mapping => "vm_args.-env ERL_CRASH_DUMP",
                    desc => ?DESC(node_crash_dump_file),
                    default => crash_dump_file_default(),
                    importance => ?IMPORTANCE_HIDDEN,
                    converter => fun ensure_unicode_path/2,
                    'readOnly' => true
                }
            )},
        {"crash_dump_seconds",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "vm_args.-env ERL_CRASH_DUMP_SECONDS",
                    default => <<"30s">>,
                    desc => ?DESC(node_crash_dump_seconds),
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"crash_dump_bytes",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "vm_args.-env ERL_CRASH_DUMP_BYTES",
                    default => <<"100MB">>,
                    desc => ?DESC(node_crash_dump_bytes),
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"dist_net_ticktime",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "vm_args.-kernel net_ticktime",
                    default => <<"2m">>,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(node_dist_net_ticktime)
                }
            )},
        {"backtrace_depth",
            sc(
                integer(),
                #{
                    mapping => "emqx_machine.backtrace_depth",
                    default => 23,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(node_backtrace_depth)
                }
            )},
        {"applications",
            sc(
                emqx_schema:comma_separated_atoms(),
                #{
                    mapping => "emqx_machine.applications",
                    default => <<"">>,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(node_applications)
                }
            )},
        {"etc_dir",
            sc(
                string(),
                #{
                    desc => ?DESC(node_etc_dir),
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    deprecated => {since, "5.0.8"}
                }
            )},
        {"cluster_call",
            sc(
                ?R_REF("cluster_call"),
                #{
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"db_backend",
            sc(
                hoconsc:enum([mnesia, rlog]),
                #{
                    mapping => "mria.db_backend",
                    default => rlog,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(db_backend)
                }
            )},
        {"role",
            sc(
                hoconsc:enum([core, replicant]),
                #{
                    mapping => "mria.node_role",
                    default => core,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIGH,
                    aliases => [db_role],
                    desc => ?DESC(db_role)
                }
            )},
        {"rpc_module",
            sc(
                hoconsc:enum([gen_rpc, rpc]),
                #{
                    mapping => "mria.rlog_rpc_module",
                    default => gen_rpc,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(db_rpc_module)
                }
            )},
        {"tlog_push_mode",
            sc(
                hoconsc:enum([sync, async]),
                #{
                    mapping => "mria.tlog_push_mode",
                    default => async,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(db_tlog_push_mode)
                }
            )},
        {"default_shard_transport",
            sc(
                hoconsc:enum([gen_rpc, distr]),
                #{
                    mapping => "mria.shard_transport",
                    importance => ?IMPORTANCE_HIDDEN,
                    default => gen_rpc,
                    desc => ?DESC(db_default_shard_transport)
                }
            )},
        {"shard_transports",
            sc(
                map(shard, hoconsc:enum([gen_rpc, distr])),
                #{
                    desc => ?DESC(db_shard_transports),
                    importance => ?IMPORTANCE_HIDDEN,
                    mapping => "emqx_machine.custom_shard_transports",
                    default => #{}
                }
            )}
    ];
fields("cluster_call") ->
    [
        {"retry_interval",
            sc(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(cluster_call_retry_interval),
                    default => <<"1m">>
                }
            )},
        {"max_history",
            sc(
                range(1, 500),
                #{
                    desc => ?DESC(cluster_call_max_history),
                    default => 100
                }
            )},
        {"cleanup_interval",
            sc(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(cluster_call_cleanup_interval),
                    default => <<"5m">>
                }
            )}
    ];
fields("rpc") ->
    [
        {"mode",
            sc(
                hoconsc:enum([sync, async]),
                #{
                    default => async,
                    desc => ?DESC(rpc_mode)
                }
            )},
        {"protocol",
            sc(
                hoconsc:enum([tcp, ssl]),
                #{
                    mapping => "gen_rpc.driver",
                    aliases => [driver],
                    default => tcp,
                    desc => ?DESC(rpc_driver)
                }
            )},
        {"async_batch_size",
            sc(
                integer(),
                #{
                    mapping => "gen_rpc.max_batch_size",
                    default => 256,
                    desc => ?DESC(rpc_async_batch_size)
                }
            )},
        {"port_discovery",
            sc(
                hoconsc:enum([manual, stateless]),
                #{
                    mapping => "gen_rpc.port_discovery",
                    default => stateless,
                    desc => ?DESC(rpc_port_discovery)
                }
            )},
        {"tcp_server_port",
            sc(
                integer(),
                #{
                    mapping => "gen_rpc.tcp_server_port",
                    default => 5369,
                    desc => ?DESC(rpc_tcp_server_port)
                }
            )},
        {"ssl_server_port",
            sc(
                integer(),
                #{
                    mapping => "gen_rpc.ssl_server_port",
                    default => 5369,
                    desc => ?DESC(rpc_ssl_server_port)
                }
            )},
        {"tcp_client_num",
            sc(
                range(1, 256),
                #{
                    default => 10,
                    desc => ?DESC(rpc_tcp_client_num)
                }
            )},
        {"connect_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.connect_timeout",
                    default => <<"5s">>,
                    desc => ?DESC(rpc_connect_timeout)
                }
            )},
        {"certfile",
            sc(
                file(),
                #{
                    mapping => "gen_rpc.certfile",
                    converter => fun ensure_unicode_path/2,
                    desc => ?DESC(rpc_certfile)
                }
            )},
        {"keyfile",
            sc(
                file(),
                #{
                    mapping => "gen_rpc.keyfile",
                    converter => fun ensure_unicode_path/2,
                    desc => ?DESC(rpc_keyfile)
                }
            )},
        {"cacertfile",
            sc(
                file(),
                #{
                    mapping => "gen_rpc.cacertfile",
                    converter => fun ensure_unicode_path/2,
                    desc => ?DESC(rpc_cacertfile)
                }
            )},
        {"send_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.send_timeout",
                    default => <<"5s">>,
                    desc => ?DESC(rpc_send_timeout)
                }
            )},
        {"authentication_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.authentication_timeout",
                    default => <<"5s">>,
                    desc => ?DESC(rpc_authentication_timeout)
                }
            )},
        {"call_receive_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.call_receive_timeout",
                    default => <<"15s">>,
                    desc => ?DESC(rpc_call_receive_timeout)
                }
            )},
        {"socket_keepalive_idle",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "gen_rpc.socket_keepalive_idle",
                    default => <<"15m">>,
                    desc => ?DESC(rpc_socket_keepalive_idle)
                }
            )},
        {"socket_keepalive_interval",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "gen_rpc.socket_keepalive_interval",
                    default => <<"75s">>,
                    desc => ?DESC(rpc_socket_keepalive_interval)
                }
            )},
        {"socket_keepalive_count",
            sc(
                integer(),
                #{
                    mapping => "gen_rpc.socket_keepalive_count",
                    default => 9,
                    desc => ?DESC(rpc_socket_keepalive_count)
                }
            )},
        {"socket_sndbuf",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "gen_rpc.socket_sndbuf",
                    default => <<"1MB">>,
                    desc => ?DESC(rpc_socket_sndbuf)
                }
            )},
        {"socket_recbuf",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "gen_rpc.socket_recbuf",
                    default => <<"1MB">>,
                    desc => ?DESC(rpc_socket_recbuf)
                }
            )},
        {"socket_buffer",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "gen_rpc.socket_buffer",
                    default => <<"1MB">>,
                    desc => ?DESC(rpc_socket_buffer)
                }
            )},
        {"insecure_fallback",
            sc(
                boolean(),
                #{
                    mapping => "gen_rpc.insecure_auth_fallback_allowed",
                    default => true,
                    desc => ?DESC(rpc_insecure_fallback)
                }
            )}
    ];
fields("log") ->
    [
        {"console",
            sc(?R_REF("console_handler"), #{
                aliases => [console_handler],
                importance => ?IMPORTANCE_HIGH
            })},
        {"file",
            sc(
                ?UNION([
                    ?R_REF("log_file_handler"),
                    ?MAP(handler_name, ?R_REF("log_file_handler"))
                ]),
                #{
                    desc => ?DESC("log_file_handlers"),
                    converter => fun ensure_file_handlers/2,
                    default => #{<<"level">> => <<"warning">>},
                    aliases => [file_handlers],
                    importance => ?IMPORTANCE_HIGH
                }
            )}
    ];
fields("console_handler") ->
    log_handler_common_confs(console);
fields("log_file_handler") ->
    [
        {"to",
            sc(
                file(),
                #{
                    desc => ?DESC("log_file_handler_file"),
                    default => <<"${EMQX_LOG_DIR}/emqx.log">>,
                    aliases => [file],
                    importance => ?IMPORTANCE_HIGH,
                    converter => fun(Path, Opts) ->
                        emqx_schema:naive_env_interpolation(ensure_unicode_path(Path, Opts))
                    end
                }
            )},
        {"rotation_count",
            sc(
                range(1, 128),
                #{
                    aliases => [rotation],
                    default => 10,
                    converter => fun convert_rotation/2,
                    desc => ?DESC("log_rotation_count"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"rotation_size",
            sc(
                hoconsc:union([infinity, emqx_schema:bytesize()]),
                #{
                    default => <<"50MB">>,
                    desc => ?DESC("log_file_handler_max_size"),
                    aliases => [max_size],
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ] ++ log_handler_common_confs(file);
fields("log_overload_kill") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC("log_overload_kill_enable")
                }
            )},
        {"mem_size",
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"30MB">>,
                    desc => ?DESC("log_overload_kill_mem_size")
                }
            )},
        {"qlen",
            sc(
                pos_integer(),
                #{
                    default => 20000,
                    desc => ?DESC("log_overload_kill_qlen")
                }
            )},
        {"restart_after",
            sc(
                hoconsc:union([emqx_schema:timeout_duration_ms(), infinity]),
                #{
                    default => <<"5s">>,
                    desc => ?DESC("log_overload_kill_restart_after")
                }
            )}
    ];
fields("log_burst_limit") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC("log_burst_limit_enable")
                }
            )},
        {"max_count",
            sc(
                pos_integer(),
                #{
                    default => 10000,
                    desc => ?DESC("log_burst_limit_max_count")
                }
            )},
        {"window_time",
            sc(
                emqx_schema:duration(),
                #{
                    default => <<"1s">>,
                    desc => ?DESC("log_burst_limit_window_time")
                }
            )}
    ];
fields("authorization") ->
    emqx_schema:authz_fields() ++
        emqx_authz_schema:authz_fields().

desc("cluster") ->
    ?DESC("desc_cluster");
desc(cluster_static) ->
    ?DESC("desc_cluster_static");
desc(cluster_mcast) ->
    ?DESC("desc_cluster_mcast");
desc(cluster_dns) ->
    ?DESC("desc_cluster_dns");
desc(cluster_etcd) ->
    ?DESC("desc_cluster_etcd");
desc(cluster_k8s) ->
    ?DESC("desc_cluster_k8s");
desc("node") ->
    ?DESC("desc_node");
desc("cluster_call") ->
    ?DESC("desc_cluster_call");
desc("rpc") ->
    ?DESC("desc_rpc");
desc("log") ->
    ?DESC("desc_log");
desc("console_handler") ->
    ?DESC("desc_console_handler");
desc("log_file_handler") ->
    ?DESC("desc_log_file_handler");
desc("log_rotation") ->
    ?DESC("desc_log_rotation");
desc("log_overload_kill") ->
    ?DESC("desc_log_overload_kill");
desc("log_burst_limit") ->
    ?DESC("desc_log_burst_limit");
desc("authorization") ->
    ?DESC("desc_authorization");
desc(_) ->
    undefined.

translations() -> ["ekka", "kernel", "emqx", "gen_rpc", "prometheus"].

translation("ekka") ->
    [{"cluster_discovery", fun tr_cluster_discovery/1}];
translation("kernel") ->
    [
        {"logger_level", fun emqx_config_logger:tr_level/1},
        {"logger", fun emqx_config_logger:tr_handlers/1},
        {"error_logger", fun(_) -> silent end}
    ];
translation("emqx") ->
    [
        {"config_files", fun tr_config_files/1},
        {"cluster_override_conf_file", fun tr_cluster_override_conf_file/1},
        {"local_override_conf_file", fun tr_local_override_conf_file/1},
        {"cluster_hocon_file", fun tr_cluster_hocon_file/1}
    ];
translation("gen_rpc") ->
    [{"default_client_driver", fun tr_default_config_driver/1}];
translation("prometheus") ->
    [
        {"vm_dist_collector_metrics", fun tr_vm_dist_collector/1},
        {"mnesia_collector_metrics", fun tr_mnesia_collector/1},
        {"vm_statistics_collector_metrics", fun tr_vm_statistics_collector/1},
        {"vm_system_info_collector_metrics", fun tr_vm_system_info_collector/1},
        {"vm_memory_collector_metrics", fun tr_vm_memory_collector/1},
        {"vm_msacc_collector_metrics", fun tr_vm_msacc_collector/1}
    ].

tr_vm_dist_collector(Conf) ->
    metrics_enabled(conf_get("prometheus.vm_dist_collector", Conf, enabled)).

tr_mnesia_collector(Conf) ->
    metrics_enabled(conf_get("prometheus.mnesia_collector", Conf, enabled)).

tr_vm_statistics_collector(Conf) ->
    metrics_enabled(conf_get("prometheus.vm_statistics_collector", Conf, enabled)).

tr_vm_system_info_collector(Conf) ->
    metrics_enabled(conf_get("prometheus.vm_system_info_collector", Conf, enabled)).

tr_vm_memory_collector(Conf) ->
    metrics_enabled(conf_get("prometheus.vm_memory_collector", Conf, enabled)).

tr_vm_msacc_collector(Conf) ->
    metrics_enabled(conf_get("prometheus.vm_msacc_collector", Conf, enabled)).

metrics_enabled(enabled) -> all;
metrics_enabled(disabled) -> [].

tr_default_config_driver(Conf) ->
    conf_get("rpc.driver", Conf).

tr_config_files(_Conf) ->
    case os:getenv("EMQX_ETC_DIR") of
        false ->
            %% testing, or running emqx app as deps
            [filename:join([code:lib_dir(emqx), "etc", "emqx.conf"])];
        Dir ->
            [filename:join([Dir, "emqx.conf"])]
    end.

tr_cluster_override_conf_file(Conf) ->
    tr_conf_file(Conf, "cluster-override.conf").

tr_local_override_conf_file(Conf) ->
    tr_conf_file(Conf, "local-override.conf").

tr_cluster_hocon_file(Conf) ->
    tr_conf_file(Conf, "cluster.hocon").

tr_conf_file(Conf, Filename) ->
    DataDir = conf_get("node.data_dir", Conf),
    %% assert, this config is not nullable
    [_ | _] = DataDir,
    filename:join([DataDir, "configs", Filename]).

tr_cluster_discovery(Conf) ->
    Strategy = conf_get("cluster.discovery_strategy", Conf),
    {Strategy, filter(cluster_options(Strategy, Conf))}.

log_handler_common_confs(Handler) ->
    %% we rarely support dynamic defaults like this
    %% for this one, we have build-time default the same as runtime default
    %% so it's less tricky
    EnableValues =
        case Handler of
            console -> ["console", "both"];
            file -> ["file", "both", "", false]
        end,
    EnvValue = os:getenv("EMQX_DEFAULT_LOG_HANDLER"),
    Enable = lists:member(EnvValue, EnableValues),
    [
        {"level",
            sc(
                log_level(),
                #{
                    default => warning,
                    desc => ?DESC("common_handler_level"),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {"enable",
            sc(
                boolean(),
                #{
                    default => Enable,
                    desc => ?DESC("common_handler_enable"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"formatter",
            sc(
                hoconsc:enum([text, json]),
                #{
                    default => text,
                    desc => ?DESC("common_handler_formatter"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"time_offset",
            sc(
                string(),
                #{
                    default => <<"system">>,
                    desc => ?DESC("common_handler_time_offset"),
                    validator => fun validate_time_offset/1,
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"chars_limit",
            sc(
                hoconsc:union([unlimited, range(100, inf)]),
                #{
                    default => unlimited,
                    desc => ?DESC("common_handler_chars_limit"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"single_line",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC("common_handler_single_line"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"sync_mode_qlen",
            sc(
                non_neg_integer(),
                #{
                    default => 100,
                    desc => ?DESC("common_handler_sync_mode_qlen"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"drop_mode_qlen",
            sc(
                pos_integer(),
                #{
                    default => 3000,
                    desc => ?DESC("common_handler_drop_mode_qlen"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"flush_qlen",
            sc(
                pos_integer(),
                #{
                    default => 8000,
                    desc => ?DESC("common_handler_flush_qlen"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"overload_kill", sc(?R_REF("log_overload_kill"), #{importance => ?IMPORTANCE_HIDDEN})},
        {"burst_limit", sc(?R_REF("log_burst_limit"), #{importance => ?IMPORTANCE_HIDDEN})},
        {"supervisor_reports",
            sc(
                hoconsc:enum([error, progress]),
                #{
                    default => error,
                    desc => ?DESC("common_handler_supervisor_reports"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"max_depth",
            sc(
                hoconsc:union([unlimited, non_neg_integer()]),
                #{
                    default => 100,
                    desc => ?DESC("common_handler_max_depth"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ].

crash_dump_file_default() ->
    case os:getenv("EMQX_LOG_DIR") of
        false ->
            %% testing, or running emqx app as deps
            <<"log/erl_crash.dump">>;
        Dir ->
            unicode:characters_to_binary(filename:join([Dir, "erl_crash.dump"]), utf8)
    end.

%% utils
-spec conf_get(string() | [string()], hocon:config()) -> term().
conf_get(Key, Conf) -> emqx_schema:conf_get(Key, Conf).

conf_get(Key, Conf, Default) -> emqx_schema:conf_get(Key, Conf, Default).

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private return a list of keys in a parent field
-spec keys(string(), hocon:config()) -> [string()].
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

%% types

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

map(Name, Type) -> hoconsc:map(Name, Type).

cluster_options(static, Conf) ->
    [{seeds, conf_get("cluster.static.seeds", Conf, [])}];
cluster_options(mcast, Conf) ->
    {ok, Addr} = inet:parse_address(conf_get("cluster.mcast.addr", Conf)),
    {ok, Iface} = inet:parse_address(conf_get("cluster.mcast.iface", Conf)),
    Ports = conf_get("cluster.mcast.ports", Conf),
    [
        {addr, Addr},
        {ports, Ports},
        {iface, Iface},
        {ttl, conf_get("cluster.mcast.ttl", Conf, 1)},
        {loop, conf_get("cluster.mcast.loop", Conf, true)}
    ];
cluster_options(dns, Conf) ->
    [
        {name, conf_get("cluster.dns.name", Conf)},
        {type, conf_get("cluster.dns.record_type", Conf)}
    ];
cluster_options(etcd, Conf) ->
    Namespace = "cluster.etcd.ssl_options",
    SslOpts = fun(C) ->
        Options = keys(Namespace, C),
        lists:map(fun(Key) -> {to_atom(Key), conf_get([Namespace, Key], Conf)} end, Options)
    end,
    [
        {server, conf_get("cluster.etcd.server", Conf)},
        {prefix, conf_get("cluster.etcd.prefix", Conf, "emqxcl")},
        {node_ttl, conf_get("cluster.etcd.node_ttl", Conf, 60)},
        {ssl_options, filter(SslOpts(Conf))}
    ];
cluster_options(k8s, Conf) ->
    [
        {apiserver, conf_get("cluster.k8s.apiserver", Conf)},
        {service_name, conf_get("cluster.k8s.service_name", Conf)},
        {address_type, conf_get("cluster.k8s.address_type", Conf, ip)},
        {namespace, conf_get("cluster.k8s.namespace", Conf)},
        {suffix, conf_get("cluster.k8s.suffix", Conf, "")}
    ];
cluster_options(manual, _Conf) ->
    [].

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).

roots(Module) ->
    lists:map(fun({_BinName, Root}) -> Root end, hocon_schema:roots(Module)).

%% Like authentication schema, authorization schema is incomplete in emqx_schema
%% module, this function replaces the root filed "authorization" with a new schema
emqx_schema_high_prio_roots() ->
    Roots = emqx_schema:roots(high),
    Authz =
        {"authorization",
            sc(
                ?R_REF("authorization"),
                #{
                    desc => ?DESC(authorization),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
    lists:keyreplace("authorization", 1, Roots, Authz).

validate_time_offset(Offset) ->
    ValidTimeOffset = "^([\\-\\+][0-1][0-9]:[0-6][0-9]|system|utc)$",
    Error =
        "Invalid time offset, should be of format: +[hh]:[mm], "
        "i.e. +08:00 or -02:00",
    validator_string_re(Offset, ValidTimeOffset, Error).

validator_string_re(Val, RE, Error) ->
    try re:run(Val, RE) of
        nomatch -> {error, Error};
        _ -> ok
    catch
        _:_ -> {error, Error}
    end.

node_array() ->
    hoconsc:union([emqx_schema:comma_separated_atoms(), hoconsc:array(atom())]).

ensure_file_handlers(Conf, _Opts) ->
    FileFields = lists:flatmap(
        fun({F, Schema}) ->
            Alias = [atom_to_binary(A) || A <- maps:get(aliases, Schema, [])],
            [list_to_binary(F) | Alias]
        end,
        fields("log_file_handler")
    ),
    HandlersWithoutName = maps:with(FileFields, Conf),
    HandlersWithName = maps:without(FileFields, Conf),
    %% Make sure the handler with name is high priority than the handler without name
    emqx_utils_maps:deep_merge(#{<<"default">> => HandlersWithoutName}, HandlersWithName).

convert_rotation(undefined, _Opts) -> undefined;
convert_rotation(#{} = Rotation, _Opts) -> maps:get(<<"count">>, Rotation, 10);
convert_rotation(Count, _Opts) when is_integer(Count) -> Count.

ensure_unicode_path(undefined, _) ->
    undefined;
ensure_unicode_path(Path, #{make_serializable := true}) ->
    %% format back to serializable string
    unicode:characters_to_binary(Path, utf8);
ensure_unicode_path(Path, Opts) when is_binary(Path) ->
    case unicode:characters_to_list(Path, utf8) of
        {R, _, _} when R =:= error orelse R =:= incomplete ->
            throw({"bad_file_path_string", Path});
        PathStr ->
            ensure_unicode_path(PathStr, Opts)
    end;
ensure_unicode_path(Path, _) when is_list(Path) ->
    Path;
ensure_unicode_path(Path, _) ->
    throw({"not_string", Path}).
