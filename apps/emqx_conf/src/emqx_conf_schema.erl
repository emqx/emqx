%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-reflect_type([ log_level/0,
                file/0,
                cipher/0]).

-export([namespace/0, roots/0, fields/1, translations/0, translation/1, validations/0]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).

%% Static apps which merge their configs into the merged emqx.conf
%% The list can not be made a dynamic read at run-time as it is used
%% by nodetool to generate app.<time>.config before EMQX is started
-define(MERGED_CONFIGS,
        [ emqx_bridge_schema
        , emqx_retainer_schema
        , emqx_statsd_schema
        , emqx_authn_schema
        , emqx_authz_schema
        , emqx_auto_subscribe_schema
        , emqx_modules_schema
        , emqx_plugins_schema
        , emqx_dashboard_schema
        , emqx_gateway_schema
        , emqx_prometheus_schema
        , emqx_rule_engine_schema
        , emqx_exhook_schema
        , emqx_psk_schema
        , emqx_limiter_schema
        , emqx_connector_schema
        , emqx_slow_subs_schema
        ]).

namespace() -> undefined.

roots() ->
    PtKey = ?EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY,
    case persistent_term:get(PtKey, undefined) of
        undefined -> persistent_term:put(PtKey, emqx_authn_schema);
        _ -> ok
    end,
    emqx_schema_high_prio_roots() ++
    [ {"node",
       sc(ref("node"),
          #{ desc => "Node name, cookie, config & data directories "
                     "and the Erlang virtual machine (BEAM) boot parameters."
           })}
    , {"cluster",
       sc(ref("cluster"),
          #{ desc => "EMQX nodes can form a cluster to scale up the total capacity.<br>"
                     "Here holds the configs to instruct how individual nodes "
                     "can discover each other."
           })}
    , {"log",
       sc(ref("log"),
          #{ desc => "Configure logging backends (to console or to file), "
                     "and logging level for each logger backend."
           })}
    , {"rpc",
       sc(ref("rpc"),
          #{ desc => "EMQX uses a library called <code>gen_rpc</code> for "
                     "inter-broker communication.<br/>Most of the time the default config "
                     "should work, but in case you need to do performance "
                     "fine-turning or experiment a bit, this is where to look."
           })}
    , {"db",
       sc(ref("db"),
          #{ desc => "Settings of the embedded database."
           })}
    ] ++
    emqx_schema:roots(medium) ++
    emqx_schema:roots(low) ++
    lists:flatmap(fun roots/1, ?MERGED_CONFIGS).

validations() ->
    hocon_schema:validations(emqx_schema) ++
    lists:flatmap(fun hocon_schema:validations/1, ?MERGED_CONFIGS).

fields("cluster") ->
    [ {"name",
       sc(atom(),
          #{ mapping => "ekka.cluster_name"
           , default => emqxcl
           })}
    , {"discovery_strategy",
       sc(hoconsc:enum([manual, static, mcast, dns, etcd, k8s]),
          #{ default => manual
           })}
    , {"autoclean",
       sc(emqx_schema:duration(),
          #{ mapping => "ekka.cluster_autoclean"
           , default => "5m"
           })}
    , {"autoheal",
       sc(boolean(),
          #{ mapping => "ekka.cluster_autoheal"
           , default => true
           })}
    , {"static",
       sc(ref(cluster_static),
          #{})}
    , {"mcast",
       sc(ref(cluster_mcast),
          #{})}
    , {"proto_dist",
       sc(hoconsc:enum([inet_tcp, inet6_tcp, inet_tls]),
          #{ mapping => "ekka.proto_dist"
           , default => inet_tcp
           })}
    , {"dns",
       sc(ref(cluster_dns),
          #{})}
    , {"etcd",
       sc(ref(cluster_etcd),
          #{})}
    , {"k8s",
       sc(ref(cluster_k8s),
          #{})}
    ];

fields(cluster_static) ->
    [ {"seeds",
      sc(hoconsc:array(atom()),
         #{ default => []
          })}
    ];

fields(cluster_mcast) ->
    [ {"addr",
       sc(string(),
          #{ default => "239.192.0.1"
           })}
    , {"ports",
       sc(hoconsc:array(integer()),
          #{ default => [4369, 4370]
           })}
    , {"iface",
       sc(string(),
          #{ default => "0.0.0.0"
           })}
    , {"ttl",
       sc(range(0, 255),
          #{ default => 255
           })}
    , {"loop",
       sc(boolean(),
          #{ default => true
           })}
    , {"sndbuf",
       sc(emqx_schema:bytesize(),
          #{ default => "16KB"
           })}
    , {"recbuf",
       sc(emqx_schema:bytesize(),
          #{ default => "16KB"
           })}
    , {"buffer",
       sc(emqx_schema:bytesize(),
          #{ default =>"32KB"
           })}
    ];

fields(cluster_dns) ->
    [ {"name",
       sc(string(),
          #{ default => "localhost"
           })}
    , {"app",
       sc(string(),
          #{ default => "emqx"
           })}
    ];

fields(cluster_etcd) ->
    [ {"server",
       sc(emqx_schema:comma_separated_list(),
          #{})}
    , {"prefix",
       sc(string(),
          #{ default => "emqxcl"
           })}
    , {"node_ttl",
       sc(emqx_schema:duration(),
          #{ default => "1m"
           })}
    , {"ssl",
       sc(hoconsc:ref(emqx_schema, ssl_client_opts),
          #{})}
    ];

fields(cluster_k8s) ->
    [ {"apiserver",
       sc(string(),
          #{})}
    , {"service_name",
       sc(string(),
          #{ default => "emqx"
           })}
    , {"address_type",
       sc(hoconsc:enum([ip, dns, hostname]),
          #{})}
    , {"app_name",
       sc(string(),
          #{ default => "emqx"
           })}
    , {"namespace",
       sc(string(),
          #{ default => "default"
           })}
    , {"suffix",
       sc(string(),
          #{default => "pod.local"
           })}
    ];

fields("node") ->
    [ {"name",
       sc(string(),
          #{ default => "emqx@127.0.0.1",
             desc => "Unique name of the EMQX node. It must follow <code>%name%@FQDN</code> or
 <code>%name%@IP</code> format."
           })}
    , {"cookie",
       sc(string(),
          #{ mapping => "vm_args.-setcookie",
             default => "emqxsecretcookie",
             sensitive => true,
             desc => "Secret cookie is a random string that should be the same on all nodes in
 the given EMQX cluster, but unique per EMQX cluster. It is used to prevent EMQX nodes that
 belong to different clusters from accidentally connecting to each other."
           })}
    , {"data_dir",
       sc(string(),
          #{ required => true,
             mapping => "emqx.data_dir",
             desc =>
"""
Path to the persistent data directory.
Possible auto-created sub-directories are:
  - `mnesia/\<node_name>`: EMQX's built-in database directory.
    For example, `mnesia/emqx@127.0.0.1`.
    There should be only one such sub directory.
    Meaning, in case the node is to be renamed (to e.g. `emqx@10.0.1.1`),
    the old dir should be deleted first.
  - `configs`: Generated configs at boot time, and cluster/local override configs.
  - `patches`: Hot-patch beam files are to be placed here.
  - `trace`: Trace log files.

**NOTE**: One data dir cannot be shared by two or more EMQX nodes.
"""
           })}
    , {"config_files",
       sc(list(string()),
          #{ mapping => "emqx.config_files"
           , default => undefined
           , desc => "List of configuration files that are read during startup. The order is
 significant: later configuration files override the previous ones."
           })}
    , {"global_gc_interval",
       sc(emqx_schema:duration(),
         #{ mapping => "emqx_machine.global_gc_interval"
          , default => "15m"
          , desc => "Periodic garbage collection interval."
          })}
    , {"crash_dump_file",
       sc(file(),
          #{ mapping => "vm_args.-env ERL_CRASH_DUMP"
           , desc => "Location of the crash dump file"
           })}
    , {"crash_dump_seconds",
       sc(emqx_schema:duration_s(),
          #{ mapping => "vm_args.-env ERL_CRASH_DUMP_SECONDS"
           , default => "30s"
           , desc => "The number of seconds that the broker is allowed to spend writing
a crash dump"
           })}
    , {"crash_dump_bytes",
       sc(emqx_schema:bytesize(),
          #{ mapping => "vm_args.-env ERL_CRASH_DUMP_BYTES"
           , default => "100MB"
           , desc => "The maximum size of a crash dump file in bytes."
           })}
    , {"dist_net_ticktime",
       sc(emqx_schema:duration(),
          #{ mapping => "vm_args.-kernel net_ticktime"
           , default => "2m"
           , desc => "This is the approximate time an EMQX node may be unresponsive "
                     "until it is considered down and thereby disconnected."
           })}
    , {"dist_listen_min",
       sc(range(1024, 65535),
          #{ mapping => "kernel.inet_dist_listen_min"
           , default => 6369
           , desc => "Lower bound for the port range where EMQX broker "
                     "listens for peer connections."
           })}
    , {"dist_listen_max",
       sc(range(1024, 65535),
          #{ mapping => "kernel.inet_dist_listen_max"
           , default => 6369
           , desc => "Upper bound for the port range where EMQX broker "
                     "listens for peer connections."
           })}
    , {"backtrace_depth",
       sc(integer(),
          #{ mapping => "emqx_machine.backtrace_depth"
           , default => 23
           , desc => "Maximum depth of the call stack printed in error messages and
 <code>process_info</code>."
           })}
    , {"applications",
       sc(emqx_schema:comma_separated_atoms(),
          #{ mapping => "emqx_machine.applications"
           , default => []
           , desc => "List of Erlang applications that shall be rebooted when the EMQX broker joins
 the cluster."
           })}
    , {"etc_dir",
       sc(string(),
          #{ desc => "<code>etc</code> dir for the node"
           }
         )}
    , {"cluster_call",
      sc(ref("cluster_call"),
        #{
          }
        )}
    ];

fields("db") ->
    [ {"backend",
       sc(hoconsc:enum([mnesia, rlog]),
          #{ mapping => "mria.db_backend"
           , default => rlog
           , desc => """
Select the backend for the embedded database.<br/>
<code>rlog</code> is the default backend, a new experimental backend
that is suitable for very large clusters.<br/>
<code>mnesia</code> is a backend that offers decent performance in small clusters.
"""
           })}
    , {"role",
       sc(hoconsc:enum([core, replicant]),
          #{ mapping => "mria.node_role"
           , default => core
           , desc => """
Select a node role.<br/>
<code>core</code> nodes provide durability of the data, and take care of writes.
It is recommended to place core nodes in different racks or different availability zones.<br/>
<code>replicant</code> nodes are ephemeral worker nodes. Removing them from the cluster
doesn't affect database redundancy<br/>
It is recommended to have more replicant nodes than core nodes.<br/>
Note: this parameter only takes effect when the <code>backend</code> is set
to <code>rlog</code>.
"""
           })}
    , {"core_nodes",
       sc(emqx_schema:comma_separated_atoms(),
          #{ mapping => "mria.core_nodes"
           , default => []
           , desc => """
List of core nodes that the replicant will connect to.<br/>
Note: this parameter only takes effect when the <code>backend</code> is set
to <code>rlog</code> and the <code>role</code> is set to <code>replicant</code>.<br/>
This values needs to be defined for manual or static cluster discovery mechanisms.<br/>
If an automatic cluster discovery mechanism is being used (such as <code>etcd</code>),
there is no need to set this value.
"""
           })}
    , {"rpc_module",
       sc(hoconsc:enum([gen_rpc, rpc]),
          #{ mapping => "mria.rlog_rpc_module"
           , default => gen_rpc
           , desc => """
Protocol used for pushing transaction logs to the replicant nodes.
"""
           })}
    , {"tlog_push_mode",
       sc(hoconsc:enum([sync, async]),
          #{ mapping => "mria.tlog_push_mode"
           , default => async
           , desc => """
In sync mode the core node waits for an ack from the replicant nodes before sending the next
transaction log entry.
"""
           })}
    ];

fields("cluster_call") ->
    [ {"retry_interval",
       sc(emqx_schema:duration(),
         #{ desc => "Time interval to retry after a failed call."
          , default => "1s"
          })}
    , {"max_history",
       sc(range(1, 500),
          #{ desc => "Retain the maximum number of completed transactions (for queries)."
           , default => 100
           })}
    , {"cleanup_interval",
       sc(emqx_schema:duration(),
          #{ desc =>
"Time interval to clear completed but stale transactions.
Ensure that the number of completed transactions is less than the max_history."
           , default => "5m"
           })}
    ];

fields("rpc") ->
    [ {"mode",
       sc(hoconsc:enum([sync, async]),
          #{ default => async
           , desc => "In <code>sync</code> mode the sending side waits for the ack from the "
                     "receiving side."
           })}
    , {"driver",
       sc(hoconsc:enum([tcp, ssl]),
          #{ mapping => "gen_rpc.driver"
           , default => tcp
           , desc => "Transport protocol used for inter-broker communication"
           })}
    , {"async_batch_size",
       sc(integer(),
          #{ mapping => "gen_rpc.max_batch_size"
           , default => 256
           , desc => "The maximum number of batch messages sent in asynchronous mode. "
                     "Note that this configuration does not work in synchronous mode."
           })}
    , {"port_discovery",
       sc(hoconsc:enum([manual, stateless]),
          #{ mapping => "gen_rpc.port_discovery"
           , default => stateless
           , desc => "<code>manual</code>: discover ports by <code>tcp_server_port</code>.<br/>"
                     "<code>stateless</code>: discover ports in a stateless manner, "
                     "using the following algorithm. "
                     "If node name is <code>emqxN@127.0.0.1</code>, where the N is an integer, "
                     "then the listening port will be 5370 + N."
           })}
    , {"tcp_server_port",
       sc(integer(),
          #{ mapping => "gen_rpc.tcp_server_port"
           , default => 5369
           , desc => "Listening port used by RPC local service.<br/> "
                     "Note that this config only takes effect when rpc.port_discovery "
                     "is set to manual."
           })}
    , {"ssl_server_port",
       sc(integer(),
          #{ mapping => "gen_rpc.ssl_server_port"
           , default => 5369
           , desc => "Listening port used by RPC local service.<br/> "
                     "Note that this config only takes effect when rpc.port_discovery "
                     "is set to manual and <code>driver</code> is set to <code>ssl</code>."
           })}
    , {"tcp_client_num",
       sc(range(1, 256),
          #{ default => 1
           , desc => "Set the maximum number of RPC communication channels initiated by this node "
                     "to each remote node."
           })}
    , {"connect_timeout",
       sc(emqx_schema:duration(),
          #{ mapping => "gen_rpc.connect_timeout"
           , default => "5s"
           , desc => "Timeout for establishing an RPC connection."
           })}
    , {"certfile",
       sc(file(),
          #{ mapping => "gen_rpc.certfile"
           , desc => "Path to TLS certificate file used to validate identity of the cluster nodes. "
                     "Note that this config only takes effect when <code>rpc.driver</code> "
                     "is set to <code>ssl</code>."
           })}
    , {"keyfile",
       sc(file(),
          #{ mapping => "gen_rpc.keyfile"
           , desc => "Path to the private key file for the <code>rpc.certfile</code>.<br/>"
                     "Note: contents of this file are secret, so "
                     "it's necessary to set permissions to 600."
           })}
    , {"cacertfile",
       sc(file(),
          #{ mapping => "gen_rpc.cacertfile"
           , desc => "Path to certification authority TLS certificate file used to validate "
                     "<code>rpc.certfile</code>.<br/>"
                     "Note: certificates of all nodes in the cluster must be signed by the same CA."
           })}
    , {"send_timeout",
       sc(emqx_schema:duration(),
          #{ mapping => "gen_rpc.send_timeout"
           , default => "5s"
           , desc => "Timeout for sending the RPC request."
           })}
    , {"authentication_timeout",
       sc(emqx_schema:duration(),
          #{ mapping=> "gen_rpc.authentication_timeout"
           , default => "5s"
           , desc => "Timeout for the remote node authentication."
           })}
    , {"call_receive_timeout",
       sc(emqx_schema:duration(),
          #{ mapping => "gen_rpc.call_receive_timeout"
           , default => "15s"
           , desc => "Timeout for the reply to a synchronous RPC."
           })}
    , {"socket_keepalive_idle",
       sc(emqx_schema:duration_s(),
          #{ mapping => "gen_rpc.socket_keepalive_idle"
           , default => "7200s"
           , desc => "How long the connections between the brokers "
                     "should remain open after the last message is sent."
           })}
    , {"socket_keepalive_interval",
       sc(emqx_schema:duration_s(),
          #{ mapping => "gen_rpc.socket_keepalive_interval"
           , default => "75s"
           , desc => "The interval between keepalive messages."
           })}
    , {"socket_keepalive_count",
       sc(integer(),
          #{ mapping => "gen_rpc.socket_keepalive_count"
           , default => 9
           , desc => "How many times the keepalive probe message can fail to receive a reply "
                     "until the RPC connection is considered lost."
           })}
    , {"socket_sndbuf",
       sc(emqx_schema:bytesize(),
          #{ mapping => "gen_rpc.socket_sndbuf"
           , default => "1MB"
           , desc => "TCP tuning parameters. TCP sending buffer size."
           })}
    , {"socket_recbuf",
       sc(emqx_schema:bytesize(),
          #{ mapping => "gen_rpc.socket_recbuf"
           , default => "1MB"
           , desc => "TCP tuning parameters. TCP receiving buffer size."
           })}
    , {"socket_buffer",
       sc(emqx_schema:bytesize(),
          #{ mapping => "gen_rpc.socket_buffer"
           , default => "1MB"
           , desc => "TCP tuning parameters. Socket buffer size in user mode."
           })}
    ];

fields("log") ->
    [ {"console_handler", ref("console_handler")}
    , {"file_handlers",
       sc(map(name, ref("log_file_handler")),
          #{})}
    , {"error_logger",
       sc(atom(),
          #{mapping => "kernel.error_logger",
            default => silent})}
    ];

fields("console_handler") ->
    log_handler_common_confs();

fields("log_file_handler") ->
    [ {"file",
       sc(file(),
          #{})}
    , {"rotation",
       sc(ref("log_rotation"),
          #{})}
    , {"max_size",
       sc(hoconsc:union([infinity, emqx_schema:bytesize()]),
          #{ default => "10MB"
           })}
    ] ++ log_handler_common_confs();

fields("log_rotation") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })}
    , {"count",
       sc(range(1, 2048),
          #{ default => 10
           })}
    ];

fields("log_overload_kill") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })}
    , {"mem_size",
       sc(emqx_schema:bytesize(),
          #{ default => "30MB"
           })}
    , {"qlen",
       sc(integer(),
          #{ default => 20000
           })}
    , {"restart_after",
       sc(hoconsc:union([emqx_schema:duration(), infinity]),
          #{ default => "5s"
           })}
    ];

fields("log_burst_limit") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })}
    , {"max_count",
       sc(integer(),
          #{ default => 10000
           })}
    , {"window_time",
       sc(emqx_schema:duration(),
          #{default => "1s"})}
    ];

fields("authorization") ->
    emqx_schema:fields("authorization") ++
    emqx_authz_schema:fields("authorization").

translations() -> ["ekka", "kernel", "emqx", "gen_rpc"].

translation("ekka") ->
    [ {"cluster_discovery", fun tr_cluster_discovery/1}];
translation("kernel") ->
    [ {"logger_level", fun tr_logger_level/1}
    , {"logger", fun tr_logger/1}];
translation("emqx") ->
    [ {"config_files", fun tr_config_files/1}
    , {"cluster_override_conf_file", fun tr_cluster_override_conf_file/1}
    , {"local_override_conf_file", fun tr_local_override_conf_file/1}
    ];
translation("gen_rpc") ->
    [ {"default_client_driver", fun tr_default_config_driver/1}
    ].

tr_default_config_driver(Conf) ->
    conf_get("rpc.driver", Conf).

tr_config_files(Conf) ->
    case conf_get("emqx.config_files", Conf) of
        [_ | _] = Files ->
            Files;
        _ ->
            case os:getenv("RUNNER_ETC_DIR") of
                false ->
                    [filename:join([code:lib_dir(emqx), "etc", "emqx.conf"])];
                Dir ->
                    [filename:join([Dir, "emqx.conf"])]
            end
    end.

tr_cluster_override_conf_file(Conf) ->
    tr_override_conf_file(Conf, "cluster-override.conf").

tr_local_override_conf_file(Conf) ->
    tr_override_conf_file(Conf, "local-override.conf").

tr_override_conf_file(Conf, Filename) ->
    DataDir = conf_get("node.data_dir", Conf),
    %% assert, this config is not nullable
    [_ | _] = DataDir,
    filename:join([DataDir, "configs", Filename]).

tr_cluster_discovery(Conf) ->
    Strategy = conf_get("cluster.discovery_strategy", Conf),
    {Strategy, filter(options(Strategy, Conf))}.

tr_logger_level(Conf) ->
    ConsoleLevel = conf_get("log.console_handler.level", Conf, undefined),
    FileLevels = [conf_get("level", SubConf) || {_, SubConf}
                    <- logger_file_handlers(Conf)],
    case FileLevels ++ [ConsoleLevel || ConsoleLevel =/= undefined] of
        [] -> warning; %% warning is the default level we should use
        Levels ->
            least_severe_log_level(Levels)
    end.

logger_file_handlers(Conf) ->
    Handlers = maps:to_list(conf_get("log.file_handlers", Conf, #{})),
    lists:filter(fun({_Name, Opts}) ->
                         B = conf_get("enable", Opts),
                         true = is_boolean(B),
                         B
                 end, Handlers).

tr_logger(Conf) ->
    %% For the default logger that outputs to console
    ConsoleHandler =
        case conf_get("log.console_handler.enable", Conf) of
            true ->
                ConsoleConf = conf_get("log.console_handler", Conf),
                [{handler, console, logger_std_h, #{
                    level => conf_get("log.console_handler.level", Conf),
                    config => (log_handler_conf(ConsoleConf)) #{type => standard_io},
                    formatter => log_formatter(ConsoleConf),
                    filters => log_filter(ConsoleConf)
                }}];
            false -> []
        end,
    %% For the file logger
    FileHandlers =
        [begin
         {handler, binary_to_atom(HandlerName, latin1), logger_disk_log_h, #{
                level => conf_get("level", SubConf),
                config => (log_handler_conf(SubConf)) #{
                    type => case conf_get("rotation.enable", SubConf) of
                                true -> wrap;
                                _ -> halt
                            end,
                    file => conf_get("file", SubConf),
                    max_no_files => conf_get("rotation.count", SubConf),
                    max_no_bytes => conf_get("max_size", SubConf)
                },
                formatter => log_formatter(SubConf),
                filters => log_filter(SubConf),
                filesync_repeat_interval => no_repeat
            }}
        end || {HandlerName, SubConf} <- logger_file_handlers(Conf)],
    [{handler, default, undefined}] ++ ConsoleHandler ++ FileHandlers.

log_handler_common_confs() ->
    [ {"enable",
       sc(boolean(),
          #{ default => false
           })}
    , {"level",
       sc(log_level(),
          #{ default => warning
           , desc => "Global log level. This includes the primary log level "
                     "and all log handlers."
           })}
    , {"time_offset",
       sc(string(),
          #{ default => "system"
           })}
    , {"chars_limit",
       sc(hoconsc:union([unlimited, range(1, inf)]),
          #{ default => unlimited
           , desc => "Set the maximum length of a single log message. "
                     "If this length is exceeded, the log message will be truncated."
           })}
    , {"formatter",
       sc(hoconsc:enum([text, json]),
          #{ default => text
           , desc => "Choose log format. <code>text</code> for free text, and "
                     "<code>json</code> for structured logging."
           })}
    , {"single_line",
       sc(boolean(),
          #{ default => true
           , desc => "Print logs in a single line if set to true. "
                     "Otherwise, log messages may span multiple lines."
           })}
    , {"sync_mode_qlen",
       sc(integer(),
          #{ default => 100
           })}
    , {"drop_mode_qlen",
       sc(integer(),
          #{ default => 3000
           })}
    , {"flush_qlen",
       sc(integer(),
          #{ default => 8000
           })}
    , {"overload_kill",
       sc(ref("log_overload_kill"),
          #{})}
    , {"burst_limit",
       sc(ref("log_burst_limit"),
          #{})}
    , {"supervisor_reports",
       sc(hoconsc:enum([error, progress]),
          #{ default => error
           })}
    , {"max_depth",
       sc(hoconsc:union([unlimited, integer()]),
          #{ default => 100
           , desc => "Maximum depth for Erlang term log formatting "
                     "and Erlang process message queue inspection."
           })}
    ].

log_handler_conf(Conf) ->
    SycModeQlen = conf_get("sync_mode_qlen", Conf),
    DropModeQlen = conf_get("drop_mode_qlen", Conf),
    FlushQlen = conf_get("flush_qlen", Conf),
    Overkill = conf_get("overload_kill", Conf),
    BurstLimit = conf_get("burst_limit", Conf),
    #{
        sync_mode_qlen => SycModeQlen,
        drop_mode_qlen => DropModeQlen,
        flush_qlen => FlushQlen,
        overload_kill_enable => conf_get("enable", Overkill),
        overload_kill_qlen => conf_get("qlen", Overkill),
        overload_kill_mem_size => conf_get("mem_size", Overkill),
        overload_kill_restart_after => conf_get("restart_after", Overkill),
        burst_limit_enable => conf_get("enable", BurstLimit),
        burst_limit_max_count => conf_get("max_count", BurstLimit),
        burst_limit_window_time => conf_get("window_time", BurstLimit)
    }.

log_formatter(Conf) ->
    CharsLimit = case conf_get("chars_limit", Conf) of
        unlimited -> unlimited;
        V when V > 0 -> V
    end,
    TimeOffSet = case conf_get("time_offset", Conf) of
        "system" -> "";
        "utc" -> 0;
        OffSetStr -> OffSetStr
    end,
    SingleLine = conf_get("single_line", Conf),
    Depth = conf_get("max_depth", Conf),
    do_formatter(conf_get("formatter", Conf), CharsLimit, SingleLine, TimeOffSet, Depth).

%% helpers
do_formatter(json, CharsLimit, SingleLine, TimeOffSet, Depth) ->
    {emqx_logger_jsonfmt,
        #{chars_limit => CharsLimit,
          single_line => SingleLine,
          time_offset => TimeOffSet,
          depth => Depth
        }};
do_formatter(text, CharsLimit, SingleLine, TimeOffSet, Depth) ->
    {emqx_logger_textfmt,
        #{template => [time," [",level,"] ", msg,"\n"],
          chars_limit => CharsLimit,
          single_line => SingleLine,
          time_offset => TimeOffSet,
          depth => Depth
        }}.

log_filter(Conf) ->
    case conf_get("supervisor_reports", Conf) of
        error -> [{drop_progress_reports, {fun logger_filters:progress/2, stop}}];
        progress -> []
    end.

least_severe_log_level(Levels) ->
    hd(sort_log_levels(Levels)).

sort_log_levels(Levels) ->
    lists:sort(fun(A, B) ->
            case logger:compare_levels(A, B) of
                R when R == lt; R == eq -> true;
                gt -> false
            end
        end, Levels).

%% utils
-spec(conf_get(string() | [string()], hocon:config()) -> term()).
conf_get(Key, Conf) ->
    ensure_list(hocon_maps:get(Key, Conf)).

conf_get(Key, Conf, Default) ->
    ensure_list(hocon_maps:get(Key, Conf, Default)).

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private return a list of keys in a parent field
-spec(keys(string(), hocon:config()) -> [string()]).
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

%% types

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

map(Name, Type) -> hoconsc:map(Name, Type).

ref(Field) -> hoconsc:ref(?MODULE, Field).

options(static, Conf) ->
    [{seeds, conf_get("cluster.static.seeds", Conf, [])}];
options(mcast, Conf) ->
    {ok, Addr} = inet:parse_address(conf_get("cluster.mcast.addr", Conf)),
    {ok, Iface} = inet:parse_address(conf_get("cluster.mcast.iface", Conf)),
    Ports = conf_get("cluster.mcast.ports", Conf),
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
        lists:map(fun(Key) -> {to_atom(Key), conf_get([Namespace, Key], Conf)} end, Options) end,
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

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).

-spec ensure_list(binary() | list(char())) -> list(char()).
ensure_list(V) ->
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

roots(Module) ->
    lists:map(fun({_BinName, Root}) -> Root end, hocon_schema:roots(Module)).

%% Like authentication schema, authorization schema is incomplete in emqx_schema
%% module, this function replaces the root filed "authorization" with a new schema
emqx_schema_high_prio_roots() ->
    Roots = emqx_schema:roots(high),
    Authz = {"authorization",
             sc(hoconsc:ref(?MODULE, "authorization"),
             #{ desc => """
Authorization a.k.a. ACL.<br>
In EMQX, MQTT client access control is extremely flexible.<br>
An out-of-the-box set of authorization data sources are supported.
For example,<br>
'file' source is to support concise and yet generic ACL rules in a file;<br>
'built_in_database' source can be used to store per-client customizable rule sets,
natively in the EMQX node;<br>
'http' source to make EMQX call an external HTTP API to make the decision;<br>
'PostgreSQL' etc. to look up clients or rules from external databases;<br>
""" })},
    lists:keyreplace("authorization", 1, Roots, Authz).
