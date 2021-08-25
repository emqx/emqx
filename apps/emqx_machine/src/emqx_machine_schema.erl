%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").

-type log_level() :: debug | info | notice | warning | error | critical | alert | emergency | all.
-type file() :: string().
-type cipher() :: map().

-behaviour(hocon_schema).

-reflect_type([ log_level/0,
                file/0,
                cipher/0]).

-export([structs/0, fields/1, translations/0, translation/1]).
-export([t/1, t/3, t/4, ref/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).

%% Static apps which merge their configs into the merged emqx.conf
%% The list can not be made a dynamic read at run-time as it is used
%% by nodetool to generate app.<time>.config before EMQ X is started
-define(MERGED_CONFIGS,
        [ emqx_schema
        , emqx_data_bridge_schema
        , emqx_retainer_schema
        , emqx_statsd_schema
        , emqx_authn_schema
        , emqx_authz_schema
        , emqx_bridge_mqtt_schema
        , emqx_modules_schema
        , emqx_management_schema
        , emqx_dashboard_schema
        , emqx_gateway_schema
        , emqx_prometheus_schema
        , emqx_rule_engine_schema
        , emqx_exhook_schema
        ]).

%% TODO: add a test case to ensure the list elements are unique
structs() ->
    ["cluster", "node", "rpc", "log"]
    ++ lists:flatmap(fun(Mod) -> Mod:structs() end, ?MERGED_CONFIGS).

fields("cluster") ->
    [ {"name", t(atom(), "ekka.cluster_name", emqxcl)}
    , {"discovery_strategy", t(union([manual, static, mcast, dns, etcd, k8s]),
        undefined, manual)}
    , {"autoclean", t(emqx_schema:duration(), "ekka.cluster_autoclean", "5m")}
    , {"autoheal", t(boolean(), "ekka.cluster_autoheal", true)}
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
    [ {"seeds", t(hoconsc:array(string()), undefined, [])}];

fields("mcast") ->
    [ {"addr", t(string(), undefined, "239.192.0.1")}
    , {"ports", t(hoconsc:array(integer()), undefined, [4369, 4370])}
    , {"iface", t(string(), undefined, "0.0.0.0")}
    , {"ttl", t(range(0, 255), undefined, 255)}
    , {"loop", t(boolean(), undefined, true)}
    , {"sndbuf", t(emqx_schema:bytesize(), undefined, "16KB")}
    , {"recbuf", t(emqx_schema:bytesize(), undefined, "16KB")}
    , {"buffer", t(emqx_schema:bytesize(), undefined, "32KB")}
    ];

fields("dns") ->
    [ {"name", t(string(), undefined, "localhost")}
    , {"app", t(string(), undefined, "emqx")}];

fields("etcd") ->
    [ {"server", t(emqx_schema:comma_separated_list())}
    , {"prefix", t(string(), undefined, "emqxcl")}
    , {"node_ttl", t(emqx_schema:duration(), undefined, "1m")}
    , {"ssl", ref("etcd_ssl")}
    ];

fields("etcd_ssl") ->
    emqx_schema:ssl(#{});

fields("k8s") ->
    [ {"apiserver", t(string())}
    , {"service_name", t(string(), undefined, "emqx")}
    , {"address_type", t(union([ip, dns, hostname]))}
    , {"app_name", t(string(), undefined, "emqx")}
    , {"namespace", t(string(), undefined, "default")}
    , {"suffix", t(string(), undefined, "pod.local")}
    ];

fields("rlog") ->
    [ {"role", t(union([core, replicant]), "ekka.node_role", core)}
    , {"core_nodes", t(emqx_schema:comma_separated_atoms(), "ekka.core_nodes", [])}
    ];

fields("node") ->
    [ {"name", hoconsc:t(string(), #{default => "emqx@127.0.0.1",
                                     override_env => "EMQX_NODE_NAME"
                                    })}
    , {"cookie", hoconsc:t(string(), #{mapping => "vm_args.-setcookie",
                                       default => "emqxsecretcookie",
                                       sensitive => true,
                                       override_env => "EMQX_NODE_COOKIE"
                                      })}
    , {"data_dir", hoconsc:t(string(), #{nullable => false})}
    , {"config_files", t(list(string()), "emqx.config_files", undefined)}
    , {"global_gc_interval", t(emqx_schema:duration(), undefined, "15m")}
    , {"crash_dump_dir", t(file(), "vm_args.-env ERL_CRASH_DUMP", undefined)}
    , {"dist_net_ticktime", t(emqx_schema:duration(), "vm_args.-kernel net_ticktime", "2m")}
    , {"dist_listen_min", t(range(1024, 65535), "kernel.inet_dist_listen_min", 6369)}
    , {"dist_listen_max", t(range(1024, 65535), "kernel.inet_dist_listen_max", 6369)}
    , {"backtrace_depth", t(integer(), "emqx_machine.backtrace_depth", 23)}
    ];

fields("rpc") ->
    [ {"mode", t(union(sync, async), undefined, async)}
    , {"async_batch_size", t(integer(), "gen_rpc.max_batch_size", 256)}
    , {"port_discovery",t(union(manual, stateless), "gen_rpc.port_discovery", stateless)}
    , {"tcp_server_port", t(integer(), "gen_rpc.tcp_server_port", 5369)}
    , {"tcp_client_num", t(range(1, 256), undefined, 1)}
    , {"connect_timeout", t(emqx_schema:duration(), "gen_rpc.connect_timeout", "5s")}
    , {"send_timeout", t(emqx_schema:duration(), "gen_rpc.send_timeout", "5s")}
    , {"authentication_timeout", t(emqx_schema:duration(), "gen_rpc.authentication_timeout", "5s")}
    , {"call_receive_timeout", t(emqx_schema:duration(), "gen_rpc.call_receive_timeout", "15s")}
    , {"socket_keepalive_idle", t(emqx_schema:duration_s(), "gen_rpc.socket_keepalive_idle", "7200s")}
    , {"socket_keepalive_interval", t(emqx_schema:duration_s(), "gen_rpc.socket_keepalive_interval", "75s")}
    , {"socket_keepalive_count", t(integer(), "gen_rpc.socket_keepalive_count", 9)}
    , {"socket_sndbuf", t(emqx_schema:bytesize(), "gen_rpc.socket_sndbuf", "1MB")}
    , {"socket_recbuf", t(emqx_schema:bytesize(), "gen_rpc.socket_recbuf", "1MB")}
    , {"socket_buffer", t(emqx_schema:bytesize(), "gen_rpc.socket_buffer", "1MB")}
    ];

fields("log") ->
    [ {"console_handler", ref("console_handler")}
    , {"file_handlers", ref("file_handlers")}
    , {"error_logger", t(atom(), "kernel.error_logger", silent)}
    ];

fields("console_handler") ->
    [ {"enable", t(boolean(), undefined, false)}
    ] ++ log_handler_common_confs();

fields("file_handlers") ->
    [ {"$name", ref("log_file_handler")}
    ];

fields("log_file_handler") ->
    [ {"file", t(file(), undefined, undefined)}
    , {"rotation", ref("log_rotation")}
    , {"max_size", #{type => union([infinity, emqx_schema:bytesize()]),
                     default => "10MB"}}
    ] ++ log_handler_common_confs();

fields("log_rotation") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"count", t(range(1, 2048), undefined, 10)}
    ];

fields("log_overload_kill") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"mem_size", t(emqx_schema:bytesize(), undefined, "30MB")}
    , {"qlen", t(integer(), undefined, 20000)}
    , {"restart_after", t(union(emqx_schema:duration(), infinity), undefined, "5s")}
    ];

fields("log_burst_limit") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_count", t(integer(), undefined, 10000)}
    , {"window_time", t(emqx_schema:duration(), undefined, "1s")}
    ];

fields(Name) ->
    find_field(Name, ?MERGED_CONFIGS).

find_field(Name, []) ->
    error({unknown_config_struct_field, Name});
find_field(Name, [SchemaModule | Rest]) ->
    case lists:member(Name, SchemaModule:structs()) orelse
        lists:keymember(Name, 2, SchemaModule:structs()) of
        true -> SchemaModule:fields(Name);
        false -> find_field(Name, Rest)
    end.

translations() -> ["ekka", "kernel", "emqx"].

translation("ekka") ->
    [ {"cluster_discovery", fun tr_cluster__discovery/1}];
translation("kernel") ->
    [ {"logger_level", fun tr_logger_level/1}
    , {"logger", fun tr_logger/1}];
translation("emqx") ->
    [ {"config_files", fun tr_config_files/1}
    , {"override_conf_file", fun tr_override_conf_fie/1}
    ].

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

tr_override_conf_fie(Conf) ->
    DataDir = conf_get("node.data_dir", Conf),
    %% assert, this config is not nullable
    [_ | _] = DataDir,
    filename:join([DataDir, "emqx_override.conf"]).

tr_cluster__discovery(Conf) ->
    Strategy = conf_get("cluster.discovery_strategy", Conf),
    {Strategy, filter(options(Strategy, Conf))}.

tr_logger_level(Conf) ->
    ConsoleLevel = conf_get("log.console_handler.level", Conf, undefined),
    FileLevels = [conf_get("level", SubConf) || {_, SubConf}
                    <- maps:to_list(conf_get("log.file_handlers", Conf, #{}))],
    case FileLevels ++ [ConsoleLevel || ConsoleLevel =/= undefined] of
        [] -> warning; %% warning is the default level we should use
        Levels ->
            least_severe_log_level(Levels)
    end.

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
        end || {HandlerName, SubConf} <- maps:to_list(conf_get("log.file_handlers", Conf, #{}))],

    [{handler, default, undefined}] ++ ConsoleHandler ++ FileHandlers.

log_handler_common_confs() ->
    [ {"level", t(log_level(), undefined, warning)}
    , {"time_offset", t(string(), undefined, "system")}
    , {"chars_limit", #{type => hoconsc:union([unlimited, range(1, inf)]),
                        default => unlimited
                       }}
    , {"formatter", t(union([text, json]), undefined, text)}
    , {"single_line", t(boolean(), undefined, true)}
    , {"sync_mode_qlen", t(integer(), undefined, 100)}
    , {"drop_mode_qlen", t(integer(), undefined, 3000)}
    , {"flush_qlen", t(integer(), undefined, 8000)}
    , {"overload_kill", ref("log_overload_kill")}
    , {"burst_limit", ref("log_burst_limit")}
    , {"supervisor_reports", t(union([error, progress]), undefined, error)}
    , {"max_depth", t(union([unlimited, integer()]), undefined, 100)}
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
    V = hocon_schema:get_value(Key, Conf),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

conf_get(Key, Conf, Default) ->
    V = hocon_schema:get_value(Key, Conf, Default),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private return a list of keys in a parent field
-spec(keys(string(), hocon:config()) -> [string()]).
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

%% types

t(Type) -> hoconsc:t(Type).

t(Type, Mapping, Default) ->
    hoconsc:t(Type, #{mapping => Mapping, default => Default}).

t(Type, Mapping, Default, OverrideEnv) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     }).

ref(Field) -> hoconsc:t(hoconsc:ref(Field)).

options(static, Conf) ->
    [{seeds, [to_atom(S) || S <- conf_get("cluster.static.seeds", Conf, [])]}];
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
