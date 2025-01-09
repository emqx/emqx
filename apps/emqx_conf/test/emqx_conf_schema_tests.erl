%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%% erlfmt-ignore
-define(BASE_CONF,
    "
             node {
                name = \"emqx1@127.0.0.1\"
                cookie = \"emqxsecretcookie\"
                data_dir = \"data\"
                max_ports = 2048
                process_limit = 10240
             }
             cluster {
                name = emqxcl
                discovery_strategy = static
                static.seeds = ~p
             }
    ").

array_nodes_test() ->
    ensure_acl_conf(),
    ExpectNodes = ['emqx1@127.0.0.1', 'emqx2@127.0.0.1'],
    lists:foreach(
        fun(Nodes) ->
            ConfFile = to_bin(?BASE_CONF, [Nodes]),
            {ok, Conf} = hocon:binary(ConfFile, #{format => richmap}),
            ConfList = hocon_tconf:generate(emqx_conf_schema, Conf),
            VMArgs = proplists:get_value(vm_args, ConfList),
            ProcLimit = proplists:get_value('+P', VMArgs),
            MaxPort = proplists:get_value('+Q', VMArgs),
            ?assertEqual(2048, MaxPort),
            ?assertEqual(MaxPort * 2, ProcLimit),

            ClusterDiscovery = proplists:get_value(
                cluster_discovery, proplists:get_value(ekka, ConfList)
            ),
            ?assertEqual(
                {static, [{seeds, ExpectNodes}]},
                ClusterDiscovery,
                Nodes
            )
        end,
        [["emqx1@127.0.0.1", "emqx2@127.0.0.1"], "emqx1@127.0.0.1, emqx2@127.0.0.1"]
    ),
    ok.

%% erlfmt-ignore
-define(OUTDATED_LOG_CONF,
    "
log.console_handler {
  burst_limit {
    enable = true
    max_count = 10000
    window_time = 1s
  }
  chars_limit = unlimited
  drop_mode_qlen = 3000
  enable = true
  flush_qlen = 8000
  formatter = text
  level = warning
  max_depth = 100
  overload_kill {
    enable = true
    mem_size = \"30MB\"
    qlen = 20000
    restart_after = \"5s\"
  }
  single_line = true
  supervisor_reports = error
  sync_mode_qlen = 100
  time_offset = \"+02:00\"
}
log.file_handlers {
  default {
    burst_limit {
      enable = true
      max_count = 10000
      window_time = 1s
    }
    chars_limit = unlimited
    drop_mode_qlen = 3000
    enable = true
    file = \"log/my-emqx.log\"
    flush_qlen = 8000
    formatter = text
    level = debug
    max_depth = 100
    max_size = \"1024MB\"
    overload_kill {
      enable = true
      mem_size = \"30MB\"
      qlen = 20000
      restart_after = \"5s\"
    }
    rotation {count = 20, enable = true}
    single_line = true
    supervisor_reports = error
    sync_mode_qlen = 100
    time_offset = \"+01:00\"
  }
}
    "
).
-define(FORMATTER(TimeOffset),
    {emqx_logger_textfmt, #{
        chars_limit => unlimited,
        depth => 100,
        single_line => true,
        template => ["[", level, "] ", msg, "\n"],
        time_offset => TimeOffset,
        timestamp_format => auto,
        with_mfa => false,
        payload_encode => text
    }}
).

-define(FILTERS, [{drop_progress_reports, {fun logger_filters:progress/2, stop}}]).
-define(LOG_CONFIG,
    (begin
        #{
            burst_limit_enable => true,
            burst_limit_max_count => 10000,
            burst_limit_window_time => 1000,
            drop_mode_qlen => 3000,
            flush_qlen => 8000,
            overload_kill_enable => true,
            overload_kill_mem_size => 31457280,
            overload_kill_qlen => 20000,
            overload_kill_restart_after => 5000,
            sync_mode_qlen => 100
        }
    end)
).

outdated_log_test() ->
    validate_log(?OUTDATED_LOG_CONF).

validate_log(Conf) ->
    ensure_acl_conf(),
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1"]),
    Conf0 = <<BaseConf/binary, (list_to_binary(Conf))/binary>>,
    {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
    ConfList = hocon_tconf:generate(emqx_conf_schema, ConfMap0),
    Kernel = proplists:get_value(kernel, ConfList),

    ?assertEqual(silent, proplists:get_value(error_logger, Kernel)),
    ?assertEqual(debug, proplists:get_value(logger_level, Kernel)),
    Loggers = proplists:get_value(logger, Kernel),
    FileHandlers = lists:filter(fun(L) -> element(3, L) =:= logger_disk_log_h end, Loggers),
    FileHandler = lists:keyfind(default, 2, FileHandlers),
    ?assertEqual(
        {handler, default, logger_disk_log_h, #{
            config => ?LOG_CONFIG#{
                type => wrap,
                file => "log/my-emqx.log",
                max_no_bytes => 1073741824,
                max_no_files => 20
            },
            filesync_repeat_interval => no_repeat,
            filters => ?FILTERS,
            formatter => ?FORMATTER("+01:00"),
            level => debug
        }},
        FileHandler
    ),
    %% audit is an EE-only feature
    ?assertNot(lists:keyfind(emqx_audit, 2, FileHandlers)),
    ConsoleHandler = lists:keyfind(logger_std_h, 3, Loggers),
    ?assertEqual(
        {handler, console, logger_std_h, #{
            config => ?LOG_CONFIG#{type => standard_io},
            filters => ?FILTERS,
            formatter => ?FORMATTER("+02:00"),
            level => warning
        }},
        ConsoleHandler
    ).

%% erlfmt-ignore
-define(FILE_LOG_BASE_CONF,
    "
    log.file.default {
        enable = true
        file = \"log/xx-emqx.log\"
        formatter = text
        level = debug
        rotation_count = ~s
        rotation_size = ~s
        time_offset = \"+01:00\"
      }
    "
).

file_log_infinity_rotation_size_test_() ->
    ensure_acl_conf(),
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1"]),
    Gen = fun(#{count := Count, size := Size}) ->
        Conf0 = to_bin(?FILE_LOG_BASE_CONF, [Count, Size]),
        Conf1 = [BaseConf, Conf0],
        {ok, Conf} = hocon:binary(Conf1, #{format => richmap}),
        ConfList = hocon_tconf:generate(emqx_conf_schema, Conf),
        Kernel = proplists:get_value(kernel, ConfList),
        Loggers = proplists:get_value(logger, Kernel),
        FileHandlers = lists:filter(fun(L) -> element(3, L) =:= logger_disk_log_h end, Loggers),
        lists:keyfind(default, 2, FileHandlers)
    end,
    [
        {"base conf: finite log (type = wrap)",
            ?_assertMatch(
                {handler, default, logger_disk_log_h, #{
                    config := #{
                        type := wrap,
                        max_no_bytes := 1073741824,
                        max_no_files := 20
                    }
                }},
                Gen(#{count => "20", size => "\"1024MB\""})
            )},
        {"rotation size = infinity (type = halt)",
            ?_assertMatch(
                {handler, default, logger_disk_log_h, #{
                    config := #{
                        type := halt,
                        max_no_bytes := infinity,
                        max_no_files := 9
                    }
                }},
                Gen(#{count => "9", size => "\"infinity\""})
            )}
    ].

%% erlfmt-ignore
-define(KERNEL_LOG_CONF,
    "
    log.console {
       enable = true
       formatter = text
       level = warning
       time_offset = \"+02:00\"
    }
    log.file {
        enable = false
        file = \"log/xx-emqx.log\"
        formatter = text
        level = debug
        rotation_count = 20
        rotation_size = \"1024MB\"
        time_offset = \"+01:00\"
      }
    log.file_handlers.default {
        enable = true
        file = \"log/my-emqx.log\"
      }
    "
).

log_test() ->
    validate_log(?KERNEL_LOG_CONF).

%% erlfmt-ignore
log_rotation_count_limit_test() ->
    ensure_acl_conf(),
    Format =
    "
    log.file {
    enable = true
    path = \"log/emqx.log\"
    formatter = text
    level = debug
    rotation = {count = ~w}
    rotation_size = \"1024MB\"
    }
    ",
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1"]),
    lists:foreach(fun({Conf, Count}) ->
        Conf0 = <<BaseConf/binary, Conf/binary>>,
        {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
        ConfList = hocon_tconf:generate(emqx_conf_schema, ConfMap0),
        Kernel = proplists:get_value(kernel, ConfList),
        Loggers = proplists:get_value(logger, Kernel),
        ?assertMatch(
            {handler, default, logger_disk_log_h, #{
                config := #{max_no_files := Count}
            }},
            lists:keyfind(default, 2, Loggers)
        )
                  end,
        [{to_bin(Format, [1]), 1}, {to_bin(Format, [128]), 128}]),
    lists:foreach(fun({Conf, Count}) ->
        Conf0 = <<BaseConf/binary, Conf/binary>>,
        {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
        ?assertThrow({emqx_conf_schema,
            [#{kind := validation_error,
            mismatches := #{"handler_name" :=
            #{kind := validation_error,
                path := "log.file.default.rotation_count",
                reason := #{expected := "1..128"},
                value := Count}
            }}]},
            hocon_tconf:generate(emqx_conf_schema, ConfMap0))
                  end, [{to_bin(Format, [0]), 0}, {to_bin(Format, [129]), 129}]).

%% erlfmt-ignore
-define(BASE_AUTHN_ARRAY,
    "
        authentication = [
          {backend = \"http\"
          body {password = \"${password}\", username = \"${username}\"}
          connect_timeout = \"15s\"
          enable_pipelining = 100
          headers {\"content-type\" = \"application/json\"}
          mechanism = \"password_based\"
          method = \"~p\"
          pool_size = 8
          request_timeout = \"5s\"
          ssl {enable = ~p, verify = \"verify_peer\"}
          url = \"~ts\"
        }
        ]
    "
).

-define(ERROR(Error),
    {emqx_conf_schema, [
        #{
            kind := validation_error,
            reason := #{error := Error}
        }
    ]}
).

authn_validations_test() ->
    ensure_acl_conf(),
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1"]),

    OKHttps = to_bin(?BASE_AUTHN_ARRAY, [post, true, <<"https://127.0.0.1:8080">>]),
    Conf0 = <<BaseConf/binary, OKHttps/binary>>,
    {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
    {_, Res0} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap0, #{format => richmap}),
    Headers0 = authentication_headers(Res0),
    ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Headers0)),
    %% accept from converter
    ?assertNot(maps:is_key(<<"accept">>, Headers0)),

    OKHttp = to_bin(?BASE_AUTHN_ARRAY, [post, false, <<"http://127.0.0.1:8080">>]),
    Conf1 = <<BaseConf/binary, OKHttp/binary>>,
    {ok, ConfMap1} = hocon:binary(Conf1, #{format => richmap}),
    {_, Res1} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap1, #{format => richmap}),
    Headers1 = authentication_headers(Res1),
    ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Headers1), Headers1),
    ?assertNot(maps:is_key(<<"accept">>, Headers1)),

    DisableSSLWithHttps = to_bin(?BASE_AUTHN_ARRAY, [post, false, <<"https://127.0.0.1:8080">>]),
    Conf2 = <<BaseConf/binary, DisableSSLWithHttps/binary>>,
    {ok, ConfMap2} = hocon:binary(Conf2, #{format => richmap}),
    ?assertThrow(
        ?ERROR(invalid_ssl_opts),
        hocon_tconf:map_translate(emqx_conf_schema, ConfMap2, #{format => richmap})
    ),

    BadHeader = to_bin(?BASE_AUTHN_ARRAY, [get, true, <<"https://127.0.0.1:8080">>]),
    Conf3 = <<BaseConf/binary, BadHeader/binary>>,
    {ok, ConfMap3} = hocon:binary(Conf3, #{format => richmap}),
    {_, Res3} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap3, #{format => richmap}),
    Headers3 = authentication_headers(Res3),
    %% remove the content-type header when get method
    ?assertNot(maps:is_key(<<"content-type">>, Headers3), Headers3),
    ?assertNot(maps:is_key(<<"accept">>, Headers3), Headers3),

    BadHeaderWithTuple = binary:replace(BadHeader, [<<"[">>, <<"]">>], <<"">>, [global]),
    Conf4 = <<BaseConf/binary, BadHeaderWithTuple/binary>>,
    {ok, ConfMap4} = hocon:binary(Conf4, #{format => richmap}),
    {_, Res4} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap4, #{}),
    Headers4 = authentication_headers(Res4),
    ?assertNot(maps:is_key(<<"content-type">>, Headers4), Headers4),
    ?assertNot(maps:is_key(<<"accept">>, Headers4), Headers4),
    ok.

%% erlfmt-ignore
-define(LISTENERS,
    "
        listeners.ssl.default.bind = 9999
        listeners.wss.default.bind = 9998
        listeners.wss.default.ssl_options.cacertfile = \"mytest/certs/cacert.pem\"
        listeners.wss.new.bind = 9997
        listeners.wss.new.websocket.mqtt_path = \"/my-mqtt\"
    "
).

listeners_test() ->
    ensure_acl_conf(),
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1"]),

    Conf = <<BaseConf/binary, ?LISTENERS>>,
    {ok, ConfMap0} = hocon:binary(Conf, #{format => richmap}),
    {_, ConfMap} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap0, #{format => richmap}),
    #{<<"listeners">> := Listeners} = hocon_util:richmap_to_map(ConfMap),
    #{
        <<"tcp">> := #{<<"default">> := Tcp},
        <<"ws">> := #{<<"default">> := Ws},
        <<"wss">> := #{<<"default">> := DefaultWss, <<"new">> := NewWss},
        <<"ssl">> := #{<<"default">> := Ssl}
    } = Listeners,
    DefaultCacertFile = <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
    DefaultCertFile = <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
    DefaultKeyFile = <<"${EMQX_ETC_DIR}/certs/key.pem">>,
    ?assertMatch(
        #{
            <<"bind">> := {{0, 0, 0, 0}, 1883},
            <<"enable">> := true
        },
        Tcp
    ),
    ?assertMatch(
        #{
            <<"bind">> := {{0, 0, 0, 0}, 8083},
            <<"enable">> := true,
            <<"websocket">> := #{<<"mqtt_path">> := "/mqtt"}
        },
        Ws
    ),
    ?assertMatch(
        #{
            <<"bind">> := 9999,
            <<"ssl_options">> := #{
                <<"cacertfile">> := DefaultCacertFile,
                <<"certfile">> := DefaultCertFile,
                <<"keyfile">> := DefaultKeyFile
            }
        },
        Ssl
    ),
    ?assertMatch(
        #{
            <<"bind">> := 9998,
            <<"websocket">> := #{<<"mqtt_path">> := "/mqtt"},
            <<"ssl_options">> :=
                #{
                    <<"cacertfile">> := <<"mytest/certs/cacert.pem">>,
                    <<"certfile">> := DefaultCertFile,
                    <<"keyfile">> := DefaultKeyFile
                }
        },
        DefaultWss
    ),
    ?assertMatch(
        #{
            <<"bind">> := 9997,
            <<"websocket">> := #{<<"mqtt_path">> := "/my-mqtt"},
            <<"ssl_options">> :=
                #{
                    <<"cacertfile">> := DefaultCacertFile,
                    <<"certfile">> := DefaultCertFile,
                    <<"keyfile">> := DefaultKeyFile
                }
        },
        NewWss
    ),
    ok.

authentication_headers(Conf) ->
    [#{<<"headers">> := Headers}] = hocon_maps:get("authentication", Conf),
    Headers.

doc_gen_test() ->
    ensure_acl_conf(),
    %% the json file too large to encode.
    {
        timeout,
        60,
        fun() ->
            Dir = "tmp",
            ok = emqx_conf:dump_schema(Dir, emqx_conf_schema)
        end
    }.

to_bin(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

ensure_acl_conf() ->
    File = emqx_schema:naive_env_interpolation(<<"${EMQX_ETC_DIR}/acl.conf">>),
    ok = filelib:ensure_dir(filename:dirname(File)),
    case filelib:is_regular(File) of
        true -> ok;
        false -> file:write_file(File, <<"">>)
    end.

log_path_test_() ->
    Fh = fun(Path) ->
        #{<<"log">> => #{<<"file_handlers">> => #{<<"name1">> => #{<<"file">> => Path}}}}
    end,
    Assert = fun(Name, Path, Conf) ->
        ?assertMatch(#{log := #{file := #{Name := #{path := Path}}}}, Conf)
    end,

    [
        {"default-values", fun() -> Assert(default, "${EMQX_LOG_DIR}/emqx.log", check(#{})) end},
        {"file path with space", fun() -> Assert(name1, "a /b", check(Fh(<<"a /b">>))) end},
        {"bad utf8", fun() ->
            ?assertThrow(
                {emqx_conf_schema, [
                    #{
                        kind := validation_error,
                        mismatches :=
                            #{
                                "handler_name" :=
                                    #{
                                        kind := validation_error,
                                        path := "log.file.name1.path",
                                        reason := {"bad_file_path_string", _}
                                    }
                            }
                    }
                ]},
                check(Fh(<<239, 32, 132, 47, 117, 116, 102, 56>>))
            )
        end},
        {"not string", fun() ->
            ?assertThrow(
                {emqx_conf_schema, [
                    #{
                        kind := validation_error,
                        mismatches :=
                            #{
                                "handler_name" :=
                                    #{
                                        kind := validation_error,
                                        path := "log.file.name1.path",
                                        reason := {"not_string", _}
                                    }
                            }
                    }
                ]},
                check(Fh(#{<<"foo">> => <<"bar">>}))
            )
        end}
    ].

check(Config) ->
    Schema = emqx_conf_schema,
    {_, Conf} = hocon_tconf:map(Schema, Config, [log], #{
        atom_key => false, required => false, format => map
    }),
    emqx_utils_maps:unsafe_atom_key_map(Conf).

with_file(Path, Content, F) ->
    ok = file:write_file(Path, Content),
    try
        F()
    after
        file:delete(Path)
    end.

load_and_check_test_() ->
    [
        {"non-existing file", fun() ->
            File = "/tmp/nonexistingfilename.hocon",
            ?assertEqual(
                {error, {enoent, File}},
                emqx_hocon:load_and_check(emqx_conf_schema, File)
            )
        end},
        {"bad syntax", fun() ->
            %% use abs path to match error return
            File = "/tmp/emqx-conf-bad-syntax-test.hocon",
            with_file(
                File,
                "{",
                fun() ->
                    ?assertMatch(
                        {error, #{file := File}},
                        emqx_hocon:load_and_check(emqx_conf_schema, File)
                    )
                end
            )
        end},
        {"type-check failure", fun() ->
            File = "emqx-conf-type-check-failure.hocon",
            %% typecheck fail because cookie is required field
            with_file(
                File,
                "node {}",
                fun() ->
                    ?assertMatch(
                        {error, #{
                            kind := validation_error,
                            path := "node.cookie",
                            reason := required_field
                        }},
                        emqx_hocon:load_and_check(emqx_conf_schema, File)
                    )
                end
            )
        end},
        {"ok load", fun() ->
            File = "emqx-conf-test-tmp-file-load-ok.hocon",
            with_file(File, "plugins: {}", fun() ->
                ?assertMatch({ok, _}, emqx_hocon:load_and_check(emqx_conf_schema, File))
            end)
        end}
    ].

%% erlfmt-ignore
dns_record_conf(NodeName, DnsRecordType) ->
    "
             node {
                name = \"" ++ NodeName ++ "\"
                data_dir = \"data\"
                cookie = cookie
                max_ports = 2048
                process_limit = 10240
             }
             cluster {
                name = emqxcl
                discovery_strategy = dns
                dns.record_type = " ++ atom_to_list(DnsRecordType) ++"
             }
    ".

a_record_with_non_ip_node_name_test_() ->
    Test = fun(DnsRecordType) ->
        {ok, ConfMap} = hocon:binary(dns_record_conf("emqx@local.host", DnsRecordType), #{
            format => map
        }),
        ?assertThrow(
            {emqx_conf_schema, [
                #{
                    reason := integrity_validation_failure,
                    result := #{domain := "local.host"},
                    kind := validation_error,
                    validation_name := check_node_name_and_discovery_strategy
                }
            ]},
            hocon_tconf:check_plain(emqx_conf_schema, ConfMap, #{required => false}, [node, cluster])
        )
    end,
    [
        {"a record", fun() -> Test(a) end},
        {"aaaa record", fun() -> Test(aaaa) end}
    ].

dns_record_type_incompatiblie_with_node_host_ip_format_test_() ->
    Test = fun(Ip, DnsRecordType) ->
        {ok, ConfMap} = hocon:binary(dns_record_conf("emqx@" ++ Ip, DnsRecordType), #{format => map}),
        ?assertThrow(
            {emqx_conf_schema, [
                #{
                    reason := integrity_validation_failure,
                    result := #{
                        record_type := DnsRecordType,
                        address_type := _
                    },
                    kind := validation_error,
                    validation_name := check_node_name_and_discovery_strategy
                }
            ]},
            hocon_tconf:check_plain(emqx_conf_schema, ConfMap, #{required => false}, [node, cluster])
        )
    end,
    [
        {"ipv4 address", fun() -> Test("::1", a) end},
        {"ipv6 address", fun() -> Test("127.0.0.1", aaaa) end}
    ].

dns_srv_record_is_ok_test() ->
    {ok, ConfMap} = hocon:binary(dns_record_conf("emqx@local.host", srv), #{format => map}),
    ?assertMatch(
        Value when is_map(Value),
        hocon_tconf:check_plain(emqx_conf_schema, ConfMap, #{required => false}, [node, cluster])
    ).

invalid_role_test() ->
    Conf = node_role_conf(dummy),
    ?assertThrow(
        {emqx_conf_schema, [#{reason := "Invalid node role: dummy"}]},
        hocon_tconf:check_plain(emqx_conf_schema, Conf, #{required => false}, [node])
    ).

unsupported_role_test() ->
    test_unsupported_role(emqx_release:edition()).

test_unsupported_role(ee) ->
    %% all roles are supported in ee
    ok;
test_unsupported_role(ce) ->
    %% replicant role is not allowed for ce since 5.8.0
    Conf = node_role_conf(replicant),
    ?assertThrow(
        {emqx_conf_schema, [
            #{reason := "Node role 'replicant' is only allowed in Enterprise edition since 5.8.0"}
        ]},
        hocon_tconf:check_plain(emqx_conf_schema, Conf, #{required => false}, [node])
    ).

node_role_conf(Role0) ->
    Role = atom_to_binary(Role0),
    Hocon = <<"node { role =", Role/binary, ", cookie = \"cookie\", data_dir = \".\" }">>,
    {ok, ConfMap} = hocon:binary(Hocon, #{format => map}),
    ConfMap.

fix_log_dir_path_test() ->
    ?assertEqual(
        "/opt/emqx/log/a.log",
        emqx_conf_schema:fix_bad_log_path("/opt/emqx/log/a.log")
    ),
    ?assertEqual(
        "/var/log/emqx/a.log",
        emqx_conf_schema:fix_bad_log_path("/var/log/emqx/a.log")
    ),
    ?assertEqual(
        "${SOMEDIR}/a.log",
        emqx_conf_schema:fix_bad_log_path("${SOMEDIR}/a.log")
    ),
    ?assertEqual(
        <<"${SOMEDIR}/a.log">>,
        emqx_conf_schema:fix_bad_log_path(<<"${SOMEDIR}/a.log">>)
    ),
    try
        os:putenv("EMQX_LOG_DIR", "foobar"),
        %% assumption: the two hard coded paths below do not exist in CT test runner
        ?assertEqual(
            "${EMQX_LOG_DIR}/a.log",
            emqx_conf_schema:fix_bad_log_path("/nosuchdir/a.log")
        ),
        %% binary in binary out
        ?assertEqual(
            <<"${EMQX_LOG_DIR}/a.log">>,
            emqx_conf_schema:fix_bad_log_path(<<"/nosuchdir/a.log">>)
        )
    after
        os:unsetenv("EMQX_LOG_DIR")
    end,
    ok.
