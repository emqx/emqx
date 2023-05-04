%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%% erlfmt-ignore
-define(BASE_CONF,
    """
             node {
                name = \"emqx1@127.0.0.1\"
                cookie = \"emqxsecretcookie\"
                data_dir = \"data\"
             }
             cluster {
                name = emqxcl
                discovery_strategy = static
                static.seeds = ~p
                core_nodes = ~p
             }
    """).

array_nodes_test() ->
    ExpectNodes = ['emqx1@127.0.0.1', 'emqx2@127.0.0.1'],
    lists:foreach(
        fun(Nodes) ->
            ConfFile = to_bin(?BASE_CONF, [Nodes, Nodes]),
            {ok, Conf} = hocon:binary(ConfFile, #{format => richmap}),
            ConfList = hocon_tconf:generate(emqx_conf_schema, Conf),
            ClusterDiscovery = proplists:get_value(
                cluster_discovery, proplists:get_value(ekka, ConfList)
            ),
            ?assertEqual(
                {static, [{seeds, ExpectNodes}]},
                ClusterDiscovery,
                Nodes
            ),
            ?assertEqual(
                ExpectNodes,
                proplists:get_value(core_nodes, proplists:get_value(mria, ConfList)),
                Nodes
            )
        end,
        [["emqx1@127.0.0.1", "emqx2@127.0.0.1"], "emqx1@127.0.0.1, emqx2@127.0.0.1"]
    ),
    ok.

%% erlfmt-ignore
-define(OUTDATED_LOG_CONF,
    """
console_handler {
  burst_limit {
    enable = true
    max_count = 10000
    window_time = 1000
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
    mem_size = 31457280
    qlen = 20000
    restart_after = 5000
  }
  single_line = true
  supervisor_reports = error
  sync_mode_qlen = 100
  time_offset = system
}
file_handlers {
  default {
    burst_limit {
      enable = true
      max_count = 10000
      window_time = 1000
    }
    chars_limit = unlimited
    drop_mode_qlen = 3000
    enable = false
    file = \"log/emqx.log\"
    flush_qlen = 8000
    formatter = text
    level = warning
    max_depth = 100
    max_size = 52428800
    overload_kill {
      enable = true
      mem_size = 31457280
      qlen = 20000
      restart_after = 5000
    }
    rotation {count = 10, enable = true}
    single_line = true
    supervisor_reports = error
    sync_mode_qlen = 100
    time_offset = \"+01:00\"
  }
}
    """
).

outdated_log_test() ->
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1", "emqx1@127.0.0.1"]),
    Conf0 = <<BaseConf/binary, ?OUTDATED_LOG_CONF>>,
    {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
    ConfList = hocon_tconf:generate(emqx_conf_schema, ConfMap0),
    ct:pal("fff:~p", [ConfList]),
    Log = proplists:get_value(log, ConfList),
    Console = proplists:get_value(console, Log),
    File = proplists:get_value(file, Log),
    ?assertEqual(1, Console, {Console, File}),
    ok.

-define(NEW_LOG_CONF,
    ""
    "\n"
    "console {\n"
    "  enable = true\n"
    "  formatter = text\n"
    "  level = warning\n"
    "  time_offset = system\n"
    "}\n"
    "file {\n"
    "    enable = true\n"
    "    file = \"log/emqx.log\"\n"
    "    formatter = text\n"
    "    level = warning\n"
    "    rotation {count = 10, enable = true}\n"
    "    time_offset = \"+01:00\"\n"
    "  }\n"
    "file_handlers.default {\n"
    "    enable = false,\n"
    "    file = \"log/file_handlres_emqx.log\"\n"
    "  }\n"
    "}\n"
    "    "
    ""
).

%% erlfmt-ignore
-define(BASE_AUTHN_ARRAY,
    """
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
    """
).

-define(ERROR(Reason),
    {emqx_conf_schema, [
        #{
            kind := validation_error,
            reason := integrity_validation_failure,
            result := _,
            validation_name := Reason
        }
    ]}
).

authn_validations_test() ->
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1", "emqx1@127.0.0.1"]),

    OKHttps = to_bin(?BASE_AUTHN_ARRAY, [post, true, <<"https://127.0.0.1:8080">>]),
    Conf0 = <<BaseConf/binary, OKHttps/binary>>,
    {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
    ?assert(is_list(hocon_tconf:generate(emqx_conf_schema, ConfMap0))),

    OKHttp = to_bin(?BASE_AUTHN_ARRAY, [post, false, <<"http://127.0.0.1:8080">>]),
    Conf1 = <<BaseConf/binary, OKHttp/binary>>,
    {ok, ConfMap1} = hocon:binary(Conf1, #{format => richmap}),
    ?assert(is_list(hocon_tconf:generate(emqx_conf_schema, ConfMap1))),

    DisableSSLWithHttps = to_bin(?BASE_AUTHN_ARRAY, [post, false, <<"https://127.0.0.1:8080">>]),
    Conf2 = <<BaseConf/binary, DisableSSLWithHttps/binary>>,
    {ok, ConfMap2} = hocon:binary(Conf2, #{format => richmap}),
    ?assertThrow(
        ?ERROR(check_http_ssl_opts),
        hocon_tconf:generate(emqx_conf_schema, ConfMap2)
    ),

    BadHeader = to_bin(?BASE_AUTHN_ARRAY, [get, true, <<"https://127.0.0.1:8080">>]),
    Conf3 = <<BaseConf/binary, BadHeader/binary>>,
    {ok, ConfMap3} = hocon:binary(Conf3, #{format => richmap}),
    ?assertThrow(
        ?ERROR(check_http_headers),
        hocon_tconf:generate(emqx_conf_schema, ConfMap3)
    ),

    BadHeaderWithTuple = binary:replace(BadHeader, [<<"[">>, <<"]">>], <<"">>, [global]),
    Conf4 = <<BaseConf/binary, BadHeaderWithTuple/binary>>,
    {ok, ConfMap4} = hocon:binary(Conf4, #{format => richmap}),
    ?assertThrow(
        ?ERROR(check_http_headers),
        hocon_tconf:generate(emqx_conf_schema, ConfMap4)
    ),
    ok.

doc_gen_test() ->
    %% the json file too large to encode.
    {
        timeout,
        60,
        fun() ->
            Dir = "tmp",
            ok = filelib:ensure_dir(filename:join("tmp", foo)),
            I18nFile = filename:join([
                "_build",
                "test",
                "lib",
                "emqx_dashboard",
                "priv",
                "i18n.conf"
            ]),
            _ = emqx_conf:dump_schema(Dir, emqx_conf_schema, I18nFile),
            ok
        end
    }.

to_bin(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).
