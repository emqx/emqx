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
log.console_handler {
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
  time_offset = \"+02:00\"
}
log.file_handlers {
  default {
    burst_limit {
      enable = true
      max_count = 10000
      window_time = 1000
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
      mem_size = 31457280
      qlen = 20000
      restart_after = 5000
    }
    rotation {count = 20, enable = true}
    single_line = true
    supervisor_reports = error
    sync_mode_qlen = 100
    time_offset = \"+01:00\"
  }
}
    """
).
-define(FORMATTER(TimeOffset),
    {emqx_logger_textfmt, #{
        chars_limit => unlimited,
        depth => 100,
        single_line => true,
        template => [time, " [", level, "] ", msg, "\n"],
        time_offset => TimeOffset
    }}
).

-define(FILTERS, [{drop_progress_reports, {fun logger_filters:progress/2, stop}}]).
-define(LOG_CONFIG, #{
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
}).

outdated_log_test() ->
    validate_log(?OUTDATED_LOG_CONF).

validate_log(Conf) ->
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1", "emqx1@127.0.0.1"]),
    Conf0 = <<BaseConf/binary, (list_to_binary(Conf))/binary>>,
    {ok, ConfMap0} = hocon:binary(Conf0, #{format => richmap}),
    ConfList = hocon_tconf:generate(emqx_conf_schema, ConfMap0),
    Kernel = proplists:get_value(kernel, ConfList),

    ?assertEqual(silent, proplists:get_value(error_logger, Kernel)),
    ?assertEqual(debug, proplists:get_value(logger_level, Kernel)),
    Loggers = proplists:get_value(logger, Kernel),
    FileHandler = lists:keyfind(logger_disk_log_h, 3, Loggers),
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
-define(KERNEL_LOG_CONF,
    """
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
    """
).

log_test() ->
    validate_log(?KERNEL_LOG_CONF).

%% erlfmt-ignore
log_rotation_count_limit_test() ->
    Format =
    """
    log.file {
    enable = true
    to = \"log/emqx.log\"
    formatter = text
    level = debug
    rotation = {count = ~w}
    rotation_size = \"1024MB\"
    }
    """,
    BaseConf = to_bin(?BASE_CONF, ["emqx1@127.0.0.1", "emqx1@127.0.0.1"]),
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
            lists:keyfind(logger_disk_log_h, 3, Loggers)
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
                reason := #{expected_type := "1..128"},
                value := Count}
            }}]},
            hocon_tconf:generate(emqx_conf_schema, ConfMap0))
                  end, [{to_bin(Format, [0]), 0}, {to_bin(Format, [129]), 129}]).

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
    {_, Res0} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap0, #{format => richmap}),
    Headers0 = authentication_headers(Res0),
    ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Headers0)),
    %% accept from converter
    ?assertEqual(<<"application/json">>, maps:get(<<"accept">>, Headers0)),

    OKHttp = to_bin(?BASE_AUTHN_ARRAY, [post, false, <<"http://127.0.0.1:8080">>]),
    Conf1 = <<BaseConf/binary, OKHttp/binary>>,
    {ok, ConfMap1} = hocon:binary(Conf1, #{format => richmap}),
    {_, Res1} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap1, #{format => richmap}),
    Headers1 = authentication_headers(Res1),
    ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Headers1), Headers1),
    ?assertEqual(<<"application/json">>, maps:get(<<"accept">>, Headers1), Headers1),

    DisableSSLWithHttps = to_bin(?BASE_AUTHN_ARRAY, [post, false, <<"https://127.0.0.1:8080">>]),
    Conf2 = <<BaseConf/binary, DisableSSLWithHttps/binary>>,
    {ok, ConfMap2} = hocon:binary(Conf2, #{format => richmap}),
    ?assertThrow(
        ?ERROR(check_http_ssl_opts),
        hocon_tconf:map_translate(emqx_conf_schema, ConfMap2, #{format => richmap})
    ),

    BadHeader = to_bin(?BASE_AUTHN_ARRAY, [get, true, <<"https://127.0.0.1:8080">>]),
    Conf3 = <<BaseConf/binary, BadHeader/binary>>,
    {ok, ConfMap3} = hocon:binary(Conf3, #{format => richmap}),
    {_, Res3} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap3, #{format => richmap}),
    Headers3 = authentication_headers(Res3),
    %% remove the content-type header when get method
    ?assertEqual(false, maps:is_key(<<"content-type">>, Headers3), Headers3),
    ?assertEqual(<<"application/json">>, maps:get(<<"accept">>, Headers3), Headers3),

    BadHeaderWithTuple = binary:replace(BadHeader, [<<"[">>, <<"]">>], <<"">>, [global]),
    Conf4 = <<BaseConf/binary, BadHeaderWithTuple/binary>>,
    {ok, ConfMap4} = hocon:binary(Conf4, #{format => richmap}),
    {_, Res4} = hocon_tconf:map_translate(emqx_conf_schema, ConfMap4, #{}),
    Headers4 = authentication_headers(Res4),
    ?assertEqual(false, maps:is_key(<<"content-type">>, Headers4), Headers4),
    ?assertEqual(<<"application/json">>, maps:get(<<"accept">>, Headers4), Headers4),
    ok.

authentication_headers(Conf) ->
    [#{<<"headers">> := Headers}] = hocon_maps:get("authentication", Conf),
    Headers.

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
