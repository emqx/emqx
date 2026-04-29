%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb_tests).

-include_lib("eunit/include/eunit.hrl").

-define(INVALID_LINES, [
    "   ",
    " \n",
    "  \n\n\n  ",
    "\n",
    "  \n\n   \n  \n",
    "measurement",
    "measurement ",
    "measurement,tag",
    "measurement field",
    "measurement,tag field",
    "measurement,tag field ${timestamp}",
    "measurement,tag=",
    "measurement,tag=tag1",
    "measurement,tag =",
    "measurement field=",
    "measurement field= ",
    "measurement field = ",
    "measurement, tag = field = ",
    "measurement, tag = field = ",
    "measurement, tag = tag_val field = field_val",
    "measurement, tag = tag_val field = field_val ${timestamp}",
    "measurement,= = ${timestamp}",
    "measurement,t=a, f=a, ${timestamp}",
    "measurement,t=a,t1=b, f=a,f1=b, ${timestamp}",
    "measurement,t=a,t1=b, f=a,f1=b,",
    "measurement,t=a, t1=b, f=a,f1=b,",
    "measurement,t=a,,t1=b, f=a,f1=b,",
    "measurement,t=a,,t1=b f=a,,f1=b",
    "measurement,t=a,,t1=b f=a,f1=b ${timestamp}",
    "measurement, f=a,f1=b",
    "measurement, f=a,f1=b ${timestamp}",
    "measurement,, f=a,f1=b ${timestamp}",
    "measurement,, f=a,f1=b",
    "measurement,, f=a,f1=b,, ${timestamp}",
    "measurement f=a,f1=b,, ${timestamp}",
    "measurement,t=a f=a,f1=b,, ${timestamp}",
    "measurement,t=a f=a,f1=b,, ",
    "measurement,t=a f=a,f1=b,,",
    "measurement, t=a  f=a,f1=b",
    "measurement,t=a f=a, f1=b",
    "measurement,t=a f=a, f1=b ${timestamp}",
    "measurement, t=a  f=a, f1=b ${timestamp}",
    "measurement,t= a f=a,f1=b ${timestamp}",
    "measurement,t= a f=a,f1 =b ${timestamp}",
    "measurement, t = a f = a,f1 = b ${timestamp}",
    "measurement,t=a f=a,f1=b \n ${timestamp}",
    "measurement,t=a \n f=a,f1=b \n ${timestamp}",
    "measurement,t=a \n f=a,f1=b \n ",
    "\n measurement,t=a \n f=a,f1=b \n ${timestamp}",
    "\n measurement,t=a \n f=a,f1=b \n",
    %% not escaped backslash in a quoted field value is invalid
    "measurement,tag=1 field=\"val\\1\""
]).

influxdb_api_v1_connector_hocon() ->
    """
    connectors.influxdb.my_influxdb {
      enable = true
      server = "127.0.0.1:8086"
      parameters {
        influxdb_type = influxdb_api_v1
        database = "mydb"
        username = "user"
        password = "pass"
      }
      ssl {
        enable = false
      }
    }
    """.

influxdb_api_v2_connector_hocon() ->
    """
    connectors.influxdb.my_influxdb {
      enable = true
      server = "127.0.0.1:8086"
      parameters {
        influxdb_type = influxdb_api_v2
        bucket = "mybucket"
        org = "myorg"
        token = "token"
      }
      ssl {
        enable = false
      }
    }
    """.

influxdb_api_v3_connector_hocon() ->
    """
    connectors.influxdb.my_influxdb {
      enable = true
      server = "127.0.0.1:8086"
      parameters {
        influxdb_type = influxdb_api_v3
        database = "mydb"
        token = "token"
      }
      ssl {
        enable = false
      }
    }
    """.

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

check_connector(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_connector_schema, Conf, #{required => false}).

-define(VALID_LINE_PARSED_PAIRS, [
    {"m1,tag=tag1 field=field1 ${timestamp1}", #{
        measurement => "m1",
        tags => [{"tag", "tag1"}],
        fields => [{"field", "field1"}],
        timestamp => "${timestamp1}"
    }},
    {"m2,tag=tag2 field=field2", #{
        measurement => "m2",
        tags => [{"tag", "tag2"}],
        fields => [{"field", "field2"}],
        timestamp => undefined
    }},
    {"m3 field=field3 ${timestamp3}", #{
        measurement => "m3",
        tags => [],
        fields => [{"field", "field3"}],
        timestamp => "${timestamp3}"
    }},
    {"m4 field=field4", #{
        measurement => "m4",
        tags => [],
        fields => [{"field", "field4"}],
        timestamp => undefined
    }},
    {"m5,tag=tag5,tag_a=tag5a,tag_b=tag5b field=field5,field_a=field5a,field_b=field5b ${timestamp5}",
        #{
            measurement => "m5",
            tags => [{"tag", "tag5"}, {"tag_a", "tag5a"}, {"tag_b", "tag5b"}],
            fields => [{"field", "field5"}, {"field_a", "field5a"}, {"field_b", "field5b"}],
            timestamp => "${timestamp5}"
        }},
    {"m6,tag=tag6,tag_a=tag6a,tag_b=tag6b field=field6,field_a=field6a,field_b=field6b", #{
        measurement => "m6",
        tags => [{"tag", "tag6"}, {"tag_a", "tag6a"}, {"tag_b", "tag6b"}],
        fields => [{"field", "field6"}, {"field_a", "field6a"}, {"field_b", "field6b"}],
        timestamp => undefined
    }},
    {"m7,tag=tag7,tag_a=\"tag7a\",tag_b=tag7b field=\"field7\",field_a=field7a,field_b=\"field7b\"",
        #{
            measurement => "m7",
            tags => [{"tag", "tag7"}, {"tag_a", "\"tag7a\""}, {"tag_b", "tag7b"}],
            fields => [
                {"field", {quoted, "field7"}},
                {"field_a", "field7a"},
                {"field_b", {quoted, "field7b"}}
            ],
            timestamp => undefined
        }},
    {"m8,tag=tag8,tag_a=\"tag8a\",tag_b=tag8b field=\"field8\",field_a=field8a,field_b=\"field8b\" ${timestamp8}",
        #{
            measurement => "m8",
            tags => [{"tag", "tag8"}, {"tag_a", "\"tag8a\""}, {"tag_b", "tag8b"}],
            fields => [
                {"field", {quoted, "field8"}},
                {"field_a", "field8a"},
                {"field_b", {quoted, "field8b"}}
            ],
            timestamp => "${timestamp8}"
        }},
    {
        "m8a,tag=tag8,tag_a=\"${tag8a}\",tag_b=tag8b field=\"${field8}\","
        "field_a=field8a,field_b=\"${field8b}\" ${timestamp8}",
        #{
            measurement => "m8a",
            tags => [{"tag", "tag8"}, {"tag_a", "\"${tag8a}\""}, {"tag_b", "tag8b"}],
            fields => [
                {"field", {quoted, "${field8}"}},
                {"field_a", "field8a"},
                {"field_b", {quoted, "${field8b}"}}
            ],
            timestamp => "${timestamp8}"
        }
    },
    {"m9,tag=tag9,tag_a=\"tag9a\",tag_b=tag9b field=\"field9\",field_a=field9a,field_b=\"\" ${timestamp9}",
        #{
            measurement => "m9",
            tags => [{"tag", "tag9"}, {"tag_a", "\"tag9a\""}, {"tag_b", "tag9b"}],
            fields => [
                {"field", {quoted, "field9"}}, {"field_a", "field9a"}, {"field_b", {quoted, ""}}
            ],
            timestamp => "${timestamp9}"
        }},
    {"m10 field=\"\" ${timestamp10}", #{
        measurement => "m10",
        tags => [],
        fields => [{"field", {quoted, ""}}],
        timestamp => "${timestamp10}"
    }}
]).

-define(VALID_LINE_EXTRA_SPACES_PARSED_PAIRS, [
    {"\n  m1,tag=tag1  field=field1  ${timestamp1} \n", #{
        measurement => "m1",
        tags => [{"tag", "tag1"}],
        fields => [{"field", "field1"}],
        timestamp => "${timestamp1}"
    }},
    {"  m2,tag=tag2  field=field2  ", #{
        measurement => "m2",
        tags => [{"tag", "tag2"}],
        fields => [{"field", "field2"}],
        timestamp => undefined
    }},
    {" m3  field=field3   ${timestamp3}  ", #{
        measurement => "m3",
        tags => [],
        fields => [{"field", "field3"}],
        timestamp => "${timestamp3}"
    }},
    {" \n m4  field=field4\n ", #{
        measurement => "m4",
        tags => [],
        fields => [{"field", "field4"}],
        timestamp => undefined
    }},
    {" \n m5,tag=tag5,tag_a=tag5a,tag_b=tag5b   field=field5,field_a=field5a,field_b=field5b    ${timestamp5}  \n",
        #{
            measurement => "m5",
            tags => [{"tag", "tag5"}, {"tag_a", "tag5a"}, {"tag_b", "tag5b"}],
            fields => [{"field", "field5"}, {"field_a", "field5a"}, {"field_b", "field5b"}],
            timestamp => "${timestamp5}"
        }},
    {" m6,tag=tag6,tag_a=tag6a,tag_b=tag6b  field=field6,field_a=field6a,field_b=field6b\n  ", #{
        measurement => "m6",
        tags => [{"tag", "tag6"}, {"tag_a", "tag6a"}, {"tag_b", "tag6b"}],
        fields => [{"field", "field6"}, {"field_a", "field6a"}, {"field_b", "field6b"}],
        timestamp => undefined
    }}
]).

-define(VALID_LINE_PARSED_ESCAPED_CHARS_PAIRS, [
    {"m\\ =1\\,,\\,tag\\ \\==\\=tag\\ 1\\, \\,fie\\ ld\\ =\\ field\\,1 ${timestamp1}", #{
        measurement => "m =1,",
        tags => [{",tag =", "=tag 1,"}],
        fields => [{",fie ld ", " field,1"}],
        timestamp => "${timestamp1}"
    }},
    {"m2,tag=tag2 field=\"field \\\"2\\\",\n\"", #{
        measurement => "m2",
        tags => [{"tag", "tag2"}],
        fields => [{"field", {quoted, "field \"2\",\n"}}],
        timestamp => undefined
    }},
    {"m\\ 3 field=\"field3\" ${payload.timestamp\\ 3}", #{
        measurement => "m 3",
        tags => [],
        fields => [{"field", {quoted, "field3"}}],
        timestamp => "${payload.timestamp 3}"
    }},
    {"m4 field=\"\\\"field\\\\4\\\"\"", #{
        measurement => "m4",
        tags => [],
        fields => [{"field", {quoted, "\"field\\4\""}}],
        timestamp => undefined
    }},
    {
        "m5\\,mA,tag=\\=tag5\\=,\\,tag_a\\,=tag\\ 5a,tag_b=tag5b \\ field\\ =field5,"
        "field\\ _\\ a=field5a,\\,field_b\\ =\\=\\,\\ field5b ${timestamp5}",
        #{
            measurement => "m5,mA",
            tags => [{"tag", "=tag5="}, {",tag_a,", "tag 5a"}, {"tag_b", "tag5b"}],
            fields => [
                {" field ", "field5"}, {"field _ a", "field5a"}, {",field_b ", "=, field5b"}
            ],
            timestamp => "${timestamp5}"
        }
    },
    {"m6,tag=tag6,tag_a=tag6a,tag_b=tag6b field=\"field6\",field_a=\"field6a\",field_b=\"field6b\"",
        #{
            measurement => "m6",
            tags => [{"tag", "tag6"}, {"tag_a", "tag6a"}, {"tag_b", "tag6b"}],
            fields => [
                {"field", {quoted, "field6"}},
                {"field_a", {quoted, "field6a"}},
                {"field_b", {quoted, "field6b"}}
            ],
            timestamp => undefined
        }},
    {
        "\\ \\ m7\\ \\ ,tag=\\ tag\\,7\\ ,tag_a=\"tag7a\",tag_b\\,tag1=tag7b field=\"field7\","
        "field_a=field7a,field_b=\"field7b\\\\\n\"",
        #{
            measurement => "  m7  ",
            tags => [{"tag", " tag,7 "}, {"tag_a", "\"tag7a\""}, {"tag_b,tag1", "tag7b"}],
            fields => [
                {"field", {quoted, "field7"}},
                {"field_a", "field7a"},
                {"field_b", {quoted, "field7b\\\n"}}
            ],
            timestamp => undefined
        }
    },
    {
        "m8,tag=tag8,tag_a=\"tag8a\",tag_b=tag8b field=\"field8\",field_a=field8a,"
        "field_b=\"\\\"field\\\" = 8b\" ${timestamp8}",
        #{
            measurement => "m8",
            tags => [{"tag", "tag8"}, {"tag_a", "\"tag8a\""}, {"tag_b", "tag8b"}],
            fields => [
                {"field", {quoted, "field8"}},
                {"field_a", "field8a"},
                {"field_b", {quoted, "\"field\" = 8b"}}
            ],
            timestamp => "${timestamp8}"
        }
    },
    {"m\\9,tag=tag9,tag_a=\"tag9a\",tag_b=tag9b field\\=field=\"field9\",field_a=field9a,field_b=\"\" ${timestamp9}",
        #{
            measurement => "m\\9",
            tags => [{"tag", "tag9"}, {"tag_a", "\"tag9a\""}, {"tag_b", "tag9b"}],
            fields => [
                {"field=field", {quoted, "field9"}},
                {"field_a", "field9a"},
                {"field_b", {quoted, ""}}
            ],
            timestamp => "${timestamp9}"
        }},
    {"m\\,10 \"field\\\\\"=\"\" ${timestamp10}", #{
        measurement => "m,10",
        tags => [],
        %% backslash should not be un-escaped in tag key
        fields => [{"\"field\\\\\"", {quoted, ""}}],
        timestamp => "${timestamp10}"
    }}
]).

-define(VALID_LINE_PARSED_ESCAPED_CHARS_EXTRA_SPACES_PAIRS, [
    {" \n m\\ =1\\,,\\,tag\\ \\==\\=tag\\ 1\\,   \\,fie\\ ld\\ =\\ field\\,1  ${timestamp1}  ", #{
        measurement => "m =1,",
        tags => [{",tag =", "=tag 1,"}],
        fields => [{",fie ld ", " field,1"}],
        timestamp => "${timestamp1}"
    }},
    {" m2,tag=tag2   field=\"field \\\"2\\\",\n\"  ", #{
        measurement => "m2",
        tags => [{"tag", "tag2"}],
        fields => [{"field", {quoted, "field \"2\",\n"}}],
        timestamp => undefined
    }},
    {"  m\\ 3   field=\"field3\"   ${payload.timestamp\\ 3}  ", #{
        measurement => "m 3",
        tags => [],
        fields => [{"field", {quoted, "field3"}}],
        timestamp => "${payload.timestamp 3}"
    }},
    {"   m4       field=\"\\\"field\\\\4\\\"\"    ", #{
        measurement => "m4",
        tags => [],
        fields => [{"field", {quoted, "\"field\\4\""}}],
        timestamp => undefined
    }},
    {
        " m5\\,mA,tag=\\=tag5\\=,\\,tag_a\\,=tag\\ 5a,tag_b=tag5b   \\ field\\ =field5,"
        "field\\ _\\ a=field5a,\\,field_b\\ =\\=\\,\\ field5b   ${timestamp5}    ",
        #{
            measurement => "m5,mA",
            tags => [{"tag", "=tag5="}, {",tag_a,", "tag 5a"}, {"tag_b", "tag5b"}],
            fields => [
                {" field ", "field5"}, {"field _ a", "field5a"}, {",field_b ", "=, field5b"}
            ],
            timestamp => "${timestamp5}"
        }
    },
    {"  m6,tag=tag6,tag_a=tag6a,tag_b=tag6b   field=\"field6\",field_a=\"field6a\",field_b=\"field6b\"  ",
        #{
            measurement => "m6",
            tags => [{"tag", "tag6"}, {"tag_a", "tag6a"}, {"tag_b", "tag6b"}],
            fields => [
                {"field", {quoted, "field6"}},
                {"field_a", {quoted, "field6a"}},
                {"field_b", {quoted, "field6b"}}
            ],
            timestamp => undefined
        }}
]).

invalid_write_syntax_line_test_() ->
    [?_assertMatch({error, _}, to_influx_lines(L)) || L <- ?INVALID_LINES].

invalid_write_syntax_multiline_test_() ->
    LinesList = [
        join("\n", ?INVALID_LINES),
        join("\n\n\n", ?INVALID_LINES),
        join("\n\n", lists:reverse(?INVALID_LINES))
    ],
    [?_assertMatch({error, _}, to_influx_lines(Lines)) || Lines <- LinesList].

valid_write_syntax_test_() ->
    test_pairs(?VALID_LINE_PARSED_PAIRS).

valid_write_syntax_with_extra_spaces_test_() ->
    test_pairs(?VALID_LINE_EXTRA_SPACES_PARSED_PAIRS).

valid_write_syntax_escaped_chars_test_() ->
    test_pairs(?VALID_LINE_PARSED_ESCAPED_CHARS_PAIRS).

valid_write_syntax_escaped_chars_with_extra_spaces_test_() ->
    test_pairs(?VALID_LINE_PARSED_ESCAPED_CHARS_EXTRA_SPACES_PAIRS).

unicode_write_syntax_test_() ->
    Tag = unicode:characters_to_list(<<"标签"/utf8>>),
    Field = unicode:characters_to_list(<<"固定中文"/utf8>>),
    Expected = #{
        measurement => "m",
        tags => [{"tag", Tag}],
        fields => [{"f", {quoted, Field}}],
        timestamp => undefined
    },
    [
        ?_assertEqual(
            [Expected],
            to_influx_lines(<<"m,tag=标签 f=\"固定中文\""/utf8>>)
        )
    ].

influxdb_api_v1_connector_ping_with_auth_test_() ->
    _ = emqx_utils:interactive_load(emqx_bridge_enterprise),
    BaseConf = parse(influxdb_api_v1_connector_hocon()),
    Override = fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            BaseConf,
            #{
                <<"connectors">> => #{
                    <<"influxdb">> => #{
                        <<"my_influxdb">> => Cfg
                    }
                }
            }
        )
    end,
    BaseConfig = #{
        server => <<"127.0.0.1:8086">>,
        pool_size => 8,
        ssl => #{enable => false},
        parameters => #{
            influxdb_type => influxdb_api_v1,
            database => <<"mydb">>,
            username => <<"user">>,
            password => <<"pass">>
        }
    },
    [
        {"schema defaults ping_with_auth to false",
            ?_assertMatch(
                #{
                    <<"connectors">> := #{
                        <<"influxdb">> := #{
                            <<"my_influxdb">> := #{
                                <<"parameters">> := #{<<"ping_with_auth">> := false}
                            }
                        }
                    }
                },
                check_connector(BaseConf)
            )},
        {"schema accepts ping_with_auth=true",
            ?_assertMatch(
                #{
                    <<"connectors">> := #{
                        <<"influxdb">> := #{
                            <<"my_influxdb">> := #{
                                <<"parameters">> := #{<<"ping_with_auth">> := true}
                            }
                        }
                    }
                },
                check_connector(
                    Override(#{<<"parameters">> => #{<<"ping_with_auth">> => true}})
                )
            )},
        {"client_config preserves legacy default when unset",
            ?_assertEqual(
                false,
                proplists:is_defined(
                    ping_with_auth,
                    emqx_bridge_influxdb_connector:client_config(test_pool, BaseConfig)
                )
            )},
        {"client_config forwards ping_with_auth=true",
            ?_assertEqual(
                true,
                proplists:get_value(
                    ping_with_auth,
                    emqx_bridge_influxdb_connector:client_config(
                        test_pool,
                        BaseConfig#{
                            parameters => maps:merge(
                                maps:get(parameters, BaseConfig),
                                #{ping_with_auth => true}
                            )
                        }
                    )
                )
            )}
    ].

influxdb_api_v2_connector_ping_with_auth_test_() ->
    _ = emqx_utils:interactive_load(emqx_bridge_enterprise),
    BaseConf = parse(influxdb_api_v2_connector_hocon()),
    Override = fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            BaseConf,
            #{
                <<"connectors">> => #{
                    <<"influxdb">> => #{
                        <<"my_influxdb">> => Cfg
                    }
                }
            }
        )
    end,
    BaseConfig = #{
        server => <<"127.0.0.1:8086">>,
        pool_size => 8,
        ssl => #{enable => false},
        parameters => #{
            influxdb_type => influxdb_api_v2,
            bucket => <<"mybucket">>,
            org => <<"myorg">>,
            token => <<"token">>
        }
    },
    [
        {"schema defaults ping_with_auth to false",
            ?_assertMatch(
                #{
                    <<"connectors">> := #{
                        <<"influxdb">> := #{
                            <<"my_influxdb">> := #{
                                <<"parameters">> := #{<<"ping_with_auth">> := false}
                            }
                        }
                    }
                },
                check_connector(BaseConf)
            )},
        {"schema accepts ping_with_auth=true",
            ?_assertMatch(
                #{
                    <<"connectors">> := #{
                        <<"influxdb">> := #{
                            <<"my_influxdb">> := #{
                                <<"parameters">> := #{<<"ping_with_auth">> := true}
                            }
                        }
                    }
                },
                check_connector(
                    Override(#{<<"parameters">> => #{<<"ping_with_auth">> => true}})
                )
            )},
        {"client_config preserves legacy default when unset",
            ?_assertEqual(
                false,
                proplists:is_defined(
                    ping_with_auth,
                    emqx_bridge_influxdb_connector:client_config(test_pool, BaseConfig)
                )
            )},
        {"client_config forwards ping_with_auth=true",
            ?_assertEqual(
                true,
                proplists:get_value(
                    ping_with_auth,
                    emqx_bridge_influxdb_connector:client_config(
                        test_pool,
                        BaseConfig#{
                            parameters => maps:merge(
                                maps:get(parameters, BaseConfig),
                                #{ping_with_auth => true}
                            )
                        }
                    )
                )
            )}
    ].

influxdb_api_v3_connector_ping_with_auth_test_() ->
    _ = emqx_utils:interactive_load(emqx_bridge_enterprise),
    BaseConf = parse(influxdb_api_v3_connector_hocon()),
    Override = fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            BaseConf,
            #{
                <<"connectors">> => #{
                    <<"influxdb">> => #{
                        <<"my_influxdb">> => Cfg
                    }
                }
            }
        )
    end,
    BaseConfig = #{
        server => <<"127.0.0.1:8086">>,
        pool_size => 8,
        ssl => #{enable => false},
        parameters => #{
            influxdb_type => influxdb_api_v3,
            database => <<"mydb">>,
            token => <<"token">>,
            ping_with_auth => true
        }
    },
    [
        {"schema defaults ping_with_auth to true",
            ?_assertMatch(
                #{
                    <<"connectors">> := #{
                        <<"influxdb">> := #{
                            <<"my_influxdb">> := #{
                                <<"parameters">> := #{<<"ping_with_auth">> := true}
                            }
                        }
                    }
                },
                check_connector(BaseConf)
            )},
        {"schema accepts ping_with_auth=false",
            ?_assertMatch(
                #{
                    <<"connectors">> := #{
                        <<"influxdb">> := #{
                            <<"my_influxdb">> := #{
                                <<"parameters">> := #{<<"ping_with_auth">> := false}
                            }
                        }
                    }
                },
                check_connector(
                    Override(#{<<"parameters">> => #{<<"ping_with_auth">> => false}})
                )
            )},
        {"client_config forwards ping_with_auth=true",
            ?_assertEqual(
                true,
                proplists:get_value(
                    ping_with_auth,
                    emqx_bridge_influxdb_connector:client_config(test_pool, BaseConfig)
                )
            )}
    ].

test_pairs(PairsList) ->
    {Lines, AllExpected} = lists:unzip(PairsList),
    JoinedLines = join("\n", Lines),
    JoinedLines1 = join("\n\n\n", Lines),
    JoinedLines2 = join("\n\n", lists:reverse(Lines)),
    SingleLineTests =
        [
            ?_assertEqual([Expected], to_influx_lines(Line))
         || {Line, Expected} <- PairsList
        ],
    JoinedLinesTests =
        [
            ?_assertEqual(AllExpected, to_influx_lines(JoinedLines)),
            ?_assertEqual(AllExpected, to_influx_lines(JoinedLines1)),
            ?_assertEqual(lists:reverse(AllExpected), to_influx_lines(JoinedLines2))
        ],
    SingleLineTests ++ JoinedLinesTests.

join(Sep, LinesList) ->
    lists:flatten(lists:join(Sep, LinesList)).

to_influx_lines(RawLines) ->
    OldLevel = emqx_logger:get_primary_log_level(),
    try
        %% mute error logs from this call
        emqx_logger:set_primary_log_level(none),
        emqx_bridge_influxdb:to_influx_lines(RawLines)
    after
        emqx_logger:set_primary_log_level(OldLevel)
    end.
