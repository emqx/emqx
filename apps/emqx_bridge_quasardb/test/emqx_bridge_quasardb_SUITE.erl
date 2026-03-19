%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_quasardb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_quasardb.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

%% Fixed; see `.ci/docker-compose-file/docker-compose-quasardb.yaml`
-define(SERVER_IP, "172.100.239.30").

-define(async, async).
-define(sync, sync).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_quasardb,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>),
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
    create_simple_table(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"dsn">> => <<"qdb">>,
        <<"uri">> => <<"qdb://", ?SERVER_IP, ":2836">>,
        <<"cluster_public_key">> => cluster_public_key(),
        <<"username">> => <<"root">>,
        <<"password">> => root_user_password(),
        <<"pool_size">> => 2,
        <<"connect_timeout">> => <<"1s">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

cluster_public_key() ->
    Path = filename:join([ci_dir(), "cluster_public_key.txt"]),
    {ok, Contents} = file:read_file(Path),
    Contents.

root_user_password() ->
    Path = filename:join([ci_dir(), "root.json"]),
    {ok, Contents} = file:read_file(Path),
    #{<<"secret_key">> := Password} = emqx_utils_json:decode(Contents),
    Password.

ci_dir() ->
    filename:join([emqx_common_test_helpers:proj_root(), ".ci", "docker-compose-file", "quasardb"]).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"sql">> =>
                ~b"""
             insert into dummy_table($timestamp, qos, payload)
             values (now(), ${.qos}, '${.payload}')
           """
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

execute_sql(SQL, _TCConfig) ->
    ConnStr = emqx_bridge_quasardb_impl:conn_str([
        {dsn, <<"qdb">>},
        {uri, <<"qdb://", ?SERVER_IP, ":2836">>},
        {username, <<"root">>},
        {password, root_user_password()},
        {cluster_public_key, cluster_public_key()}
    ]),
    {ok, Conn} = odbc:connect(ConnStr, []),
    try
        odbc:sql_query(Conn, [SQL])
    after
        odbc:disconnect(Conn)
    end.

create_simple_table(TCConfig) ->
    on_exit(fun() ->
        SQL = <<"drop table dummy_table">>,
        execute_sql(SQL, TCConfig)
    end),
    SQL =
        ~b"""
          create table if not exists dummy_table (
            qos int64,
            payload blob
          )
    """,
    Res = execute_sql(SQL, TCConfig),
    ok = handle_empty_res(Res),
    ok.

create_simple_table2(TCConfig) ->
    on_exit(fun() ->
        SQL = <<"drop table dummy_table2">>,
        execute_sql(SQL, TCConfig)
    end),
    SQL =
        ~b"""
          create table if not exists dummy_table2 (
            int_val int64,
            blob_val blob,
            str_val string,
            double_val double,
            ts_val timestamp,
            sym_val symbol(my_sym_type)
          )
    """,
    Res = execute_sql(SQL, TCConfig),
    ok = handle_empty_res(Res),
    ok.

handle_empty_res({error, Msg}) when is_list(Msg) ->
    case re:run(Msg, <<"SQLSTATE IS: 01000">>, [{capture, none}]) of
        match ->
            %% Yes...  A successful result for this integration is interpreted by the OTP
            %% ODBC wrapper as an error...  with the following message:
            %%
            %% "General warning: no results SQLSTATE IS: 01000"
            ok;
        nomatch ->
            {error, Msg}
    end;
handle_empty_res(X) ->
    X.

execute_select(SQL, TCConfig) ->
    maybe
        {selected, Cols0, Rows0} ?= execute_sql(SQL, TCConfig),
        Cols = lists:map(fun bin/1, Cols0),
        Rows = lists:map(
            fun(Row0) ->
                Row1 = tuple_to_list(Row0),
                Row = lists:zip(Cols, Row1),
                maps:from_list(Row)
            end,
            Rows0
        ),
        {ok, Rows}
    end.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            TCConfig
        )
    ).

update_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(TCConfig, Overrides)
    ).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

json_encode(X) ->
    emqx_utils_json:encode(X).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "quasardb_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := ExpectedPayload} = Context,
        ExpectedPayloadStr = str(ExpectedPayload),
        ?retry(
            200,
            10,
            ?assertMatch(
                {ok, [#{<<"payload">> := ExpectedPayloadStr}]},
                execute_select(<<"select * from dummy_table">>, TCConfig)
            )
        ),
        ok
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

-doc """
Checks that, when `health_check_table` is define, we check that the provided table exists
during action health checks and, if not, mark the action as `unhealthy_target`.
""".
t_health_check_table(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"health_check_table">> => <<"dummy_table">>}
    }),
    ok = handle_empty_res(execute_sql(<<"drop table dummy_table">>, TCConfig)),
    ExpectedError = iolist_to_binary(
        io_lib:format("~0p", [{unhealthy_target, <<"Table not found">>}])
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := ExpectedError
            }},
            get_action_api(TCConfig)
        )
    ),
    ok.

-doc """
Explores insertion to a table with varied data types.
""".
t_value_types(TCConfig) ->
    ok = create_simple_table2(TCConfig),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            %% timestamp values do not need/accept quotes
            <<"sql">> =>
                ~b"""
              insert into dummy_table2(
                $timestamp,
                int_val, blob_val, str_val, double_val, ts_val, sym_val
              ) values (
                now(),
                ${.payload.int}, '${.payload.blob}',
                '${.payload.str}', ${.payload.double},
                ${.payload.ts}, '${.payload.sym}'
              )
            """,
            <<"health_check_table">> => <<"dummy_table2">>
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = json_encode(#{
        <<"int">> => 123,
        <<"double">> => 4.0,
        <<"blob">> => <<"DEADBEEF">>,
        <<"str">> => <<"hello world">>,
        <<"ts">> => <<"2026-03-09T11:38:55">>,
        <<"sym">> => <<"CADR">>
    }),
    emqtt:publish(C, RuleTopic, Payload),
    ?retry(
        200,
        10,
        ?assertMatch(
            {ok, [
                #{
                    <<"blob_val">> := "DEADBEEF",
                    <<"double_val">> := 4.0,
                    <<"int_val">> := "123",
                    <<"str_val">> := "hello world",
                    <<"sym_val">> := "CADR",
                    <<"ts_val">> := {_Date, _Time}
                }
            ]},
            execute_select(<<"select * from dummy_table2">>, TCConfig)
        )
    ),
    ok.

-doc """
Verifies that we insert `undefined` and missing values as `null`.
""".
t_undefined_value(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> =>
                ~b"""
                  insert into dummy_table($timestamp, qos, payload)
                  values (now(), ${.payload.int}, '${.payload.blob}')
                """
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = json_encode(#{<<"int">> => null}),
    emqtt:publish(C, RuleTopic, Payload),
    %% The OTP ODBC driver program really does not play well with quasardb's ODBC
    %% driver...  The usual `select * from dummy_table` (or `select qos from dummy_table`)
    %% returns `"Unknown column type"`...  So we have to work around that...
    ?retry(
        200,
        10,
        ?assertMatch(
            {ok, [#{<<"1">> := "1"}]},
            execute_select(<<"select 1 from dummy_table where qos = null">>, TCConfig)
        )
    ),
    %% Obviously, since we have to manually quote strings, this null gets stored as a
    %% string...
    ?assertMatch(
        {ok, [#{<<"payload">> := "null"}]},
        execute_select(<<"select payload from dummy_table">>, TCConfig)
    ),
    ok.

-doc """
Verifies that some bad insert scenarios result in unrecoverable errors.

  - Type mismatch.
  - Inexistent table (and `health_check_table` is unset).
""".
t_bad_values(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> =>
                ~b"""
                  insert into dummy_table($timestamp, qos, payload)
                  values (now(), ${.payload.int}, '${.payload.blob}')
                """
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),

    %% Type mismatch.
    Payload1 = json_encode(#{<<"int">> => <<"I'm not an int">>, <<"blob">> => <<"x">>}),
    emqtt:publish(C, RuleTopic, Payload1),

    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"failed">> := 1,
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),

    %% Inexistent table (and `health_check_table` is unset).
    {200, #{<<"status">> := <<"connected">>}} = update_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> =>
                ~b"""
                  insert into inexistent_table($timestamp, qos, payload)
                  values (now(), ${.payload.int}, '${.payload.blob}')
                """
        }
    }),
    Payload2 = json_encode(#{<<"int">> => 10, <<"blob">> => <<"x">>}),
    emqtt:publish(C, RuleTopic, Payload2),

    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"failed">> := 1,
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),

    ok.

-doc """
Exercises the code paths where the provided action SQL is somehow invalid.

  - Not an insert statement.
  - Insert statement with an `ON` clause.
  - Something else entirely.
""".
t_bad_sql_types(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),

    %% Not an insert statement.
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{invalid_sql_type,update}">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"update dummy_table set qos = 3 where true">>
            }
        })
    ),
    {204, _} = delete_action_api(TCConfig),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{invalid_sql_type,delete}">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"delete from dummy_table">>
            }
        })
    ),
    {204, _} = delete_action_api(TCConfig),

    %% Insert statement with an `ON` clause.
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{unsupported_clause,", _/binary>>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"insert into x(x) values (x) on duplicate key update x = x">>
            }
        })
    ),
    {204, _} = delete_action_api(TCConfig),

    %% Something else entirely.
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{failed_to_parse_type,unknown}">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"foo">>
            }
        })
    ),
    {204, _} = delete_action_api(TCConfig),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{failed_to_parse_type,unknown}">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"1">>
            }
        })
    ),
    {204, _} = delete_action_api(TCConfig),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{failed_to_parse,", _/binary>>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"insert into ${x}(x) values (x)">>
            }
        })
    ),
    {204, _} = delete_action_api(TCConfig),

    ok.
