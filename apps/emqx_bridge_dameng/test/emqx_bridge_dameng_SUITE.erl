%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_dameng_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, dameng).
-define(CONNECTOR_TYPE_BIN, <<"dameng">>).
-define(ACTION_TYPE, dameng).
-define(ACTION_TYPE_BIN, <<"dameng">>).

-define(DM_HOST, list_to_binary(os:getenv("DM_HOST", "dameng"))).
%% The docker testbed runs the `xuxuclassmate/dameng' image, whose DM8 instance
%% listens on the DM default port 5236.
-define(DM_PORT, list_to_integer(os:getenv("DM_PORT", "5236"))).
-define(DM_USERNAME, list_to_binary(os:getenv("DM_USER", "SYSDBA"))).
-define(DM_PASSWORD, list_to_binary(os:getenv("DM_PASSWORD", "SYSDBA001"))).
-define(DM_DRIVER, <<"DM8 ODBC DRIVER">>).
-define(DM_TABLE, <<"SYSDBA.T_MQTT_MSG">>).
-define(DM_BLOB_TABLE, <<"SYSDBA.T_MQTT_BLOB">>).
-define(DM_DSN, <<"dm8">>).

%% Typed columns exercise the value conversion of every supported ODBC type.
-define(TYPED_INSERT, <<
    "insert into ",
    ?DM_TABLE/binary,
    " (msgid, big_col, dec_col, num_col, date_col, time_col, ts_col, w_col, ch_col)"
    " values (${id}, ${big_v}, ${dec_v}, ${num_v}, ${date_v}, ${time_v}, ${ts_v}, ${w_v}, ${ch_v})"
>>).

-define(TYPED_RULE_SQL, <<
    "select *,"
    " payload.big_v as big_v,"
    " payload.dec_v as dec_v,"
    " payload.num_v as num_v,"
    " payload.date_v as date_v,"
    " payload.time_v as time_v,"
    " payload.ts_v as ts_v,"
    " payload.w_v as w_v,"
    " payload.ch_v as ch_v"
    " from \"${t}\" "
>>).

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
    case check_odbc_available() of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_dameng,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
            ),
            %% Connect directly to DM. Removing a stale table is best-effort;
            %% creating a fresh one must succeed (a failure aborts init_per_suite
            %% loudly instead of silently auto-skipping every test case).
            connect_and_drop_table(),
            connect_and_create_table(),
            [{apps, Apps} | TCConfig];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_dameng_odbc);
                _ ->
                    {skip, no_dameng_odbc}
            end
    end.

end_per_suite(TCConfig) ->
    Apps = proplists:get_value(apps, TCConfig),
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
    connect_and_clear_table(),
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
    connect_and_clear_table(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Connectivity / skip helper
%%------------------------------------------------------------------------------

%% @doc Return true when the ODBC driver is configured and DM is reachable.
%% Modeled on `emqx_bridge_datalayers_connector_SUITE' so that the whole suite
%% is skipped (or aborted in CI) when the environment is not prepared.
%% The DM8 container starts `dmserver' asynchronously, so retry the probe for a
%% while before giving up.
check_odbc_available() ->
    check_odbc_available(30).

check_odbc_available(0) ->
    false;
check_odbc_available(Tries) ->
    try
        %% The OTP `odbc' application is needed by `emqx_odbc:connect/1'; it is
        %% started here (before emqx_cth_suite starts the other apps) so the
        %% connectivity probe does not fail with `odbc_not_started'.
        {ok, _} = application:ensure_all_started(odbc),
        with_conn(fun(Conn) ->
            {selected, _, [{1}]} = emqx_odbc:sql_query(Conn, <<"SELECT 1">>, 5_000),
            ok
        end),
        true
    catch
        _:_ ->
            timer:sleep(2_000),
            check_odbc_available(Tries - 1)
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"server">> => ?DM_HOST,
        <<"port">> => ?DM_PORT,
        <<"username">> => ?DM_USERNAME,
        <<"password">> => ?DM_PASSWORD,
        <<"pool_size">> => 1,
        <<"driver">> => ?DM_DRIVER,
        <<"charset">> => <<"utf8">>,
        <<"resource_opts">> => emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"sql">> =>
                iolist_to_binary([
                    <<"insert into ", ?DM_TABLE/binary, " (msgid, topic, qos, payload)">>,
                    <<" values ( ${id}, ${topic}, ${qos}, ${payload})">>
                ]),
            <<"undefined_vars_as_null">> => false
        },
        <<"resource_opts">> => emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).

conn_map() ->
    #{
        server => ?DM_HOST,
        port => ?DM_PORT,
        username => ?DM_USERNAME,
        password => emqx_secret:wrap(?DM_PASSWORD),
        driver => ?DM_DRIVER,
        charset => <<"utf8">>
    }.

connect() ->
    {ok, Conn} = emqx_odbc:connect(conn_map()),
    Conn.

with_conn(Fn) ->
    Conn = connect(),
    try
        Fn(Conn)
    after
        ok = emqx_odbc:disconnect(Conn)
    end.

connect_and_create_table() ->
    SQL = <<
        "create table ",
        ?DM_TABLE/binary,
        " (msgid varchar(64),"
        " topic varchar(100),"
        " qos int,"
        " payload varchar(200),"
        " big_col bigint,"
        " dec_col decimal(38,2),"
        " num_col numeric(18,6),"
        " date_col date,"
        " time_col time,"
        " ts_col timestamp,"
        " w_col nvarchar(50),"
        " ch_col char(8))"
    >>,
    BlobSQL = <<
        "create table ",
        ?DM_BLOB_TABLE/binary,
        " (id int, blob_col blob)"
    >>,
    with_conn(fun(Conn) ->
        {updated, _} = emqx_odbc:sql_query(Conn, SQL, 2_000),
        {updated, _} = emqx_odbc:sql_query(Conn, BlobSQL, 2_000)
    end).

connect_and_drop_table() ->
    with_conn(fun(Conn) ->
        %% Best-effort: the table may not exist yet (e.g. first run). Ignore any
        %% drop failure so the subsequent create always runs.
        _ = emqx_odbc:sql_query(Conn, <<"drop table ", ?DM_TABLE/binary>>, 2_000),
        _ = emqx_odbc:sql_query(Conn, <<"drop table ", ?DM_BLOB_TABLE/binary>>, 2_000),
        ok
    end).

connect_and_clear_table() ->
    SQL = <<"delete from ", ?DM_TABLE/binary>>,
    with_conn(fun(Conn) ->
        {updated, _} = emqx_odbc:sql_query(Conn, SQL, 2_000)
    end).

connect_and_get_payload() ->
    SQL = <<"select payload from ", ?DM_TABLE/binary>>,
    with_conn(fun(Conn) ->
        {selected, _, Rows} = emqx_odbc:sql_query(Conn, SQL, 2_000),
        Rows
    end).

connect_and_get_id_payload_pairs() ->
    SQL = <<"select msgid, payload from ", ?DM_TABLE/binary>>,
    with_conn(fun(Conn) ->
        {selected, _, Rows} = emqx_odbc:sql_query(Conn, SQL, 2_000),
        Rows
    end).

%% Seed one row per id so that every statement of a batch has a visible effect.
seed_rows(Ids) ->
    with_conn(fun(Conn) ->
        lists:foreach(
            fun(Id) ->
                {updated, _} = emqx_odbc:sql_query(
                    Conn,
                    <<
                        "insert into ",
                        ?DM_TABLE/binary,
                        " (msgid, topic, qos, payload) values ('",
                        Id/binary,
                        "', 't/1', 0, 'old')"
                    >>,
                    2_000
                )
            end,
            Ids
        )
    end).

%% Read back the typed columns; the table is cleared around every test case, so
%% a single row is expected.
connect_and_get_typed_row() ->
    SQL = <<
        "select big_col, dec_col, num_col, date_col, time_col, ts_col, w_col, ch_col from ",
        ?DM_TABLE/binary
    >>,
    with_conn(fun(Conn) ->
        {selected, _, [Row]} = emqx_odbc:sql_query(Conn, SQL, 2_000),
        [normalize_value(Value) || Value <- tuple_to_list(Row)]
    end).

%% Wide character columns are returned as raw UTF-16LE bytes, and CHAR columns
%% are padded by the driver; numeric formatting (trailing zeros) is driver
%% dependent.
normalize_value(Value) when is_binary(Value) ->
    normalize_number(normalize_time(string:trim(decode_wide(Value), trailing)));
normalize_value(Value) ->
    Value.

decode_wide(Bin) ->
    case binary:match(Bin, <<0>>) of
        nomatch ->
            Bin;
        _ ->
            case unicode:characters_to_binary(Bin, {utf16, little}, utf8) of
                Decoded when is_binary(Decoded) -> strip_trailing_nul(Decoded);
                _ -> Bin
            end
    end.

strip_trailing_nul(Bin) when byte_size(Bin) > 0 ->
    case binary:last(Bin) of
        0 -> strip_trailing_nul(binary:part(Bin, 0, byte_size(Bin) - 1));
        _ -> Bin
    end;
strip_trailing_nul(Bin) ->
    Bin.

normalize_time(Bin) ->
    case re:run(Bin, <<"^(\\d{2}:\\d{2}:\\d{2})(\\.0+)?$">>, [{capture, all_but_first, binary}]) of
        {match, [Time | _]} -> Time;
        nomatch -> Bin
    end.

normalize_number(Bin) ->
    case binary:split(Bin, <<".">>) of
        [Int, Frac] ->
            case string:trim(Frac, trailing, "0") of
                <<>> -> Int;
                Frac1 -> <<Int/binary, ".", Frac1/binary>>
            end;
        [_] ->
            Bin
    end.

connect_and_get_count() ->
    SQL = <<"select count(*) from ", ?DM_TABLE/binary>>,
    with_conn(fun(Conn) ->
        {selected, _, [{Count}]} = emqx_odbc:sql_query(Conn, SQL, 2_000),
        count_to_int(Count)
    end).

count_to_int(C) when is_integer(C) -> C;
count_to_int(C) when is_binary(C) -> binary_to_integer(C);
count_to_int(C) when is_list(C) -> list_to_integer(C).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_connector_api(TCConfig) ->
    #{connector_type := ConnectorType, connector_name := ConnectorName} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(ConnectorType, ConnectorName)
    ).

simple_create_rule_api(TCConfig) ->
    simple_create_rule_api(<<"select * from \"${t}\" ">>, TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, dameng_connector_on_stop).

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
        #{payload := Payload} = Context,
        ?retry(
            200,
            10,
            ?assertMatch(
                [{Payload}],
                connect_and_get_payload(),
                #{payload => Payload}
            )
        )
    end,
    Opts = #{post_publish_fn => PostPublishFn},
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_undefined_vars_as_null(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => true}
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"select id, qos, timestamp from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>),
                #{?snk_kind := dameng_connector_query_return},
                10_000
            ),
            ?assertMatch([{null}], connect_and_get_payload()),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{result := ok}], ?of_kind(dameng_connector_query_return, Trace)),
            ok
        end
    ),
    ok.

t_batch_write() ->
    [{matrix, true}].
t_batch_write(matrix) ->
    [[?sync, ?with_batch], [?async, ?with_batch]];
t_batch_write(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Payloads = [unique_payload() || _ <- lists:seq(1, BatchSize)],
    ?check_trace(
        begin
            ?wait_async_action(
                lists:foreach(
                    fun(Payload) -> emqx:publish(emqx_message:make(Topic, Payload)) end,
                    Payloads
                ),
                #{?snk_kind := dameng_connector_query_return},
                10_000
            ),
            ?retry(200, 10, ?assertEqual(BatchSize, connect_and_get_count())),
            %% The rows are written in batches, so compare them as a set.
            ?assertEqual(
                lists:sort(Payloads),
                lists:sort([P || {P} <- connect_and_get_payload()])
            )
        end,
        fun(Trace0) ->
            Trace = ?of_kind(dameng_connector_query_return, Trace0),
            case BatchSize of
                1 ->
                    ?assertMatch([#{result := ok}], Trace);
                _ ->
                    [?assertMatch(#{result := ok}, Trace1) || Trace1 <- Trace]
            end,
            ok
        end
    ),
    ok.

%% Instead of bringing the DB down via toxiproxy, point the connector at a
%% non-routable endpoint so it reports `?status_disconnected'.
t_create_disconnected(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"server">> => <<"localhost">>,
        <<"port">> => 1
    }),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"disconnected">>}},
            get_connector_api(TCConfig)
        )
    ),
    ok.

t_create_with_invalid_password(TCConfig) ->
    ?check_trace(
        begin
            ?assertMatch(
                {201, _},
                create_connector_api(TCConfig, #{<<"password">> => <<"wrong_password">>})
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(dameng_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_health_check_return_error(TCConfig) ->
    Ets = ets:new(test, [public]),
    true = ets:insert(Ets, {result, {selected, ["1"], [{1}]}}),
    meck:new(emqx_odbc, [passthrough, no_link, no_history]),
    meck:expect(emqx_odbc, sql_query, fun(Conn, _SQL, _Timeout) ->
        case ets:lookup(Ets, result) of
            [{result, R}] ->
                R;
            _ ->
                meck:passthrough([Conn, _SQL, _Timeout])
        end
    end),
    try
        {201, _} = create_connector_api(TCConfig, #{}),
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        ),
        true = ets:insert(Ets, {result, {error, <<"injected failure">>}}),
        ?retry(
            200,
            10,
            ?assertMatch(
                {200, #{<<"status">> := <<"disconnected">>}},
                get_connector_api(TCConfig)
            )
        ),
        true = ets:insert(Ets, {result, {selected, ["1"], [{1}]}}),
        ?retry(
            200,
            10,
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_connector_api(TCConfig)
            )
        )
    after
        meck:unload(emqx_odbc),
        ets:delete(Ets)
    end,
    ok.

t_table_not_found(TCConfig) ->
    %% The connector probes the table via `describe_table' at channel creation.
    %% An INSERT referencing a non-existent table must be flagged there (the
    %% action is created with `error' status and its resource goes down),
    %% instead of silently allowing a runtime insert to fail.
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"error">> := _}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<"insert into SYSDBA.I_DON_EXIST(id) values ( ${id} )">>
            }
        })
    ),
    ok.

t_missing_column_type(TCConfig) ->
    %% An INSERT mentioning a column that is not present in the table must be
    %% flagged at channel creation (describe-probe), surfacing as an `error'.
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"error">> := _}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> =>
                    <<"insert into ", ?DM_TABLE/binary, " (no_such_column) values ( ${id} )">>
            }
        })
    ),
    ok.

t_ecpool_workers_crash(TCConfig) ->
    ok = emqx_bridge_v2_testlib:t_ecpool_workers_crash(TCConfig),
    ok.

%% A message that does not provide a variable referenced by the SQL template
%% must be rejected while `undefined_vars_as_null' is `false' (the default).
t_missing_data(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => false}
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"select id, qos, timestamp from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>),
                #{?snk_kind := dameng_connector_query_return},
                10_000
            ),
            %% Nothing may be written when a referenced variable is missing.
            ?assertEqual([], connect_and_get_payload()),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := {unrecoverable_error, undefined_var}}],
                ?of_kind(dameng_connector_query_return, Trace)
            ),
            ok
        end
    ),
    ok.

%% Non-INSERT statements (here an UPDATE) go through the literal SQL path
%% instead of `param_query'.
t_non_insert_statement(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> => <<"update ", ?DM_TABLE/binary, " set payload = ${payload}">>
        }
    }),
    %% Seed one row so that the UPDATE has a target and the result is visible.
    with_conn(fun(Conn) ->
        {updated, _} = emqx_odbc:sql_query(
            Conn,
            <<
                "insert into ",
                ?DM_TABLE/binary,
                " (msgid, topic, qos, payload) values ('1', 't/1', 0, 'old')"
            >>,
            2_000
        )
    end),
    #{topic := Topic} = simple_create_rule_api(<<"select * from \"${t}\" ">>, TCConfig),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"updated">>),
                #{?snk_kind := dameng_connector_query_return},
                10_000
            ),
            ?retry(200, 10, ?assertEqual([{<<"updated">>}], connect_and_get_payload())),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{result := ok}],
                ?of_kind(dameng_connector_query_return, Trace)
            ),
            ok
        end
    ),
    ok.

%% Batching is enabled in the action by default, and non-INSERT statements
%% cannot be combined into a single statement: a coalesced burst of UPDATEs must
%% be executed sequentially instead of the whole batch being rejected.
t_non_insert_batch() ->
    [{matrix, true}].
t_non_insert_batch(matrix) ->
    [[?sync, ?with_batch]];
t_non_insert_batch(TCConfig) when is_list(TCConfig) ->
    BatchSize = 10,
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> => <<
                "update ",
                ?DM_TABLE/binary,
                " set payload = ${payload} where msgid = ${id}"
            >>
        },
        %% A wide batch window makes the burst below coalesce into one batch.
        <<"resource_opts">> => #{<<"batch_time">> => <<"1s">>}
    }),
    Ids = [integer_to_binary(Id) || Id <- lists:seq(1, BatchSize)],
    seed_rows(Ids),
    #{topic := Topic} = simple_create_rule_api(
        <<"select payload.id as id, payload.payload as payload from \"${t}\" ">>,
        TCConfig
    ),
    ?check_trace(
        begin
            lists:foreach(
                fun(Id) ->
                    Payload = emqx_utils_json:encode(#{
                        <<"id">> => Id,
                        <<"payload">> => <<"new-", Id/binary>>
                    }),
                    emqx:publish(emqx_message:make(Topic, Payload))
                end,
                Ids
            ),
            ?retry(
                200,
                30,
                ?assertEqual(
                    lists:sort([{Id, <<"new-", Id/binary>>} || Id <- Ids]),
                    lists:sort(connect_and_get_id_payload_pairs())
                )
            ),
            ok
        end,
        fun(Trace) ->
            %% The burst must have been processed as a batch (of more than one
            %% request), which is the code path the shipped defaults produce.
            Batches = [
                N
             || #{batch_size := N} <- ?of_kind(dameng_connector_query_return, Trace)
            ],
            ?assert(lists:any(fun(N) -> N >= 2 end, Batches)),
            ok
        end
    ),
    ok.

%% Every supported column type is written and read back through the rule engine
%% and the action: the values are converted with `odbc:param_query' according to
%% the type reported by `describe_table', including the types that have no
%% native binding in the OTP `odbc' application (BIGINT, DATE, TIME) and the
%% wide character encoding.  The DM8 driver reports NVARCHAR as a narrow
%% varchar, so the value travels as UTF-8 in that case.
t_typed_columns(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"sql">> => ?TYPED_INSERT}
    }),
    #{topic := Topic} = simple_create_rule_api(?TYPED_RULE_SQL, TCConfig),
    C = start_client(),
    Payload = emqx_utils_json:encode(#{
        <<"big_v">> => <<"9007199254740993">>,
        <<"dec_v">> => <<"12345678901234567890.12">>,
        <<"num_v">> => <<"1.5">>,
        <<"date_v">> => <<"2026-01-02">>,
        <<"time_v">> => <<"12:34:56">>,
        <<"ts_v">> => <<"2026-01-02 12:34:56">>,
        <<"w_v">> => <<"café"/utf8>>,
        <<"ch_v">> => <<"ab">>
    }),
    Expected = [
        <<"9007199254740993">>,
        <<"12345678901234567890.12">>,
        <<"1.5">>,
        <<"2026-01-02">>,
        <<"12:34:56">>,
        {{2026, 1, 2}, {12, 34, 56}},
        <<"café"/utf8>>,
        <<"ab">>
    ],
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := dameng_connector_query_return},
                10_000
            ),
            ?retry(200, 10, ?assertEqual(Expected, connect_and_get_typed_row())),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{result := ok}], ?of_kind(dameng_connector_query_return, Trace)),
            ok
        end
    ),
    ok.

%% Bind one value per typed column on its own with the binding the connector
%% uses and assert that the driver accepts all of them.  Probing the columns one
%% by one attributes a rejected conversion to the column that caused it (a
%% combined insert only reports `22018 Invalid convert string'), and it pins the
%% bindings the OTP `odbc' application chooses, which are not obvious: DM8
%% reports NVARCHAR as a narrow varchar, and a string bound DECIMAL/NUMERIC must
%% carry its own NUL terminator.
t_typed_bindings(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    with_conn(fun(Conn) ->
        {ok, Cols} = emqx_odbc:describe_table(Conn, ?DM_TABLE),
        Types = maps:from_list([{norm_column(Name), Type} || {Name, Type} <- Cols]),
        Results = [
            typed_binding_result(Conn, Types, Column, Value)
         || {Column, Value} <- typed_values()
        ],
        %% `io:format/3' is used on purpose: `ct:pal/2' output is only shown for
        %% failing test cases, and this report must always be visible.
        lists:foreach(
            fun({Column, Type, Result}) ->
                io:format(user, "dameng_typed_binding ~s ~p ~p~n", [Column, Type, Result])
            end,
            Results
        ),
        ?assertEqual([], [
            {Column, Type, Result}
         || {Column, Type, Result} <- Results,
            Result =/= ok
        ])
    end),
    ok.

typed_binding_result(Conn, Types, Column, Value) ->
    Type = maps:get(norm_column(Column), Types),
    {Column, Type, insert_typed(Conn, Column, Type, Value)}.

%% One value per typed column, using the type `describe_table' reports for it.
typed_values() ->
    [
        {<<"big_col">>, <<"9007199254740993">>},
        {<<"dec_col">>, <<"12345678901234567890.12">>},
        {<<"num_col">>, <<"1.5">>},
        {<<"date_col">>, <<"2026-01-02">>},
        {<<"time_col">>, <<"12:34:56">>},
        {<<"ts_col">>, <<"2026-01-02 12:34:56">>},
        {<<"w_col">>, <<"café"/utf8>>},
        {<<"ch_col">>, <<"ab">>}
    ].

norm_column(Name) ->
    string:uppercase(unicode:characters_to_binary(Name)).

insert_typed(Conn, Column, Type, Value) ->
    case emqx_odbc:to_param_type(Type) of
        {error, Reason} ->
            {error, Reason};
        ParamType ->
            case emqx_odbc:to_odbc_value(Value, Type, false) of
                {ok, BindValue} ->
                    BindType = emqx_odbc:fit_param_type(ParamType, [BindValue]),
                    MsgId = emqx_guid:to_hexstr(emqx_guid:gen()),
                    IdType = emqx_odbc:fit_param_type({sql_varchar, 64}, [MsgId]),
                    SQL = <<
                        "insert into ",
                        ?DM_TABLE/binary,
                        " (msgid, ",
                        Column/binary,
                        ") values (?, ?)"
                    >>,
                    case
                        emqx_odbc:param_query(
                            Conn, SQL, [{IdType, [MsgId]}, {BindType, [BindValue]}], 5_000
                        )
                    of
                        {updated, _} -> ok;
                        Other -> {error, Other}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% A value that does not fit the target column is rejected instead of being
%% bound into a buffer that cannot hold it.
t_value_too_long(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = binary:copy(<<"x">>, 300),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := dameng_connector_query_return},
                10_000
            ),
            ?assertEqual([], connect_and_get_payload()),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        error :=
                            {unrecoverable_error, {invalid_value, {value_too_long, _, 200, 300}}}
                    }
                ],
                ?of_kind(dameng_connector_query_return, Trace)
            ),
            ok
        end
    ),
    ok.

%% A DSN based connector uses the `[dm8]' data source created by the testbed's
%% ODBC setup instead of the `driver'/`server' fields.
t_dsn_mode(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{<<"dsn">> => ?DM_DSN}),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    ok.

%% Columns whose type `odbc:param_query' cannot bind must be rejected when the
%% action is created instead of failing on every message.
t_unsupported_column_type(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"error">> := _}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"sql">> => <<
                    "insert into ",
                    ?DM_BLOB_TABLE/binary,
                    " (id, blob_col) values ( ${id}, ${payload} )"
                >>
            }
        })
    ),
    ok.
