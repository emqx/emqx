%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_snowflake_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-elvis([{elvis_text_style, line_length, #{skip_comments => whole_line}}]).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_bridge_snowflake.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%%------------------------------------------------------------------------------
%% Definitions
%%------------------------------------------------------------------------------

-define(CONN_MOD, emqx_bridge_snowflake_connector).

-define(DATABASE, <<"testdatabase">>).
-define(SCHEMA, <<"public">>).
-define(STAGE, <<"teststage0">>).
-define(TABLE, <<"test0">>).
-define(WAREHOUSE, <<"testwarehouse">>).
-define(PIPE, <<"testpipe0">>).
-define(PIPE_USER, <<"snowpipeuser">>).
-define(PIPE_USER_RO, <<"snowpipe_ro_user">>).

-define(CONF_COLUMN_ORDER, ?CONF_COLUMN_ORDER([])).
-define(CONF_COLUMN_ORDER(T), [
    <<"publish_received_at">>,
    <<"clientid">>,
    <<"topic">>,
    <<"payload">>
    | T
]).

-define(tpal(MSG), begin
    ct:pal(MSG),
    ?tp(notice, MSG, #{})
end).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case os:getenv("SNOWFLAKE_ACCOUNT_ID", "") of
        "" ->
            Mock = true,
            AccountId = "mocked_orgid-mocked_account_id",
            Server = <<"mocked_orgid-mocked_account_id.snowflakecomputing.com">>,
            Username = <<"mock_username">>,
            Password = <<"mock_password">>;
        AccountId ->
            Mock = false,
            Server = iolist_to_binary([AccountId, ".snowflakecomputing.com"]),
            Username = os:getenv("SNOWFLAKE_USERNAME"),
            Password = os:getenv("SNOWFLAKE_PASSWORD")
    end,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_snowflake,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    case Mock of
        true ->
            ct:print(asciiart:visible($%, "running with MOCKED snowflake", []));
        false ->
            ct:print(asciiart:visible($%, "running with REAL snowflake", []))
    end,
    [
        {mock, Mock},
        {apps, Apps},
        {account_id, AccountId},
        {server, Server},
        {username, Username},
        {password, Password}
        | Config
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config0) ->
    %% See `../docs/dev-quick-ref.md' for how to create the DB objects and roles.
    ct:timetrap(timetrap(Config0)),
    AccountId = ?config(account_id, Config0),
    Username = ?config(username, Config0),
    Password = ?config(password, Config0),
    Server = ?config(server, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, AccountId, Server, Username, Password),
    ActionConfig0 = aggregated_action_config(#{connector => Name}),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(?ACTION_TYPE_BIN, Name, ActionConfig0),
    ExtraConfig0 = maybe_mock_snowflake(Config0),
    ConnPid = new_odbc_client(Config0),
    Config =
        ExtraConfig0 ++
            [
                {bridge_kind, action},
                {action_type, ?ACTION_TYPE_BIN},
                {action_name, Name},
                {action_config, ActionConfig},
                {connector_name, Name},
                {connector_type, ?CONNECTOR_TYPE_BIN},
                {connector_config, ConnectorConfig},
                {odbc_client, ConnPid}
                | Config0
            ],
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true ->
            ?MODULE:TestCase(init, Config);
        false ->
            Config
    end.

end_per_testcase(_Testcase, Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = clear_stage(Config),
    ok = truncate_table(Config),
    stop_odbc_client(Config),
    emqx_common_test_helpers:call_janitor(),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

timetrap(Config) ->
    case ?config(mock, Config) of
        true ->
            {seconds, 20};
        false ->
            {seconds, 150}
    end.

maybe_mock_snowflake(Config) when is_list(Config) ->
    maybe_mock_snowflake(maps:from_list(Config));
maybe_mock_snowflake(#{mock := true}) ->
    mock_snowflake();
maybe_mock_snowflake(#{mock := false}) ->
    [].

mock_snowflake() ->
    TId = ets:new(snowflake, [public, ordered_set]),
    Mod = ?CONN_MOD,
    ok = meck:new(Mod, [passthrough, no_history]),
    on_exit(fun() -> meck:unload() end),
    meck:expect(Mod, connect, fun(_Opts) -> spawn_dummy_connection_pid() end),
    meck:expect(Mod, disconnect, fun(_ConnPid) -> ok end),
    meck:expect(Mod, do_stage_file, fun(
        _ConnPid, Filename, _Database, _Schema, _Stage, _ActionName
    ) ->
        %% Todo: handle other container types
        {ok, Content} = file:read_file(Filename),
        {ok, [Headers0 | Rows0]} = erl_csv:decode(Content),
        Headers1 = lists:map(fun string:uppercase/1, Headers0),
        Rows1 = lists:map(
            fun(R0) ->
                csv_row_to_mocked_row(Headers1, R0)
            end,
            Rows0
        ),
        ets:insert(TId, {Filename, Rows1}),
        Headers = [
            "source",
            "target",
            "source_size",
            "target_size",
            "source_compression",
            "target_compression",
            "status",
            "encryption",
            "message"
        ],
        Rows = [
            {
                str(Filename),
                str(Filename) ++ ".gz",
                "5",
                "32",
                "none",
                "gzip",
                "UPLOADED",
                "ENCRYPTED",
                ""
            }
        ],
        {selected, Headers, Rows}
    end),
    meck:expect(Mod, do_health_check_connector, fun(_ConnPid) -> true end),
    %% Used in health checks
    meck:expect(Mod, do_insert_report_request, fun(_HTTPPool, _Req, _RequestTTL, _MaxRetries) ->
        Headers = [],
        Body = emqx_utils_json:encode(#{}),
        {ok, 200, Headers, Body}
    end),
    meck:expect(Mod, do_insert_files_request, fun(_HTTPPool, _Req, _RequestTTL, _MaxRetries) ->
        Headers = [],
        Body = emqx_utils_json:encode(#{}),
        ?tp("mock_snowflake_insert_file_request", #{}),
        {ok, 200, Headers, Body}
    end),
    [{mocked_table, TId}].

csv_row_to_mocked_row(Headers, Row) ->
    maps:map(
        fun
            (<<"PUBLISH_RECEIVED_AT">>, DTBin) ->
                {ok, EpochMS} = emqx_utils_calendar:to_epoch_millisecond(str(DTBin)),
                calendar:system_time_to_local_time(EpochMS, millisecond);
            (<<"PAYLOAD">>, <<>>) ->
                null;
            (_K, V) ->
                V
        end,
        maps:from_list(lists:zip(Headers, Row))
    ).

spawn_dummy_connection_pid() ->
    DummyConnPid = spawn_link(fun() ->
        receive
            die -> ok
        end
    end),
    on_exit(fun() ->
        Ref = monitor(process, DummyConnPid),
        DummyConnPid ! die,
        receive
            {'DOWN', Ref, process, DummyConnPid, _} ->
                ok
        after 200 ->
            ct:fail("dummy connection ~p didn't die", [DummyConnPid])
        end
    end),
    {ok, DummyConnPid}.

connector_config(Name, AccountId, Server, Username, Password) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"server">> => Server,
            <<"username">> => Username,
            <<"password">> => Password,
            <<"account">> => AccountId,
            <<"dsn">> => <<"snowflake">>,
            <<"pool_size">> => 1,
            <<"ssl">> => #{<<"enable">> => false},
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, Name, InnerConfigMap0).

aggregated_action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"mode">> => <<"aggregated">>,
                    <<"aggregation">> => #{
                        <<"container">> => #{
                            <<"type">> => <<"csv">>,
                            <<"column_order">> => ?CONF_COLUMN_ORDER
                        },
                        <<"time_interval">> => <<"5s">>,
                        <<"max_records">> => 3
                    },
                    <<"private_key">> => private_key(),
                    <<"database">> => ?DATABASE,
                    <<"schema">> => ?SCHEMA,
                    <<"pipe">> => ?PIPE,
                    <<"stage">> => ?STAGE,
                    <<"pipe_user">> => ?PIPE_USER,
                    <<"connect_timeout">> => <<"5s">>,
                    <<"pipelining">> => 100,
                    <<"pool_size">> => 1,
                    <<"max_retries">> => 3
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"1s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"15s">>,
                <<"resume_interval">> => <<"1s">>,
                <<"worker_pool_size">> => 1
            }
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

private_key() ->
    case os:getenv("SNOWFLAKE_PRIVATE_KEY") of
        false ->
            JWK = jose_jwk:generate_key({rsa, 2048}),
            {_, PEM} = jose_jwk:to_pem(JWK),
            PEM;
        FileURI ->
            %% file:///path/to/private-key.pem
            list_to_binary(FileURI)
    end.

aggreg_id(Config) ->
    ActionName = ?config(action_name, Config),
    {?ACTION_TYPE_BIN, ActionName}.

new_odbc_client(Config) when is_list(Config) ->
    new_odbc_client(maps:from_list(Config));
new_odbc_client(#{mock := true} = _Config) ->
    self();
new_odbc_client(#{mock := false} = Config) ->
    #{
        username := Username,
        password := Password,
        server := Server,
        account_id := AccountId
    } = Config,
    DSN = <<"snowflake">>,
    Opts = [
        {username, Username},
        {password, Password},
        {account, AccountId},
        {server, Server},
        {dsn, DSN}
    ],
    {ok, ConnPid} = emqx_bridge_snowflake_connector:connect(Opts),
    ConnPid.

stop_odbc_client(Config) ->
    ConnPid = ?config(odbc_client, Config),
    ok = emqx_bridge_snowflake_connector:disconnect(ConnPid).

str(IOList) ->
    emqx_utils_conv:str(iolist_to_binary(IOList)).

fqn(Database, Schema, Thing) ->
    [Database, <<".">>, Schema, <<".">>, Thing].

clear_stage(Config) when is_list(Config) ->
    clear_stage(maps:from_list(Config));
clear_stage(#{mock := true} = _Config) ->
    ok;
clear_stage(#{mock := false} = Config) ->
    #{odbc_client := ConnPid} = Config,
    SQL = str([
        <<"remove @">>,
        fqn(?DATABASE, ?SCHEMA, ?STAGE)
    ]),
    {selected, _Header, _Rows} = sql_query(ConnPid, SQL),
    ok.

create_table(Config) ->
    ConnPid = ?config(odbc_client, Config),
    SQL = str([
        <<"create or replace table ">>,
        fqn(?DATABASE, ?SCHEMA, ?TABLE),
        <<" (">>,
        <<"clientid string">>,
        <<", topic string">>,
        <<", payload binary">>,
        <<", publish_received_at timestamp_ltz">>,
        <<")">>
    ]),
    {updated, _} = sql_query(ConnPid, SQL),
    ok.

truncate_table(Config) when is_list(Config) ->
    truncate_table(maps:from_list(Config));
truncate_table(#{mock := true} = _Config) ->
    ok;
truncate_table(Config) ->
    #{odbc_client := ConnPid} = Config,
    SQL = str([
        <<"truncate ">>,
        fqn(?DATABASE, ?SCHEMA, ?TABLE)
    ]),
    {updated, _} = sql_query(ConnPid, SQL),
    ok.

row_to_map(Row0, Headers) ->
    Row1 = tuple_to_list(Row0),
    Row2 = lists:map(
        fun
            (Str) when is_list(Str) ->
                emqx_utils_conv:bin(Str);
            (Cell) ->
                Cell
        end,
        Row1
    ),
    Row = lists:zip(Headers, Row2),
    maps:from_list(Row).

get_all_rows(Config) when is_list(Config) ->
    get_all_rows(maps:from_list(Config));
get_all_rows(#{mock := true} = Config) ->
    #{mocked_table := TId} = Config,
    lists:sort(
        fun(#{<<"CLIENTID">> := CIdA}, #{<<"CLIENTID">> := CIdB}) ->
            CIdA =< CIdB
        end,
        [Row || {_File, Rows} <- ets:tab2list(TId), Row <- Rows]
    );
get_all_rows(#{mock := false} = Config) ->
    #{odbc_client := ConnPid} = Config,
    SQL0 = str([
        <<"use warehouse ">>,
        ?WAREHOUSE
    ]),
    {updated, _} = sql_query(ConnPid, SQL0),
    SQL1 = str([
        <<"select * from ">>,
        fqn(?DATABASE, ?SCHEMA, ?TABLE),
        <<" order by clientid">>
    ]),
    {selected, Headers0, Rows} = sql_query(ConnPid, SQL1),
    Headers = lists:map(fun list_to_binary/1, Headers0),
    lists:map(fun(R) -> row_to_map(R, Headers) end, Rows).

sql_query(ConnPid, SQL) ->
    ct:pal("running:\n  ~s", [SQL]),
    Res = odbc:sql_query(ConnPid, SQL),
    ct:pal("result:\n  ~p", [Res]),
    Res.

mk_message({ClientId, Topic, Payload}) ->
    emqx_message:make(bin(ClientId), bin(Topic), Payload).

publish_messages(MessageEvents) ->
    lists:foreach(fun emqx:publish/1, MessageEvents).

get_begin_mark(Config, ActionResId) when is_list(Config) ->
    get_begin_mark(maps:from_list(Config), ActionResId);
get_begin_mark(#{mock := true}, _ActionResId) ->
    <<"mocked">>;
get_begin_mark(#{mock := false}, ActionResId) ->
    {ok, #{<<"nextBeginMark">> := BeginMark}} =
        emqx_bridge_snowflake_connector:insert_report(ActionResId, #{}),
    BeginMark.

wait_until_processed(Config, ActionResId, BeginMark) ->
    wait_until_processed(Config, ActionResId, BeginMark, _ExpectedNumFiles = 1).

wait_until_processed(Config, ActionResId, BeginMark, ExpectedNumFiles) when is_list(Config) ->
    wait_until_processed(maps:from_list(Config), ActionResId, BeginMark, ExpectedNumFiles);
wait_until_processed(#{mock := true} = Config, _ActionResId, _BeginMark, ExpectedNumFiles) ->
    snabbkaffe:block_until(
        ?match_n_events(
            ExpectedNumFiles,
            #{?snk_kind := "mock_snowflake_insert_file_request"}
        ),
        _Timeout = infinity,
        _BackInTIme = infinity
    ),
    InsertRes = maps:get(mocked_insert_report, Config, #{}),
    {ok, InsertRes};
wait_until_processed(#{mock := false} = Config, ActionResId, BeginMark, ExpectedNumFiles) ->
    {ok, Res} =
        emqx_bridge_snowflake_connector:insert_report(ActionResId, #{begin_mark => BeginMark}),
    ct:pal("insert report (begin mark ~s):\n  ~p", [BeginMark, Res]),
    case Res of
        #{
            <<"files">> := Files,
            <<"statistics">> := #{<<"activeFilesCount">> := 0}
        } when length(Files) >= ExpectedNumFiles ->
            ct:pal("insertReport response:\n  ~p", [Res]),
            {ok, Res};
        _ ->
            ct:sleep(2_000),
            wait_until_processed(Config, ActionResId, BeginMark, ExpectedNumFiles)
    end.

bin2hex(Bin) ->
    emqx_rule_funcs:bin2hexstr(Bin).

sql1() ->
    <<
        "SELECT "
        "  clientid,"
        "  topic,"
        %% NOTE: binary columns in snowflake must be hex-encoded...
        "  bin2hexstr(payload) as payload,"
        "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') "
        "    as publish_received_at "
        "FROM 'sf/#'"
    >>.

set_max_records(TCConfig, MaxRecords) ->
    emqx_bridge_v2_testlib:proplist_update(TCConfig, action_config, fun(Old) ->
        emqx_utils_maps:deep_put(
            [<<"parameters">>, <<"aggregation">>, <<"max_records">>],
            Old,
            MaxRecords
        )
    end).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "snowflake_connector_stop"),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

%% Unfortunately, there's no way to use toxiproxy to proxy to the real snowflake, as it
%% requires SSL and the hostname mismatch impedes the connection...
t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

%% Happy path smoke test for aggregated mode upload.
t_aggreg_upload(Config) ->
    AggregId = aggreg_id(Config),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            %% Create a bridge with the sample configuration.
            ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            BeginMark = get_begin_mark(Config, ActionResId),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => sql1()
                    }
                ),
            Messages1 = lists:map(fun mk_message/1, [
                {<<"C1">>, T1 = <<"sf/a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
                {<<"C2">>, T2 = <<"sf/foo/bar">>, P2 = <<"baz">>},
                {<<"C3">>, T3 = <<"sf/t/42">>, P3 = <<"">>},
                %% Won't match rule filter
                {<<"C4">>, <<"t/42">>, <<"won't appear in results">>}
            ]),
            ok = publish_messages(Messages1),
            ?tpal("published first batch"),
            %% Wait until the delivery is completed.
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
            ?tpal("first batch delivered"),
            %% Send a second batch of messages to be staged in a second file
            Messages2 = lists:map(fun mk_message/1, [
                {<<"C4">>, T4 = <<"sf/a/b/c">>, P4 = <<"{\"hello\":\"world\"}">>},
                {<<"C5">>, T5 = <<"sf/foo/bar">>, P5 = <<"baz">>},
                {<<"C6">>, T6 = <<"sf/t/42">>, P6 = <<"">>}
            ]),
            {ok, {ok, _}} =
                ?wait_async_action(
                    begin
                        publish_messages(Messages2),
                        ?tpal("published second batch")
                    end,
                    #{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}
                ),
            ?tpal("second batch delivered"),
            %% Check the uploaded objects.
            ExpectedNumFiles = 2,
            wait_until_processed(Config, ActionResId, BeginMark, ExpectedNumFiles),
            Rows = get_all_rows(Config),
            [
                P1Hex,
                P2Hex,
                _P3Hex,
                P4Hex,
                P5Hex,
                _P6Hex
            ] = lists:map(fun bin2hex/1, [P1, P2, P3, P4, P5, P6]),
            ?assertMatch(
                [
                    #{
                        <<"CLIENTID">> := <<"C1">>,
                        <<"PAYLOAD">> := P1Hex,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T1
                    },
                    #{
                        <<"CLIENTID">> := <<"C2">>,
                        <<"PAYLOAD">> := P2Hex,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T2
                    },
                    #{
                        <<"CLIENTID">> := <<"C3">>,
                        <<"PAYLOAD">> := null,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T3
                    },
                    #{
                        <<"CLIENTID">> := <<"C4">>,
                        <<"PAYLOAD">> := P4Hex,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T4
                    },
                    #{
                        <<"CLIENTID">> := <<"C5">>,
                        <<"PAYLOAD">> := P5Hex,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T5
                    },
                    #{
                        <<"CLIENTID">> := <<"C6">>,
                        <<"PAYLOAD">> := null,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T6
                    }
                ],
                Rows
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind("snowflake_stage_file_skipped", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we flush any pending data when `process_complete' is called.
t_aggreg_upload_flush_on_complete(Config) ->
    AggregId = aggreg_id(Config),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            %% Create a bridge with the sample configuration.
            ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            BeginMark = get_begin_mark(Config, ActionResId),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => sql1()
                    }
                ),
            Messages = lists:map(fun mk_message/1, [
                {<<"C1">>, T1 = <<"sf/a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>}
            ]),
            ok = publish_messages(Messages),
            %% Wait until the delivery is completed.
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
            %% Check the uploaded objects.
            wait_until_processed(Config, ActionResId, BeginMark),
            Rows = get_all_rows(Config),
            P1Hex = bin2hex(P1),
            ?assertMatch(
                [
                    #{
                        <<"CLIENTID">> := <<"C1">>,
                        <<"PAYLOAD">> := P1Hex,
                        %% {Day, Time}
                        <<"PUBLISH_RECEIVED_AT">> := {_, _},
                        <<"TOPIC">> := T1
                    }
                ],
                Rows
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind("snowflake_flush_on_complete", Trace)),
            ok
        end
    ),
    ok.

t_aggreg_upload_restart_corrupted(Config0) ->
    MaxRecords = 10,
    BatchSize = MaxRecords div 2,
    Config = set_max_records(Config0, MaxRecords),
    Opts = #{
        aggreg_id => aggreg_id(Config),
        batch_size => BatchSize,
        rule_sql => sql1(),
        prepare_fn => fun(Context) ->
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            BeginMark = get_begin_mark(Config, ActionResId),
            Context#{begin_mark => BeginMark}
        end,
        make_message_fn => fun(N) ->
            mk_message(
                {integer_to_binary(N), <<"sf/a/b/c">>, <<"{\"hello\":\"world\"}">>}
            )
        end,
        message_check_fn => fun(Context) ->
            #{
                messages_before := Messages1,
                messages_after := Messages2,
                begin_mark := BeginMark
            } = Context,
            %% Check the uploaded objects.
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            wait_until_processed(Config, ActionResId, BeginMark),
            Rows = get_all_rows(Config),
            NRows = length(Rows),
            ?assert(NRows > BatchSize, #{rows => Rows}),
            Expected0 = [
                {ClientId, Topic, bin2hex(Payload)}
             || #message{
                    from = ClientId,
                    topic = Topic,
                    payload = Payload
                } <- lists:sublist(Messages1, NRows - BatchSize) ++ Messages2
            ],
            Expected = lists:sort(fun({CIdA, _, _}, {CIdB, _, _}) -> CIdA =< CIdB end, Expected0),
            ?assertEqual(
                Expected,
                [
                    {ClientId, Topic, Payload}
                 || #{
                        <<"CLIENTID">> := ClientId,
                        <<"TOPIC">> := Topic,
                        <<"PAYLOAD">> := Payload
                    } <- Rows
                ],
                #{rows => Rows}
            ),

            ok
        end
    },
    emqx_bridge_v2_testlib:t_aggreg_upload_restart_corrupted(Config, Opts),
    ok.

%% Verifies Snowflake's behavior when the rule outputs more columns than it expects.
%% Currently, using CSV containers, it'll simply drop the file and import nothing.
t_aggreg_wrong_number_of_columns(init, Config) when is_list(Config) ->
    t_aggreg_wrong_number_of_columns(init, maps:from_list(Config));
t_aggreg_wrong_number_of_columns(init, #{mock := true} = Config) ->
    InsertReport = #{
        <<"completeResult">> => true,
        <<"files">> =>
            [
                #{
                    <<"complete">> => true,
                    <<"errorLimit">> => 3,
                    <<"errorsSeen">> => 3,
                    <<"fileSize">> => 336,
                    <<"firstError">> =>
                        <<
                            "Number of columns in file (16) does not match"
                            "that of the corresponding table (4), use file format"
                            "option error_on_column_count_mismatch=false to ignore this error"
                        >>,
                    <<"firstErrorCharacterPos">> => 1,
                    <<"firstErrorColumnName">> =>
                        <<"\"TEST1\"[16]">>,
                    <<"firstErrorLineNum">> => 3,
                    <<"lastInsertTime">> =>
                        <<"2024-09-03T18:06:00.406Z">>,
                    <<"path">> =>
                        <<
                            "action:snowflake:t_aggreg_wrong_number_of_columns"
                            "-576460752303421691:connector:snowflake:"
                            "t_aggreg_wrong_number_of_columns-576460752303421691_0.csv.gz"
                        >>,
                    <<"rowsInserted">> => 0,
                    <<"rowsParsed">> => 3,
                    <<"stageLocation">> =>
                        <<"stages/709e202f-fc1f-42a0-be13-d8ba2f1e33c6/">>,
                    <<"status">> => <<"LOAD_FAILED">>,
                    <<"timeReceived">> =>
                        <<"2024-09-03T18:05:43.648Z">>
                }
            ],
        <<"nextBeginMark">> => <<"4_15">>,
        <<"pipe">> => <<"TESTDATABASE.PUBLIC.TESTPIPE0">>,
        <<"statistics">> =>
            #{<<"activeFilesCount">> => 0}
    },
    %% Snowflake stages the file, but inserts no rows
    meck:expect(?CONN_MOD, do_stage_file, fun(
        _ConnPid, Filename, _Database, _Schema, _Stage, _ActionNmame
    ) ->
        Headers = [
            "source",
            "target",
            "source_size",
            "target_size",
            "source_compression",
            "target_compression",
            "status",
            "encryption",
            "message"
        ],
        Rows = [
            {
                str(Filename),
                str(Filename) ++ ".gz",
                "5",
                "32",
                "none",
                "gzip",
                "UPLOADED",
                "ENCRYPTED",
                ""
            }
        ],
        {selected, Headers, Rows}
    end),
    [{mocked_insert_report, InsertReport} | maps:to_list(Config)];
t_aggreg_wrong_number_of_columns(init, #{} = Config) ->
    maps:to_list(Config).
t_aggreg_wrong_number_of_columns(Config) ->
    AggregId = aggreg_id(Config),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {ok, _Bridge} = emqx_bridge_v2_testlib:create_bridge_api(Config),
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            BeginMark = get_begin_mark(Config, ActionResId),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => <<
                            "SELECT "
                            "  *,"
                            "  clientid,"
                            "  topic,"
                            %% NOTE: binary columns in snowflake must be hex-encoded...
                            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') "
                            "    as publish_received_at "
                            "FROM 'sf/#'"
                        >>
                    }
                ),
            Messages = lists:map(fun mk_message/1, [
                {<<"C1">>, <<"sf/a/b/c">>, <<"{\"hello\":\"world\"}">>},
                {<<"C2">>, <<"sf/foo/bar">>, <<"baz">>},
                {<<"C3">>, <<"sf/t/42">>, <<"">>}
            ]),
            ok = publish_messages(Messages),
            %% Wait until the delivery is completed.
            ct:pal("waiting for delivery process to finish..."),
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
            %% Check the uploaded objects.
            ct:pal("waiting for uploads to be processed..."),
            {ok, InsertRes} = wait_until_processed(Config, ActionResId, BeginMark),
            Rows = get_all_rows(Config),
            %% Snowflake inserts nothing in this case
            ?assertMatch([], Rows),
            ?assertMatch(
                #{
                    <<"files">> := [
                        #{
                            <<"rowsInserted">> := 0,
                            <<"rowsParsed">> := 3
                        }
                    ]
                },
                InsertRes
            ),
            ok
        end,
        []
    ),
    ok.

%% Verifies Snowflake's behavior when there are problems with input data (wrong types).
t_aggreg_invalid_column_values(init, Config) when is_list(Config) ->
    t_aggreg_invalid_column_values(init, maps:from_list(Config));
t_aggreg_invalid_column_values(init, #{mock := true} = Config) ->
    #{mocked_table := TId} = Config,
    InsertReport = #{
        <<"completeResult">> => true,
        <<"files">> =>
            [
                #{
                    <<"complete">> => true,
                    <<"errorLimit">> => 5,
                    <<"errorsSeen">> => 3,
                    <<"fileSize">> => 160,
                    <<"firstError">> =>
                        <<"The following string is not a legal hex-encoded value: 'a'">>,
                    <<"firstErrorCharacterPos">> => 55,
                    <<"firstErrorColumnName">> => <<"\"TEST1\"[\"PAYLOAD\":3]">>,
                    <<"firstErrorLineNum">> => 3,
                    <<"lastInsertTime">> => <<"2024-09-03T19:05:08.387Z">>,
                    <<"path">> =>
                        <<
                            "action:snowflake:t_aggreg_invalid_column_values-576460752303421883"
                            ":connector:snowflake:t_aggreg_invalid_column_values"
                            "-576460752303421883_0.csv.gz"
                        >>,
                    <<"rowsInserted">> => 2,
                    <<"rowsParsed">> => 5,
                    <<"stageLocation">> =>
                        <<"stages/709e202f-fc1f-42a0-be13-d8ba2f1e33c6/">>,
                    <<"status">> => <<"PARTIALLY_LOADED">>,
                    <<"timeReceived">> => <<"2024-09-03T19:04:50.631Z">>
                }
            ],
        <<"nextBeginMark">> => <<"8_27">>,
        <<"pipe">> => <<"TESTDATABASE.PUBLIC.TESTPIPE0">>,
        <<"statistics">> => #{<<"activeFilesCount">> => 0}
    },
    meck:expect(?CONN_MOD, do_stage_file, fun(
        _ConnPid, Filename, _Database, _Schema, _Stage, _ActionName
    ) ->
        {ok, Content} = file:read_file(Filename),
        {ok, [Headers0 | Rows0]} = erl_csv:decode(Content),
        Headers1 = lists:map(fun string:uppercase/1, Headers0),
        Rows1 = lists:map(
            fun(R0) ->
                csv_row_to_mocked_row(Headers1, R0)
            end,
            Rows0
        ),
        Rows2 = lists:filter(
            fun(#{<<"CLIENTID">> := CId}) ->
                lists:member(CId, [<<"ok">>, <<"null">>])
            end,
            Rows1
        ),
        ets:insert(TId, {Filename, Rows2}),
        Headers = [
            "source",
            "target",
            "source_size",
            "target_size",
            "source_compression",
            "target_compression",
            "status",
            "encryption",
            "message"
        ],
        Rows = [
            {
                str(Filename),
                str(Filename) ++ ".gz",
                "5",
                "32",
                "none",
                "gzip",
                "UPLOADED",
                "ENCRYPTED",
                ""
            }
        ],
        {selected, Headers, Rows}
    end),
    [{mocked_insert_report, InsertReport} | maps:to_list(Config)];
t_aggreg_invalid_column_values(init, #{} = Config) ->
    maps:to_list(Config).
t_aggreg_invalid_column_values(Config0) ->
    MaxRecords = 5,
    Config = set_max_records(Config0, MaxRecords),
    AggregId = aggreg_id(Config),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {ok, _Bridge} = emqx_bridge_v2_testlib:create_bridge_api(Config),
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            BeginMark = get_begin_mark(Config, ActionResId),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => <<
                            "SELECT "
                            "  clientid,"
                            "  topic,"
                            "  CASE "
                            "    WHEN clientid = 'null' THEN null "
                            "    WHEN clientid = 'missing-encode' THEN payload "
                            "    WHEN clientid = 'number' THEN 123 "
                            "    WHEN clientid = 'bool' THEN false "
                            "    ELSE bin2hexstr(payload) "
                            "  END as payload,"
                            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') "
                            "    as publish_received_at "
                            "FROM 'sf/#'"
                        >>
                    }
                ),
            Messages = lists:map(fun mk_message/1, [
                {<<"null">>, <<"sf/a/b/c">>, <<"a">>},
                {<<"missing-encode">>, <<"sf/a/b/c">>, <<"a">>},
                {<<"number">>, <<"sf/a/b/c">>, <<"a">>},
                {<<"bool">>, <<"sf/a/b/c">>, <<"a">>},
                {<<"ok">>, <<"sf/a/b/c">>, OkPayload = <<"a">>}
            ]),
            ok = publish_messages(Messages),
            %% Wait until the delivery is completed.
            ct:pal("waiting for delivery process to finish..."),
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
            %% Check the uploaded objects.
            ct:pal("waiting for uploads to be processed..."),
            {ok, InsertRes} = wait_until_processed(Config, ActionResId, BeginMark),
            Rows = get_all_rows(Config),
            %% Snowflake skips rows with problems, when `ON_ERROR = CONTINUE'.
            OkPayloadEnc = bin2hex(OkPayload),
            ?assertMatch(
                [
                    #{
                        <<"CLIENTID">> := <<"null">>,
                        <<"PAYLOAD">> := null
                    },
                    #{
                        <<"CLIENTID">> := <<"ok">>,
                        <<"PAYLOAD">> := OkPayloadEnc
                    }
                ],
                Rows
            ),
            ?assertMatch(
                #{
                    <<"files">> := [
                        #{
                            <<"rowsInserted">> := 2,
                            <<"rowsParsed">> := 5
                        }
                    ]
                },
                InsertRes
            ),
            ok
        end,
        []
    ),
    ok.

%% Checks that we enqueue aggregated buffer errors if the delivery fails, and that
%% reflects on the action status.
t_aggreg_failed_delivery(init, Config) when is_list(Config) ->
    t_aggreg_failed_delivery(init, maps:from_list(Config));
t_aggreg_failed_delivery(init, #{mock := true} = Config) ->
    Mod = ?CONN_MOD,
    meck:expect(Mod, do_insert_files_request, fun(
        _HTTPPool, _Req, _RequestTTL, _MaxRetries
    ) ->
        Headers = [
            {<<"content-type">>, <<"application/json">>},
            {<<"date">>, <<"Wed, 09 Oct 2024 13:12:55 GMT">>},
            {<<"strict-transport-security">>, <<"max-age=31536000">>},
            {<<"x-content-type-options">>, <<"nosniff">>},
            {<<"x-frame-options">>, <<"deny">>},
            {<<"content-length">>, <<"175">>},
            {<<"connection">>, <<"keep-alive">>}
        ],
        Body = <<
            "{\n  \"data\" : null,\n  \"code\" : \"390403\",\n  "
            "\"message\" : \"Not authorized to manage the specified object. "
            "Pipe access permission denied\",\n  \"success\" : false,\n  "
            "\"headers\" : null\n}"
        >>,
        {ok, 403, Headers, Body}
    end),
    maps:to_list(Config);
t_aggreg_failed_delivery(init, #{} = Config) ->
    maps:to_list(Config).
t_aggreg_failed_delivery(Config) ->
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {ok, _} = emqx_bridge_v2_testlib:create_bridge_api(
                Config,
                #{<<"parameters">> => #{<<"pipe_user">> => ?PIPE_USER_RO}}
            ),
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            %% BeginMark = get_begin_mark(Config, ActionResId),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => sql1()
                    }
                ),
            Messages1 = lists:map(fun mk_message/1, [
                {<<"C1">>, <<"sf/a/b/c">>, <<"{\"hello\":\"world\"}">>},
                {<<"C2">>, <<"sf/foo/bar">>, <<"baz">>},
                {<<"C3">>, <<"sf/t/42">>, <<"">>}
            ]),
            ok = publish_messages(Messages1),
            %% Wait until the insert files request fails
            ct:pal("waiting for delivery to fail..."),
            ?block_until(#{?snk_kind := "aggregated_buffer_delivery_failed"}),
            %% When channel health check happens, we check aggregator for errors.
            %% Current implementation will mark the action as unhealthy.
            ct:pal("waiting for delivery failure to be noticed by health check..."),
            ?block_until(#{?snk_kind := "snowflake_check_aggreg_upload_error_found"}),

            ?retry(
                _Sleep = 500,
                _Retries = 10,
                ?assertMatch(
                    {200, #{
                        <<"error">> :=
                            <<"{unhealthy_target,", _/binary>>
                    }},
                    emqx_bridge_v2_testlib:simplify_result(
                        emqx_bridge_v2_testlib:get_action_api(Config)
                    )
                )
            ),

            ?assertEqual(3, emqx_resource_metrics:matched_get(ActionResId)),
            %% Currently, failure metrics are not bumped when aggregated uploads fail
            ?assertEqual(0, emqx_resource_metrics:failed_get(ActionResId)),

            ok
        end,
        []
    ),
    ok.

%% Checks that we detect early that the configured snowpipe user does not have the proper
%% credentials (or does not exist) for accessing Snowpipe's REST API.
t_wrong_snowpipe_user(init, Config) when is_list(Config) ->
    t_wrong_snowpipe_user(init, maps:from_list(Config));
t_wrong_snowpipe_user(init, #{mock := true} = Config) ->
    Mod = ?CONN_MOD,
    InsertReportResponse = #{
        <<"code">> => <<"390144">>,
        <<"data">> => null,
        <<"headers">> => null,
        <<"message">> => <<"JWT token is invalid. [92d86b2e-d652-4d2d-9780-a6ed28b38356]">>,
        <<"success">> => false
    },
    meck:expect(Mod, do_insert_report_request, fun(_HTTPPool, _Req, _RequestTTL, _MaxRetries) ->
        Headers = [],
        Body = emqx_utils_json:encode(InsertReportResponse),
        {ok, 401, Headers, Body}
    end),
    meck:expect(Mod, do_get_login_failure_details, fun(_Connpid, _RequestId) ->
        Details = #{
            <<"clientIP">> => <<"127.0.0.1">>,
            <<"clientType">> => <<"OTHER">>,
            <<"clientVersion">> => <<"">>,
            <<"errorCode">> => <<"JWT_TOKEN_INVALID_ISSUE_TIME">>,
            <<"timestamp">> => 1728418411,
            <<"username">> => null
        },
        Col = binary_to_list(emqx_utils_json:encode(Details)),
        {selected, ["SYSTEM$GET_LOGIN_FAILURE_DETAILS('92D86B2E-D652-4D2D-9780-A6ED28B38356')"], [
            {Col}
        ]}
    end),
    maps:to_list(Config);
t_wrong_snowpipe_user(init, #{} = Config) ->
    maps:to_list(Config).
t_wrong_snowpipe_user(Config) ->
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {ok, _} = emqx_bridge_v2_testlib:create_connector_api(Config),
            ?assertMatch(
                {ok,
                    {{_, 201, _}, _, #{
                        <<"status">> := <<"disconnected">>,
                        <<"error">> := <<"{unhealthy_target,", _/binary>>
                    }}},
                emqx_bridge_v2_testlib:create_kind_api(
                    Config,
                    #{<<"parameters">> => #{<<"pipe_user">> => <<"idontexist">>}}
                )
            ),
            ok
        end,
        []
    ),
    ok.

%% Todo: test scenarios
%% * User error in rule definition; e.g.:
%%    - forgot to use `bin2hexstr' to encode the payload
%% * Incoming data does not conform to the table schema
%%    - Different `ON_ERROR' options:
%%       + `SKIP_FILE'
%%       + `CONTINUE'
%%       + Not supported when using pipes: `ABORT_STATEMENT'
%%    - Missing data for a required column
%% * Transient failure when staging file
