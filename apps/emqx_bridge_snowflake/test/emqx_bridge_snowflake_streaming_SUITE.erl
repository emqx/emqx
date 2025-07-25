%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_streaming_SUITE).

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

-define(CONN_MOD_STREAM, emqx_bridge_snowflake_streaming_impl).
-define(CHAN_CLIENT_MOD, emqx_bridge_snowflake_channel_client).

-define(DATABASE, <<"testdatabase">>).
-define(SCHEMA, <<"public">>).
-define(TABLE, <<"test0">>).
-define(TABLE_STREAMING, <<"emqx">>).
-define(WAREHOUSE, <<"testwarehouse">>).
-define(PIPE_STREAMING, <<"emqxstreaming">>).
-define(PIPE_USER, <<"snowpipeuser">>).

-define(tpal(MSG), begin
    ct:pal(MSG),
    ?tp(notice, MSG, #{})
end).

-define(batching, batching).
-define(not_batching, not_batching).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                get_tc_prop(TestCase, matrix, false);
            (TestCase) ->
                get_tc_prop(TestCase, matrix, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

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
    Server = ?config(server, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, AccountId, Server),
    ActionConfig0 = action_config(#{connector => Name}, Config0),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        ?ACTION_TYPE_STREAM_BIN, Name, ActionConfig0
    ),
    ExtraConfig0 = maybe_mock_snowflake(Config0),
    ConnPid = emqx_bridge_snowflake_aggregated_SUITE:new_odbc_client(Config0),
    Config =
        ExtraConfig0 ++
            [
                {bridge_kind, action},
                {action_type, ?ACTION_TYPE_STREAM_BIN},
                {action_name, Name},
                {action_config, ActionConfig},
                {connector_name, Name},
                {connector_type, ?CONNECTOR_TYPE_STREAM_BIN},
                {connector_config, ConnectorConfig},
                {odbc_client, ConnPid}
                | Config0
            ],
    ok = truncate_table(Config),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true ->
            ?MODULE:TestCase(init, Config);
        false ->
            Config
    end.

end_per_testcase(_Testcase, Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = truncate_table(Config),
    emqx_bridge_snowflake_aggregated_SUITE:stop_odbc_client(Config),
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

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
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

get_matrix_prop(TCConfig, Alternatives, Default) ->
    GroupPath = group_path(TCConfig, [Default]),
    case lists:filter(fun(G) -> lists:member(G, Alternatives) end, GroupPath) of
        [] ->
            Default;
        [Opt] ->
            Opt
    end.

is_batching(TCConfig) ->
    get_matrix_prop(TCConfig, [?not_batching, ?batching], ?batching).

maybe_mock_snowflake(Config) when is_list(Config) ->
    maybe_mock_snowflake(maps:from_list(Config));
maybe_mock_snowflake(#{mock := true}) ->
    mock_snowflake();
maybe_mock_snowflake(#{mock := false}) ->
    [].

mock_snowflake() ->
    TId = ets:new(snowflake, [public, ordered_set]),
    Mod = ?CONN_MOD_STREAM,
    ok = meck:new(Mod, [passthrough, no_history]),
    ok = meck:new(?CHAN_CLIENT_MOD, [passthrough, no_history]),
    on_exit(fun() -> meck:unload() end),
    {ok, {Port, _}} = emqx_utils_http_test_server:start_link(random, "/", server_ssl_opts()),
    on_exit(fun() ->
        persistent_term:erase({?CONN_MOD_STREAM, streaming_port})
    end),
    persistent_term:put({?CONN_MOD_STREAM, streaming_port}, Port),
    meck:expect(Mod, do_get_streaming_hostname, fun(_HTTPPool, _Req, _RequestTTL, _MaxRetries) ->
        Headers = [],
        Body = <<"127.0.0.1">>,
        ?tp("mock_snowflake_get_streaming_hostname", #{}),
        {ok, 200, Headers, Body}
    end),
    meck:expect(?CHAN_CLIENT_MOD, do_open_channel, fun(_HTTPPool, _Req, _RequestTTL, _MaxRetries) ->
        Headers = [],
        Body = emqx_utils_json:encode(#{<<"next_continuation_token">> => <<"0_1">>}),
        ?tp("mock_snowflake_open_channel", #{}),
        {ok, 200, Headers, Body}
    end),
    meck:expect(?CHAN_CLIENT_MOD, do_append_rows, fun(_HTTPPool, Req, _RequestTTL, _MaxRetries) ->
        {_, _, BodyRaw} = Req,
        Records = emqx_connector_aggreg_json_lines_test_utils:decode(iolist_to_binary(BodyRaw)),
        Rows = lists:map(fun streaming_record_to_mocked_row/1, Records),
        ets:insert(TId, {erlang:monotonic_time(), Rows}),
        Headers = [],
        Body = emqx_utils_json:encode(#{<<"next_continuation_token">> => <<"1_1">>}),
        ?tp("mock_snowflake_append_rows", #{}),
        {ok, 200, Headers, Body}
    end),
    [{mocked_table, TId}].

generate_dummy_jwt() ->
    NowS = erlang:system_time(second),
    Exp = erlang:convert_time_unit(timer:hours(1), millisecond, second),
    ExpTime = NowS + Exp,
    Key = crypto:strong_rand_bytes(32),
    JWK = #{
        <<"kty">> => <<"oct">>,
        <<"k">> => jose_base64url:encode(Key)
    },
    JWS = #{
        <<"alg">> => <<"HS256">>
    },
    JWT = #{
        <<"iss">> => <<"ct">>,
        <<"exp">> => ExpTime
    },
    Signed = jose_jwt:sign(JWK, JWS, JWT),
    {_, Token} = jose_jws:compact(Signed),
    Token.

server_ssl_opts() ->
    [
        {keyfile, cert_path("server.key")},
        {certfile, cert_path("server.crt")},
        {cacertfile, cert_path("ca.crt")},
        {verify, verify_none},
        {versions, ['tlsv1.2', 'tlsv1.3']},
        {ciphers, ["ECDHE-RSA-AES256-GCM-SHA384", "TLS_CHACHA20_POLY1305_SHA256"]}
    ].

cert_path(FileName) ->
    Dir = code:lib_dir(emqx_auth),
    filename:join([Dir, <<"test/data/certs">>, FileName]).

streaming_record_to_mocked_row(Record0) ->
    maps:fold(
        fun
            (<<"payload">> = K, <<"">>, Acc) ->
                Acc#{string:uppercase(K) => null};
            (K, V, Acc) ->
                Acc#{string:uppercase(K) => V}
        end,
        #{},
        Record0
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

connector_config(Name, AccountId, Server) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"private_key">> => private_key(),
            <<"private_key_password">> => null,
            <<"pipe_user">> => ?PIPE_USER,
            <<"account">> => AccountId,
            <<"server">> => Server,
            <<"connect_timeout">> => <<"5s">>,
            <<"max_inactive">> => <<"10s">>,
            <<"pipelining">> => 100,
            <<"max_retries">> => 3,
            <<"ssl">> => #{<<"enable">> => false},
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_STREAM_BIN, Name, InnerConfigMap0
    ).

action_config(Overrides0, TCConfig) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"database">> => ?DATABASE,
                    <<"schema">> => ?SCHEMA,
                    <<"pipe">> => ?PIPE_STREAMING,
                    <<"connect_timeout">> => <<"5s">>,
                    <<"max_inactive">> => <<"10s">>,
                    <<"pipelining">> => 100,
                    <<"pool_size">> => 1,
                    <<"max_retries">> => 3
                },
            <<"resource_opts">> =>
                maps:merge(
                    emqx_bridge_v2_testlib:common_action_resource_opts(),
                    batch_opts(TCConfig)
                )
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

batch_opts(TCConfig) ->
    case is_batching(TCConfig) of
        ?batching ->
            #{
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>
            };
        ?not_batching ->
            #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>
            }
    end.

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

private_key_path() ->
    case os:getenv("SNOWFLAKE_PRIVATE_KEY_PATH") of
        false ->
            <<"/path/to/private.key">>;
        Filepath ->
            emqx_utils_conv:bin(Filepath)
    end.

private_key_password() ->
    case os:getenv("SNOWFLAKE_PRIVATE_KEY_PASSWORD") of
        false ->
            <<"supersecret">>;
        Password ->
            emqx_utils_conv:bin(Password)
    end.

str(IOList) ->
    emqx_utils_conv:str(iolist_to_binary(IOList)).

fqn(Database, Schema, Thing) ->
    [Database, <<".">>, Schema, <<".">>, Thing].

sql_query(ConnPid, SQL) ->
    emqx_bridge_snowflake_aggregated_SUITE:sql_query(ConnPid, SQL).

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
        fqn(?DATABASE, ?SCHEMA, table_of(Config))
    ]),
    {updated, _} = sql_query(ConnPid, SQL),
    ok.

table_of(_TCConfig) ->
    ?TABLE_STREAMING.

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
    Table = table_of(Config),
    SQL1 = str([
        <<"select * from ">>,
        fqn(?DATABASE, ?SCHEMA, Table),
        <<" order by clientid">>
    ]),
    {selected, Headers0, Rows} = sql_query(ConnPid, SQL1),
    Headers = lists:map(fun list_to_binary/1, Headers0),
    lists:map(fun(R) -> row_to_map(R, Headers) end, Rows).

mk_message({ClientId, Topic, Payload}) ->
    emqx_message:make(bin(ClientId), bin(Topic), Payload).

publish_messages(MessageEvents) ->
    lists:foreach(fun emqx:publish/1, MessageEvents).

bin2hex(Bin) ->
    emqx_rule_funcs:bin2hexstr(Bin).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(TCConfig, "snowflake_connector_stop"),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

%% Unfortunately, there's no way to use toxiproxy to proxy to the real snowflake, as it
%% requires SSL and the hostname mismatch impedes the connection...
t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

%% Smoke test for streaming mode.
t_streaming() ->
    [{matrix, true}].
t_streaming(matrix) ->
    [
        [?batching],
        [?not_batching]
    ];
t_streaming(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(
            2_000,
            10,
            ?assertMatch(
                [#{<<"PAYLOAD">> := Payload}],
                get_all_rows(TCConfig)
            )
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).
