%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_hstreamdb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_bridge_hstreamdb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% SQL definitions

-define(STREAM, "demo_stream").
%% could not be "stream" in Production Environment
%% especially not in hstreamdb_sql CLI client

-define(REPLICATION_FACTOR, 1).

%% in seconds
-define(BACKLOG_RETENTION_SECOND, (24 * 60 * 60)).
-define(SHARD_COUNT, 1).

-define(BRIDGE_NAME, <<"hstreamdb_demo_bridge">>).
-define(RECORD_TEMPLATE,
    "{ \"temperature\": ${payload.temperature}, \"humidity\": ${payload.humidity} }"
).

-define(POOL_SIZE, 8).
-define(BATCH_SIZE, 10).
-define(GRPC_TIMEOUT, "1s").

-define(WORKER_POOL_SIZE, 4).

-define(WITH_CLIENT(Process),
    Client = connect_direct_hstream(_Name = test_c, Config),
    Process,
    ok = disconnect(Client)
).

%% How to run it locally (all commands are run in $PROJ_ROOT dir):
%%   A: run ct on host
%%     1. Start all deps services
%%       ```bash
%%       sudo docker compose -f .ci/docker-compose-file/docker-compose.yaml \
%%                           -f .ci/docker-compose-file/docker-compose-hstreamdb.yaml \
%%                           -f .ci/docker-compose-file/docker-compose-toxiproxy.yaml \
%%                           up --build
%%       ```
%%
%%     2. Run use cases with special environment variables
%%       6570 is toxiproxy exported port.
%%       Local:
%%       ```bash
%%       HSTREAMDB_HOST=$REAL_TOXIPROXY_IP HSTREAMDB_PORT=6570 \
%%           PROXY_HOST=$REAL_TOXIPROXY_IP PROXY_PORT=6570 \
%%           ./rebar3 as test ct -c -v --readable true --name ct@127.0.0.1 \
%%                               --suite apps/emqx_bridge_hstreamdb/test/emqx_bridge_hstreamdb_SUITE.erl
%%       ```
%%
%%   B: run ct in docker container
%%     run script:
%%     ```bash
%%     ./scripts/ct/run.sh --ci --app apps/emqx_bridge_hstreamdb/ -- \
%%                         --name 'test@127.0.0.1' -c -v --readable true \
%%                         --suite apps/emqx_bridge_hstreamdb/test/emqx_bridge_hstreamdb_SUITE.erl
%%     ````

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, sync}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    NonBatchCases = [t_write_timeout],
    BatchingGroups = [{group, with_batch}, {group, without_batch}],
    [
        {sync, BatchingGroups},
        {with_batch, TCs -- NonBatchCases},
        {without_batch, TCs}
    ].

init_per_group(sync, Config) ->
    [{query_mode, sync} | Config];
init_per_group(with_batch, Config0) ->
    Config = [{enable_batch, true} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{enable_batch, false} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    Apps = ?config(apps, Config),
    connect_and_delete_stream(Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_resource, emqx_conf, hstreamdb_erl]),
    ok.

init_per_testcase(t_to_hrecord_failed, Config) ->
    init_per_testcase_common(),
    meck:new([hstreamdb], [passthrough, no_history, no_link]),
    meck:expect(hstreamdb, to_record, fun(_, _, _) -> error(trans_to_hrecord_failed) end),
    Config;
init_per_testcase(_Testcase, Config) ->
    init_per_testcase_common(),
    %% drop stream and will create a new one in common_init/1
    %% TODO: create a new stream for each test case
    delete_bridge(Config),
    snabbkaffe:start_trace(),
    Config.

init_per_testcase_common() ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors().

end_per_testcase(t_to_hrecord_failed, _Config) ->
    meck:unload([hstreamdb]);
end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = snabbkaffe:stop(),
    delete_bridge(Config),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Data = rand_data(),
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, Data)),
                #{?snk_kind := hstreamdb_connector_query_append_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(hstreamdb_connector_query_append_return, Trace0),
            lists:foreach(
                fun(EachTrace) ->
                    case ?config(enable_batch, Config) of
                        true ->
                            ?assertMatch(#{result := ok, is_batch := true}, EachTrace);
                        false ->
                            ?assertMatch(
                                #{result := #{'batchId' := _}, is_batch := false}, EachTrace
                            )
                    end
                end,
                Trace
            ),
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    Name = ?config(hstreamdb_name, Config),
    HStreamDBConfig0 = ?config(hstreamdb_config, Config),
    HStreamDBConfig = HStreamDBConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(HStreamDBConfig)
    ),
    Data = rand_data(),
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, Data)),
                #{?snk_kind := hstreamdb_connector_query_append_return},
                10_000
            ),
            ok
        end,
        fun(Trace) ->
            lists:foreach(
                fun(EachTrace) ->
                    case ?config(enable_batch, Config) of
                        true ->
                            ?assertMatch(#{result := ok, is_batch := true}, EachTrace);
                        false ->
                            ?assertMatch(
                                #{result := #{'batchId' := _}, is_batch := false}, EachTrace
                            )
                    end
                end,
                ?of_kind(hstreamdb_connector_query_append_return, Trace)
            ),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),

    health_check_resource_ok(Config),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        health_check_resource_down(Config)
    end),
    ok.

t_create_disconnected(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),

    ?check_trace(
        emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
            ?assertMatch({ok, _}, create_bridge(Config))
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := client_not_alive}],
                ?of_kind(hstreamdb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    %% TODO: Investigate why reconnection takes at least 5 seconds during ct.
    %% While in practical applications, recovers to the 'connected' state
    %% within 3 seconds after toxiproxy being enabled.'"
    %% timer:sleep(10000),
    restart_resource(Config),
    health_check_resource_ok(Config),
    ok.

t_write_failure(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    QueryMode = ?config(query_mode, Config),
    Data = rand_data(),
    {{ok, _}, {ok, _}} =
        ?wait_async_action(
            create_bridge(Config),
            #{?snk_kind := resource_connected_enter},
            20_000
        ),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        health_check_resource_down(Config),
        case QueryMode of
            sync ->
                %% Error (call timeout) is expected for both with_batch and without_batch.
                %% `health_check_resource_down(Config)` above calls health check and asserts
                %% that resource is already down.
                %% So, emqx_resource_manager updates it state to disconnected before returning health_check result.
                %% After that, emqx_resource_buffer_worker reads resource state and doesn't even attempt calling
                %% hstreamdb connector, since it is disconnected, see: emqx_resource_buffer_worker.erl:1163:
                %%   ```
                %%     do_call_query(_QM, _Id, _Index, _Ref, _Query, _QueryOpts, _Data) ->
                %%         ?RESOURCE_ERROR(not_connected, "resource not connected").
                %%   ```
                ?assertMatch({error, _}, send_message(Config, Data));
            async ->
                %% TODO: async mode is not supported yet,
                %% but it will return ok if calling emqx_resource_buffer_worker:async_query/3,
                ?assertMatch(
                    ok,
                    send_message(Config, Data)
                )
        end
    end),
    ok.

t_simple_query(Config) ->
    BatchSize = batch_size(Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Type = ?config(hstreamdb_bridge_type, Config),
    Name = ?config(hstreamdb_name, Config),
    ActionId = emqx_bridge_v2:id(Type, Name),
    Requests = gen_batch_req(BatchSize, ActionId),
    ?check_trace(
        begin
            ?wait_async_action(
                lists:foreach(
                    fun(Request) ->
                        ?assertEqual(ok, query_resource(Config, Request))
                    end,
                    Requests
                ),
                #{?snk_kind := hstreamdb_connector_query_append_return},
                10_000
            )
        end,
        fun(Trace) ->
            lists:foreach(
                fun(EachTrace) ->
                    case ?config(enable_batch, Config) of
                        true ->
                            ?assertMatch(#{result := ok, is_batch := true}, EachTrace);
                        false ->
                            ?assertMatch(
                                #{result := #{'batchId' := _}, is_batch := false}, EachTrace
                            )
                    end
                end,
                ?of_kind(hstreamdb_connector_query_append_return, Trace)
            ),
            ok
        end
    ),
    ok.

t_to_hrecord_failed(Config) ->
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Result = send_message(Config, #{}),
    case QueryMode of
        sync ->
            ?assertMatch(
                {error, {unrecoverable_error, failed_to_apply_hrecord_template}},
                Result
            )
        %% TODO: async mode is not supported yet
    end,
    ok.

%% Connector Action Tests

t_action_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}).

t_action_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config).

t_action_sync_query(Config) ->
    MakeMessageFun = fun() -> rand_data() end,
    IsSuccessCheck = fun(Result) -> ?assertEqual(ok, Result) end,
    TracePoint = hstreamdb_connector_query_append_return,
    emqx_bridge_v2_testlib:t_sync_query(Config, MakeMessageFun, IsSuccessCheck, TracePoint).

t_action_start_stop(Config) ->
    StopTracePoint = hstreamdb_connector_on_stop,
    emqx_bridge_v2_testlib:t_start_stop(Config, StopTracePoint).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(ConfigT) ->
    Host = os:getenv("HSTREAMDB_HOST", "toxiproxy"),
    RawPort = os:getenv("HSTREAMDB_PORT", str(?HSTREAMDB_DEFAULT_PORT)),
    Port = list_to_integer(RawPort),
    URL = "http://" ++ Host ++ ":" ++ RawPort,

    Config0 = [
        {bridge_type, <<"hstreamdb">>},
        {bridge_name, <<"my_hstreamdb_action">>},
        {connector_type, <<"hstreamdb">>},
        {connector_name, <<"my_hstreamdb_connector">>},
        {hstreamdb_host, Host},
        {hstreamdb_port, Port},
        {hstreamdb_url, URL},
        %% see also for `proxy_name` : $PROJ_ROOT/.ci/docker-compose-file/toxiproxy.json
        {proxy_name, "hstreamdb"},
        {batch_size, batch_size(ConfigT)}
        | ConfigT
    ],

    BridgeType = proplists:get_value(bridge_type, Config0, <<"hstreamdb">>),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx_bridge_hstreamdb,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            % Connect to hstreamdb directly
            % drop old stream and then create new one
            connect_and_delete_stream(Config0),
            connect_and_create_stream(Config0),
            {Name, HStreamDBConf} = hstreamdb_config(BridgeType, Config0),
            Config =
                [
                    {apps, Apps},
                    {hstreamdb_config, HStreamDBConf},
                    {hstreamdb_bridge_type, BridgeType},
                    {hstreamdb_name, Name},
                    {bridge_config, action_config(Config0)},
                    {connector_config, connector_config(Config0)},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_hstreamdb);
                _ ->
                    {skip, no_hstreamdb}
            end
    end.

hstreamdb_config(BridgeType, Config) ->
    Port = integer_to_list(?config(hstreamdb_port, Config)),
    URL = "http://" ++ ?config(hstreamdb_host, Config) ++ ":" ++ Port,
    Name = ?BRIDGE_NAME,
    BatchSize = batch_size(Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  url = ~p\n"
            "  stream = ~p\n"
            "  record_template = ~p\n"
            "  pool_size = ~p\n"
            "  grpc_timeout = ~p\n"
            "  resource_opts = {\n"
            %% always sync
            "    query_mode = sync\n"
            "    request_ttl = 10000ms\n"
            "    batch_size = ~b\n"
            "    worker_pool_size = ~b\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                URL,
                ?STREAM,
                ?RECORD_TEMPLATE,
                ?POOL_SIZE,
                ?GRPC_TIMEOUT,
                BatchSize,
                ?WORKER_POOL_SIZE
            ]
        ),
    {Name, parse_and_check(ConfigString, BridgeType, Name)}.

action_config(Config) ->
    ConnectorName = ?config(connector_name, Config),
    BatchSize = batch_size(Config),
    #{
        <<"connector">> => ConnectorName,
        <<"enable">> => true,
        <<"parameters">> =>
            #{
                <<"aggregation_pool_size">> => ?POOL_SIZE,
                <<"record_template">> => ?RECORD_TEMPLATE,
                <<"stream">> => ?STREAM,
                <<"writer_pool_size">> => ?POOL_SIZE
            },
        <<"resource_opts">> =>
            #{
                <<"batch_size">> => BatchSize,
                <<"health_check_interval">> => <<"15s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"45s">>,
                <<"worker_pool_size">> => ?POOL_SIZE
            }
    }.

connector_config(Config) ->
    Port = integer_to_list(?config(hstreamdb_port, Config)),
    URL = "http://" ++ ?config(hstreamdb_host, Config) ++ ":" ++ Port,
    #{
        <<"url">> => URL,
        <<"ssl">> =>
            #{<<"enable">> => false, <<"verify">> => <<"verify_peer">>},
        <<"grpc_timeout">> => <<"30s">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"start_timeout">> => <<"5s">>
        }
    }.

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := Config}}} = RawConf,
    Config.

-define(RPC_OPTIONS, #{pool_size => 4}).

-define(CONN_ATTEMPTS, 10).

default_options(Config) ->
    #{
        url => ?config(hstreamdb_url, Config),
        rpc_options => ?RPC_OPTIONS
    }.

connect_direct_hstream(Name, Config) ->
    client(Name, Config, ?CONN_ATTEMPTS).

client(_Name, _Config, N) when N =< 0 -> error(cannot_connect);
client(Name, Config, N) ->
    try
        _ = hstreamdb:stop_client(Name),
        {ok, Client} = hstreamdb:start_client(Name, default_options(Config)),
        ok = hstreamdb_client:echo(Client),
        Client
    catch
        Class:Error ->
            ct:print("Error connecting: ~p", [{Class, Error}]),
            ct:sleep(timer:seconds(1)),
            client(Name, Config, N - 1)
    end.

disconnect(Client) ->
    hstreamdb:stop_client(Client).

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    Name = ?config(hstreamdb_name, Config),
    HSDBConfig0 = ?config(hstreamdb_config, Config),
    HSDBConfig = emqx_utils_maps:deep_merge(HSDBConfig0, Overrides),
    emqx_bridge:create(BridgeType, Name, HSDBConfig).

delete_bridge(Config) ->
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    Name = ?config(hstreamdb_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Data) ->
    Name = ?config(hstreamdb_name, Config),
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Data).

query_resource(Config, Request) ->
    Name = ?config(hstreamdb_name, Config),
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    ID = emqx_bridge_v2:id(BridgeType, Name),
    ResID = emqx_connector_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ID, Request, #{timeout => 1_000, connector_resource_id => ResID}).

restart_resource(Config) ->
    BridgeName = ?config(hstreamdb_name, Config),
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    emqx_bridge:disable_enable(disable, BridgeType, BridgeName),
    timer:sleep(200),
    emqx_bridge:disable_enable(enable, BridgeType, BridgeName).

resource_id(Config) ->
    BridgeName = ?config(hstreamdb_name, Config),
    BridgeType = ?config(hstreamdb_bridge_type, Config),
    _ResourceID = emqx_bridge_resource:resource_id(BridgeType, BridgeName).

action_id(Config) ->
    ActionName = ?config(hstreamdb_name, Config),
    ActionType = ?config(hstreamdb_bridge_type, Config),
    _ActionID = emqx_bridge_v2:id(ActionType, ActionName).

health_check_resource_ok(Config) ->
    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(resource_id(Config))),
    ActionName = ?config(hstreamdb_name, Config),
    ActionType = ?config(hstreamdb_bridge_type, Config),
    ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(ActionType, ActionName)).

health_check_resource_down(Config) ->
    case emqx_resource_manager:health_check(resource_id(Config)) of
        {ok, Status} when Status =:= disconnected orelse Status =:= connecting ->
            ok;
        {error, timeout} ->
            ok;
        Other ->
            ?assert(
                false, lists:flatten(io_lib:format("invalid health check result:~p~n", [Other]))
            )
    end,
    ActionName = ?config(hstreamdb_name, Config),
    ActionType = ?config(hstreamdb_bridge_type, Config),
    #{status := StatusV2} = emqx_bridge_v2:health_check(ActionType, ActionName),
    case StatusV2 of
        disconnected ->
            ok;
        connecting ->
            ok;
        OtherV2 ->
            ?assert(
                false, lists:flatten(io_lib:format("invalid health check result:~p~n", [OtherV2]))
            )
    end.

% These funs start and then stop the hstreamdb connection
connect_and_create_stream(Config) ->
    ?WITH_CLIENT(
        _ = hstreamdb_client:create_stream(
            Client, ?STREAM, ?REPLICATION_FACTOR, ?BACKLOG_RETENTION_SECOND, ?SHARD_COUNT
        )
    ),
    %% force write to stream to make it created and ready to be written data for test cases
    ProducerOptions = #{
        stream => ?STREAM,
        buffer_options => #{
            interval => 1000,
            callback => {?MODULE, on_flush_result, [<<"WHAT">>]},
            max_records => 1,
            max_batches => 1
        },
        buffer_pool_size => 1,
        writer_options => #{
            grpc_timeout => 100
        },
        writer_pool_size => 1,
        client_options => default_options(Config)
    },

    ?WITH_CLIENT(
        begin
            ok = hstreamdb:start_producer(test_producer, ProducerOptions),
            _ = hstreamdb:append_flush(test_producer, hstreamdb:to_record([], raw, rand_payload())),
            _ = hstreamdb:stop_producer(test_producer)
        end
    ).

on_flush_result({{flush, _Stream, _Records}, {ok, _Resp}}) ->
    ok;
on_flush_result({{flush, _Stream, _Records}, {error, _Reason}}) ->
    ok.

connect_and_delete_stream(Config) ->
    ?WITH_CLIENT(
        _ = hstreamdb_client:delete_stream(Client, ?STREAM)
    ).

%%--------------------------------------------------------------------
%% help functions
%%--------------------------------------------------------------------

batch_size(Config) ->
    case ?config(enable_batch, Config) of
        true -> ?BATCH_SIZE;
        false -> 1
    end.

rand_data() ->
    #{
        %% Raw MTTT Payload in binary
        payload => rand_payload(),
        id => <<"0005F8F84FFFAFB9F44200000D810002">>,
        topic => <<"test/topic">>,
        qos => 0
    }.

rand_payload() ->
    emqx_utils_json:encode(#{
        temperature => rand:uniform(40), humidity => rand:uniform(100)
    }).

gen_batch_req(Count, ActionId) when
    is_integer(Count) andalso Count > 0
->
    [{ActionId, rand_data()} || _Val <- lists:seq(1, Count)];
gen_batch_req(Count, _ActionId) ->
    ct:pal("Gen batch requests failed with unexpected Count: ~p", [Count]).

str(List) when is_list(List) ->
    unicode:characters_to_list(List, utf8);
str(Bin) when is_binary(Bin) ->
    unicode:characters_to_list(Bin, utf8);
str(Num) when is_number(Num) ->
    number_to_list(Num).

number_to_list(Int) when is_integer(Int) ->
    integer_to_list(Int);
number_to_list(Float) when is_float(Float) ->
    float_to_list(Float, [{decimals, 10}, compact]).
