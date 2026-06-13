%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_postgresql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TOOL_TYPE, <<"postgresql__query">>).
-define(TOOL_ID, <<"pg-test">>).
-define(CONNECTION_ID, <<"pg-test-conn">>).
-define(INVOKE_TOPIC(ReqId), <<"$cap/postgresql__query/pg-test/request/", ReqId/binary>>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_resource, emqx_connector, emqx_agent],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    ok = emqx_agent_tool_postgresql:init(),
    ok = create_test_connection(),
    ok = ensure_test_data(),
    ok = register_tool(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_tool_postgresql:deinit(),
    ok = emqx_agent_plugin_config_fixture:teardown().

t_create_returns_tool(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(?TOOL_TYPE, ?TOOL_ID),
    ?assertMatch(#{tool_id := ?TOOL_ID}, Tool),
    ?assertEqual(?TOOL_TYPE, maps:get(type, Tool)).

t_destroy_accepts_runtime_tool(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(?TOOL_TYPE, ?TOOL_ID),
    ?assertEqual(ok, emqx_agent_tool_postgresql:destroy(Tool)).

t_multiple_instances(_Config) ->
    Ctx2 = test_context(#{<<"id">> => <<"pg-test-2">>}),
    ok = register_tool(Ctx2),
    {ok, S1} = emqx_agent_tool_registry:lookup(?TOOL_TYPE, ?TOOL_ID),
    {ok, S2} = emqx_agent_tool_registry:lookup(?TOOL_TYPE, <<"pg-test-2">>),
    ?assertNotEqual(maps:get(tool_id, S1), maps:get(tool_id, S2)),
    emqx_agent_config:delete_tool(?TOOL_TYPE, <<"pg-test-2">>).

t_invoke_queries_postgresql(_Config) ->
    ReqId = <<"req-PG-001">>,
    ReplyTopic = <<"$cap/postgresql__query/", ?TOOL_ID/binary, "/response/", ReqId/binary>>,
    ok = emqx:subscribe(ReplyTopic),

    Invoke = #{
        <<"trace_id">> => <<"tr-pg">>,
        <<"iid">> => <<"inst-pg-1">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => #{<<"device_id">> => <<"dev-42">>}
    },
    Payload = emqx_utils_json:encode(Invoke),
    Msg = emqx_message:make(?TOOL_ID, 0, ?INVOKE_TOPIC(ReqId), Payload),
    _ = emqx_broker:publish(Msg),

    Reply =
        receive
            #deliver{topic = ReplyTopic, message = #message{payload = P}} ->
                emqx_utils_json:decode(P)
        after 3000 ->
            ct:fail(no_reply_received)
        end,

    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),
    ?assertMatch(
        #{<<"type">> := ?TOOL_TYPE, <<"id">> := ?TOOL_ID},
        maps:get(<<"tool">>, Reply)
    ),
    Response = emqx_agent_tool_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"ok">>}, Response),
    Result = maps:get(<<"result">>, Response),
    Rows = maps:get(<<"rows">>, Result),
    ?assertEqual(2, length(Rows)),
    Metrics = maps:from_list([
        {maps:get(<<"metric">>, Row), maps:get(<<"value">>, Row)}
     || Row <- Rows
    ]),
    ?assertMatch(#{<<"temperature">> := 42}, Metrics),
    ?assertMatch(#{<<"humidity">> := 55}, Metrics),

    ok = emqx:unsubscribe(ReplyTopic).

ensure_test_data() ->
    ResId = emqx_agent_tool_postgresql:resource_id(?CONNECTION_ID),
    ok = expect_query_ok(
        emqx_resource:simple_sync_query(
            ResId,
            {query, <<
                "CREATE TABLE IF NOT EXISTS agent_tool_metrics ("
                "device_id TEXT NOT NULL, metric TEXT NOT NULL, value INT NOT NULL)"
            >>}
        )
    ),
    ok = expect_query_ok(
        emqx_resource:simple_sync_query(
            ResId,
            {query, <<"DELETE FROM agent_tool_metrics">>}
        )
    ),
    ok = expect_query_ok(
        emqx_resource:simple_sync_query(
            ResId,
            {query,
                <<
                    "INSERT INTO agent_tool_metrics(device_id, metric, value) VALUES "
                    "($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"
                >>,
                [
                    <<"dev-42">>,
                    <<"temperature">>,
                    42,
                    <<"dev-42">>,
                    <<"humidity">>,
                    55,
                    <<"dev-99">>,
                    <<"temperature">>,
                    11
                ]}
        )
    ),
    ok.

expect_query_ok({ok, _, _}) ->
    ok;
expect_query_ok({ok, _}) ->
    ok.

create_test_connection() ->
    _ = emqx_agent_service:connection_delete(?CONNECTION_ID),
    ok = emqx_agent_service:connection_create(#{
        <<"id">> => ?CONNECTION_ID,
        <<"type">> => <<"postgresql">>,
        <<"enable">> => true,
        <<"config">> => #{
            <<"server">> => <<"pgsql:5432">>,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"root">>,
            <<"password">> => <<"public">>,
            <<"pool_size">> => 1,
            <<"connect_timeout">> => 5000,
            <<"disable_prepared_statements">> => true,
            <<"ssl">> => #{<<"enable">> => false}
        }
    }).

test_context() ->
    test_context(#{}).

test_context(Overrides) ->
    maps:merge(
        #{
            <<"id">> => ?TOOL_ID,
            <<"desc">> => <<"Query PostgreSQL telemetry by device ID.">>,
            <<"resource">> => ?CONNECTION_ID,
            <<"query">> =>
                <<"SELECT metric, value FROM agent_tool_metrics WHERE device_id = ${device_id} ORDER BY metric">>
        },
        Overrides
    ).

register_tool(Context) ->
    emqx_agent_config:create_tool(context_to_body(Context)).

context_to_body(Context) ->
    Context#{
        <<"type">> => ?TOOL_TYPE
    }.
