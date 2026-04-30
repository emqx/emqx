%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_postgresql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_TYPE, <<"postgresql.query">>).
-define(SKILL_ID, <<"pg-test">>).
-define(INVOKE_TOPIC, <<"cap/postgresql.query/pg-test/request">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_resource, emqx_agent],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_postgresql:init(),
    ok = ensure_test_data(),
    ok = emqx_agent_skill_postgresql:create(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_postgresql:destroy(?SKILL_ID),
    ok = emqx_agent_skill_postgresql:deinit().

t_create_registers_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID),
    ?assertEqual(?SKILL_ID, maps:get(skill_id, Skill)),
    ?assertEqual(?SKILL_TYPE, maps:get(type, Skill)).

t_destroy_unregisters_skill(_Config) ->
    ok = emqx_agent_skill_postgresql:destroy(?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID)),
    ok = emqx_agent_skill_postgresql:create(test_context()).

t_multiple_instances(_Config) ->
    Ctx2 = test_context(#{skill_id => <<"pg-test-2">>}),
    ok = emqx_agent_skill_postgresql:create(Ctx2),
    {ok, S1} = emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID),
    {ok, S2} = emqx_agent_skill_registry:lookup(?SKILL_TYPE, <<"pg-test-2">>),
    ?assertNotEqual(maps:get(skill_id, S1), maps:get(skill_id, S2)),
    ok = emqx_agent_skill_postgresql:destroy(<<"pg-test-2">>).

t_invoke_queries_postgresql(_Config) ->
    ReqId = <<"req-PG-001">>,
    ReplyTopic = <<"cap/postgresql.query/", ?SKILL_ID/binary, "/response/", ReqId/binary>>,
    ok = emqx:subscribe(ReplyTopic),

    Invoke = #{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-pg">>,
        <<"iid">> => <<"inst-pg-1">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => #{<<"device_id">> => <<"dev-42">>}
    },
    Payload = emqx_utils_json:encode(Invoke),
    Msg = emqx_message:make(?SKILL_ID, 0, ?INVOKE_TOPIC, Payload),
    _ = emqx_broker:publish(Msg),

    Reply =
        receive
            #deliver{topic = ReplyTopic, message = #message{payload = P}} ->
                emqx_utils_json:decode(P)
        after 3000 ->
            ct:fail(no_reply_received)
        end,

    ?assertEqual(ReqId, maps:get(<<"req_id">>, Reply)),
    ?assertMatch(
        #{<<"type">> := ?SKILL_TYPE, <<"id">> := ?SKILL_ID},
        maps:get(<<"skill">>, Reply)
    ),
    Data = maps:get(<<"data">>, Reply),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Data)),
    Rows = maps:get(<<"rows">>, Data),
    ?assertEqual(2, length(Rows)),
    Metrics = maps:from_list([
        {maps:get(<<"metric">>, Row), maps:get(<<"value">>, Row)}
     || Row <- Rows
    ]),
    ?assertEqual(42, maps:get(<<"temperature">>, Metrics)),
    ?assertEqual(55, maps:get(<<"humidity">>, Metrics)),

    ok = emqx:unsubscribe(ReplyTopic).

ensure_test_data() ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    ok = expect_query_ok(
        emqx_resource:simple_sync_query(
            ResId,
            {query, <<
                "CREATE TABLE IF NOT EXISTS agent_skill_metrics ("
                "device_id TEXT NOT NULL, metric TEXT NOT NULL, value INT NOT NULL)"
            >>}
        )
    ),
    ok = expect_query_ok(
        emqx_resource:simple_sync_query(
            ResId,
            {query, <<"DELETE FROM agent_skill_metrics">>}
        )
    ),
    ok = expect_query_ok(
        emqx_resource:simple_sync_query(
            ResId,
            {query,
                <<
                    "INSERT INTO agent_skill_metrics(device_id, metric, value) VALUES "
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

test_context() ->
    test_context(#{}).

test_context(Overrides) ->
    maps:merge(
        #{
            skill_id => ?SKILL_ID,
            desc => <<"Query PostgreSQL telemetry by device ID.">>,
            query =>
                <<"SELECT metric, value FROM agent_skill_metrics WHERE device_id = $1 ORDER BY metric">>,
            arg_keys => [<<"device_id">>],
            input_schema => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"device_id">> => #{<<"type">> => <<"string">>}
                },
                <<"required">> => [<<"device_id">>]
            },
            output_schema => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"status">> => #{<<"type">> => <<"string">>},
                    <<"rows">> => #{<<"type">> => <<"array">>}
                },
                <<"required">> => [<<"status">>, <<"rows">>]
            }
        },
        Overrides
    ).
