%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_clickhouse_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_TYPE, <<"clickhouse.history">>).
-define(SKILL_VERSION, <<"1">>).
-define(SKILL_ID, <<"ch-test">>).
-define(INVOKE_TOPIC, <<"cap/invoke/clickhouse.history/ch-test/1">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx_agent], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_clickhouse:init(),
    ok = emqx_agent_skill_clickhouse:create(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_clickhouse:destroy(?SKILL_ID),
    ok = emqx_agent_skill_clickhouse:deinit().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_create_registers_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(?SKILL_ID),
    ?assertEqual(?SKILL_ID, maps:get(skill_id, Skill)),
    ?assertEqual(?SKILL_TYPE, maps:get(type, Skill)),
    ?assertEqual(?SKILL_VERSION, maps:get(version, Skill)),
    InputSchema = maps:get(input_schema, Skill),
    ?assertEqual(<<"object">>, maps:get(<<"type">>, InputSchema)),
    ?assert(is_map(maps:get(<<"properties">>, InputSchema))),
    ?assert(is_list(maps:get(<<"required">>, InputSchema))),
    OutputSchema = maps:get(output_schema, Skill),
    ?assertEqual(<<"object">>, maps:get(<<"type">>, OutputSchema)),
    ?assert(is_map(maps:get(<<"properties">>, OutputSchema))),
    ?assert(is_list(maps:get(<<"required">>, OutputSchema))).

t_schema_required_matches_props(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(?SKILL_ID),
    InputSchema = maps:get(input_schema, Skill),
    Props = maps:get(<<"properties">>, InputSchema),
    Required = maps:get(<<"required">>, InputSchema),
    ?assertEqual(lists:sort(maps:keys(Props)), lists:sort(Required)).

t_destroy_unregisters_skill(_Config) ->
    ok = emqx_agent_skill_clickhouse:destroy(?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(?SKILL_ID)),
    %% Re-create so end_per_testcase:destroy() doesn't crash
    ok = emqx_agent_skill_clickhouse:create(test_context()).

t_multiple_instances(_Config) ->
    Ctx2 = test_context(#{skill_id => <<"ch-test-2">>}),
    ok = emqx_agent_skill_clickhouse:create(Ctx2),
    {ok, S1} = emqx_agent_skill_registry:lookup(?SKILL_ID),
    {ok, S2} = emqx_agent_skill_registry:lookup(<<"ch-test-2">>),
    ?assertEqual(?SKILL_TYPE, maps:get(type, S1)),
    ?assertEqual(?SKILL_TYPE, maps:get(type, S2)),
    ?assertNotEqual(maps:get(skill_id, S1), maps:get(skill_id, S2)),
    ok = emqx_agent_skill_clickhouse:destroy(<<"ch-test-2">>).

t_invoke_publishes_reply(_Config) ->
    ReqId = <<"req-CH-001">>,
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,

    ok = emqx:subscribe(ReplyTopic),

    Invoke = #{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-aaa">>,
        <<"iid">> => <<"inst-1001">>,
        <<"sid">> => null,
        <<"caller">> => #{<<"kind">> => <<"agent">>, <<"id">> => <<"agent-1">>},
        <<"mode">> => <<"unary">>,
        <<"args">> => #{
            <<"device_id">> => <<"dev-42">>,
            <<"metric">> => <<"temperature">>,
            <<"window_min">> => 60
        },
        <<"limits">> => #{
            <<"timeout_ms">> => 8000,
            <<"max_rows">> => 10000,
            <<"max_bytes">> => 1048576
        }
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
    ?assertEqual(<<"tr-aaa">>, maps:get(<<"trace_id">>, Reply)),
    ?assertEqual(<<"inst-1001">>, maps:get(<<"iid">>, Reply)),
    ?assertEqual(<<"unary">>, maps:get(<<"frame">>, Reply)),
    ?assertMatch(
        #{<<"type">> := ?SKILL_TYPE, <<"id">> := ?SKILL_ID, <<"version">> := ?SKILL_VERSION},
        maps:get(<<"skill">>, Reply)
    ),
    Data = maps:get(<<"data">>, Reply),
    ?assert(is_list(maps:get(<<"rows">>, Data))),

    ok = emqx:unsubscribe(ReplyTopic).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context() ->
    test_context(#{}).

test_context(Overrides) ->
    maps:merge(
        #{
            skill_id => ?SKILL_ID,
            desc => <<"Query historical telemetry for a device and metric.">>,
            input => #{
                <<"device_id">> => #{<<"type">> => <<"string">>},
                <<"metric">> => #{<<"type">> => <<"string">>},
                <<"window_min">> => #{
                    <<"type">> => <<"integer">>, <<"minimum">> => 1, <<"maximum">> => 1440
                }
            },
            output => #{
                <<"rows">> => #{<<"type">> => <<"array">>}
            }
        },
        Overrides
    ).
