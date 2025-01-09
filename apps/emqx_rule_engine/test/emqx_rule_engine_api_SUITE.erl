%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).
-compile(nowarn_update_literal).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SIMPLE_RULE(NAME_SUFFIX), #{
    <<"description">> => <<"A simple rule">>,
    <<"enable">> => true,
    <<"actions">> => [#{<<"function">> => <<"console">>}],
    <<"sql">> => <<"SELECT * from \"t/1\"">>,
    <<"name">> => <<"test_rule", NAME_SUFFIX/binary>>
}).
-define(SIMPLE_RULE(ID, NAME_SUFFIX), ?SIMPLE_RULE(NAME_SUFFIX)#{<<"id">> => ID}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_crud_rule_api, Config) ->
    meck:new(emqx_utils_json, [passthrough]),
    init_per_testcase(common, Config);
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_crud_rule_api, Config) ->
    meck:unload(emqx_utils_json),
    end_per_testcase(common, Config);
end_per_testcase(_, _Config) ->
    {200, #{data := Rules}} =
        emqx_rule_engine_api:'/rules'(get, #{query_string => #{}}),
    lists:foreach(
        fun(#{id := Id}) ->
            {204} = emqx_rule_engine_api:'/rules/:id'(
                delete,
                #{bindings => #{id => Id}}
            )
        end,
        Rules
    ).

t_crud_rule_api(_Config) ->
    RuleId = <<"my_rule">>,
    Rule = simple_rule_fixture(RuleId, <<>>),
    ?assertEqual(RuleId, maps:get(id, Rule)),

    {200, #{data := Rules}} = emqx_rule_engine_api:'/rules'(get, #{query_string => #{}}),
    ct:pal("RList : ~p", [Rules]),
    ?assert(length(Rules) > 0),

    %% if we post again with the same id, it return with 400 "rule id already exists"
    ?assertMatch(
        {400, #{code := _, message := _Message}},
        emqx_rule_engine_api:'/rules'(post, #{body => ?SIMPLE_RULE(RuleId, <<"some_other">>)})
    ),

    {204} = emqx_rule_engine_api:'/rules/:id/metrics/reset'(put, #{
        bindings => #{id => RuleId}
    }),

    {200, Rule1} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleId}}),
    ct:pal("RShow : ~p", [Rule1]),
    ?assertEqual(Rule, Rule1),

    {200, Metrics} = emqx_rule_engine_api:'/rules/:id/metrics'(get, #{bindings => #{id => RuleId}}),
    ct:pal("RMetrics : ~p", [Metrics]),
    ?assertMatch(#{id := RuleId, metrics := _, node_metrics := _}, Metrics),

    %% simulating a node joining a cluster and lagging the configuration replication; in
    %% such cases, when fetching metrics, a rule may exist in the cluster but not on the
    %% new node.  We just check that it doesn't provoke a crash.
    emqx_common_test_helpers:with_mock(
        emqx_metrics_worker,
        get_metrics,
        fun(HandlerName, MetricId) ->
            %% change the metric id to some unknown id.
            meck:passthrough([HandlerName, <<"unknown-", MetricId/binary>>])
        end,
        fun() ->
            {200, Metrics1} = emqx_rule_engine_api:'/rules/:id/metrics'(get, #{
                bindings => #{id => RuleId}
            }),
            ct:pal("RMetrics : ~p", [Metrics1]),
            ?assertMatch(#{id := RuleId, metrics := _, node_metrics := _}, Metrics1),
            ok
        end
    ),

    {200, Rule2} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleId},
        body => ?SIMPLE_RULE(RuleId)#{<<"sql">> => <<"select * from \"t/b\"">>}
    }),

    {200, Rule3} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleId}}),
    %ct:pal("RShow : ~p", [Rule3]),
    ?assertEqual(Rule3, Rule2),
    ?assertEqual(<<"select * from \"t/b\"">>, maps:get(sql, Rule3)),

    {404, _} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => <<"unknown_rule">>}}),
    {404, _} = emqx_rule_engine_api:'/rules/:id/metrics'(get, #{
        bindings => #{id => <<"unknown_rule">>}
    }),
    {404, _} = emqx_rule_engine_api:'/rules/:id/metrics/reset'(put, #{
        bindings => #{id => <<"unknown_rule">>}
    }),

    ?assertMatch(
        {204},
        emqx_rule_engine_api:'/rules/:id'(
            delete,
            #{bindings => #{id => RuleId}}
        )
    ),

    ?assertMatch(
        {404, #{code := 'NOT_FOUND'}},
        emqx_rule_engine_api:'/rules/:id'(
            delete,
            #{bindings => #{id => RuleId}}
        )
    ),

    ?assertMatch(
        {404, #{code := _, message := _Message}},
        emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleId}})
    ),

    {400, #{
        code := 'BAD_REQUEST',
        message := SelectAndTransformJsonError
    }} =
        emqx_rule_engine_api:'/rule_test'(
            post,
            test_rule_params(<<"SELECT\n  payload.msg\nFROM\n  \"t/#\"">>, <<"{\"msg\": \"hel">>)
        ),
    ?assertMatch(
        #{<<"select_and_transform_error">> := <<"decode_json_failed">>},
        emqx_utils_json:decode(SelectAndTransformJsonError, [return_maps])
    ),
    {400, #{
        code := 'BAD_REQUEST',
        message := SelectAndTransformBadArgError
    }} =
        emqx_rule_engine_api:'/rule_test'(
            post,
            test_rule_params(
                <<"SELECT\n  payload.msg > 1\nFROM\n  \"t/#\"">>, <<"{\"msg\": \"hello\"}">>
            )
        ),
    ?assertMatch(
        #{<<"select_and_transform_error">> := <<"badarg">>},
        emqx_utils_json:decode(SelectAndTransformBadArgError, [return_maps])
    ),
    {400, #{
        code := 'BAD_REQUEST',
        message := BadSqlMessage
    }} = emqx_rule_engine_api:'/rule_test'(
        post,
        test_rule_params(
            <<"BAD_SQL">>, <<"{\"msg\": \"hello\"}">>
        )
    ),
    ?assertMatch({match, _}, re:run(BadSqlMessage, "syntax error")),
    meck:expect(emqx_utils_json, safe_encode, 1, {error, foo}),
    ?assertMatch(
        {400, #{
            code := 'BAD_REQUEST',
            message := <<"{select_and_transform_error,badarg}">>
        }},
        emqx_rule_engine_api:'/rule_test'(
            post,
            test_rule_params(
                <<"SELECT\n  payload.msg > 1\nFROM\n  \"t/#\"">>, <<"{\"msg\": \"hello\"}">>
            )
        )
    ),
    ok.

t_list_rule_api(_Config) ->
    AddIds = rules_fixture(20),
    ct:pal("rule ids: ~p", [AddIds]),
    {200, #{data := Rules, meta := #{count := Count}}} =
        emqx_rule_engine_api:'/rules'(get, #{query_string => #{}}),
    ?assertEqual(20, length(AddIds)),
    ?assertEqual(20, length(Rules)),
    ?assertEqual(20, Count),

    [RuleId | _] = AddIds,
    UpdateParams = #{
        <<"description">> => <<"中文的描述也能搜索"/utf8>>,
        <<"enable">> => false,
        <<"actions">> => [#{<<"function">> => <<"console">>}],
        <<"sql">> => <<"SELECT * from \"t/1/+\"">>,
        <<"name">> => <<"test_rule_update1">>
    },
    {200, _Rule2} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleId},
        body => UpdateParams
    }),
    QueryStr1 = #{query_string => #{<<"enable">> => false}},
    {200, Result1 = #{meta := #{count := Count1}}} = emqx_rule_engine_api:'/rules'(get, QueryStr1),
    ?assertEqual(1, Count1),

    QueryStr2 = #{query_string => #{<<"like_description">> => <<"也能"/utf8>>}},
    {200, Result2} = emqx_rule_engine_api:'/rules'(get, QueryStr2),
    ?assertEqual(maps:get(data, Result1), maps:get(data, Result2)),

    QueryStr3 = #{query_string => #{<<"from">> => <<"t/1">>}},
    {200, #{data := Data3}} = emqx_rule_engine_api:'/rules'(get, QueryStr3),
    ?assertEqual(19, length(Data3)),

    QueryStr4 = #{query_string => #{<<"like_from">> => <<"t/1/+">>}},
    {200, Result4} = emqx_rule_engine_api:'/rules'(get, QueryStr4),
    ?assertEqual(maps:get(data, Result1), maps:get(data, Result4)),

    QueryStr5 = #{query_string => #{<<"match_from">> => <<"t/+/+">>}},
    {200, Result5} = emqx_rule_engine_api:'/rules'(get, QueryStr5),
    ?assertEqual(maps:get(data, Result1), maps:get(data, Result5)),

    QueryStr6 = #{query_string => #{<<"like_id">> => RuleId}},
    {200, Result6} = emqx_rule_engine_api:'/rules'(get, QueryStr6),
    ?assertEqual(maps:get(data, Result1), maps:get(data, Result6)),
    ok.

t_reset_metrics_on_disable(_Config) ->
    #{id := RuleId} = simple_rule_fixture(),

    %% generate some fake metrics
    emqx_metrics_worker:inc(rule_metrics, RuleId, 'matched', 10),
    emqx_metrics_worker:inc(rule_metrics, RuleId, 'passed', 10),
    {200, #{metrics := Metrics0}} = emqx_rule_engine_api:'/rules/:id/metrics'(
        get,
        #{bindings => #{id => RuleId}}
    ),
    ?assertMatch(#{passed := 10, matched := 10}, Metrics0),

    %% disable the rule; metrics should be reset
    {200, _Rule2} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleId},
        body => #{<<"enable">> => false}
    }),

    {200, #{metrics := Metrics1}} = emqx_rule_engine_api:'/rules/:id/metrics'(
        get,
        #{bindings => #{id => RuleId}}
    ),
    ?assertMatch(#{passed := 0, matched := 0}, Metrics1),
    ok.

test_rule_params(Sql, Payload) ->
    #{
        body => #{
            <<"context">> =>
                #{
                    <<"clientid">> => <<"c_emqx">>,
                    <<"event_type">> => <<"message_publish">>,
                    <<"payload">> => Payload,
                    <<"qos">> => 1,
                    <<"topic">> => <<"t/a">>,
                    <<"username">> => <<"u_emqx">>
                },
            <<"sql">> => Sql
        }
    }.

t_rule_engine(_) ->
    _ = simple_rule_fixture(),
    {200, Config} = emqx_rule_engine_api:'/rule_engine'(get, #{}),
    ?assert(not maps:is_key(rules, Config)),
    {200, #{
        jq_function_default_timeout := 12000
        % hidden! jq_implementation_module := jq_port
    }} = emqx_rule_engine_api:'/rule_engine'(put, #{
        body => #{
            <<"jq_function_default_timeout">> => <<"12s">>,
            <<"jq_implementation_module">> => <<"jq_port">>
        }
    }),
    SomeRule = #{<<"sql">> => <<"SELECT * FROM \"t/#\"">>},
    {400, _} = emqx_rule_engine_api:'/rule_engine'(put, #{
        body => #{<<"rules">> => #{<<"some_rule">> => SomeRule}}
    }),
    {400, _} = emqx_rule_engine_api:'/rule_engine'(put, #{body => #{<<"something">> => <<"weird">>}}).

t_dont_downgrade_bridge_type(_) ->
    case emqx_release:edition() of
        ee ->
            do_t_dont_downgrade_bridge_type();
        ce ->
            %% downgrade is not supported in CE
            ok
    end.

do_t_dont_downgrade_bridge_type() ->
    %% Create a rule using a bridge V1 ID
    #{id := RuleId} = create_rule((?SIMPLE_RULE(<<>>))#{<<"actions">> => [<<"kafka:name">>]}),
    ?assertMatch(
        %% returns an action ID
        {200, #{data := [#{actions := [<<"kafka_producer:name">>]}]}},
        emqx_rule_engine_api:'/rules'(get, #{query_string => #{}})
    ),
    ?assertMatch(
        %% returns an action ID
        {200, #{actions := [<<"kafka_producer:name">>]}},
        emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleId}})
    ),
    ok.

rules_fixture(N) ->
    lists:map(
        fun(Seq0) ->
            Seq = integer_to_binary(Seq0),
            #{id := Id} = simple_rule_fixture(Seq),
            Id
        end,
        lists:seq(1, N)
    ).

simple_rule_fixture() ->
    simple_rule_fixture(<<>>).

simple_rule_fixture(NameSuffix) ->
    create_rule(?SIMPLE_RULE(NameSuffix)).

simple_rule_fixture(Id, NameSuffix) ->
    create_rule(?SIMPLE_RULE(Id, NameSuffix)).

create_rule(Params) ->
    {201, Rule} = emqx_rule_engine_api:'/rules'(post, #{body => Params}),
    Rule.
