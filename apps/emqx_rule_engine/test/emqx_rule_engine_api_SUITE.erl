-module(emqx_rule_engine_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"rule_engine {rules {}}">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = emqx_common_test_helpers:load_config(emqx_rule_engine_schema, ?CONF_DEFAULT),
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_rule_engine]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_rule_engine]),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    {200, #{data := Rules}} =
        emqx_rule_engine_api:'/rules'(get, #{query_string => #{}}),
    lists:foreach(
        fun(#{id := Id}) ->
            emqx_rule_engine_api:'/rules/:id'(
                delete,
                #{bindings => #{id => Id}}
            )
        end,
        Rules
    ).

t_crud_rule_api(_Config) ->
    RuleID = <<"my_rule">>,
    Params0 = #{
        <<"description">> => <<"A simple rule">>,
        <<"enable">> => true,
        <<"id">> => RuleID,
        <<"actions">> => [#{<<"function">> => <<"console">>}],
        <<"sql">> => <<"SELECT * from \"t/1\"">>,
        <<"name">> => <<"test_rule">>
    },
    {201, Rule} = emqx_rule_engine_api:'/rules'(post, #{body => Params0}),
    %% if we post again with the same params, it return with 400 "rule id already exists"
    ?assertMatch(
        {400, #{code := _, message := _Message}},
        emqx_rule_engine_api:'/rules'(post, #{body => Params0})
    ),

    ?assertEqual(RuleID, maps:get(id, Rule)),
    {200, #{data := Rules}} = emqx_rule_engine_api:'/rules'(get, #{query_string => #{}}),
    ct:pal("RList : ~p", [Rules]),
    ?assert(length(Rules) > 0),

    {204} = emqx_rule_engine_api:'/rules/:id/metrics/reset'(put, #{
        bindings => #{id => RuleID}
    }),

    {200, Rule1} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleID}}),
    ct:pal("RShow : ~p", [Rule1]),
    ?assertEqual(Rule, Rule1),

    {200, Metrics} = emqx_rule_engine_api:'/rules/:id/metrics'(get, #{bindings => #{id => RuleID}}),
    ct:pal("RMetrics : ~p", [Metrics]),
    ?assertMatch(#{id := RuleID, metrics := _, node_metrics := _}, Metrics),

    {200, Rule2} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleID},
        body => Params0#{<<"sql">> => <<"select * from \"t/b\"">>}
    }),

    {200, Rule3} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleID}}),
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
            #{bindings => #{id => RuleID}}
        )
    ),

    %ct:pal("Show After Deleted: ~p", [NotFound]),
    ?assertMatch(
        {404, #{code := _, message := _Message}},
        emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleID}})
    ),

    ?assertMatch(
        {400, #{
            code := 'BAD_REQUEST',
            message := <<"{select_and_transform_error,{error,{decode_json_failed,", _/binary>>
        }},
        emqx_rule_engine_api:'/rule_test'(post, test_rule_params())
    ),
    ok.

t_list_rule_api(_Config) ->
    AddIds =
        lists:map(
            fun(Seq0) ->
                Seq = integer_to_binary(Seq0),
                Params = #{
                    <<"description">> => <<"A simple rule">>,
                    <<"enable">> => true,
                    <<"actions">> => [#{<<"function">> => <<"console">>}],
                    <<"sql">> => <<"SELECT * from \"t/1\"">>,
                    <<"name">> => <<"test_rule", Seq/binary>>
                },
                {201, #{id := Id}} = emqx_rule_engine_api:'/rules'(post, #{body => Params}),
                Id
            end,
            lists:seq(1, 20)
        ),

    {200, #{data := Rules, meta := #{count := Count}}} =
        emqx_rule_engine_api:'/rules'(get, #{query_string => #{}}),
    ?assertEqual(20, length(AddIds)),
    ?assertEqual(20, length(Rules)),
    ?assertEqual(20, Count),

    [RuleID | _] = AddIds,
    UpdateParams = #{
        <<"description">> => <<"中文的描述也能搜索"/utf8>>,
        <<"enable">> => false,
        <<"actions">> => [#{<<"function">> => <<"console">>}],
        <<"sql">> => <<"SELECT * from \"t/1/+\"">>,
        <<"name">> => <<"test_rule_update1">>
    },
    {200, _Rule2} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleID},
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

    QueryStr6 = #{query_string => #{<<"like_id">> => RuleID}},
    {200, Result6} = emqx_rule_engine_api:'/rules'(get, QueryStr6),
    ?assertEqual(maps:get(data, Result1), maps:get(data, Result6)),
    ok.

test_rule_params() ->
    #{
        body => #{
            <<"context">> =>
                #{
                    <<"clientid">> => <<"c_emqx">>,
                    <<"event_type">> => <<"message_publish">>,
                    <<"payload">> => <<"{\"msg\": \"hel">>,
                    <<"qos">> => 1,
                    <<"topic">> => <<"t/a">>,
                    <<"username">> => <<"u_emqx">>
                },
            <<"sql">> =>
                <<"SELECT\n  payload.msg\nFROM\n  \"t/#\"">>
        }
    }.
