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
    ok.

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
    {200, Rules} = emqx_rule_engine_api:'/rules'(get, #{}),
    ct:pal("RList : ~p", [Rules]),
    ?assert(length(Rules) > 0),

    {200, Rule0} = emqx_rule_engine_api:'/rules/:id/reset_metrics'(put, #{
        bindings => #{id => RuleID}
    }),
    ?assertEqual(<<"Reset Success">>, Rule0),

    {200, Rule1} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleID}}),
    ct:pal("RShow : ~p", [Rule1]),
    ?assertEqual(Rule, Rule1),

    {200, Rule2} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleID},
        body => Params0#{<<"sql">> => <<"select * from \"t/b\"">>}
    }),

    {200, Rule3} = emqx_rule_engine_api:'/rules/:id'(get, #{bindings => #{id => RuleID}}),
    %ct:pal("RShow : ~p", [Rule3]),
    ?assertEqual(Rule3, Rule2),
    ?assertEqual(<<"select * from \"t/b\"">>, maps:get(sql, Rule3)),

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
