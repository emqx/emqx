%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_modules_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_modules
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_CaseName, Conf) ->
    Conf.

end_per_testcase(_CaseName, _Conf) ->
    [emqx_modules_conf:remove_topic_metrics(T) || T <- emqx_modules_conf:topic_metrics()],
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_topic_metrics_add_remove(_) ->
    ?assertEqual([], emqx_modules_conf:topic_metrics()),
    ?assertMatch({ok, _}, emqx_modules_conf:add_topic_metrics(<<"test-topic">>)),
    ?assertEqual([<<"test-topic">>], emqx_modules_conf:topic_metrics()),
    ?assertEqual(ok, emqx_modules_conf:remove_topic_metrics(<<"test-topic">>)),
    ?assertEqual([], emqx_modules_conf:topic_metrics()),
    ?assertMatch({error, _}, emqx_modules_conf:remove_topic_metrics(<<"test-topic">>)).

t_topic_metrics_merge_update(_) ->
    ?assertEqual([], emqx_modules_conf:topic_metrics()),
    ?assertMatch({ok, _}, emqx_modules_conf:add_topic_metrics(<<"test-topic-before-import1">>)),
    ?assertMatch({ok, _}, emqx_modules_conf:add_topic_metrics(<<"test-topic-before-import2">>)),
    ImportConf = #{
        <<"topic_metrics">> =>
            [
                #{<<"topic">> => <<"imported_topic1">>},
                #{<<"topic">> => <<"imported_topic2">>}
            ]
    },
    ?assertMatch({ok, _}, emqx_modules_conf:import_config(?global_ns, ImportConf)),
    ExpTopics = [
        <<"test-topic-before-import1">>,
        <<"test-topic-before-import2">>,
        <<"imported_topic1">>,
        <<"imported_topic2">>
    ],
    ?assertEqual(ExpTopics, emqx_modules_conf:topic_metrics()).

t_topic_metrics_update(_) ->
    ?assertEqual([], emqx_modules_conf:topic_metrics()),
    ?assertMatch({ok, _}, emqx_modules_conf:add_topic_metrics(<<"test-topic-before-update1">>)),
    ?assertMatch({ok, _}, emqx_modules_conf:add_topic_metrics(<<"test-topic-before-update2">>)),
    UpdConf = [#{<<"topic">> => <<"new_topic1">>}, #{<<"topic">> => <<"new_topic2">>}],
    ?assertMatch({ok, _}, emqx_conf:update([topic_metrics], UpdConf, #{override_to => cluster})),
    ?assertEqual([<<"new_topic1">>, <<"new_topic2">>], emqx_modules_conf:topic_metrics()).
