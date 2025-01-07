%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
    ?assertMatch({ok, _}, emqx_modules_conf:import_config(ImportConf)),
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
