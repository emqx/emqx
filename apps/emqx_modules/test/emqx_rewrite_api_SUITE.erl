%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rewrite_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, #{}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx_modules, #{config => ?BASE_CONF}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_mqtt_topic_rewrite(_) ->
    Rules = [
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"test/*">>,
            <<"dest_topic">> => <<"test1/$2">>,
            <<"action">> => <<"publish">>
        }
    ],

    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["mqtt", "topic_rewrite"]),
            Rules
        )
    ),

    {ok, 200, Result} =
        request(get, uri(["mqtt", "topic_rewrite"])),

    ?assertEqual(
        Rules,
        emqx_utils_json:decode(Result, [return_maps])
    ).

t_mqtt_topic_rewrite_limit(_) ->
    Rule =
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"test/*">>,
            <<"dest_topic">> => <<"test1/$2">>,
            <<"action">> => <<"publish">>
        },

    Rules = [Rule || _ <- lists:seq(1, 21)],

    ?assertMatch(
        {ok, 413, _},
        request(
            put,
            uri(["mqtt", "topic_rewrite"]),
            Rules
        )
    ).

t_mqtt_topic_rewrite_wildcard(_) ->
    BadRules = [
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"^test/(.+)$">>,
            <<"dest_topic">> => <<"bad/test/#">>
        },
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"^test/(.+)$">>,
            <<"dest_topic">> => <<"bad/#/test">>
        },
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"^test/(.+)$">>,
            <<"dest_topic">> => <<"bad/test/+">>
        },
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"^test/(.+)$">>,
            <<"dest_topic">> => <<"bad/+/test">>
        }
    ],

    Rules = lists:flatten(
        lists:map(
            fun(Rule) ->
                [Rule#{<<"action">> => <<"publish">>}, Rule#{<<"action">> => <<"all">>}]
            end,
            BadRules
        )
    ),
    lists:foreach(
        fun(Rule) ->
            ?assertMatch(
                {ok, 400, _},
                request(
                    put,
                    uri(["mqtt", "topic_rewrite"]),
                    [Rule]
                )
            )
        end,
        Rules
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

request(Method, Url) ->
    request(Method, Url, []).
