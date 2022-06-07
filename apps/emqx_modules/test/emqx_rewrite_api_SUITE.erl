%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_dashboard_api_test_helpers, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, #{}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?BASE_CONF, #{
        raw_with_default => true
    }),

    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_modules, emqx_dashboard],
        fun set_special_configs/1
    ),

    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_dashboard, emqx_modules]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(_App) ->
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
        jsx:decode(Result)
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

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

request(Method, Url) ->
    request(Method, Url, []).
