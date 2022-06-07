%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_topic_metrics_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_dashboard_api_test_helpers, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, #{
    <<"topic_metrics">> => []
}).

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    lists:foreach(
        fun emqx_modules_conf:remove_topic_metrics/1,
        emqx_modules_conf:topic_metrics()
    ),
    Config.

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?BASE_CONF, #{
        raw_with_default => true
    }),

    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_modules, emqx_dashboard],
        fun set_special_configs/1
    ),

    %% When many tests run in an obscure order, it may occur that
    %% `gen_rpc` started with its default settings before `emqx_conf`.
    %% `gen_rpc` and `emqx_conf` have different default `port_discovery` modes,
    %% so we reinitialize `gen_rpc` explicitly.
    ok = application:stop(gen_rpc),
    ok = application:start(gen_rpc),

    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_dashboard, emqx_modules]),
    application:stop(gen_rpc),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_mqtt_topic_metrics_collection(_) ->
    {ok, 200, Result0} = request(
        get,
        uri(["mqtt", "topic_metrics"])
    ),

    ?assertEqual(
        [],
        jsx:decode(Result0)
    ),

    {ok, 200, _} = request(
        post,
        uri(["mqtt", "topic_metrics"]),
        #{<<"topic">> => <<"topic/1/2">>}
    ),

    {ok, 200, Result1} = request(
        get,
        uri(["mqtt", "topic_metrics"])
    ),

    ?assertMatch(
        [
            #{
                <<"topic">> := <<"topic/1/2">>,
                <<"metrics">> := #{}
            }
        ],
        jsx:decode(Result1)
    ),

    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["mqtt", "topic_metrics"]),
            #{
                <<"topic">> => <<"topic/1/2">>,
                <<"action">> => <<"reset">>
            }
        )
    ),

    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["mqtt", "topic_metrics"]),
            #{<<"action">> => <<"reset">>}
        )
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            put,
            uri(["mqtt", "topic_metrics"]),
            #{
                <<"topic">> => <<"unknown_topic/1/2">>,
                <<"action">> => <<"reset">>
            }
        )
    ),
    ?assertMatch(
        {ok, 204, _},
        request(
            delete,
            uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
        )
    ).

t_mqtt_topic_metrics(_) ->
    {ok, 200, _} = request(
        post,
        uri(["mqtt", "topic_metrics"]),
        #{<<"topic">> => <<"topic/1/2">>}
    ),

    {ok, 200, Result0} = request(
        get,
        uri(["mqtt", "topic_metrics"])
    ),

    ?assertMatch([_], jsx:decode(Result0)),

    {ok, 200, Result1} = request(
        get,
        uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
    ),

    ?assertMatch(
        #{
            <<"topic">> := <<"topic/1/2">>,
            <<"metrics">> := #{}
        },
        jsx:decode(Result1)
    ),

    ?assertMatch(
        {ok, 204, _},
        request(
            delete,
            uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
        )
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            get,
            uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
        )
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            delete,
            uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
        )
    ).

t_bad_reqs(_) ->
    %% empty topic
    ?assertMatch(
        {ok, 400, _},
        request(
            post,
            uri(["mqtt", "topic_metrics"]),
            #{<<"topic">> => <<"">>}
        )
    ),

    %% wildcard
    ?assertMatch(
        {ok, 400, _},
        request(
            post,
            uri(["mqtt", "topic_metrics"]),
            #{<<"topic">> => <<"foo/+/bar">>}
        )
    ),

    {ok, 200, _} = request(
        post,
        uri(["mqtt", "topic_metrics"]),
        #{<<"topic">> => <<"topic/1/2">>}
    ),

    %% existing topic
    ?assertMatch(
        {ok, 400, _},
        request(
            post,
            uri(["mqtt", "topic_metrics"]),
            #{<<"topic">> => <<"topic/1/2">>}
        )
    ),

    ok = emqx_modules_conf:remove_topic_metrics(<<"topic/1/2">>),

    %% limit
    Responses = lists:map(
        fun(N) ->
            Topic = iolist_to_binary([
                <<"topic/">>,
                integer_to_binary(N)
            ]),
            request(
                post,
                uri(["mqtt", "topic_metrics"]),
                #{<<"topic">> => Topic}
            )
        end,
        lists:seq(1, 513)
    ),

    ?assertMatch(
        [{ok, 409, _}, {ok, 200, _} | _],
        lists:reverse(Responses)
    ),

    %% limit && wildcard
    ?assertMatch(
        {ok, 400, _},
        request(
            post,
            uri(["mqtt", "topic_metrics"]),
            #{<<"topic">> => <<"a/+">>}
        )
    ).

t_node_aggregation(_) ->
    TwoNodeResult = [
        #{
            create_time => <<"2022-03-30T13:54:10+03:00">>,
            metrics => #{'messages.dropped.count' => 1},
            reset_time => <<"2022-03-30T13:54:10+03:00">>,
            topic => <<"topic/1/2">>
        },
        #{
            create_time => <<"2022-03-30T13:54:10+03:00">>,
            metrics => #{'messages.dropped.count' => 2},
            reset_time => <<"2022-03-30T13:54:10+03:00">>,
            topic => <<"topic/1/2">>
        }
    ],

    meck:new(emqx_topic_metrics_proto_v1, [passthrough]),
    meck:expect(emqx_topic_metrics_proto_v1, metrics, 2, {TwoNodeResult, []}),

    {ok, 200, Result} = request(
        get,
        uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
    ),

    ?assertMatch(
        #{
            <<"topic">> := <<"topic/1/2">>,
            <<"metrics">> := #{<<"messages.dropped.count">> := 3}
        },
        jsx:decode(Result)
    ),

    meck:unload(emqx_topic_metrics_proto_v1).
t_badrpc(_) ->
    meck:new(emqx_topic_metrics_proto_v1, [passthrough]),
    meck:expect(emqx_topic_metrics_proto_v1, metrics, 2, {[], [node()]}),

    ?assertMatch(
        {ok, 500, _},
        request(
            get,
            uri(["mqtt", "topic_metrics", emqx_http_lib:uri_encode("topic/1/2")])
        )
    ),

    meck:unload(emqx_topic_metrics_proto_v1).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

request(Method, Url) ->
    request(Method, Url, []).
