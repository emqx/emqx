%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rewrite_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(REWRITE, #{
    <<"rewrite">> => [
        #{
            <<"action">> => <<"publish">>,
            <<"dest_topic">> => <<"z/y/$1">>,
            <<"re">> => <<"^x/y/(.+)$">>,
            <<"source_topic">> => <<"x/#">>
        },
        #{
            <<"action">> => <<"publish">>,
            <<"dest_topic">> => <<"pub/${username}/$1">>,
            <<"re">> => <<"^name/(.+)$">>,
            <<"source_topic">> => <<"name/#">>
        },
        #{
            <<"action">> => <<"publish">>,
            <<"dest_topic">> => <<"pub/${clientid}/$1">>,
            <<"re">> => <<"^c/(.+)$">>,
            <<"source_topic">> => <<"c/#">>
        },
        #{
            <<"action">> => <<"subscribe">>,
            <<"dest_topic">> => <<"y/z/$2">>,
            <<"re">> => <<"^y/(.+)/z/(.+)$">>,
            <<"source_topic">> => <<"y/+/z/#">>
        },
        #{
            <<"action">> => <<"subscribe">>,
            <<"dest_topic">> => <<"sub/${username}/$1">>,
            <<"re">> => <<"^name/(.+)$">>,
            <<"source_topic">> => <<"name/#">>
        },
        #{
            <<"action">> => <<"subscribe">>,
            <<"dest_topic">> => <<"sub/${clientid}/$1">>,
            <<"re">> => <<"^c/(.+)$">>,
            <<"source_topic">> => <<"c/#">>
        },
        #{
            <<"action">> => <<"all">>,
            <<"dest_topic">> => <<"all/x/$2">>,
            <<"re">> => <<"^all/(.+)/x/(.+)$">>,
            <<"source_topic">> => <<"all/+/x/#">>
        }
    ]
}).

all() -> emqx_common_test_helpers:all(?MODULE).

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

init_per_testcase(t_get_basic_usage_info, Config) ->
    ok = emqx_rewrite:update([]),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_get_basic_usage_info, _Config) ->
    ok = emqx_rewrite:update([]),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_subscribe_rewrite(_Config) ->
    {ok, Conn} = init(),
    SubOrigTopics = [<<"y/a/z/b">>, <<"y/def">>, <<"name/1">>, <<"c/1">>],
    SubDestTopics = [<<"y/z/b">>, <<"y/def">>, <<"sub/u1/1">>, <<"sub/c1/1">>],
    {ok, _Props1, _} = emqtt:subscribe(Conn, [{Topic, ?QOS_1} || Topic <- SubOrigTopics]),
    timer:sleep(150),
    Subscriptions = emqx_broker:subscriptions(<<"c1">>),
    ?assertEqual(SubDestTopics, [Topic || {Topic, _SubOpts} <- Subscriptions]),
    RecvTopics = [
        begin
            ok = emqtt:publish(Conn, Topic, <<"payload">>),
            {ok, #{topic := RecvTopic}} = receive_publish(100),
            RecvTopic
        end
     || Topic <- SubDestTopics
    ],
    ?assertEqual(SubDestTopics, RecvTopics),
    {ok, _, _} = emqtt:unsubscribe(Conn, SubOrigTopics),
    timer:sleep(100),
    ?assertEqual([], emqx_broker:subscriptions(<<"c1">>)),

    terminate(Conn).

t_publish_rewrite(_Config) ->
    {ok, Conn} = init(),
    PubOrigTopics = [<<"x/y/2">>, <<"x/1/2">>, <<"name/1">>, <<"c/1">>],
    PubDestTopics = [<<"z/y/2">>, <<"x/1/2">>, <<"pub/u1/1">>, <<"pub/c1/1">>],
    {ok, _Props2, _} = emqtt:subscribe(Conn, [{Topic, ?QOS_1} || Topic <- PubDestTopics]),
    RecvTopics = [
        begin
            ok = emqtt:publish(Conn, Topic, <<"payload">>),
            {ok, #{topic := RecvTopic}} = receive_publish(100),
            RecvTopic
        end
     || Topic <- PubOrigTopics
    ],
    ?assertEqual(PubDestTopics, RecvTopics),
    {ok, _, _} = emqtt:unsubscribe(Conn, PubDestTopics),
    terminate(Conn).

t_rewrite_rule(_Config) ->
    {PubRules, SubRules, []} = emqx_rewrite:compile(emqx:get_config([rewrite])),
    ?assertEqual(<<"z/y/2">>, emqx_rewrite:match_and_rewrite(<<"x/y/2">>, PubRules, [])),
    ?assertEqual(<<"x/1/2">>, emqx_rewrite:match_and_rewrite(<<"x/1/2">>, PubRules, [])),
    ?assertEqual(<<"y/z/b">>, emqx_rewrite:match_and_rewrite(<<"y/a/z/b">>, SubRules, [])),
    ?assertEqual(<<"y/def">>, emqx_rewrite:match_and_rewrite(<<"y/def">>, SubRules, [])).

t_rewrite_re_error(_Config) ->
    Rules = [
        #{
            action => subscribe,
            source_topic => "y/+/z/#",
            re => "{^y/(.+)/z/(.+)$*",
            dest_topic => "\"y/z/$2"
        }
    ],
    Error = {
        "y/+/z/#",
        "{^y/(.+)/z/(.+)$*",
        "\"y/z/$2",
        {"nothing to repeat", 16}
    },
    ?assertEqual({[], [], [Error]}, emqx_rewrite:compile(Rules)),
    ok.

t_list(_Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?REWRITE),
    Expect = maps:get(<<"rewrite">>, ?REWRITE),
    ?assertEqual(Expect, emqx_rewrite:list()),
    ok.

t_update(_Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?REWRITE),
    Init = emqx_rewrite:list(),
    Rules = [
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => <<"test/*">>,
            <<"dest_topic">> => <<"test1/$2">>,
            <<"action">> => <<"publish">>
        }
    ],
    ok = emqx_rewrite:update(Rules),
    ?assertEqual(Rules, emqx_rewrite:list()),
    ok = emqx_rewrite:update(Init),
    ok.

t_update_disable(_Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?REWRITE),
    ?assertEqual(ok, emqx_rewrite:update([])),
    timer:sleep(150),

    Subs = emqx_hooks:lookup('client.subscribe'),
    UnSubs = emqx_hooks:lookup('client.unsubscribe'),
    MessagePub = emqx_hooks:lookup('message.publish'),
    Filter = fun({_, {Mod, _, _}, _, _}) -> Mod =:= emqx_rewrite end,

    ?assertEqual([], lists:filter(Filter, Subs)),
    ?assertEqual([], lists:filter(Filter, UnSubs)),
    ?assertEqual([], lists:filter(Filter, MessagePub)),
    ok.

t_update_re_failed(_Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?REWRITE),
    Re = <<"*^test/*">>,
    Rules = [
        #{
            <<"source_topic">> => <<"test/#">>,
            <<"re">> => Re,
            <<"dest_topic">> => <<"test1/$2">>,
            <<"action">> => <<"publish">>
        }
    ],
    ?assertThrow(
        #{
            kind := validation_error,
            path := "rewrite.1.re",
            reason := #{
                regexp := <<"*^test/*">>,
                compile_error := {"nothing to repeat", 0}
            },
            value := <<"*^test/*">>
        },
        emqx_rewrite:update(Rules)
    ),
    ok.

t_get_basic_usage_info(_Config) ->
    ?assertEqual(#{topic_rewrite_rule_count => 0}, emqx_rewrite:get_basic_usage_info()),
    RewriteTopics =
        lists:map(
            fun(N) ->
                Num = integer_to_binary(N),
                DestTopic = <<"rewrite/dest/", Num/binary>>,
                SourceTopic = <<"rewrite/source/", Num/binary>>,
                #{
                    <<"source_topic">> => SourceTopic,
                    <<"dest_topic">> => DestTopic,
                    <<"action">> => all,
                    <<"re">> => DestTopic
                }
            end,
            lists:seq(1, 2)
        ),
    ok = emqx_rewrite:update(RewriteTopics),
    ?assertEqual(#{topic_rewrite_rule_count => 2}, emqx_rewrite:get_basic_usage_info()),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after Timeout -> {error, timeout}
    end.

init() ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?REWRITE),
    ok = emqx_rewrite:enable(),
    {ok, C} = emqtt:start_link([{clientid, <<"c1">>}, {username, <<"u1">>}]),
    {ok, _} = emqtt:connect(C),
    {ok, C}.

terminate(Conn) ->
    ok = emqtt:disconnect(Conn),
    ok = emqx_rewrite:disable().
