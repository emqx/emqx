%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_rewrite_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(RULES, [{rewrite, pub, <<"x/#">>,<<"^x/y/(.+)$">>,<<"z/y/$1">>},
                {rewrite, pub, <<"name/#">>,<<"^name/(.+)$">>,<<"pub/%u/$1">>},
                {rewrite, pub, <<"c/#">>,<<"^c/(.+)$">>,<<"pub/%c/$1">>},
                {rewrite, sub, <<"y/+/z/#">>,<<"^y/(.+)/z/(.+)$">>,<<"y/z/$2">>},
                {rewrite, sub, <<"name/#">>,<<"^name/(.+)$">>,<<"sub/%u/$1">>},
                {rewrite, sub, <<"c/#">>,<<"^c/(.+)$">>,<<"sub/%c/$1">>}
               ]).

-define(BAD_RULES_1, [{rewrite, pub, <<"x/#">>,<<"^x/y/(.+)$">>,<<"z/y/+">>}]).

%% empty topic filter/name won't be ranched cased `emqx.conf` will be checked before emqx started
%% but we need this check for emqx-ee modules api
-define(BAD_RULES_2, [{rewrite, pub, <<"">>,<<"^x/y/(.+)$">>,<<"z/y/+">>}]).
-define(BAD_RULES_3, [{rewrite, pub, <<"name/#">>,<<"^name/(.+)$">>,<<"">>}]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx_modules]),
    %% Ensure all the modules unloaded.
    ok = emqx_modules:unload(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules]).

%% Test case for emqx_mod_write
t_mod_rewrite(_Config) ->
    ok = emqx_mod_rewrite:load(?RULES),
    {ok, C} = emqtt:start_link([{clientid, <<"c1">>}, {username , <<"u1">>}]),
    {ok, _} = emqtt:connect(C),

    PubOrigTopics = [<<"x/y/2">>, <<"x/1/2">>, <<"name/1">>, <<"c/1">>],
    PubDestTopics = [<<"z/y/2">>, <<"x/1/2">>, <<"pub/u1/1">>, <<"pub/c1/1">>],
    SubOrigTopics = [<<"y/a/z/b">>, <<"y/def">>, <<"name/1">>, <<"c/1">>],
    SubDestTopics = [<<"y/z/b">>, <<"y/def">>, <<"sub/u1/1">>, <<"sub/c1/1">>],

    %% Sub Rules
    {ok, _Props, _} = emqtt:subscribe(C, [{Topic, ?QOS_1} || Topic <- SubOrigTopics]),
    timer:sleep(100),
    Subscriptions = emqx_broker:subscriptions(<<"c1">>),
    ?assertEqual(SubDestTopics, [Topic || {Topic, _SubOpts} <- Subscriptions]),
    RecvTopics1 = [begin
                      ok = emqtt:publish(C, Topic, <<"payload">>),
                      {ok, #{topic := RecvTopic}} = receive_publish(100),
                      RecvTopic
                  end || Topic <- SubDestTopics],
    ?assertEqual(SubDestTopics, RecvTopics1),
    {ok, _, _} = emqtt:unsubscribe(C, SubOrigTopics),
    timer:sleep(100),
    ?assertEqual([], emqx_broker:subscriptions(<<"c1">>)),

    %% Pub Rules
    {ok, _, _} = emqtt:subscribe(C, [{Topic, ?QOS_1} || Topic <- PubDestTopics]),
    RecvTopics2 = [begin
                      ok = emqtt:publish(C, Topic, <<"payload">>),
                      {ok, #{topic := RecvTopic}} = receive_publish(100),
                      RecvTopic
                  end || Topic <- PubOrigTopics],
    ?assertEqual(PubDestTopics, RecvTopics2),
    {ok, _, _} = emqtt:unsubscribe(C, PubDestTopics),

    ok = emqtt:disconnect(C),
    ok = emqx_mod_rewrite:unload(?RULES).

t_rewrite_rule(_Config) ->
    {PubRules, SubRules} = emqx_mod_rewrite:compile_rules(?RULES),
    %% assert ordering
    ?assertMatch([{rewrite, <<"x/#">>, _, <<"z/y/$1">>},
                  {rewrite, <<"name/#">>, _, <<"pub/%u/$1">>},
                  {rewrite, <<"c/#">>, _, <<"pub/%c/$1">>}],
                 PubRules),
    ?assertMatch([{rewrite, <<"y/+/z/#">>, _, <<"y/z/$2">>},
                  {rewrite, <<"name/#">>, _, <<"sub/%u/$1">>},
                  {rewrite, <<"c/#">>, _, <<"sub/%c/$1">>}],
                 SubRules),

    ?assertEqual(<<"z/y/2">>, emqx_mod_rewrite:match_and_rewrite(<<"x/y/2">>, PubRules, [])),
    ?assertEqual(<<"x/1/2">>, emqx_mod_rewrite:match_and_rewrite(<<"x/1/2">>, PubRules, [])),
    ?assertEqual(<<"y/z/b">>, emqx_mod_rewrite:match_and_rewrite(<<"y/a/z/b">>, SubRules, [])),
    ?assertEqual(<<"y/def">>, emqx_mod_rewrite:match_and_rewrite(<<"y/def">>, SubRules, [])).

t_rewrite_bad_rule_1(_Config) ->
    ?assertEqual({[], []}, emqx_mod_rewrite:compile_rules(?BAD_RULES_1)).

t_rewrite_bad_rule_2(_Config) ->
    ?assertEqual({[], []}, emqx_mod_rewrite:compile_rules(?BAD_RULES_2)).

t_rewrite_bad_rule_3(_Config) ->
    ?assertEqual({[], []}, emqx_mod_rewrite:compile_rules(?BAD_RULES_3)).
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.
