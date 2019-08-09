%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_mod_rewrite,
        [ rewrite_subscribe/4
        , rewrite_unsubscribe/4
        , rewrite_publish/2
        ]).

-include_lib("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_RULES, [<<"x/# ^x/y/(.+)$ z/y/$1">>,
                     <<"y/+/z/# ^y/(.+)/z/(.+)$ y/z/$2">>
                    ]).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_rewrite_subscribe(_) ->
    ?assertEqual({ok, [{<<"test">>, #{}}]},
                 rewrite(subscribe, [{<<"test">>, #{}}])),
    ?assertEqual({ok, [{<<"z/y/test">>, #{}}]},
                 rewrite(subscribe, [{<<"x/y/test">>, #{}}])),
    ?assertEqual({ok, [{<<"y/z/test_topic">>, #{}}]},
                 rewrite(subscribe, [{<<"y/test/z/test_topic">>, #{}}])).

t_rewrite_unsubscribe(_) ->
    ?assertEqual({ok, [{<<"test">>, #{}}]},
                 rewrite(unsubscribe, [{<<"test">>, #{}}])),
    ?assertEqual({ok, [{<<"z/y/test">>, #{}}]},
                 rewrite(unsubscribe, [{<<"x/y/test">>, #{}}])),
    ?assertEqual({ok, [{<<"y/z/test_topic">>, #{}}]},
                 rewrite(unsubscribe, [{<<"y/test/z/test_topic">>, #{}}])).

t_rewrite_publish(_) ->
    ?assertMatch({ok, #message{topic = <<"test">>}},
                 rewrite(publish, #message{topic = <<"test">>})),
    ?assertMatch({ok, #message{topic = <<"z/y/test">>}},
                 rewrite(publish, #message{topic = <<"x/y/test">>})),
    ?assertMatch({ok, #message{topic = <<"y/z/test_topic">>}},
                 rewrite(publish, #message{topic = <<"y/test/z/test_topic">>})).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

rewrite(subscribe, TopicFilters) ->
    rewrite_subscribe(#{}, #{}, TopicFilters, rules());
rewrite(unsubscribe, TopicFilters) ->
    rewrite_unsubscribe(#{}, #{}, TopicFilters, rules());
rewrite(publish, Msg) -> rewrite_publish(Msg, rules()).

rules() ->
    [begin
         [Topic, Re, Dest] = string:split(Rule, " ", all),
         {ok, MP} = re:compile(Re),
         {rewrite, Topic, MP, Dest}
     end || Rule <- ?TEST_RULES].

