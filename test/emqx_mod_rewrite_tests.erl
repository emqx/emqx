%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_rewrite_tests).

-include_lib("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").


rules() ->
    Rawrules1 = "x/# ^x/y/(.+)$ z/y/$1",
    Rawrules2 = "y/+/z/# ^y/(.+)/z/(.+)$ y/z/$2",
    Rawrules = [Rawrules1, Rawrules2],
    Rules = lists:map(fun(Rule) ->
                              [Topic, Re, Dest] = string:tokens(Rule, " "),
                              {rewrite,
                               list_to_binary(Topic),
                               list_to_binary(Re),
                               list_to_binary(Dest)}
                      end, Rawrules),
    lists:map(fun({rewrite, Topic, Re, Dest}) ->
                      {ok, MP} = re:compile(Re),
                      {rewrite, Topic, MP, Dest}
              end, Rules).

rewrite_subscribe_test() ->
    Rules = rules(),
    io:format("Rules: ~p",[Rules]),
    ?assertEqual({ok, [{<<"test">>, opts}]},
                 emqx_mod_rewrite:rewrite_subscribe(credentials, [{<<"test">>, opts}], Rules)),
    ?assertEqual({ok, [{<<"z/y/test">>, opts}]},
                 emqx_mod_rewrite:rewrite_subscribe(credentials, [{<<"x/y/test">>, opts}], Rules)),
    ?assertEqual({ok, [{<<"y/z/test_topic">>, opts}]},
                 emqx_mod_rewrite:rewrite_subscribe(credentials, [{<<"y/test/z/test_topic">>, opts}], Rules)).

rewrite_unsubscribe_test() ->
    Rules = rules(),
    ?assertEqual({ok, [{<<"test">>, opts}]},
                 emqx_mod_rewrite:rewrite_subscribe(credentials, [{<<"test">>, opts}], Rules)),
    ?assertEqual({ok, [{<<"z/y/test">>, opts}]},
                 emqx_mod_rewrite:rewrite_subscribe(credentials, [{<<"x/y/test">>, opts}], Rules)),
    ?assertEqual({ok, [{<<"y/z/test_topic">>, opts}]},
                 emqx_mod_rewrite:rewrite_subscribe(credentials, [{<<"y/test/z/test_topic">>, opts}], Rules)).

rewrite_publish_test() ->
    Rules = rules(),
    ?assertMatch({ok, #message{topic = <<"test">>}},
                 emqx_mod_rewrite:rewrite_publish(#message{topic = <<"test">>}, Rules)),
    ?assertMatch({ok, #message{topic = <<"z/y/test">>}},
                 emqx_mod_rewrite:rewrite_publish(#message{topic = <<"x/y/test">>}, Rules)),
    ?assertMatch({ok, #message{topic = <<"y/z/test_topic">>}},
                 emqx_mod_rewrite:rewrite_publish(#message{topic = <<"y/test/z/test_topic">>}, Rules)).
