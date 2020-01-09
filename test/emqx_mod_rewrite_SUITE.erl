%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(RULES, [{rewrite,<<"x/#">>,<<"^x/y/(.+)$">>,<<"z/y/$1">>},
                {rewrite,<<"y/+/z/#">>,<<"^y/(.+)/z/(.+)$">>,<<"y/z/$2">>}
               ]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    %% Ensure all the modules unloaded.
    ok = emqx_modules:unload(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%% Test case for emqx_mod_write
t_mod_rewrite(_Config) ->
    ok = emqx_mod_rewrite:load(?RULES),
    {ok, C} = emqtt:start_link([{clientid, <<"rewrite_client">>}]),
    {ok, _} = emqtt:connect(C),
    OrigTopics = [<<"x/y/2">>, <<"x/1/2">>, <<"y/a/z/b">>, <<"y/def">>],
    DestTopics = [<<"z/y/2">>, <<"x/1/2">>, <<"y/z/b">>, <<"y/def">>],
    %% Subscribe
    {ok, _Props, _} = emqtt:subscribe(C, [{Topic, ?QOS_1} || Topic <- OrigTopics]),
    timer:sleep(100),
    Subscriptions = emqx_broker:subscriptions(<<"rewrite_client">>),
    ?assertEqual(DestTopics, [Topic || {Topic, _SubOpts} <- Subscriptions]),
    %% Publish
    RecvTopics = [begin
                      ok = emqtt:publish(C, Topic, <<"payload">>),
                      {ok, #{topic := RecvTopic}} = receive_publish(100),
                      RecvTopic
                  end || Topic <- OrigTopics],
    ?assertEqual(DestTopics, RecvTopics),
    %% Unsubscribe
    {ok, _, _} = emqtt:unsubscribe(C, OrigTopics),
    timer:sleep(100),
    ?assertEqual([], emqx_broker:subscriptions(<<"rewrite_client">>)),
    ok = emqtt:disconnect(C),
    ok = emqx_mod_rewrite:unload(?RULES).

t_rewrite_rule(_Config) ->
    Rules = emqx_mod_rewrite:compile(?RULES),
    ?assertEqual(<<"z/y/2">>, emqx_mod_rewrite:match_and_rewrite(<<"x/y/2">>, Rules)),
    ?assertEqual(<<"x/1/2">>, emqx_mod_rewrite:match_and_rewrite(<<"x/1/2">>, Rules)),
    ?assertEqual(<<"y/z/b">>, emqx_mod_rewrite:match_and_rewrite(<<"y/a/z/b">>, Rules)),
    ?assertEqual(<<"y/def">>, emqx_mod_rewrite:match_and_rewrite(<<"y/def">>, Rules)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.
