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

-include("emqx_mqtt.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(rules, [{rewrite,<<"x/#">>,<<"^x/y/(.+)$">>,<<"z/y/$1">>},
                {rewrite,<<"y/+/z/#">>,<<"^y/(.+)/z/(.+)$">>,<<"y/z/$2">>}]).

all() -> emqx_ct:all(?MODULE).

t_rewrite_rule(_Config) ->
    {ok, _} = emqx_hooks:start_link(),
    ok = emqx_mod_rewrite:load(?rules),
    RawTopicFilters = [{<<"x/y/2">>, opts},
                       {<<"x/1/2">>, opts},
                       {<<"y/a/z/b">>, opts},
                       {<<"y/def">>, opts}],
    SubTopicFilters = emqx_hooks:run_fold('client.subscribe', [client, properties], RawTopicFilters),
    UnSubTopicFilters = emqx_hooks:run_fold('client.unsubscribe', [client, properties], RawTopicFilters),
    Messages = [emqx_hooks:run_fold('message.publish', [], emqx_message:make(Topic, <<"payload">>))
                || {Topic, _Opts} <- RawTopicFilters],
    ExpectedTopicFilters = [{<<"z/y/2">>, opts},
                            {<<"x/1/2">>, opts},
                            {<<"y/z/b">>, opts},
                            {<<"y/def">>, opts}],
    ?assertEqual(ExpectedTopicFilters, SubTopicFilters),
    ?assertEqual(ExpectedTopicFilters, UnSubTopicFilters),
    [?assertEqual(ExpectedTopic, emqx_message:topic(Message))
     || {{ExpectedTopic, _opts}, Message} <- lists:zip(ExpectedTopicFilters, Messages)],
    ok = emqx_mod_rewrite:unload(?rules),
    ok = emqx_hooks:stop().
