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

-module(emqx_mod_topic_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx_modules]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules]).

t_nonexistent_topic_metrics(_) ->
    emqx_mod_topic_metrics:load([]),
    ?assertEqual({error, topic_not_found}, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, topic_not_found}, emqx_mod_topic_metrics:inc(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, topic_not_found}, emqx_mod_topic_metrics:rate(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, topic_not_found}, emqx_mod_topic_metrics:rates(<<"a/b/c">>, 'messages.in')),
    emqx_mod_topic_metrics:register(<<"a/b/c">>),
    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, invalid_metric}, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'invalid.metrics')),
    ?assertEqual({error, invalid_metric}, emqx_mod_topic_metrics:inc(<<"a/b/c">>, 'invalid.metrics')),
    ?assertEqual({error, invalid_metric}, emqx_mod_topic_metrics:rate(<<"a/b/c">>, 'invalid.metrics')),
    ?assertEqual({error, invalid_metric}, emqx_mod_topic_metrics:rates(<<"a/b/c">>, 'invalid.metrics')),
    emqx_mod_topic_metrics:unregister(<<"a/b/c">>),
    emqx_mod_topic_metrics:unload([]).

t_topic_metrics(_) ->
    emqx_mod_topic_metrics:load([]),

    ?assertEqual(false, emqx_mod_topic_metrics:is_registered(<<"a/b/c">>)),
    ?assertEqual([], emqx_mod_topic_metrics:all_registered_topics()),
    emqx_mod_topic_metrics:register(<<"a/b/c">>),
    ?assertEqual(true, emqx_mod_topic_metrics:is_registered(<<"a/b/c">>)),
    ?assertEqual([<<"a/b/c">>], emqx_mod_topic_metrics:all_registered_topics()),

    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(ok, emqx_mod_topic_metrics:inc(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assert(emqx_mod_topic_metrics:rate(<<"a/b/c">>, 'messages.in') =:= 0),
    ?assert(emqx_mod_topic_metrics:rates(<<"a/b/c">>, 'messages.in') =:= #{long => 0,medium => 0,short => 0}),
    emqx_mod_topic_metrics:unregister(<<"a/b/c">>),
    emqx_mod_topic_metrics:unload([]).

t_hook(_) ->
    emqx_mod_topic_metrics:load([]),
    emqx_mod_topic_metrics:register(<<"a/b/c">>),

    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.in')),
    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.out')),
    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.out')),
    ?assertEqual(0, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.dropped')),

    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {clientid, "myclient"},
                                {username, "myuser"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, 0),
    ct:sleep(100),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.in')),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.dropped')),

    emqtt:subscribe(C, <<"a/b/c">>),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, 0),
    ct:sleep(100),
    ?assertEqual(2, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(2, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.in')),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.out')),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.out')),
    ?assertEqual(1, emqx_mod_topic_metrics:val(<<"a/b/c">>, 'messages.dropped')),
    emqx_mod_topic_metrics:unregister(<<"a/b/c">>),
    emqx_mod_topic_metrics:unload([]).
