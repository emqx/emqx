%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_topic_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(TOPIC, #{<<"topic_metrics">> => []}).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?TOPIC, #{
        raw_with_default => true
    }),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_modules]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_modules, emqx_conf]).

init_per_testcase(_Case, Config) ->
    emqx_topic_metrics:enable(),
    emqx_topic_metrics:deregister_all(),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_topic_metrics:deregister_all(),
    emqx_config:put([topic_metrics], []),
    emqx_topic_metrics:disable().

t_nonexistent_topic_metrics(_) ->
    ?assertEqual({error, topic_not_found}, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, topic_not_found}, emqx_topic_metrics:inc(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, topic_not_found}, emqx_topic_metrics:rate(<<"a/b/c">>, 'messages.in')),
    % ?assertEqual({error, topic_not_found}, emqx_topic_metrics:rates(<<"a/b/c">>, 'messages.in')),
    emqx_topic_metrics:register(<<"a/b/c">>),
    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual({error, invalid_metric}, emqx_topic_metrics:val(<<"a/b/c">>, 'invalid.metrics')),
    ?assertEqual({error, invalid_metric}, emqx_topic_metrics:inc(<<"a/b/c">>, 'invalid.metrics')),
    ?assertEqual({error, invalid_metric}, emqx_topic_metrics:rate(<<"a/b/c">>, 'invalid.metrics')),

    %% ?assertEqual(
    %%     {error, invalid_metric},
    %%     emqx_topic_metrics:rates(<<"a/b/c">>, 'invalid.metrics')
    %% ),

    ok = emqx_topic_metrics:deregister(<<"a/b/c">>).

t_topic_metrics(_) ->
    ?assertEqual(false, emqx_topic_metrics:is_registered(<<"a/b/c">>)),
    ?assertEqual([], emqx_topic_metrics:all_registered_topics()),
    emqx_topic_metrics:register(<<"a/b/c">>),
    ?assertEqual(true, emqx_topic_metrics:is_registered(<<"a/b/c">>)),
    ?assertEqual([<<"a/b/c">>], emqx_topic_metrics:all_registered_topics()),

    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(ok, emqx_topic_metrics:inc(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assert(emqx_topic_metrics:rate(<<"a/b/c">>, 'messages.in') =:= 0),

    %% ?assert(
    %%     emqx_topic_metrics:rates(<<"a/b/c">>, 'messages.in') =:=
    %%         #{long => 0, medium => 0, short => 0}
    %% ),

    ok = emqx_topic_metrics:deregister(<<"a/b/c">>).

t_hook(_) ->
    emqx_topic_metrics:register(<<"a/b/c">>),

    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.in')),
    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.out')),
    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.out')),
    ?assertEqual(0, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.dropped')),

    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {clientid, "myclient"},
        {username, "myuser"}
    ]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, [{qos, 0}]),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, [{qos, 1}]),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, [{qos, 2}]),
    ct:sleep(100),
    ?assertEqual(3, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.in')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos1.in')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos2.in')),
    ?assertEqual(3, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.dropped')),

    emqtt:subscribe(C, <<"a/b/c">>, [{qos, 2}]),
    ct:sleep(100),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, [{qos, 0}]),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, [{qos, 1}]),
    emqtt:publish(C, <<"a/b/c">>, <<"Hello world">>, [{qos, 2}]),
    ct:sleep(100),
    ?assertEqual(6, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.in')),
    ?assertEqual(2, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.in')),
    ?assertEqual(2, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos1.in')),
    ?assertEqual(2, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos2.in')),
    ?assertEqual(3, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.out')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos0.out')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos1.out')),
    ?assertEqual(1, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.qos2.out')),
    ?assertEqual(3, emqx_topic_metrics:val(<<"a/b/c">>, 'messages.dropped')),
    ok = emqx_topic_metrics:deregister(<<"a/b/c">>).

t_topic_server_restart(_) ->
    emqx_config:put([topic_metrics], [#{topic => <<"a/b/c">>}]),
    ?check_trace(
        begin
            ?wait_async_action(
                erlang:exit(whereis(emqx_topic_metrics), kill),
                #{?snk_kind := emqx_topic_metrics_started},
                500
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [_ | _],
                ?of_kind(emqx_topic_metrics_started, Trace)
            )
        end
    ),

    ?assertEqual(
        [<<"a/b/c">>],
        emqx_topic_metrics:all_registered_topics()
    ).

t_unknown_messages(_) ->
    OldPid = whereis(emqx_topic_metrics),
    ?check_trace(
        begin
            ?wait_async_action(
                OldPid ! unknown,
                #{?snk_kind := emqx_topic_metrics_unexpected_info},
                500
            ),
            ?wait_async_action(
                gen_server:cast(OldPid, unknown),
                #{?snk_kind := emqx_topic_metrics_unexpected_cast},
                500
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [_ | _],
                ?of_kind(emqx_topic_metrics_unexpected_info, Trace)
            ),
            ?assertMatch(
                [_ | _],
                ?of_kind(emqx_topic_metrics_unexpected_cast, Trace)
            )
        end
    ),

    %% emqx_topic_metrics did not crash from unexpected calls
    ?assertEqual(
        OldPid,
        whereis(emqx_topic_metrics)
    ).
