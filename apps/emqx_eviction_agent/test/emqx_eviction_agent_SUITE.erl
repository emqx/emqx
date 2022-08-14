%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_eviction_agent_test_helpers,
        [emqtt_connect/0, emqtt_connect/2]).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_eviction_agent]).

init_per_testcase(t_explicit_session_takeover, Config) ->
    _ = emqx_eviction_agent:disable(test_eviction),
    Node = emqx_node_helpers:start_slave(
             evacuate1,
             #{start_apps => [emqx, emqx_eviction_agent]}),
    [{evacuate_node, Node} | Config];
init_per_testcase(_TestCase, Config) ->
    _ = emqx_eviction_agent:disable(test_eviction),
    Config.

end_per_testcase(t_explicit_session_takeover, Config) ->
    _ = emqx_node_helpers:stop_slave(?config(evacuate_node, Config)),
    _ = emqx_eviction_agent:disable(test_eviction);
end_per_testcase(_TestCase, _Config) ->
    _ = emqx_eviction_agent:disable(test_eviction).

t_enable_disable(_Config) ->
    erlang:process_flag(trap_exit, true),

    ?assertMatch(
       disabled,
       emqx_eviction_agent:status()),

    {ok, C0} = emqtt_connect(),
    ok = emqtt:disconnect(C0),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertMatch(
       {error, eviction_agent_busy},
       emqx_eviction_agent:enable(bar, undefined)),

    ?assertMatch(
       ok,
       emqx_eviction_agent:enable(test_eviction, <<"srv">>)),

    ?assertMatch(
        {enabled, #{}},
        emqx_eviction_agent:status()),

    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_connect()),

    ?assertMatch(
        {error, eviction_agent_busy},
        emqx_eviction_agent:disable(bar)),

    ?assertMatch(
        ok,
        emqx_eviction_agent:disable(test_eviction)),

    ?assertMatch(
        {error, disabled},
        emqx_eviction_agent:disable(test_eviction)),

    ?assertMatch(
       disabled,
       emqx_eviction_agent:status()),

    {ok, C1} = emqtt_connect(),
    ok = emqtt:disconnect(C1).


t_evict_connections_status(_Config) ->
    erlang:process_flag(trap_exit, true),

    {ok, _C} = emqtt_connect(),

    {error, disabled} = emqx_eviction_agent:evict_connections(1),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertMatch(
       {enabled, #{connections := 1, sessions := _}},
       emqx_eviction_agent:status()),

    ok = emqx_eviction_agent:evict_connections(1),

    ct:sleep(100),

    ?assertMatch(
       {enabled, #{connections := 0, sessions := _}},
       emqx_eviction_agent:status()),

    ok = emqx_eviction_agent:disable(test_eviction).


t_explicit_session_takeover(Config) ->
    erlang:process_flag(trap_exit, true),

    {ok, C0} = emqtt_connect(<<"client_with_session">>, false),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertEqual(
       1,
       emqx_eviction_agent:connection_count()),

    ok = emqx_eviction_agent:evict_connections(1),

    receive
        {'EXIT', C0, {disconnected, ?RC_USE_ANOTHER_SERVER, _}} -> ok
    after 1000 ->
              ?assert(false, "Connection not evicted")
    end,

    ?assertEqual(
       0,
       emqx_eviction_agent:connection_count()),

    ?assertEqual(
       1,
       emqx_eviction_agent:session_count()),

    %% First, evacuate to the same node

    ?check_trace(
       ?wait_async_action(
          emqx_eviction_agent:evict_sessions(1, node()),
          #{?snk_kind := emqx_channel_takeover_end},
          1000),
       fun(_Result, Trace) ->
        ?assertMatch(
           [#{clientid := <<"client_with_session">>} | _ ],
           ?of_kind(emqx_channel_takeover_end, Trace))
       end),

    ok = emqx_eviction_agent:disable(test_eviction),
    ok = connect_and_publish(<<"t1">>, <<"MessageToEvictedSession1">>),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    %% Evacuate to another node

    TargetNodeForEvacuation = ?config(evacuate_node, Config),
    ?check_trace(
       ?wait_async_action(
          emqx_eviction_agent:evict_sessions(1, TargetNodeForEvacuation),
          #{?snk_kind := emqx_channel_takeover_end},
          1000),
       fun(_Result, Trace) ->
        ?assertMatch(
           [#{clientid := <<"client_with_session">>} | _ ],
           ?of_kind(emqx_channel_takeover_end, Trace))
       end),

    ?assertEqual(
       0,
       emqx_eviction_agent:session_count()),

    ?assertEqual(
       1,
       rpc:call(TargetNodeForEvacuation, emqx_eviction_agent, session_count, [])),

    ok = emqx_eviction_agent:disable(test_eviction),

    ct:pal("evicted chann info: ~p", [emqx_cm:get_chan_info(<<"client_with_session">>)]),

    ok = connect_and_publish(<<"t1">>, <<"MessageToEvictedSession2">>),
    ct:sleep(100),

    {ok, C2} = emqtt_connect(<<"client_with_session">>, false),

    ok = assert_receive_publish(
           [#{payload => <<"MessageToEvictedSession1">>, topic => <<"t1">>},
            #{payload => <<"MessageToEvictedSession2">>, topic => <<"t1">>}]),
    ok = emqtt:disconnect(C2).

t_disable_on_restart(_Config) ->
    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ok = supervisor:terminate_child(emqx_eviction_agent_sup, emqx_eviction_agent),
    {ok, _} = supervisor:restart_child(emqx_eviction_agent_sup, emqx_eviction_agent),

    ?assertEqual(
        disabled,
        emqx_eviction_agent:status()).

assert_receive_publish([]) -> ok;
assert_receive_publish([#{payload := Msg, topic := Topic} | Rest]) ->
    receive
        {publish, #{payload := Msg,
                    topic := Topic}} ->
            assert_receive_publish(Rest)
    after 1000 ->
              ?assert(false, "Message `" ++ binary_to_list(Msg) ++ "` is lost")
    end.

connect_and_publish(Topic, Message) ->
    {ok, C} = emqtt_connect(),
    emqtt:publish(C, Topic, Message),
    ok = emqtt:disconnect(C).
