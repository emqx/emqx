%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

all() ->
    [ {group, all_cases}
    , {group, connected_client_count_group}
    ].

groups() ->
    TCs = emqx_ct:all(?MODULE),
    ConnClientTCs = [ t_connected_client_count_persistent
                    , t_connected_client_count_anonymous
                    , t_connected_client_count_transient_takeover
                    , t_connected_client_stats
                    ],
    OtherTCs = TCs -- ConnClientTCs,
    [ {all_cases, [], OtherTCs}
    , {connected_client_count_group, [ {group, tcp}
                                     , {group, ws}
                                     ]}
    , {tcp, [], ConnClientTCs}
    , {ws, [], ConnClientTCs}
    ].

init_per_group(connected_client_count_group, Config) ->
    Config;
init_per_group(tcp, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    [{conn_fun, connect} | Config];
init_per_group(ws, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    [ {ssl, false}
    , {enable_websocket, true}
    , {conn_fun, ws_connect}
    , {port, 8083}
    , {host, "localhost"}
    | Config
    ];
init_per_group(_Group, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_group(connected_client_count_group, _Config) ->
    ok;
end_per_group(_Group, _Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

t_subscribed({init, Config}) ->
    emqx_broker:subscribe(<<"topic">>),
    Config;
t_subscribed(Config) when is_list(Config) ->
    ?assertEqual(false, emqx_broker:subscribed(undefined, <<"topic">>)),
    ?assertEqual(true, emqx_broker:subscribed(self(), <<"topic">>));
t_subscribed({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"topic">>).

t_subscribed_2({init, Config}) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    Config;
t_subscribed_2(Config) when is_list(Config) ->
    ?assertEqual(true, emqx_broker:subscribed(self(), <<"topic">>));
t_subscribed_2({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"topic">>).

t_subopts({init, Config}) -> Config;
t_subopts(Config) when is_list(Config) ->
    ?assertEqual(false, emqx_broker:set_subopts(<<"topic">>, #{qos => 1})),
    ?assertEqual(undefined, emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(undefined, emqx_broker:get_subopts(<<"clientid">>, <<"topic">>)),
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 1}),
    timer:sleep(200),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(<<"clientid">>,<<"topic">>)),

    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 2}),
    ?assertEqual(#{nl => 0, qos => 2, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(self(), <<"topic">>)),

    ?assertEqual(true, emqx_broker:set_subopts(<<"topic">>, #{qos => 0})),
    ?assertEqual(#{nl => 0, qos => 0, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(self(), <<"topic">>));
t_subopts({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"topic">>).

t_topics({init, Config}) ->
    Topics = [<<"topic">>, <<"topic/1">>, <<"topic/2">>],
    [{topics, Topics} | Config];
t_topics(Config) when is_list(Config) ->
    Topics = [T1, T2, T3] = proplists:get_value(topics, Config),
    ok = emqx_broker:subscribe(T1, <<"clientId">>),
    ok = emqx_broker:subscribe(T2, <<"clientId">>),
    ok = emqx_broker:subscribe(T3, <<"clientId">>),
    Topics1 = emqx_broker:topics(),
    ?assertEqual(true, lists:foldl(fun(Topic, Acc) ->
                                       case lists:member(Topic, Topics1) of
                                           true -> Acc;
                                           false -> false
                                       end
                                   end, true, Topics));
t_topics({'end', Config}) ->
    Topics = proplists:get_value(topics, Config),
    lists:foreach(fun(T) -> emqx_broker:unsubscribe(T) end, Topics).

t_subscribers({init, Config}) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    Config;
t_subscribers(Config) when is_list(Config) ->
    ?assertEqual([self()], emqx_broker:subscribers(<<"topic">>));
t_subscribers({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"topic">>).

t_subscriptions({init, Config}) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 1}),
    Config;
t_subscriptions(Config) when is_list(Config) ->
    ct:sleep(100),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 proplists:get_value(<<"topic">>, emqx_broker:subscriptions(self()))),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 proplists:get_value(<<"topic">>, emqx_broker:subscriptions(<<"clientid">>)));
t_subscriptions({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"topic">>).

t_sub_pub({init, Config}) ->
    ok = emqx_broker:subscribe(<<"topic">>),
    Config;
t_sub_pub(Config) when is_list(Config) ->
    ct:sleep(100),
    emqx_broker:safe_publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assert(
        receive
            {deliver, <<"topic">>, #message{payload = <<"hello">>}} ->
                true;
            _ ->
                false
        after 100 ->
            false
        end);
t_sub_pub({'end', _Config}) ->
    ok = emqx_broker:unsubscribe(<<"topic">>).

t_nosub_pub({init, Config}) -> Config;
t_nosub_pub({'end', _Config}) -> ok;
t_nosub_pub(Config) when is_list(Config) ->
    ?assertEqual(0, emqx_metrics:val('messages.dropped')),
    emqx_broker:publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assertEqual(1, emqx_metrics:val('messages.dropped')).

t_shared_subscribe({init, Config}) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{share => <<"group">>}),
    ct:sleep(100),
    Config;
t_shared_subscribe(Config) when is_list(Config) ->
    emqx_broker:safe_publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assert(receive
                {deliver, <<"topic">>, #message{payload = <<"hello">>}} ->
                    true;
                Msg ->
                    ct:pal("Msg: ~p", [Msg]),
                    false
            after 100 ->
                false
            end);
t_shared_subscribe({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"$share/group/topic">>).

t_shared_subscribe_2({init, Config}) -> Config;
t_shared_subscribe_2({'end', _Config}) -> ok;
t_shared_subscribe_2(_) ->
    {ok, ConnPid} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid">>}]),
    {ok, _} = emqtt:connect(ConnPid),
    {ok, _, [0]} = emqtt:subscribe(ConnPid, <<"$share/group/topic">>, 0),

    {ok, ConnPid2} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid2">>}]),
    {ok, _} = emqtt:connect(ConnPid2),
    {ok, _, [0]} = emqtt:subscribe(ConnPid2, <<"$share/group2/topic">>, 0),

    ct:sleep(10),
    ok = emqtt:publish(ConnPid, <<"topic">>, <<"hello">>, 0),
    Msgs = recv_msgs(2),
    ?assertEqual(2, length(Msgs)),
    ?assertEqual(true, lists:foldl(fun(#{payload := <<"hello">>, topic := <<"topic">>}, Acc) ->
                                       Acc;
                                      (_, _) ->
                                       false
                                   end, true, Msgs)),
    emqtt:disconnect(ConnPid),
    emqtt:disconnect(ConnPid2).

t_shared_subscribe_3({init, Config}) -> Config;
t_shared_subscribe_3({'end', _Config}) -> ok;
t_shared_subscribe_3(_) ->
    {ok, ConnPid} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid">>}]),
    {ok, _} = emqtt:connect(ConnPid),
    {ok, _, [0]} = emqtt:subscribe(ConnPid, <<"$share/group/topic">>, 0),

    {ok, ConnPid2} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid2">>}]),
    {ok, _} = emqtt:connect(ConnPid2),
    {ok, _, [0]} = emqtt:subscribe(ConnPid2, <<"$share/group/topic">>, 0),

    ct:sleep(10),
    ok = emqtt:publish(ConnPid, <<"topic">>, <<"hello">>, 0),
    Msgs = recv_msgs(2),
    ?assertEqual(1, length(Msgs)),
    emqtt:disconnect(ConnPid),
    emqtt:disconnect(ConnPid2).

t_shard({init, Config}) ->
    ok = meck:new(emqx_broker_helper, [passthrough, no_history]),
    ok = meck:expect(emqx_broker_helper, get_sub_shard, fun(_, _) -> 1 end),
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    Config;
t_shard(Config) when is_list(Config) ->
    ct:sleep(100),
    emqx_broker:safe_publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assert(
        receive
            {deliver, <<"topic">>, #message{payload = <<"hello">>}} ->
                true;
            _ ->
                false
        after 100 ->
            false
        end);
t_shard({'end', _Config}) ->
    emqx_broker:unsubscribe(<<"topic">>),
    ok = meck:unload(emqx_broker_helper).

t_stats_fun({init, Config}) ->
    Parent = self(),
    F = fun Loop() ->
                N1 = emqx_stats:getstat('subscribers.count'),
                N2 = emqx_stats:getstat('subscriptions.count'),
                N3 = emqx_stats:getstat('suboptions.count'),
                case N1 + N2 + N3 =:= 0 of
                    true ->
                        Parent ! {ready, self()},
                        exit(normal);
                    false ->
                        receive
                            stop ->
                                exit(normal)
                        after
                            100 ->
                                Loop()
                        end
                end
        end,
    Pid = spawn_link(F),
    receive
        {ready, P} when P =:= Pid->
            Config
    after
        5000 ->
            Pid ! stop,
            ct:fail("timedout_waiting_for_sub_stats_to_reach_zero")
    end;
t_stats_fun(Config) when is_list(Config) ->
    ok = emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    ok = emqx_broker:subscribe(<<"topic2">>, <<"clientid">>),
    %% ensure stats refreshed
    emqx_broker:stats_fun(),
    %% emqx_stats:set_stat is a gen_server cast
    %% make a synced call sync
    ignored = gen_server:call(emqx_stats, call, infinity),
    ?assertEqual(2, emqx_stats:getstat('subscribers.count')),
    ?assertEqual(2, emqx_stats:getstat('subscribers.max')),
    ?assertEqual(2, emqx_stats:getstat('subscriptions.count')),
    ?assertEqual(2, emqx_stats:getstat('subscriptions.max')),
    ?assertEqual(2, emqx_stats:getstat('suboptions.count')),
    ?assertEqual(2, emqx_stats:getstat('suboptions.max'));
t_stats_fun({'end', _Config}) ->
    ok = emqx_broker:unsubscribe(<<"topic">>),
    ok = emqx_broker:unsubscribe(<<"topic2">>).

%% persistent sessions, when gone, do not contribute to connected
%% client count
t_connected_client_count_persistent({init, Config}) ->
    ok = snabbkaffe:start_trace(),
    process_flag(trap_exit, true),
    Config;
t_connected_client_count_persistent(Config) when is_list(Config) ->
    ConnFun = ?config(conn_fun, Config),
    ClientID = <<"clientid">>,
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    {ok, ConnPid0} = emqtt:start_link([ {clean_start, false}
                                      , {clientid, ClientID}
                                      | Config]),
    {{ok, _}, {ok, [_]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid0) end,
                             [emqx_cm_connected_client_count_inc]
                            ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    {ok, {ok, [_]}} = wait_for_events(
                        fun() -> emqtt:disconnect(ConnPid0) end,
                        [emqx_cm_connected_client_count_dec]
                       ),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    %% reconnecting
    {ok, ConnPid1} = emqtt:start_link([ {clean_start, false}
                                      , {clientid, ClientID}
                                      | Config
                                      ]),
    {{ok, _}, {ok, [_]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid1) end,
                             [emqx_cm_connected_client_count_inc]
                            ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    %% taking over
    {ok, ConnPid2} = emqtt:start_link([ {clean_start, false}
                                      , {clientid, ClientID}
                                      | Config
                                      ]),
    {{ok, _}, {ok, [_, _]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid2) end,
                             [ emqx_cm_connected_client_count_inc
                             , emqx_cm_connected_client_count_dec
                             ],
                             500
                            ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    %% abnormal exit of channel process
    ChanPids = emqx_cm:all_channels(),
    {ok, {ok, [_, _]}} = wait_for_events(
                           fun() ->
                                   lists:foreach(
                                     fun(ChanPid) -> exit(ChanPid, kill) end,
                                     ChanPids)
                           end,
                           [ emqx_cm_connected_client_count_dec
                           , emqx_cm_process_down
                           ]
                          ),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    ok;
t_connected_client_count_persistent({'end', _Config}) ->
    snabbkaffe:stop(),
    ok.

%% connections without client_id also contribute to connected client
%% count
t_connected_client_count_anonymous({init, Config}) ->
    ok = snabbkaffe:start_trace(),
    process_flag(trap_exit, true),
    Config;
t_connected_client_count_anonymous(Config) when is_list(Config) ->
    ConnFun = ?config(conn_fun, Config),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    %% first client
    {ok, ConnPid0} = emqtt:start_link([ {clean_start, true}
                                      | Config]),
    {{ok, _}, {ok, [_]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid0) end,
                             [emqx_cm_connected_client_count_inc]
                            ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    %% second client
    {ok, ConnPid1} = emqtt:start_link([ {clean_start, true}
                                      | Config]),
    {{ok, _}, {ok, [_]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid1) end,
                             [emqx_cm_connected_client_count_inc]
                            ),
    ?assertEqual(2, emqx_cm:get_connected_client_count()),
    %% when first client disconnects, shouldn't affect the second
    {ok, {ok, [_, _]}} = wait_for_events(
                        fun() -> emqtt:disconnect(ConnPid0) end,
                        [ emqx_cm_connected_client_count_dec
                        , emqx_cm_process_down
                        ]
                       ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    %% reconnecting
    {ok, ConnPid2} = emqtt:start_link([ {clean_start, true}
                                      | Config
                                      ]),
    {{ok, _}, {ok, [_]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid2) end,
                             [emqx_cm_connected_client_count_inc]
                            ),
    ?assertEqual(2, emqx_cm:get_connected_client_count()),
    {ok, {ok, [_, _]}} = wait_for_events(
                           fun() -> emqtt:disconnect(ConnPid1) end,
                           [ emqx_cm_connected_client_count_dec
                           , emqx_cm_process_down
                           ]
                          ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    %% abnormal exit of channel process
    Chans = emqx_cm:all_channels(),
    {ok, {ok, [_, _]}} = wait_for_events(
                           fun() ->
                                   lists:foreach(
                                     fun(ChanPid) -> exit(ChanPid, kill) end,
                                     Chans)
                           end,
                           [ emqx_cm_connected_client_count_dec
                           , emqx_cm_process_down
                           ]
                          ),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    ok;
t_connected_client_count_anonymous({'end', _Config}) ->
    snabbkaffe:stop(),
    ok.

t_connected_client_count_transient_takeover({init, Config}) ->
    ok = snabbkaffe:start_trace(),
    process_flag(trap_exit, true),
    Config;
t_connected_client_count_transient_takeover(Config) when is_list(Config) ->
    ConnFun = ?config(conn_fun, Config),
    ClientID = <<"clientid">>,
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    %% we spawn several clients simultaneously to cause the race
    %% condition for the client id lock
    NumClients = 20,
    {ok, {ok, [_, _]}} =
        wait_for_events(
          fun() ->
                  lists:foreach(
                    fun(_) ->
                            spawn(
                              fun() ->
                                      {ok, ConnPid} =
                                          emqtt:start_link([ {clean_start, true}
                                                           , {clientid, ClientID}
                                                           | Config]),
                                      %% don't assert the result: most of them fail
                                      %% during the race
                                      emqtt:ConnFun(ConnPid),
                                      ok
                              end),
                            ok
                    end,
                    lists:seq(1, NumClients))
          end,
          %% there can be only one channel that wins the race for the
          %% lock for this client id.  we also expect a decrement
          %% event because the client dies along with the ephemeral
          %% process.
          [ emqx_cm_connected_client_count_inc
          , emqx_cm_connected_client_count_dec
          ],
          1000),
    %% Since more than one pair of inc/dec may be emitted, we need to
    %% wait for full stabilization
    timer:sleep(100),
    %% It must be 0 again because we spawn-linked the clients in
    %% ephemeral processes above, and all should be dead now.
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    %% connecting again
    {ok, ConnPid1} = emqtt:start_link([ {clean_start, true}
                                      , {clientid, ClientID}
                                      | Config
                                      ]),
    {{ok, _}, {ok, [_]}} =
        wait_for_events(
          fun() -> emqtt:ConnFun(ConnPid1) end,
          [emqx_cm_connected_client_count_inc]
         ),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    %% abnormal exit of channel process
    [ChanPid] = emqx_cm:all_channels(),
    {ok, {ok, [_, _]}} =
        wait_for_events(
          fun() ->
                  exit(ChanPid, kill),
                  ok
          end,
          [ emqx_cm_connected_client_count_dec
          , emqx_cm_process_down
          ]
         ),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    ok;
t_connected_client_count_transient_takeover({'end', _Config}) ->
    snabbkaffe:stop(),
    ok.

t_connected_client_stats({init, Config}) ->
    ok = supervisor:terminate_child(emqx_kernel_sup, emqx_stats),
    {ok, _} = supervisor:restart_child(emqx_kernel_sup, emqx_stats),
    ok = snabbkaffe:start_trace(),
    Config;
t_connected_client_stats(Config) when is_list(Config) ->
    ConnFun = ?config(conn_fun, Config),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    ?assertEqual(0, emqx_stats:getstat('live_connections.count')),
    ?assertEqual(0, emqx_stats:getstat('live_connections.max')),
    {ok, ConnPid} = emqtt:start_link([ {clean_start, true}
                                     , {clientid, <<"clientid">>}
                                     | Config
                                     ]),
    {{ok, _}, {ok, [_]}} = wait_for_events(
                             fun() -> emqtt:ConnFun(ConnPid) end,
                             [emqx_cm_connected_client_count_inc]
                            ),
    %% ensure stats are synchronized
    {_, {ok, [_]}} = wait_for_stats(
                       fun emqx_cm:stats_fun/0,
                       [#{count_stat => 'live_connections.count',
                          max_stat => 'live_connections.max'}]
                      ),
    ?assertEqual(1, emqx_stats:getstat('live_connections.count')),
    ?assertEqual(1, emqx_stats:getstat('live_connections.max')),
    {ok, {ok, [_]}} = wait_for_events(
                        fun() -> emqtt:disconnect(ConnPid) end,
                        [emqx_cm_connected_client_count_dec]
                       ),
    %% ensure stats are synchronized
    {_, {ok, [_]}} = wait_for_stats(
                       fun emqx_cm:stats_fun/0,
                       [#{count_stat => 'live_connections.count',
                          max_stat => 'live_connections.max'}]
                      ),
    ?assertEqual(0, emqx_stats:getstat('live_connections.count')),
    ?assertEqual(1, emqx_stats:getstat('live_connections.max')),
    ok;
t_connected_client_stats({'end', _Config}) ->
    ok = snabbkaffe:stop(),
    ok = supervisor:terminate_child(emqx_kernel_sup, emqx_stats),
    {ok, _} = supervisor:restart_child(emqx_kernel_sup, emqx_stats),
    ok.

%% the count must be always non negative
t_connect_client_never_negative({init, Config}) ->
    Config;
t_connect_client_never_negative(Config) when is_list(Config) ->
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    %% would go to -1
    ChanPid = list_to_pid("<0.0.1>"),
    emqx_cm:mark_channel_disconnected(ChanPid),
    ?assertEqual(0, emqx_cm:get_connected_client_count()),
    %% would be 0, if really went to -1
    emqx_cm:mark_channel_connected(ChanPid),
    ?assertEqual(1, emqx_cm:get_connected_client_count()),
    ok;
t_connect_client_never_negative({'end', _Config}) ->
    ok.

t_connack_auth_error({init, Config}) ->
    process_flag(trap_exit, true),
    emqx_ct_helpers:stop_apps([]),
    emqx_ct_helpers:boot_modules(all),
    Handler =
        fun(emqx) ->
                application:set_env(emqx, acl_nomatch, deny),
                application:set_env(emqx, allow_anonymous, false),
                application:set_env(emqx, enable_acl_cache, false),
                ok;
           (_) ->
                ok
        end,
    emqx_ct_helpers:start_apps([], Handler),
    Config;
t_connack_auth_error({'end', _Config}) ->
    emqx_ct_helpers:stop_apps([]),
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    ok;
t_connack_auth_error(Config) when is_list(Config) ->
    %% MQTT 3.1
    ?assertEqual(0, emqx_metrics:val('packets.connack.auth_error')),
    {ok, C0} = emqtt:start_link([{proto_ver, v4}]),
    ?assertEqual({error, {unauthorized_client, undefined}}, emqtt:connect(C0)),
    ?assertEqual(1, emqx_metrics:val('packets.connack.auth_error')),
    %% MQTT 5.0
    {ok, C1} = emqtt:start_link([{proto_ver, v5}]),
    ?assertEqual({error, {not_authorized, #{}}}, emqtt:connect(C1)),
    ?assertEqual(2, emqx_metrics:val('packets.connack.auth_error')),
    ok.

t_handle_in_empty_client_subscribe_hook({init, Config}) ->
    Config;
t_handle_in_empty_client_subscribe_hook({'end', _Config}) ->
    ok;
t_handle_in_empty_client_subscribe_hook(Config) when is_list(Config) ->
    Hook = fun(_ClientInfo, _Username, TopicFilter) ->
                   EmptyFilters = [{T, Opts#{deny_subscription => true}} || {T, Opts} <- TopicFilter],
                   {stop, EmptyFilters}
           end,
    ok = emqx:hook('client.subscribe', Hook, []),
    try
        {ok, C} = emqtt:start_link(),
        {ok, _} = emqtt:connect(C),
        {ok, _, RCs} = emqtt:subscribe(C, <<"t">>),
        ?assertEqual([?RC_UNSPECIFIED_ERROR], RCs),
        ok
    after
        ok = emqx:unhook('client.subscribe', Hook)
    end.

wait_for_events(Action, Kinds) ->
    wait_for_events(Action, Kinds, 500).

wait_for_events(Action, Kinds, Timeout) ->
    Predicate = fun(#{?snk_kind := K}) ->
                        lists:member(K, Kinds)
                end,
    N = length(Kinds),
    {ok, Sub} = snabbkaffe_collector:subscribe(Predicate, N, Timeout, 0),
    Res = Action(),
    case snabbkaffe_collector:receive_events(Sub) of
        {timeout, _} ->
            {Res, timeout};
        {ok, Events} ->
            {Res, {ok, Events}}
    end.

wait_for_stats(Action, Stats) ->
    Predicate = fun(Event = #{?snk_kind := emqx_stats_setstat}) ->
                        Stat = maps:with(
                                 [ count_stat
                                 , max_stat
                                 ], Event),
                        lists:member(Stat, Stats);
                   (_) ->
                        false
                end,
    N = length(Stats),
    Timeout = 500,
    {ok, Sub} = snabbkaffe_collector:subscribe(Predicate, N, Timeout, 0),
    Res = Action(),
    case snabbkaffe_collector:receive_events(Sub) of
        {timeout, _} ->
            {Res, timeout};
        {ok, Events} ->
            {Res, {ok, Events}}
    end.

insert_fake_channels() ->
    %% Insert copies to simulate missed counts
    Tab = emqx_channel_info,
    Key = ets:first(Tab),
    [{_Chan, ChanInfo = #{conn_state := connected}, Stats}] = ets:lookup(Tab, Key),
    ets:insert(Tab, [ {{"fake" ++ integer_to_list(N), undefined}, ChanInfo, Stats}
                     || N <- lists:seq(1, 9)]),
    %% these should not be counted
    ets:insert(Tab, [ { {"fake" ++ integer_to_list(N), undefined}
                      , ChanInfo#{conn_state := disconnected}, Stats}
                     || N <- lists:seq(10, 20)]).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count-1, [Msg|Msgs]);
        _Other -> recv_msgs(Count, Msgs)
    after 100 ->
        Msgs
    end.
