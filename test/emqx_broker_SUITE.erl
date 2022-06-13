%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

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
