%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx_retainer).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    application:stop(emqx_retainer),
    emqx_ct_helpers:start_apps([emqx_retainer], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_retainer]).

init_per_testcase(TestCase, Config) ->
    emqx_retainer:clean(),
    DefaultCfg = new_emqx_retainer_conf(),
    NewCfg = case TestCase of
                 t_message_expiry_2 ->
                     DefaultCfg#{msg_expiry_interval := 2000};
                 t_flow_control ->
                     DefaultCfg#{flow_control := #{max_read_number => 1,
                                                   msg_deliver_quota => 1,
                                                   quota_release_interval => timer:seconds(1)}};
                 _ ->
                     DefaultCfg
             end,
    emqx_retainer:update_config(NewCfg),
    application:ensure_all_started(emqx_retainer),
    Config.

set_special_configs(emqx_retainer) ->
    init_emqx_retainer_conf();
set_special_configs(_) ->
    ok.

init_emqx_retainer_conf() ->
    emqx_config:put([?APP], new_emqx_retainer_conf()).

new_emqx_retainer_conf() ->
    #{enable => true,
      msg_expiry_interval => 0,
      msg_clear_interval => 0,
      connector => [#{type => mnesia,
                      config =>
                          #{max_retained_messages => 0,
                            storage_type => ram}}],
      flow_control => #{max_read_number => 0,
                        msg_deliver_quota => 0,
                        quota_release_interval => 0},
                      max_payload_size => 1024 * 1024}.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_store_and_clean(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    emqtt:publish(C1, <<"retained">>, <<"this is a retained message">>, [{qos, 0}, {retain, true}]),
    timer:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),
    timer:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),

    ok = emqtt:disconnect(C1).

t_retain_handling(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    %% rh = 0, no wildcard, and with empty retained message
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    %% rh = 0, has wildcard, and with empty retained message
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/#">>),

    emqtt:publish(C1, <<"retained">>, <<"this is a retained message">>, [{qos, 0}, {retain, true}]),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 1}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 1}]),
    ?assertEqual(0, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 2}]),
    ?assertEqual(0, length(receive_messages(1))),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),
    ok = emqtt:disconnect(C1).

t_wildcard_subscription(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(C1, <<"retained/0">>, <<"this is a retained message 0">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"this is a retained message 1">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/a/b/c">>, <<"this is a retained message 2">>, [{qos, 0}, {retain, true}]),

    {ok, #{}, [0]} = emqtt:subscribe(C1,  <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1,  <<"retained/+/b/#">>, 0),
    ?assertEqual(3, length(receive_messages(3))),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/a/b/c">>, <<"">>, [{qos, 0}, {retain, true}]),
    ok = emqtt:disconnect(C1).

t_message_expiry(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    emqtt:publish(C1, <<"retained/0">>, #{'Message-Expiry-Interval' => 0}, <<"don't expire">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, #{'Message-Expiry-Interval' => 2}, <<"expire">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/2">>, #{'Message-Expiry-Interval' => 5}, <<"don't expire">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/3">>, <<"don't expire">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"$SYS/retained/4">>, <<"don't expire">>, [{qos, 0}, {retain, true}]),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"$SYS/retained/+">>, 0),
    ?assertEqual(5, length(receive_messages(5))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/+">>),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"$SYS/retained/+">>),

    timer:sleep(3000),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"$SYS/retained/+">>, 0),
    ?assertEqual(4, length(receive_messages(5))),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/2">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/3">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"$SYS/retained/4">>, <<"">>, [{qos, 0}, {retain, true}]),

    ok = emqtt:disconnect(C1).

t_message_expiry_2(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(C1, <<"retained">>, <<"expire">>, [{qos, 0}, {retain, true}]),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),
    timer:sleep(3000),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),

    ok = emqtt:disconnect(C1).

t_clean(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(C1, <<"retained/0">>, <<"this is a retained message 0">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"this is a retained message 1">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/test/0">>, <<"this is a retained message 2">>, [{qos, 0}, {retain, true}]),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(3, length(receive_messages(3))),

    ok = emqx_retainer:delete(<<"retained/test/0">>),
    ok = emqx_retainer:delete(<<"retained/+">>),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(3))),

    ok = emqtt:disconnect(C1).

t_flow_control(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(C1, <<"retained/0">>, <<"this is a retained message 0">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"this is a retained message 1">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/3">>, <<"this is a retained message 3">>, [{qos, 0}, {retain, true}]),
    Begin = erlang:system_time(millisecond),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(3, length(receive_messages(3))),
    End = erlang:system_time(millisecond),
    Diff = End - Begin,

    %% msg_deliver_quota = 1 and quota_release_interval = 1, and there has three message
    %% so total wait time is between in 1 ~ 2s(may be timer will delay, so plus 0.5s to maximum)
    ?assert(Diff > timer:seconds(1) andalso Diff < timer:seconds(2.5)),

    ok = emqtt:disconnect(C1).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            receive_messages(Count-1, [Msg|Msgs]);
        Other ->
            ct:log("Other Msg: ~p~n",[Other]),
            receive_messages(Count, Msgs)
    after 2000 ->
            Msgs
    end.
