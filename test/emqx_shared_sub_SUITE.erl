
%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_shared_sub_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_random_basic/1, t_random/1, t_round_robin/1, t_sticky/1, t_hash/1, t_not_so_sticky/1]).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(wait(For, Timeout), wait_for(?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout)).

all() -> [t_random_basic, t_random, t_round_robin, t_sticky, t_hash, t_not_so_sticky].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_random_basic(_) ->
    application:set_env(?APPLICATION, shared_subscription_strategy, random),
    ClientId = <<"ClientId">>,
    {ok, ConnPid} = emqx_mock_client:start_link(ClientId),
    {ok, SPid} = emqx_mock_client:open_session(ConnPid, ClientId, internal),
    Message1 = emqx_message:make(<<"ClientId">>, 2, <<"foo">>, <<"hello">>),
    emqx_session:subscribe(SPid, [{<<"foo">>, #{qos => 2, share => <<"group1">>}}]),
    %% wait for the subscription to show up
    ?wait(ets:lookup(emqx_alive_shared_subscribers, SPid) =:= [{SPid}], 1000),
    emqx_session:publish(SPid, 1, Message1),
    ?wait(case emqx_mock_client:get_last_message(ConnPid) of
              {publish, 1, _} -> true;
              Other -> Other
          end, 1000),
    emqx_session:puback(SPid, 2),
    emqx_session:puback(SPid, 3, reasoncode),
    emqx_session:pubrec(SPid, 4),
    emqx_session:pubrec(SPid, 5, reasoncode),
    emqx_session:pubrel(SPid, 6, reasoncode),
    emqx_session:pubcomp(SPid, 7, reasoncode),
    emqx_mock_client:close_session(ConnPid, SPid),
    ok.

t_random(_) ->
    test_two_messages(random).

t_round_robin(_) ->
    test_two_messages(round_robin).

t_sticky(_) ->
    test_two_messages(sticky).

t_hash(_) ->
    test_two_messages(hash).

%% if the original subscriber dies, change to another one alive
t_not_so_sticky(_) ->
    application:set_env(?APPLICATION, shared_subscription_strategy, sticky),
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqx_mock_client:start_link(ClientId1),
    {ok, ConnPid2} = emqx_mock_client:start_link(ClientId2),
    {ok, SPid1} = emqx_mock_client:open_session(ConnPid1, ClientId1, internal),
    {ok, SPid2} = emqx_mock_client:open_session(ConnPid2, ClientId2, internal),
    Message1 = emqx_message:make(ClientId1, 0, <<"foo/bar">>, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, <<"foo/bar">>, <<"hello2">>),
    emqx_session:subscribe(SPid1, [{<<"foo/bar">>, #{qos => 0, share => <<"group1">>}}]),
    %% wait for the subscription to show up
    ?wait(ets:lookup(emqx_alive_shared_subscribers, SPid1) =:= [{SPid1}], 1000),
    emqx_session:publish(SPid1, 1, Message1),
    ?wait(case emqx_mock_client:get_last_message(ConnPid1) of
              {publish, _, #message{payload = <<"hello1">>}} -> true;
              Other -> Other
          end, 1000),
    emqx_mock_client:close_session(ConnPid1, SPid1),
    ?wait(ets:lookup(emqx_alive_shared_subscribers, SPid1) =:= [], 1000),
    emqx_session:subscribe(SPid2, [{<<"foo/#">>, #{qos => 0, share => <<"group1">>}}]),
    ?wait(ets:lookup(emqx_alive_shared_subscribers, SPid2) =:= [{SPid2}], 1000),
    emqx_session:publish(SPid2, 2, Message2),
    ?wait(case emqx_mock_client:get_last_message(ConnPid2) of
              {publish, _, #message{payload = <<"hello2">>}} -> true;
              Other -> Other
          end, 1000),
    emqx_mock_client:close_session(ConnPid2, SPid2),
    ?wait(ets:tab2list(emqx_alive_shared_subscribers) =:= [], 1000),
    ok.

test_two_messages(Strategy) ->
    application:set_env(?APPLICATION, shared_subscription_strategy, Strategy),
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqx_mock_client:start_link(ClientId1),
    {ok, ConnPid2} = emqx_mock_client:start_link(ClientId2),
    {ok, SPid1} = emqx_mock_client:open_session(ConnPid1, ClientId1, internal),
    {ok, SPid2} = emqx_mock_client:open_session(ConnPid2, ClientId2, internal),
    Message1 = emqx_message:make(ClientId1, 0, <<"foo/bar">>, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, <<"foo/bar">>, <<"hello2">>),
    emqx_session:subscribe(SPid1, [{<<"foo/bar">>, #{qos => 0, share => <<"group1">>}}]),
    emqx_session:subscribe(SPid2, [{<<"foo/bar">>, #{qos => 0, share => <<"group1">>}}]),
    %% wait for the subscription to show up
    ?wait(ets:lookup(emqx_alive_shared_subscribers, SPid1) =:= [{SPid1}] andalso
          ets:lookup(emqx_alive_shared_subscribers, SPid2) =:= [{SPid2}], 1000),
    emqx_session:publish(SPid1, 1, Message1),
    Me = self(),
    WaitF = fun(ExpectedPayload) ->
                    case last_message(ExpectedPayload, [ConnPid1, ConnPid2]) of
                        {true, Pid} ->
                            Me ! {subscriber, Pid},
                            true;
                        Other ->
                            Other
                    end
            end,
    ?wait(WaitF(<<"hello1">>), 2000),
    UsedSubPid1 = receive {subscriber, P1} -> P1 end,
    %% publish both messages with SPid1
    emqx_session:publish(SPid1, 2, Message2),
    ?wait(WaitF(<<"hello2">>), 2000),
    UsedSubPid2 = receive {subscriber, P2} -> P2 end,
    case Strategy of
        sticky -> ?assert(UsedSubPid1 =:= UsedSubPid2);
        round_robin -> ?assert(UsedSubPid1 =/= UsedSubPid2);
        hash -> ?assert(UsedSubPid1 =:= UsedSubPid2);
        _ -> ok
    end,
    emqx_mock_client:close_session(ConnPid1, SPid1),
    emqx_mock_client:close_session(ConnPid2, SPid2),
    ok.

last_message(_ExpectedPayload, []) -> <<"not yet?">>;
last_message(ExpectedPayload, [Pid | Pids]) ->
    case emqx_mock_client:get_last_message(Pid) of
        {publish, _, #message{payload = ExpectedPayload}} -> {true, Pid};
        _Other -> last_message(ExpectedPayload, Pids)
    end.

%%------------------------------------------------------------------------------
%% help functions
%%------------------------------------------------------------------------------

wait_for(Fn, Ln, F, Timeout) ->
    {Pid, Mref} = erlang:spawn_monitor(fun() -> wait_loop(F, catch_call(F)) end),
    wait_for_down(Fn, Ln, Timeout, Pid, Mref, false).

wait_for_down(Fn, Ln, Timeout, Pid, Mref, Kill) ->
    receive
        {'DOWN', Mref, process, Pid, normal} ->
            ok;
        {'DOWN', Mref, process, Pid, {C, E, S}} ->
            erlang:raise(C, {Fn, Ln, E}, S)
    after
        Timeout ->
            case Kill of
                true ->
                    erlang:demonitor(Mref, [flush]),
                    erlang:exit(Pid, kill),
                    erlang:error({Fn, Ln, timeout});
                false ->
                    Pid ! stop,
                    wait_for_down(Fn, Ln, Timeout, Pid, Mref, true)
            end
    end.

wait_loop(_F, true) -> exit(normal);
wait_loop(F, LastRes) ->
    Res = catch_call(F),
    receive
        stop -> erlang:exit(LastRes)
    after
        100 -> wait_loop(F, Res)
    end.

catch_call(F) ->
    try
        case F() of
            true -> true;
            Other -> erlang:error({unexpected, Other})
        end
    catch
        C : E : S ->
            {C, E, S}
    end.

