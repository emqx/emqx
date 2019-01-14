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

-module(emqx_portal_tests).
-behaviour(emqx_portal_connect).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_mqtt.hrl").

-define(PORTAL_NAME, test_portal).
-define(WAIT(PATTERN, TIMEOUT),
        receive
            PATTERN ->
                ok
        after
            TIMEOUT ->
                error(timeout)
        end).

%% stub callbacks
-export([start/1, send/2, stop/2]).

start(#{connect_result := Result, test_pid := Pid, test_ref := Ref}) ->
    case is_pid(Pid) of
        true -> Pid ! {connection_start_attempt, Ref};
        false -> ok
    end,
    Result.

send(SendFun, Batch) when is_function(SendFun, 1) ->
    SendFun(Batch).

stop(_Ref, _Pid) -> ok.

%% portal worker should retry connecting remote node indefinitely
reconnect_test() ->
    Ref = make_ref(),
    Config = make_config(Ref, self(), {error, test}),
    {ok, Pid} = emqx_portal:start_link(?PORTAL_NAME, Config),
    %% assert name registered
    ?assertEqual(Pid, whereis(?PORTAL_NAME)),
    ?WAIT({connection_start_attempt, Ref}, 1000),
    %% expect same message again
    ?WAIT({connection_start_attempt, Ref}, 1000),
    ok = emqx_portal:stop(?PORTAL_NAME),
    ok.

%% connect first, disconnect, then connect again
disturbance_test() ->
    Ref = make_ref(),
    Config = make_config(Ref, self(), {ok, Ref, connection}),
    {ok, Pid} = emqx_portal:start_link(?PORTAL_NAME, Config),
    ?assertEqual(Pid, whereis(?PORTAL_NAME)),
    ?WAIT({connection_start_attempt, Ref}, 1000),
    Pid ! {disconnected, Ref, test},
    ?WAIT({connection_start_attempt, Ref}, 1000),
    ok = emqx_portal:stop(?PORTAL_NAME).

%% buffer should continue taking in messages when disconnected
buffer_when_disconnected_test_() ->
    {timeout, 5000, fun test_buffer_when_disconnected/0}.

test_buffer_when_disconnected() ->
    Ref = make_ref(),
    Nums = lists:seq(1, 100),
    Sender = spawn_link(fun() -> receive {portal, Pid} -> sender_loop(Pid, Nums, _Interval = 5) end end),
    SenderMref = monitor(process, Sender),
    Receiver = spawn_link(fun() -> receive {portal, Pid} -> receiver_loop(Pid, Nums, _Interval = 1) end end),
    ReceiverMref = monitor(process, Receiver),
    SendFun = fun(Batch) ->
                      BatchRef = make_ref(),
                      Receiver ! {batch, BatchRef, Batch},
                      {ok, BatchRef}
              end,
    Config0 = make_config(Ref, false, {ok, Ref, SendFun}),
    Config = Config0#{reconnect_delay_ms => 100},
    {ok, Pid} = emqx_portal:start_link(?PORTAL_NAME, Config),
    Sender ! {portal, Pid},
    Receiver ! {portal, Pid},
    ?assertEqual(Pid, whereis(?PORTAL_NAME)),
    Pid ! {disconnected, Ref, test},
    ?WAIT({'DOWN', SenderMref, process, Sender, normal}, 2000),
    ?WAIT({'DOWN', ReceiverMref, process, Receiver, normal}, 1000),
    ok = emqx_portal:stop(?PORTAL_NAME).

%% Feed messages to portal
sender_loop(_Pid, [], _) -> exit(normal);
sender_loop(Pid, [Num | Rest], Interval) ->
    random_sleep(Interval),
    Pid ! {dispatch, dummy, make_msg(Num)},
    sender_loop(Pid, Rest, Interval).

%% Feed acknowledgments to portal
receiver_loop(_Pid, [], _) -> ok;
receiver_loop(Pid, Nums, Interval) ->
    receive
        {batch, BatchRef, Batch} ->
            Rest = match_nums(Batch, Nums),
            random_sleep(Interval),
            emqx_portal:handle_ack(Pid, BatchRef),
            receiver_loop(Pid, Rest, Interval)
    end.

random_sleep(MaxInterval) ->
    case rand:uniform(MaxInterval) - 1 of
        0 -> ok;
        T -> timer:sleep(T)
    end.

match_nums([], Rest) -> Rest;
match_nums([#{payload := P} | Rest], Nums) ->
    I = binary_to_integer(P),
    case Nums of
        [I | NumsLeft] -> match_nums(Rest, NumsLeft);
        _ -> error({I, Nums})
    end.

make_config(Ref, TestPid, Result) ->
    #{test_pid => TestPid,
      test_ref => Ref,
      connect_module => ?MODULE,
      reconnect_delay_ms => 50,
      connect_result => Result
     }.

make_msg(I) ->
    Payload = integer_to_binary(I),
    #{qos => ?QOS_1,
      dup => false,
      retain => false,
      topic => <<"test/topic">>,
      properties => [],
      payload => Payload
     }.

