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

-module(emqx_bridge_worker_tests).
-behaviour(emqx_bridge_connect).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(BRIDGE_NAME, test).
-define(BRIDGE_REG_NAME, emqx_bridge_worker_test).
-define(WAIT(PATTERN, TIMEOUT),
        receive
            PATTERN ->
                ok
        after
            TIMEOUT ->
                error(timeout)
        end).

%% stub callbacks
-export([start/1, send/2, stop/1]).

start(#{connect_result := Result, test_pid := Pid, test_ref := Ref}) ->
    case is_pid(Pid) of
        true -> Pid ! {connection_start_attempt, Ref};
        false -> ok
    end,
    Result.

send(SendFun, Batch) when is_function(SendFun, 2) ->
    SendFun(Batch).

stop(_Pid) -> ok.

%% bridge worker should retry connecting remote node indefinitely
% reconnect_test() ->
%     emqx_metrics:start_link(),
%     emqx_bridge_worker:register_metrics(),
%     Ref = make_ref(),
%     Config = make_config(Ref, self(), {error, test}),
%     {ok, Pid} = emqx_bridge_worker:start_link(?BRIDGE_NAME, Config),
%     %% assert name registered
%     ?assertEqual(Pid, whereis(?BRIDGE_REG_NAME)),
%     ?WAIT({connection_start_attempt, Ref}, 1000),
%     %% expect same message again
%     ?WAIT({connection_start_attempt, Ref}, 1000),
%     ok = emqx_bridge_worker:stop(?BRIDGE_REG_NAME),
%     emqx_metrics:stop(),
%     ok.

%% connect first, disconnect, then connect again
disturbance_test() ->
    emqx_metrics:start_link(),
    emqx_bridge_worker:register_metrics(),
    Ref = make_ref(),
    TestPid = self(),
    Config = make_config(Ref, TestPid, {ok, #{client_pid =>  TestPid}}),
    {ok, Pid} = emqx_bridge_worker:start_link(?BRIDGE_NAME, Config),
    ?assertEqual(Pid, whereis(?BRIDGE_REG_NAME)),
    ?WAIT({connection_start_attempt, Ref}, 1000),
    Pid ! {disconnected, TestPid, test},
    ?WAIT({connection_start_attempt, Ref}, 1000),
    emqx_metrics:stop(),
    ok = emqx_bridge_worker:stop(?BRIDGE_REG_NAME).

% % %% buffer should continue taking in messages when disconnected
% buffer_when_disconnected_test_() ->
%     {timeout, 10000, fun test_buffer_when_disconnected/0}.

% test_buffer_when_disconnected() ->
%     Ref = make_ref(),
%     Nums = lists:seq(1, 100),
%     Sender = spawn_link(fun() -> receive {bridge, Pid} -> sender_loop(Pid, Nums, _Interval = 5) end end),
%     SenderMref = monitor(process, Sender),
%     Receiver = spawn_link(fun() -> receive {bridge, Pid} -> receiver_loop(Pid, Nums, _Interval = 1) end end),
%     ReceiverMref = monitor(process, Receiver),
%     SendFun = fun(Batch) ->
%                       BatchRef = make_ref(),
%                       Receiver ! {batch, BatchRef, Batch},
%                       {ok, BatchRef}
%               end,
%     Config0 = make_config(Ref, false, {ok, #{client_pid => undefined}}),
%     Config = Config0#{reconnect_delay_ms => 100},
%     emqx_metrics:start_link(),
%     emqx_bridge_worker:register_metrics(),
%     {ok, Pid} = emqx_bridge_worker:start_link(?BRIDGE_NAME, Config),
%     Sender ! {bridge, Pid},
%     Receiver ! {bridge, Pid},
%     ?assertEqual(Pid, whereis(?BRIDGE_REG_NAME)),
%     Pid ! {disconnected, Ref, test},
%     ?WAIT({'DOWN', SenderMref, process, Sender, normal}, 5000),
%     ?WAIT({'DOWN', ReceiverMref, process, Receiver, normal}, 1000),
%     ok = emqx_bridge_worker:stop(?BRIDGE_REG_NAME),
%     emqx_metrics:stop().

manual_start_stop_test() ->
    emqx_metrics:start_link(),
    emqx_bridge_worker:register_metrics(),
    Ref = make_ref(),
    TestPid = self(),
    Config0 = make_config(Ref, TestPid, {ok, #{client_pid =>  TestPid}}),
    Config = Config0#{start_type := manual},
    {ok, Pid} = emqx_bridge_worker:start_link(?BRIDGE_NAME, Config),
    %% call ensure_started again should yeld the same result
    ok = emqx_bridge_worker:ensure_started(?BRIDGE_NAME),
    ?assertEqual(Pid, whereis(?BRIDGE_REG_NAME)),
    emqx_bridge_worker:ensure_stopped(unknown),
    emqx_bridge_worker:ensure_stopped(Pid),
    emqx_bridge_worker:ensure_stopped(?BRIDGE_REG_NAME),
    emqx_metrics:stop().

make_config(Ref, TestPid, Result) ->
    #{test_pid => TestPid,
      test_ref => Ref,
      connect_module => ?MODULE,
      reconnect_delay_ms => 50,
      connect_result => Result,
      start_type => auto
     }.
