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

-module(emqx_connector_mqtt_worker_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(BRIDGE_NAME, test).
-define(BRIDGE_REG_NAME, emqx_connector_mqtt_worker_test).
-define(WAIT(PATTERN, TIMEOUT),
    receive
        PATTERN ->
            ok
    after TIMEOUT ->
        error(timeout)
    end
).

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

%% connect first, disconnect, then connect again
disturbance_test() ->
    meck:new(emqx_connector_mqtt_mod, [passthrough, no_history]),
    meck:expect(emqx_connector_mqtt_mod, start, 1, fun(Conf) -> start(Conf) end),
    meck:expect(emqx_connector_mqtt_mod, send, 2, fun(SendFun, Batch) -> send(SendFun, Batch) end),
    meck:expect(emqx_connector_mqtt_mod, stop, 1, fun(Pid) -> stop(Pid) end),
    try
        emqx_metrics:start_link(),
        Ref = make_ref(),
        TestPid = self(),
        Config = make_config(Ref, TestPid, {ok, #{client_pid => TestPid}}),
        {ok, Pid} = emqx_connector_mqtt_worker:start_link(Config#{name => bridge_disturbance}),
        ?assertEqual(Pid, whereis(bridge_disturbance)),
        ?WAIT({connection_start_attempt, Ref}, 1000),
        Pid ! {disconnected, TestPid, test},
        ?WAIT({connection_start_attempt, Ref}, 1000),
        emqx_metrics:stop(),
        ok = emqx_connector_mqtt_worker:stop(Pid)
    after
        meck:unload(emqx_connector_mqtt_mod)
    end.

manual_start_stop_test() ->
    meck:new(emqx_connector_mqtt_mod, [passthrough, no_history]),
    meck:expect(emqx_connector_mqtt_mod, start, 1, fun(Conf) -> start(Conf) end),
    meck:expect(emqx_connector_mqtt_mod, send, 2, fun(SendFun, Batch) -> send(SendFun, Batch) end),
    meck:expect(emqx_connector_mqtt_mod, stop, 1, fun(Pid) -> stop(Pid) end),
    try
        emqx_metrics:start_link(),
        Ref = make_ref(),
        TestPid = self(),
        BridgeName = manual_start_stop,
        Config0 = make_config(Ref, TestPid, {ok, #{client_pid => TestPid}}),
        Config = Config0#{start_type := manual},
        {ok, Pid} = emqx_connector_mqtt_worker:start_link(Config#{name => BridgeName}),
        %% call ensure_started again should yield the same result
        ok = emqx_connector_mqtt_worker:ensure_started(BridgeName),
        emqx_connector_mqtt_worker:ensure_stopped(BridgeName),
        emqx_metrics:stop(),
        ok = emqx_connector_mqtt_worker:stop(Pid)
    after
        meck:unload(emqx_connector_mqtt_mod)
    end.

make_config(Ref, TestPid, Result) ->
    #{
        start_type => auto,
        subscriptions => undefined,
        forwards => undefined,
        reconnect_interval => 50,
        test_pid => TestPid,
        test_ref => Ref,
        connect_result => Result
    }.
