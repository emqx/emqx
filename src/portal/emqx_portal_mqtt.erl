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

%% @doc This module implements EMQX Portal transport layer on top of MQTT protocol

-module(emqx_portal_mqtt).
-behaviour(emqx_portal_connect).

%% behaviour callbacks
-export([start/1,
         send/2,
         stop/2
        ]).

-include("emqx_mqtt.hrl").

-define(ACK_REF(ClientPid, PktId), {ClientPid, PktId}).

%% Messages towards ack collector process
-define(SENT(MaxPktId), {sent, MaxPktId}).
-define(ACKED(AnyPktId), {acked, AnyPktId}).
-define(STOP(Ref), {stop, Ref}).

start(Config) ->
    Ref = make_ref(),
    Parent = self(),
    AckCollector = spawn_link(fun() -> ack_collector(Parent, Ref) end),
    Handlers = make_hdlr(Parent, AckCollector, Ref),
    case emqx_client:start_link(Config#{msg_handler => Handlers, owner => AckCollector}) of
        {ok, Pid} ->
            case emqx_client:connect(Pid) of
                {ok, _} ->
                    %% ack collector is always a new pid every reconnect.
                    %% use it as a connection reference
                    {ok, Ref, #{ack_collector => AckCollector,
                                client_pid => Pid}};
                {error, Reason} ->
                    ok = stop(AckCollector, Pid),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(Ref, #{ack_collector := AckCollector,
            client_pid := Pid}) ->
    MRef = monitor(process, AckCollector),
    unlink(AckCollector),
    _ = AckCollector ! ?STOP(Ref),
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    after
        1000 ->
            exit(AckCollector, kill)
    end,
    _ = emqx_client:stop(Pid),
    ok.

send(#{client_pid := ClientPid, ack_collector := AckCollector}, Batch) ->
    send_loop(ClientPid, AckCollector, Batch).

send_loop(ClientPid, AckCollector, [Msg | Rest]) ->
    case emqx_client:publish(ClientPid, Msg) of
        {ok, PktId} when Rest =:= [] ->
            Rest =:= [] andalso AckCollector ! ?SENT(PktId),
            {ok, PktId};
        {ok, _PktId} ->
            send_loop(ClientPid, AckCollector, Rest);
        {error, {_PacketId, inflight_full}} ->
            timer:sleep(100),
            send_loop(ClientPid, AckCollector, [Msg | Rest]);
        {error, Reason} ->
            %% There is no partial sucess of a batch and recover from the middle
            %% only to retry all messages in one batch
            {error, Reason}
    end.

ack_collector(Parent, ConnRef) ->
    ack_collector(Parent, ConnRef, []).

ack_collector(Parent, ConnRef, PktIds) ->
    NewIds =
        receive
            ?STOP(ConnRef) ->
                exit(normal);
            ?SENT(PktId) ->
                %% this ++ only happens per-BATCH, hence no optimization
                PktIds ++ [PktId];
            ?ACKED(PktId) ->
                handle_ack(Parent, PktId, PktIds)
        after
            200 ->
                PktIds
        end,
   ack_collector(Parent, ConnRef, NewIds).

handle_ack(Parent, PktId, [PktId | Rest]) ->
    %% A batch is finished, time to ack portal
    ok = emqx_portal:handle_ack(Parent, PktId),
    Rest;
handle_ack(_Parent, PktId, [BatchMaxPktId | _] = All) ->
    %% partial ack of a batch, terminate here.
    true = (PktId < BatchMaxPktId), %% bad order otherwise
    All.

%% When puback for QoS-1 message is received from remote MQTT broker
%% NOTE: no support for QoS-2
handle_puback(AckCollector, #{packet_id := PktId, reason_code := RC}) ->
    RC =:= ?RC_SUCCESS andalso error(RC),
    AckCollector ! ?ACKED(PktId),
    ok.

%% Message published from remote broker. Import to local broker.
import_msg(Msg) ->
    %% auto-ack should be enabled in emqx_client, hence dummy ack-fun.
    emqx_portal:import_batch([Msg], _AckFun = fun() -> ok end).

make_hdlr(Parent, AckCollector, Ref) ->
    #{puback => fun(Ack) -> handle_puback(AckCollector, Ack) end,
      publish => fun(Msg) -> import_msg(Msg) end,
      disconnected => fun(RC, _Properties) -> Parent ! {disconnected, Ref, RC}, ok end
     }.

