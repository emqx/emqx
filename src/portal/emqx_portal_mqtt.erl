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

%% optional behaviour callbacks
-export([ensure_subscribed/3,
         ensure_unsubscribed/2
        ]).

-include("emqx_mqtt.hrl").

-define(ACK_REF(ClientPid, PktId), {ClientPid, PktId}).

%% Messages towards ack collector process
-define(RANGE(Min, Max), {Min, Max}).
-define(SENT(PktIdRange), {sent, PktIdRange}).
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
                    try
                        subscribe_remote_topics(Pid, maps:get(subscriptions, Config, [])),
                        %% ack collector is always a new pid every reconnect.
                        %% use it as a connection reference
                        {ok, Ref, #{ack_collector => AckCollector,
                                    client_pid => Pid}}
                    catch
                        throw : Reason ->
                            ok = stop(AckCollector, Pid),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ok = stop(AckCollector, Pid),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(Ref, #{ack_collector := AckCollector, client_pid := Pid}) ->
    safe_stop(AckCollector, fun() -> AckCollector ! ?STOP(Ref) end, 1000),
    safe_stop(Pid, fun() -> emqx_client:stop(Pid) end, 1000),
    ok.

ensure_subscribed(#{client_pid := Pid}, Topic, QoS) when is_pid(Pid) ->
    emqx_client:subscribe(Pid, Topic, QoS);
ensure_subscribed(_Conn, _Topic, _QoS) ->
    %% return ok for now, next re-connect should should call start with new topic added to config
    ok.

ensure_unsubscribed(#{client_pid := Pid}, Topic) when is_pid(Pid) ->
    emqx_client:unsubscribe(Pid, Topic);
ensure_unsubscribed(_, _) ->
    %% return ok for now, next re-connect should should call start with this topic deleted from config
    ok.

safe_stop(Pid, StopF, Timeout) ->
    MRef = monitor(process, Pid),
    unlink(Pid),
    try
        StopF()
    catch
        _ : _ ->
            ok
    end,
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    after
        Timeout ->
            exit(Pid, kill)
    end.

send(#{client_pid := ClientPid, ack_collector := AckCollector} = Conn, Batch) ->
    case emqx_client:publish(ClientPid, Batch) of
        {ok, BasePktId} ->
            LastPktId = ?BUMP_PACKET_ID(BasePktId, length(Batch) - 1),
            AckCollector ! ?SENT(?RANGE(BasePktId, LastPktId)),
            %% return last pakcet id as batch reference
            {ok, LastPktId};
        {error, {_PacketId, inflight_full}} ->
            timer:sleep(100),
            send(Conn, Batch);
        {error, Reason} ->
            %% NOTE: There is no partial sucess of a batch and recover from the middle
            %% only to retry all messages in one batch
            {error, Reason}
    end.

ack_collector(Parent, ConnRef) ->
    ack_collector(Parent, ConnRef, queue:new(), []).

ack_collector(Parent, ConnRef, Acked, Sent) ->
    {NewAcked, NewSent} =
        receive
            ?STOP(ConnRef) ->
                exit(normal);
            ?ACKED(PktId) ->
                match_acks(Parent, queue:in(PktId, Acked), Sent);
            ?SENT(Range) ->
                %% this message only happens per-batch, hence ++ is ok
                match_acks(Parent, Acked, Sent ++ [Range])
        after
            200 ->
                {Acked, Sent}
        end,
   ack_collector(Parent, ConnRef, NewAcked, NewSent).

match_acks(_Parent, Acked, []) -> {Acked, []};
match_acks(Parent, Acked, Sent) ->
    match_acks_1(Parent, queue:out(Acked), Sent).

match_acks_1(_Parent, {empty, Empty}, Sent) -> {Empty, Sent};
match_acks_1(Parent, {{value, PktId}, Acked}, [?RANGE(PktId, PktId) | Sent]) ->
    %% batch finished
    ok = emqx_portal:handle_ack(Parent, PktId),
    match_acks(Parent, Acked, Sent);
match_acks_1(Parent, {{value, PktId}, Acked}, [?RANGE(PktId, Max) | Sent]) ->
    match_acks(Parent, Acked, [?RANGE(PktId + 1, Max) | Sent]).

%% When puback for QoS-1 message is received from remote MQTT broker
%% NOTE: no support for QoS-2
handle_puback(AckCollector, #{packet_id := PktId, reason_code := RC}) ->
    RC =:= ?RC_SUCCESS orelse error({puback_error_code, RC}),
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

subscribe_remote_topics(ClientPid, Subscriptions) ->
    lists:foreach(fun({Topic, Qos}) ->
                          case emqx_client:subscribe(ClientPid, Topic, Qos) of
                              {ok, _, _} -> ok;
                              Error -> throw(Error)
                          end
                  end, Subscriptions).

