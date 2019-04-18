%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements EMQX Bridge transport layer on top of MQTT protocol

-module(emqx_bridge_mqtt).

-behaviour(emqx_bridge_connect).

%% behaviour callbacks
-export([ start/1
        , send/2
        , stop/2
        ]).

%% optional behaviour callbacks
-export([ ensure_subscribed/3
        , ensure_unsubscribed/2
        ]).

-include("emqx_mqtt.hrl").

-define(ACK_REF(ClientPid, PktId), {ClientPid, PktId}).

%% Messages towards ack collector process
-define(RANGE(Min, Max), {Min, Max}).
-define(REF_IDS(Ref, Ids), {Ref, Ids}).
-define(SENT(RefIds), {sent, RefIds}).
-define(ACKED(AnyPktId), {acked, AnyPktId}).
-define(STOP(Ref), {stop, Ref}).

%%------------------------------------------------------------------------------
%% emqx_bridge_connect callbacks
%%------------------------------------------------------------------------------

start(Config = #{address := Address}) ->
    Ref = make_ref(),
    Parent = self(),
    AckCollector = spawn_link(fun() -> ack_collector(Parent, Ref) end),
    Handlers = make_hdlr(Parent, AckCollector, Ref),
    {Host, Port} = case string:tokens(Address, ":") of
                       [H] -> {H, 1883};
                       [H, P] -> {H, list_to_integer(P)}
                   end,
    ClientConfig = Config#{msg_handler => Handlers,
                           owner => AckCollector,
                           host => Host,
                           port => Port,
                           bridge_mode => true
                          },
    case emqx_client:start_link(ClientConfig) of
        {ok, Pid} ->
            case emqx_client:connect(Pid) of
                {ok, _} ->
                    try
                        subscribe_remote_topics(Pid, maps:get(subscriptions, Config, [])),
                        {ok, Ref, #{ack_collector => AckCollector,
                                    client_pid => Pid}}
                    catch
                        throw : Reason ->
                            ok = stop(AckCollector, Pid),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ok = stop(Ref, #{ack_collector => AckCollector, client_pid => Pid}),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(Ref, #{ack_collector := AckCollector, client_pid := Pid}) ->
    safe_stop(Pid, fun() -> emqx_client:stop(Pid) end, 1000),
    safe_stop(AckCollector, fun() -> AckCollector ! ?STOP(Ref) end, 1000),
    ok.

ensure_subscribed(#{client_pid := Pid}, Topic, QoS) when is_pid(Pid) ->
    case emqx_client:subscribe(Pid, Topic, QoS) of
        {ok, _, _} -> ok;
        Error -> Error
    end;
ensure_subscribed(_Conn, _Topic, _QoS) ->
    %% return ok for now, next re-connect should should call start with new topic added to config
    ok.

ensure_unsubscribed(#{client_pid := Pid}, Topic) when is_pid(Pid) ->
    case emqx_client:unsubscribe(Pid, Topic) of
        {ok, _, _} -> ok;
        Error -> Error
    end;
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

send(Conn, Batch) ->
    send(Conn, Batch, []).

send(#{client_pid := ClientPid, ack_collector := AckCollector} = Conn, [Msg | Rest], Acc) ->
    case emqx_client:publish(ClientPid, Msg) of
        {ok, PktId} when Rest =:= [] ->
            %% last one sent
            Ref = make_ref(),
            AckCollector ! ?SENT(?REF_IDS(Ref, lists:reverse([PktId | Acc]))),
            {ok, Ref};
        {ok, PktId} ->
            send(Conn, Rest, [PktId | Acc]);
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
            ?SENT(RefIds) ->
                %% this message only happens per-batch, hence ++ is ok
                match_acks(Parent, Acked, Sent ++ [RefIds])
        after
            200 ->
                {Acked, Sent}
        end,
   ack_collector(Parent, ConnRef, NewAcked, NewSent).

match_acks(_Parent, Acked, []) -> {Acked, []};
match_acks(Parent, Acked, Sent) ->
    match_acks_1(Parent, queue:out(Acked), Sent).

match_acks_1(_Parent, {empty, Empty}, Sent) -> {Empty, Sent};
match_acks_1(Parent, {{value, PktId}, Acked}, [?REF_IDS(Ref, [PktId]) | Sent]) ->
    %% batch finished
    ok = emqx_bridge:handle_ack(Parent, Ref),
    match_acks(Parent, Acked, Sent);
match_acks_1(Parent, {{value, PktId}, Acked}, [?REF_IDS(Ref, [PktId | RestIds]) | Sent]) ->
    %% one message finished, but not the whole batch
    match_acks(Parent, Acked, [?REF_IDS(Ref, RestIds) | Sent]).


%% When puback for QoS-1 message is received from remote MQTT broker
%% NOTE: no support for QoS-2
handle_puback(AckCollector, #{packet_id := PktId, reason_code := RC}) ->
    RC =:= ?RC_SUCCESS orelse error({puback_error_code, RC}),
    AckCollector ! ?ACKED(PktId),
    ok.

%% Message published from remote broker. Import to local broker.
import_msg(Msg) ->
    %% auto-ack should be enabled in emqx_client, hence dummy ack-fun.
    emqx_bridge:import_batch([Msg], _AckFun = fun() -> ok end).

make_hdlr(Parent, AckCollector, Ref) ->
    #{puback => fun(Ack) -> handle_puback(AckCollector, Ack) end,
      publish => fun(Msg) -> import_msg(Msg) end,
      disconnected => fun(Reason) -> Parent ! {disconnected, Ref, Reason}, ok end
     }.

subscribe_remote_topics(ClientPid, Subscriptions) ->
    lists:foreach(fun({Topic, Qos}) ->
                          case emqx_client:subscribe(ClientPid, Topic, Qos) of
                              {ok, _, _} -> ok;
                              Error -> throw(Error)
                          end
                  end, Subscriptions).
