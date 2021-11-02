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

%% @doc This module implements EMQX Bridge transport layer on top of MQTT protocol

-module(emqx_connector_mqtt_mod).

-export([ start/1
        , send/2
        , stop/1
        , ping/1
        ]).

-export([ ensure_subscribed/3
        , ensure_unsubscribed/2
        ]).

%% callbacks for emqtt
-export([ handle_puback/2
        , handle_publish/2
        , handle_disconnected/2
        ]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(ACK_REF(ClientPid, PktId), {ClientPid, PktId}).

%% Messages towards ack collector process
-define(REF_IDS(Ref, Ids), {Ref, Ids}).

%%--------------------------------------------------------------------
%% emqx_bridge_connect callbacks
%%--------------------------------------------------------------------

start(Config) ->
    Parent = self(),
    {Host, Port} = maps:get(server, Config),
    Mountpoint = maps:get(receive_mountpoint, Config, undefined),
    Subscriptions = maps:get(subscriptions, Config, undefined),
    Vars = emqx_connector_mqtt_msg:make_pub_vars(Mountpoint, Subscriptions),
    Handlers = make_hdlr(Parent, Vars),
    Config1 = Config#{
        msg_handler => Handlers,
        host => Host,
        port => Port,
        force_ping => true,
        proto_ver => maps:get(proto_ver, Config, v4)
    },
    case emqtt:start_link(process_config(Config1)) of
        {ok, Pid} ->
            case emqtt:connect(Pid) of
                {ok, _} ->
                    try
                        ok = from_remote_topics(Pid, Subscriptions),
                        {ok, #{client_pid => Pid, subscriptions => Subscriptions}}
                    catch
                        throw : Reason ->
                            ok = stop(#{client_pid => Pid}),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ok = stop(#{client_pid => Pid}),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(#{client_pid := Pid}) ->
    safe_stop(Pid, fun() -> emqtt:stop(Pid) end, 1000),
    ok.

ping(#{client_pid := Pid}) ->
    emqtt:ping(Pid).

ensure_subscribed(#{client_pid := Pid, subscriptions := Subs} = Conn, Topic, QoS) when is_pid(Pid) ->
    case emqtt:subscribe(Pid, Topic, QoS) of
        {ok, _, _} -> Conn#{subscriptions => [{Topic, QoS}|Subs]};
        Error -> {error, Error}
    end;
ensure_subscribed(_Conn, _Topic, _QoS) ->
    %% return ok for now
    %% next re-connect should should call start with new topic added to config
    ok.

ensure_unsubscribed(#{client_pid := Pid, subscriptions := Subs} = Conn, Topic) when is_pid(Pid) ->
    case emqtt:unsubscribe(Pid, Topic) of
        {ok, _, _} -> Conn#{subscriptions => lists:keydelete(Topic, 1, Subs)};
        Error -> {error, Error}
    end;
ensure_unsubscribed(Conn, _) ->
    %% return ok for now
    %% next re-connect should should call start with this topic deleted from config
    Conn.

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

send(Conn, Msgs) ->
    send(Conn, Msgs, []).

send(_Conn, [], []) ->
    %% all messages in the batch are QoS-0
    Ref = make_ref(),
    %% QoS-0 messages do not have packet ID
    %% the batch ack is simulated with a loop-back message
    self() ! {batch_ack, Ref},
    {ok, Ref};
send(_Conn, [], PktIds) ->
    %% PktIds is not an empty list if there is any non-QoS-0 message in the batch,
    %% And the worker should wait for all acks
    {ok, PktIds};
send(#{client_pid := ClientPid} = Conn, [Msg | Rest], PktIds) ->
    case emqtt:publish(ClientPid, Msg) of
        ok ->
            send(Conn, Rest, PktIds);
        {ok, PktId} ->
            send(Conn, Rest, [PktId | PktIds]);
        {error, Reason} ->
            %% NOTE: There is no partial sucess of a batch and recover from the middle
            %% only to retry all messages in one batch
            {error, Reason}
    end.

handle_puback(#{packet_id := PktId, reason_code := RC}, Parent)
  when RC =:= ?RC_SUCCESS;
       RC =:= ?RC_NO_MATCHING_SUBSCRIBERS ->
    Parent ! {batch_ack, PktId}, ok;
handle_puback(#{packet_id := PktId, reason_code := RC}, _Parent) ->
    ?SLOG(warning, #{msg => "publish to remote node falied",
        packet_id => PktId, reason_code => RC}).

handle_publish(Msg, undefined) ->
    ?SLOG(error, #{msg => "cannot publish to local broker as"
                          " 'ingress' is not configured",
                   message => Msg});
handle_publish(Msg, Vars) ->
    ?SLOG(debug, #{msg => "publish to local broker",
                   message => Msg, vars => Vars}),
    emqx_metrics:inc('bridge.mqtt.message_received_from_remote', 1),
    case Vars of
        #{on_message_received := {Mod, Func, Args}} ->
            _ = erlang:apply(Mod, Func, [Msg | Args]);
        _ -> ok
    end,
    case maps:get(to_local_topic, Vars, undefined) of
        undefined -> ok;
        _Topic ->
            emqx_broker:publish(emqx_connector_mqtt_msg:to_broker_msg(Msg, Vars))
    end.

handle_disconnected(Reason, Parent) ->
    Parent ! {disconnected, self(), Reason}.

make_hdlr(Parent, Vars) ->
    #{puback => {fun ?MODULE:handle_puback/2, [Parent]},
      publish => {fun ?MODULE:handle_publish/2, [Vars]},
      disconnected => {fun ?MODULE:handle_disconnected/2, [Parent]}
     }.

from_remote_topics(_ClientPid, undefined) -> ok;
from_remote_topics(ClientPid, #{from_remote_topic := FromTopic, subscribe_qos := QoS}) ->
    case emqtt:subscribe(ClientPid, FromTopic, QoS) of
        {ok, _, _} -> ok;
        Error -> throw(Error)
    end.

process_config(Config) ->
    maps:without([conn_type, address, receive_mountpoint, subscriptions, name], Config).
