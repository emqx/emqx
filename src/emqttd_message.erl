%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc MQTT Message Functions
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_message).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([make/3, make/4, from_packet/1, from_packet/2, to_packet/1]).

-export([set_flag/1, set_flag/2, unset_flag/1, unset_flag/2]).

-export([format/1]).

%%------------------------------------------------------------------------------
%% @doc Make a message
%% @end
%%------------------------------------------------------------------------------
-spec make(From, Topic, Payload) -> mqtt_message() when
    From    :: atom() | binary(),
    Topic   :: binary(),
    Payload :: binary().
make(From, Topic, Payload) ->
    #mqtt_message{topic     = Topic,
                  from      = From,
                  payload   = Payload,
                  timestamp = os:timestamp()}.

-spec make(From, Qos, Topic, Payload) -> mqtt_message() when
    From    :: atom() | binary(),
    Qos     :: mqtt_qos() | mqtt_qos_name(),
    Topic   :: binary(),
    Payload :: binary().
make(From, Qos, Topic, Payload) ->
    #mqtt_message{msgid     = msgid(?QOS_I(Qos)),
                  topic     = Topic,
                  from      = From,
                  qos       = ?QOS_I(Qos),
                  payload   = Payload,
                  timestamp = os:timestamp()}.

%%------------------------------------------------------------------------------
%% @doc Message from Packet
%% @end
%%------------------------------------------------------------------------------
-spec from_packet(mqtt_packet()) -> mqtt_message().
from_packet(#mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                        retain = Retain,
                                                        qos    = Qos,
                                                        dup    = Dup}, 
                         variable = #mqtt_packet_publish{topic_name = Topic,
                                                         packet_id  = PacketId},
                         payload  = Payload}) ->
    #mqtt_message{msgid    = msgid(Qos),
                  pktid    = PacketId,
                  qos      = Qos,
                  retain   = Retain,
                  dup      = Dup,
                  topic    = Topic,
                  payload  = Payload,
                  timestamp = os:timestamp()};

from_packet(#mqtt_packet_connect{will_flag  = false}) ->
    undefined;

from_packet(#mqtt_packet_connect{will_retain = Retain,
                                 will_qos    = Qos,
                                 will_topic  = Topic,
                                 will_msg    = Msg}) ->
    #mqtt_message{msgid     = msgid(Qos),
                  topic     = Topic,
                  retain    = Retain,
                  qos       = Qos,
                  dup       = false,
                  payload   = Msg, 
                  timestamp = os:timestamp()}.

from_packet(ClientId, Packet) ->
    Msg = from_packet(Packet), Msg#mqtt_message{from = ClientId}.

msgid(?QOS_0) ->
    undefined;
msgid(Qos) when Qos =:= ?QOS_1 orelse Qos =:= ?QOS_2 ->
    emqttd_guid:gen().

%%------------------------------------------------------------------------------
%% @doc Message to packet
%% @end
%%------------------------------------------------------------------------------
-spec to_packet(mqtt_message()) -> mqtt_packet().
to_packet(#mqtt_message{pktid   = PkgId,
                        qos     = Qos,
                        retain  = Retain,
                        dup     = Dup,
                        topic   = Topic,
                        payload = Payload}) ->

    #mqtt_packet{header = #mqtt_packet_header{type   = ?PUBLISH,
                                              qos    = Qos,
                                              retain = Retain,
                                              dup    = Dup},
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 packet_id  = if 
                                                                  Qos =:= ?QOS_0 -> undefined;
                                                                  true -> PkgId
                                                              end  
                                                },
                 payload = Payload}.

%%------------------------------------------------------------------------------
%% @doc set dup, retain flag
%% @end
%%------------------------------------------------------------------------------
-spec set_flag(mqtt_message()) -> mqtt_message().
set_flag(Msg) ->
    Msg#mqtt_message{dup = true, retain = true}.

-spec set_flag(atom(), mqtt_message()) -> mqtt_message().
set_flag(dup, Msg = #mqtt_message{dup = false}) -> 
    Msg#mqtt_message{dup = true};
set_flag(sys, Msg = #mqtt_message{sys = false}) -> 
    Msg#mqtt_message{sys = true};
set_flag(retain, Msg = #mqtt_message{retain = false}) ->
    Msg#mqtt_message{retain = true};
set_flag(Flag, Msg) when Flag =:= dup orelse Flag =:= retain -> Msg.

%%------------------------------------------------------------------------------
%% @doc Unset dup, retain flag
%% @end
%%------------------------------------------------------------------------------
-spec unset_flag(mqtt_message()) -> mqtt_message().
unset_flag(Msg) ->
    Msg#mqtt_message{dup = false, retain = false}.

-spec unset_flag(dup | retain | atom(), mqtt_message()) -> mqtt_message().
unset_flag(dup, Msg = #mqtt_message{dup = true}) -> 
    Msg#mqtt_message{dup = false};
unset_flag(retain, Msg = #mqtt_message{retain = true}) ->
    Msg#mqtt_message{retain = false};
unset_flag(Flag, Msg) when Flag =:= dup orelse Flag =:= retain -> Msg.

%%------------------------------------------------------------------------------
%% @doc Format MQTT Message
%% @end
%%------------------------------------------------------------------------------
format(#mqtt_message{msgid = MsgId, pktid = PktId, from = From,
                     qos = Qos, retain = Retain, dup = Dup, topic =Topic}) ->
    io_lib:format("Message(Q~p, R~p, D~p, MsgId=~p, PktId=~p, From=~s, Topic=~s)",
                  [i(Qos), i(Retain), i(Dup), MsgId, PktId, From, Topic]).

i(true)  -> 1;
i(false) -> 0;
i(I) when is_integer(I) -> I.

