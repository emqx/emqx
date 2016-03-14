%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc MQTT Message Functions
-module(emqttd_message).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([make/3, make/4, from_packet/1, from_packet/2, from_packet/3,
         to_packet/1]).

-export([set_flag/1, set_flag/2, unset_flag/1, unset_flag/2]).

-export([format/1]).

%% @doc Make a message
-spec(make(From, Topic, Payload) -> mqtt_message() when
      From    :: atom() | binary(),
      Topic   :: binary(),
      Payload :: binary()).
make(From, Topic, Payload) ->
    #mqtt_message{topic     = Topic,
                  from      = From,
                  payload   = Payload,
                  timestamp = os:timestamp()}.

-spec(make(From, Qos, Topic, Payload) -> mqtt_message() when
      From    :: atom() | binary(),
      Qos     :: mqtt_qos() | mqtt_qos_name(),
      Topic   :: binary(),
      Payload :: binary()).
make(From, Qos, Topic, Payload) ->
    #mqtt_message{msgid     = msgid(?QOS_I(Qos)),
                  topic     = Topic,
                  from      = From,
                  qos       = ?QOS_I(Qos),
                  payload   = Payload,
                  timestamp = os:timestamp()}.

%% @doc Message from Packet
-spec(from_packet(mqtt_packet()) -> mqtt_message()).
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

from_packet(#mqtt_packet_connect{client_id   = ClientId,
                                 username    = Username,
                                 will_retain = Retain,
                                 will_qos    = Qos,
                                 will_topic  = Topic,
                                 will_msg    = Msg}) ->
    #mqtt_message{msgid     = msgid(Qos),
                  topic     = Topic,
                  from      = ClientId,
                  sender    = Username,
                  retain    = Retain,
                  qos       = Qos,
                  dup       = false,
                  payload   = Msg, 
                  timestamp = os:timestamp()}.

from_packet(ClientId, Packet) ->
    Msg = from_packet(Packet),
    Msg#mqtt_message{from = ClientId}.

from_packet(Username, ClientId, Packet) ->
    Msg = from_packet(Packet),
    Msg#mqtt_message{from = ClientId, sender = Username}.

msgid(?QOS_0) ->
    undefined;
msgid(Qos) when Qos =:= ?QOS_1 orelse Qos =:= ?QOS_2 ->
    emqttd_guid:gen().

%% @doc Message to packet
-spec(to_packet(mqtt_message()) -> mqtt_packet()).
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

%% @doc set dup, retain flag
-spec(set_flag(mqtt_message()) -> mqtt_message()).
set_flag(Msg) ->
    Msg#mqtt_message{dup = true, retain = true}.

-spec(set_flag(atom(), mqtt_message()) -> mqtt_message()).
set_flag(dup, Msg = #mqtt_message{dup = false}) -> 
    Msg#mqtt_message{dup = true};
set_flag(sys, Msg = #mqtt_message{sys = false}) -> 
    Msg#mqtt_message{sys = true};
set_flag(retain, Msg = #mqtt_message{retain = false}) ->
    Msg#mqtt_message{retain = true};
set_flag(Flag, Msg) when Flag =:= dup orelse Flag =:= retain -> Msg.

%% @doc Unset dup, retain flag
-spec(unset_flag(mqtt_message()) -> mqtt_message()).
unset_flag(Msg) ->
    Msg#mqtt_message{dup = false, retain = false}.

-spec(unset_flag(dup | retain | atom(), mqtt_message()) -> mqtt_message()).
unset_flag(dup, Msg = #mqtt_message{dup = true}) -> 
    Msg#mqtt_message{dup = false};
unset_flag(retain, Msg = #mqtt_message{retain = true}) ->
    Msg#mqtt_message{retain = false};
unset_flag(Flag, Msg) when Flag =:= dup orelse Flag =:= retain -> Msg.

%% @doc Format MQTT Message
format(#mqtt_message{msgid = MsgId, pktid = PktId, from = From, sender = Sender,
                     qos = Qos, retain = Retain, dup = Dup, topic =Topic}) ->
    io_lib:format("Message(Q~p, R~p, D~p, MsgId=~p, PktId=~p, From=~s, Sender=~s, Topic=~s)",
                  [i(Qos), i(Retain), i(Dup), MsgId, PktId, From, Sender, Topic]).

i(true)  -> 1;
i(false) -> 0;
i(I) when is_integer(I) -> I.

