%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt_message).

-author('feng@emqtt.io').

-include("emqtt.hrl").

-include("emqtt_packet.hrl").

-export([from_packet/1, to_packet/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec( from_packet( mqtt_packet() ) -> mqtt_message() | undefined ).

-spec( to_packet( mqtt_message() ) -> mqtt_packet() ).

-endif.

%%----------------------------------------------------------------------------

%%
%% @doc message from packet
%%
from_packet(#mqtt_packet{ header = #mqtt_packet_header{ type   = ?PUBLISH,
                                                        retain = Retain, 
                                                        qos    = Qos,
                                                        dup    = Dup }, 
                          variable = #mqtt_packet_publish{ topic_name = Topic, 
                                                           packet_id  = PacketId }, 
                          payload = Payload }) ->
	#mqtt_message{ msgid    = PacketId,
                   qos      = Qos,
                   retain   = Retain,
                   dup      = Dup,
                   topic    = Topic,
                   payload  = Payload };

from_packet(#mqtt_packet_connect{ will_flag  = false }) ->
    undefined;

from_packet(#mqtt_packet_connect{ will_retain = Retain, 
                                  will_qos    = Qos, 
                                  will_topic  = Topic, 
                                  will_msg    = Msg }) ->
    #mqtt_message{ retain  = Retain, 
                   qos     = Qos,
                   topic   = Topic, 
                   dup     = false, 
                   payload = Msg }.

%%
%% @doc message to packet
%%
to_packet(#mqtt_message{ msgid   = MsgId,
                         qos     = Qos,
                         retain  = Retain,
                         dup     = Dup,
                         topic   = Topic,
                         payload = Payload }) ->

    PacketId = if 
        Qos =:= ?QOS_0 -> undefined; 
        true -> MsgId 
    end,  

    #mqtt_packet{ header = #mqtt_packet_header { type 	 = ?PUBLISH, 
                                                 qos    = Qos, 
                                                 retain = Retain, 
                                                 dup    = Dup }, 
                  variable = #mqtt_packet_publish { topic_name = Topic, 
                                                    packet_id  = PacketId }, 
                  payload = Payload }.

