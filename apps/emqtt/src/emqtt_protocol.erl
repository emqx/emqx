%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@slimchat.io>
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

-module(emqtt_protocol).

-include("emqtt.hrl").

-include("emqtt_log.hrl").

-include("emqtt_frame.hrl").

-export([process_request/3]).

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                          username   = Username,
                                          password   = Password,
                                          proto_ver  = ProtoVersion,
                                          clean_sess = CleanSess,
										  keep_alive = AlivePeriod,
                                          client_id  = ClientId } = Var}, #state{socket = Sock} = State) ->
    {ReturnCode, State1} =
        case {lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)),
              valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, State};
            {_, false} ->
                {?CONNACK_INVALID_ID, State};
            _ ->
                case emqtt_auth:check(Username, Password) of
                    false ->
                        ?ERROR_MSG("MQTT login failed - no credentials"),
                        {?CONNACK_CREDENTIALS, State};
                    true ->
						?INFO("connect from clientid: ~p, ~p", [ClientId, AlivePeriod]),
						emqtt_cm:create(ClientId, self()),
						KeepAlive = emqtt_keep_alive:new(AlivePeriod*1500, keep_alive_timeout),
						{?CONNACK_ACCEPT,
						 State #state{ will_msg   = make_will_msg(Var),
											 client_id  = ClientId,
											 keep_alive = KeepAlive}}
                end
        end,
		?INFO("recv conn...:~p", [ReturnCode]),
		send_frame(Sock, #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?CONNACK},
								 variable = #mqtt_frame_connack{
                                         return_code = ReturnCode }}),
    {ok, State1};

process_request(?PUBLISH, Frame=#mqtt_frame{
									fixed = #mqtt_frame_fixed{qos = ?QOS_0}}, State) ->
	emqtt_pubsub:publish(make_msg(Frame)),
	{ok, State};

process_request(?PUBLISH,
                Frame=#mqtt_frame{
                  fixed = #mqtt_frame_fixed{qos    = ?QOS_1},
                  variable = #mqtt_frame_publish{message_id = MsgId}}, 
				State=#state{socket=Sock}) ->
	emqtt_pubsub:publish(make_msg(Frame)),
	send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{ type = ?PUBACK },
							  variable = #mqtt_frame_publish{ message_id = MsgId}}),
	{ok, State};

process_request(?PUBLISH,
                Frame=#mqtt_frame{
                  fixed = #mqtt_frame_fixed{qos    = ?QOS_2},
                  variable = #mqtt_frame_publish{message_id = MsgId}}, 
				State=#state{socket=Sock}) ->
	emqtt_pubsub:publish(make_msg(Frame)),
	put({msg, MsgId}, pubrec),
	send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?PUBREC},
			  variable = #mqtt_frame_publish{ message_id = MsgId}}),

	{ok, State};

process_request(?PUBACK, #mqtt_frame{}, State) ->
	%TODO: fixme later
	{ok, State};

process_request(?PUBREC, #mqtt_frame{
	variable = #mqtt_frame_publish{message_id = MsgId}}, 
	State=#state{socket=Sock}) ->
	%TODO: fixme later
	send_frame(Sock,
	  #mqtt_frame{fixed    = #mqtt_frame_fixed{ type = ?PUBREL},
				  variable = #mqtt_frame_publish{ message_id = MsgId}}),
	{ok, State};

process_request(?PUBREL,
                #mqtt_frame{
                  variable = #mqtt_frame_publish{message_id = MsgId}},
				  State=#state{socket=Sock}) ->
	erase({msg, MsgId}),
	send_frame(Sock,
	  #mqtt_frame{fixed    = #mqtt_frame_fixed{ type = ?PUBCOMP},
				  variable = #mqtt_frame_publish{ message_id = MsgId}}),
	{ok, State};

process_request(?PUBCOMP, #mqtt_frame{
	variable = #mqtt_frame_publish{message_id = _MsgId}}, State) ->
	%TODO: fixme later
	{ok, State};

process_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{message_id  = MessageId,
                                                   topic_table = Topics},
                  payload = undefined},
                #state{socket=Sock} = State) ->

	[emqtt_pubsub:subscribe({Name, Qos}, self()) || 
			#mqtt_topic{name=Name, qos=Qos} <- Topics],

    GrantedQos = [Qos || #mqtt_topic{qos=Qos} <- Topics],

    send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?SUBACK},
								 variable = #mqtt_frame_suback{
											 message_id = MessageId,
											 qos_table  = GrantedQos}}),

    {ok, State};

process_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{message_id  = MessageId,
                                                   topic_table = Topics },
                  payload = undefined}, #state{socket = Sock, client_id = ClientId} = State) ->

	
	[emqtt_pubsub:unsubscribe(Name, self()) || #mqtt_topic{name=Name} <- Topics], 

    send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?UNSUBACK },
                             	 variable = #mqtt_frame_suback{message_id = MessageId }}),

    {ok, State};

process_request(?PINGREQ, #mqtt_frame{}, #state{socket=Sock, keep_alive=KeepAlive}=State) ->
	%Keep alive timer
	KeepAlive1 = emqtt_keep_alive:reset(KeepAlive),
    send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{ type = ?PINGRESP }}),
    {ok, State#state{keep_alive=KeepAlive1}};

process_request(?DISCONNECT, #mqtt_frame{}, State=#state{client_id=ClientId}) ->
	?INFO("~s disconnected", [ClientId]),
    {stop, State}.

send_frame(Sock, Frame) ->
	?INFO("send frame:~p", [Frame]),
    erlang:port_command(Sock, emqtt_frame:serialise(Frame)).

valid_client_id(ClientId) ->
    ClientIdLen = size(ClientId),
    1 =< ClientIdLen andalso ClientIdLen =< ?CLIENT_ID_MAXLEN.

validate_frame(_Type, _Frame) ->
	ok.

make_msg(#mqtt_frame{
			  fixed = #mqtt_frame_fixed{qos    = Qos,
										retain = Retain,
										dup    = Dup},
			  variable = #mqtt_frame_publish{topic_name = Topic,
											 message_id = MessageId},
			  payload = Payload}) ->
	#mqtt_msg{retain     = Retain,
			  qos        = Qos,
			  topic      = Topic,
			  dup        = Dup,
			  msgid      = MessageId,
			  payload    = Payload}.

make_will_msg(#mqtt_frame_connect{ will_flag   = false }) ->
    undefined;
make_will_msg(#mqtt_frame_connect{ will_retain = Retain,
                                   will_qos    = Qos,
                                   will_topic  = Topic,
                                   will_msg    = Msg }) ->
    #mqtt_msg{retain  = Retain,
              qos     = Qos,
              topic   = Topic,
              dup     = false,
              payload = Msg }.
