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

-record(proto_state, {
		socket,
		message_id,
		client_id,
		clean_sess,
		will_msg,
		awaiting_ack,
		subtopics,
		awaiting_rel
}).

-type proto_state() :: #proto_state{}.

-export([initial_state/1, handle_frame/2, send_message/2, client_terminated/1]).

-export([info/1]).

-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).

initial_state(Socket) ->
	#proto_state{
		socket			= Socket,
		message_id		= 1,
		awaiting_ack	= gb_trees:empty(),
		subtopics		= [],
		awaiting_rel	= gb_trees:empty()
	}. 

info(#proto_state{ message_id	= MsgId,
				   client_id	= ClientId,
				   clean_sess	= CleanSess,
				   will_msg		= WillMsg,
				   subtopics	= SubTopics}) ->
	[ {message_id, MsgId},
	  {client_id, ClientId},
	  {clean_sess, CleanSess},
	  {will_msg, WillMsg},
	  {subtopics, SubTopics} ].

-spec handle_frame(Frame, State) -> {ok, NewState} | {error, any()} when 
	Frame :: #mqtt_frame{}, 
	State :: proto_state(),
	NewState :: proto_state().

handle_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}, 
				State = #proto_state{client_id = ClientId}) ->
	?INFO("frame from ~s: ~p", [ClientId, Frame]),
	case validate_frame(Type, Frame) of	
	ok ->
		handle_request(Type, Frame, State);
	{error, Reason} ->
		{error, Reason, State}
	end.

handle_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                          username   = Username,
                                          password   = Password,
                                          proto_ver  = ProtoVersion,
                                          clean_sess = CleanSess,
										  keep_alive = AlivePeriod,
                                          client_id  = ClientId } = Var}, State = #proto_state{socket = Sock}) ->
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
						%%TODO: 
						%%KeepAlive = emqtt_keep_alive:new(AlivePeriod*1500, keep_alive_timeout),
						emqtt_cm:create(ClientId, self()),
						{?CONNACK_ACCEPT,
						 State #proto_state{ will_msg   = make_will_msg(Var),
											 client_id  = ClientId }}
                end
        end,
		?INFO("recv conn...:~p", [ReturnCode]),
		send_frame(Sock, #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?CONNACK},
								 variable = #mqtt_frame_connack{
                                         return_code = ReturnCode }}),
    {ok, State1};

handle_request(?PUBLISH, Frame=#mqtt_frame{
									fixed = #mqtt_frame_fixed{qos = ?QOS_0}}, State) ->
	emqtt_router:route(make_msg(Frame)),
	{ok, State};

handle_request(?PUBLISH,
                Frame=#mqtt_frame{
                  fixed = #mqtt_frame_fixed{qos    = ?QOS_1},
                  variable = #mqtt_frame_publish{message_id = MsgId}}, 
				State=#proto_state{socket=Sock}) ->
	emqtt_pubsub:publish(make_msg(Frame)),
	send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{ type = ?PUBACK },
							  variable = #mqtt_frame_publish{ message_id = MsgId}}),
	{ok, State};

handle_request(?PUBLISH,
                Frame=#mqtt_frame{
                  fixed = #mqtt_frame_fixed{qos    = ?QOS_2},
                  variable = #mqtt_frame_publish{message_id = MsgId}}, 
				State=#proto_state{socket=Sock}) ->
	emqtt_pubsub:publish(make_msg(Frame)),
	put({msg, MsgId}, pubrec),
	send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?PUBREC},
			  variable = #mqtt_frame_publish{ message_id = MsgId}}),

	{ok, State};

handle_request(?PUBACK, #mqtt_frame{}, State) ->
	%TODO: fixme later
	{ok, State};

handle_request(?PUBREC, #mqtt_frame{
	variable = #mqtt_frame_publish{message_id = MsgId}}, 
	State=#proto_state{socket=Sock}) ->
	%TODO: fixme later
	send_frame(Sock,
	  #mqtt_frame{fixed    = #mqtt_frame_fixed{ type = ?PUBREL},
				  variable = #mqtt_frame_publish{ message_id = MsgId}}),
	{ok, State};

handle_request(?PUBREL,
                #mqtt_frame{
                  variable = #mqtt_frame_publish{message_id = MsgId}},
				  State=#proto_state{socket=Sock}) ->
	erase({msg, MsgId}),
	send_frame(Sock,
	  #mqtt_frame{fixed    = #mqtt_frame_fixed{ type = ?PUBCOMP},
				  variable = #mqtt_frame_publish{ message_id = MsgId}}),
	{ok, State};

handle_request(?PUBCOMP, #mqtt_frame{
	variable = #mqtt_frame_publish{message_id = _MsgId}}, State) ->
	%TODO: fixme later
	{ok, State};

handle_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{message_id  = MessageId,
                                                   topic_table = Topics},
                  payload = undefined},
                #proto_state{socket=Sock} = State) ->

	[emqtt_pubsub:subscribe({Name, Qos}, self()) || 
			#mqtt_topic{name=Name, qos=Qos} <- Topics],

    GrantedQos = [Qos || #mqtt_topic{qos=Qos} <- Topics],

    send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?SUBACK},
								 variable = #mqtt_frame_suback{
											 message_id = MessageId,
											 qos_table  = GrantedQos}}),

    {ok, State};

handle_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{message_id  = MessageId,
                                                   topic_table = Topics },
                  payload = undefined}, #proto_state{socket = Sock, client_id = ClientId} = State) ->

	
	[emqtt_pubsub:unsubscribe(Name, self()) || #mqtt_topic{name=Name} <- Topics], 

    send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?UNSUBACK },
                             	 variable = #mqtt_frame_suback{message_id = MessageId }}),

    {ok, State};

%, keep_alive=KeepAlive
handle_request(?PINGREQ, #mqtt_frame{}, #proto_state{socket=Sock}=State) ->
	%Keep alive timer
	%%TODO:...
	%%KeepAlive1 = emqtt_keep_alive:reset(KeepAlive),
    send_frame(Sock, #mqtt_frame{fixed = #mqtt_frame_fixed{ type = ?PINGRESP }}),
    {ok, State};

handle_request(?DISCONNECT, #mqtt_frame{}, State=#proto_state{client_id=ClientId}) ->
	?INFO("~s disconnected", [ClientId]),
    {stop, State}.

-spec send_message(Message, State) -> {ok, NewState} when
	Message 	:: mqtt_msg(),
	State		:: proto_state(),
	NewState	:: proto_state().

send_message(Message, State = #proto_state{socket = Sock, message_id = MsgId}) ->

	#mqtt_msg{retain     = Retain,
			  qos        = Qos,
			  topic      = Topic,
			  dup        = Dup,
			  payload    = Payload,
			  encoder 	 = Encoder} = Message,
	
	Payload1 = 
	if
	Encoder == undefined -> Payload;
	true -> Encoder(Payload)
	end,
	Frame = #mqtt_frame{
		fixed = #mqtt_frame_fixed{type 	 = ?PUBLISH,
								  qos    = Qos,
								  retain = Retain,
								  dup    = Dup},
		variable = #mqtt_frame_publish{topic_name = Topic,
									   message_id = if
													Qos == ?QOS_0 -> undefined;
													true -> MsgId
													end},
		payload = Payload1},

	send_frame(Sock, Frame),
	if
	Qos == ?QOS_0 ->
		{ok, State};
	true ->
		{ok, next_msg_id(State)}
	end.

send_frame(Sock, Frame) ->
	?INFO("send frame:~p", [Frame]),
    erlang:port_command(Sock, emqtt_frame:serialise(Frame)).

%%TODO: fix me later...
client_terminated(#proto_state{client_id = ClientId} = State) ->
    emqtt_cm:destroy(ClientId, self()).

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

next_msg_id(State = #proto_state{ message_id = 16#ffff }) ->
    State #proto_state{ message_id = 1 };
next_msg_id(State = #proto_state{ message_id = MsgId }) ->
    State #proto_state{ message_id = MsgId + 1 }.

valid_client_id(ClientId) ->
    ClientIdLen = size(ClientId),
    1 =< ClientIdLen andalso ClientIdLen =< ?CLIENT_ID_MAXLEN.

validate_frame(?PUBLISH, #mqtt_frame{variable = #mqtt_frame_publish{topic_name = Topic}}) ->
	case emqtt_topic:validate({publish, Topic}) of
	true -> ok;
	false -> {error, badtopic}
	end;

validate_frame(?UNSUBSCRIBE, #mqtt_frame{variable = #mqtt_frame_subscribe{topic_table = Topics}}) ->
	ErrTopics = [Topic || #mqtt_topic{name=Topic, qos=Qos} <- Topics,
						not emqtt_topic:validate({subscribe, Topic})],
	case ErrTopics of
	[] -> ok;
	_ -> ?ERROR("error topics: ~p", [ErrTopics]), {error, badtopic}
	end;

validate_frame(?SUBSCRIBE, #mqtt_frame{variable = #mqtt_frame_subscribe{topic_table = Topics}}) ->
	ErrTopics = [Topic || #mqtt_topic{name=Topic, qos=Qos} <- Topics,
						not (emqtt_topic:validate({subscribe, Topic}) and (Qos < 3))],
	case ErrTopics of
	[] -> ok;
	_ -> ?ERROR("error topics: ~p", [ErrTopics]), {error, badtopic}
	end;

validate_frame(_Type, _Frame) ->
	ok.

maybe_clean_sess(false, _Conn, _ClientId) ->
    % todo: establish subscription to deliver old unacknowledged messages
    ok.

%%----------------------------------------------------------------------------

send_will_msg(#proto_state{will_msg = undefined}) ->
	ignore;
send_will_msg(#proto_state{will_msg = WillMsg }) ->
	emqtt_pubsub:publish(WillMsg).
