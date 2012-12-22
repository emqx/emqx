-module(emqtt_client).

-behaviour(gen_server2).

-export([start_link/0, go/2, info/1]).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
        code_change/3,
		terminate/2]).

-include("emqtt.hrl").
-include("emqtt_frame.hrl").

-define(CLIENT_ID_MAXLEN, 23).

-record(state, 	{ socket,
				  conn_name,
				  await_recv,
				  connection_state,
				  conserve,
				  parse_state,
                  message_id,
                  client_id,
                  clean_sess,
                  will_msg,
				 awaiting_ack,
                 subscriptions
			 }).

-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).

start_link() ->
    gen_server2:start_link(?MODULE, [], []).

go(Pid, Sock) ->
	gen_server2:call(Pid, {go, Sock}).

info(Pid) ->
	gen_server2:call(Pid, info).

init([]) ->
    {ok, undefined, hibernate, {backoff, 1000, 1000, 10000}}.

handle_call(info, _From, #state{conn_name=ConnName, 
	message_id=MsgId, client_id=ClientId} = State) ->
	Info = [{conn_name, ConnName},
			{message_id, MsgId},
			{client_id, ClientId}],
	{reply, Info, State};

handle_call({go, Sock}, _From, _State) ->
    process_flag(trap_exit, true),
    ok = throw_on_error(
           inet_error, fun () -> emqtt_net:tune_buffer_size(Sock) end),
    {ok, ConnStr} = emqtt_net:connection_string(Sock, inbound),
    error_logger:info_msg("accepting MQTT connection (~s)~n", [ConnStr]),
    {reply, ok, 
	  control_throttle(
       #state{ socket           = Sock,
               conn_name        = ConnStr,
               await_recv       = false,
               connection_state = running,
               conserve         = false,
               parse_state      = emqtt_frame:initial_state(),
			   message_id		= 1,
               subscriptions 	= dict:new(),
			   awaiting_ack		= gb_trees:empty()})}.

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info({route, Msg}, #state{socket = Sock} = State) ->
	#mqtt_msg{ retain     = Retain,
		qos        = Qos,
		topic      = Topic,
		dup        = Dup,
		%message_id = MessageId,
		payload    = Payload } = Msg,
	
	Frame = #mqtt_frame{fixed = #mqtt_frame_fixed{ 
							type = ?PUBLISH,
							qos    = Qos,
							retain = Retain,
							dup    = Dup },
						variable = #mqtt_frame_publish{ 
								topic_name = Topic,
								message_id = 1},
						payload = Payload },

	send_frame(Sock, Frame),
    {noreply, State};

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}}, #state{ socket = Sock }=State) ->
    process_received_bytes(
      Data, control_throttle(State #state{ await_recv = false }));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
	error_logger:info_msg("sock error: ~p~n", [Reason]), 
	{noreply, State};

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
throw_on_error(E, Thunk) ->
    case Thunk() of
	{error, Reason} -> throw({E, Reason});
	{ok, Res}       -> Res;
	Res             -> Res
    end.

async_recv(Sock, Length, infinity) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, -1);

async_recv(Sock, Length, Timeout) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, Timeout).

process_received_bytes(<<>>, State) ->
    {noreply, State};
process_received_bytes(Bytes,
                       State = #state{ parse_state = ParseState,
                                       conn_name   = ConnStr }) ->
    case
        emqtt_frame:parse(Bytes, ParseState) of
            {more, ParseState1} ->
                {noreply,
                 control_throttle( State #state{ parse_state = ParseState1 }),
                 hibernate};
            {ok, Frame, Rest} ->
                case process_frame(Frame, State) of
                    {ok, State1} ->
                        PS = emqtt_frame:initial_state(),
                        process_received_bytes(
                          Rest,
                          State1 #state{ parse_state = PS});
                    {err, Reason, State1} ->
                        ?ERROR("MQTT protocol error ~p for connection ~p~n",
                                  [Reason, ConnStr]),
                        stop({shutdown, Reason}, State1);
                    {stop, State1} ->
                        stop(normal, State1)
                end;
            {error, Error} ->
                ?ERROR("MQTT detected framing error ~p for connection ~p~n",
                           [ConnStr, Error]),
                stop({shutdown, Error}, State)
    end.

process_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              State ) ->
	?INFO("~p", [Frame]),
    process_request(Type, Frame, State).

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                          username   = Username,
                                          password   = Password,
                                          proto_ver  = ProtoVersion,
                                          clean_sess = CleanSess,
                                          client_id  = ClientId } = Var}, #state{socket = Sock} = State) ->
    {ReturnCode, State1} =
        case {ProtoVersion =:= ?MQTT_PROTO_MAJOR,
              valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, State};
            {_, false} ->
                {?CONNACK_INVALID_ID, State};
            _ ->
                case emqtt_auth:check(Username, Password) of
                    false ->
                        error_logger:error_msg("MQTT login failed - no credentials~n"),
                        {?CONNACK_CREDENTIALS, State};
                    true ->
						{?CONNACK_ACCEPT,
						 State #state{ will_msg   = make_will_msg(Var),
											 client_id  = ClientId }}
                end
        end,
		send_frame(Sock, #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?CONNACK},
								 variable = #mqtt_frame_connack{
                                         return_code = ReturnCode }}),
    {ok, State1};

process_request(?PUBACK,
                #mqtt_frame{
                  variable = #mqtt_frame_publish{ message_id = MessageId }},
                #state{awaiting_ack = Awaiting } = State) ->
    {ok, State #state{ awaiting_ack = gb_trees:delete( MessageId, Awaiting)}};

process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos = ?QOS_2 }}, State) ->
    {err, qos2_not_supported, State};

process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos    = Qos,
                                             retain = Retain,
                                             dup    = Dup },
                  variable = #mqtt_frame_publish{ topic_name = Topic,
                                                  message_id = MessageId },
                  payload = Payload }, #state{ socket=Sock, message_id = MsgId } = State) ->
	Msg =  #mqtt_msg{ retain     = Retain,
					 qos        = Qos,
					 topic      = Topic,
					 dup        = Dup,
					 message_id = MessageId,
					 payload    = Payload },
	emqtt_router:publish(Topic, Msg),
	
	send_frame(Sock,
	  #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
				   variable = #mqtt_frame_publish{ message_id = MsgId}}),
    {ok, State};

process_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined },
                #state{socket=Sock} = State0) ->
    QosResponse =
	lists:foldl(fun (#mqtt_topic{ name = TopicName,
								   qos  = Qos }, QosList) ->
				   SupportedQos = supported_subs_qos(Qos),
				   [SupportedQos | QosList]
			   end, [], Topics),

	[emqtt_router:subscribe(Name, self()) || #mqtt_topic{name=Name} <- Topics],

    send_frame(Sock, #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?SUBACK },
                             variable = #mqtt_frame_suback{
                                         message_id = MessageId,
                                         qos_table  = QosResponse }}),

    {ok, State0};

process_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined }, #state{ socket = Sock, client_id     = ClientId,
                                                      subscriptions = Subs0} = State) ->

	
	[emqtt_router:unsubscribe(Name, self()) || #mqtt_topic{name=Name} <- Topics],
	
    send_frame(Sock, #mqtt_frame{ fixed    = #mqtt_frame_fixed { type       = ?UNSUBACK },
                             variable = #mqtt_frame_suback{ message_id = MessageId }}),

    {ok, State #state{ subscriptions = Subs0 }};

process_request(?PINGREQ, #mqtt_frame{}, #state{socket=Sock}=State) ->
    send_frame(Sock, #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PINGRESP }}),
    {ok, State};

process_request(?DISCONNECT, #mqtt_frame{}, State) ->
    {stop, State}.

next_msg_id(State = #state{ message_id = 16#ffff }) ->
    State #state{ message_id = 1 };
next_msg_id(State = #state{ message_id = MsgId }) ->
    State #state{ message_id = MsgId + 1 }.

maybe_clean_sess(false, _Conn, _ClientId) ->
    % todo: establish subscription to deliver old unacknowledged messages
    ok.

%%----------------------------------------------------------------------------

make_will_msg(#mqtt_frame_connect{ will_flag   = false }) ->
    undefined;
make_will_msg(#mqtt_frame_connect{ will_retain = Retain,
                                   will_qos    = Qos,
                                   will_topic  = Topic,
                                   will_msg    = Msg }) ->
    #mqtt_msg{ retain  = Retain,
               qos     = Qos,
               topic   = Topic,
               dup     = false,
               payload = Msg }.

creds(User, Pass) ->
	{User, Pass}.

supported_subs_qos(?QOS_0) -> ?QOS_0;
supported_subs_qos(?QOS_1) -> ?QOS_1;
supported_subs_qos(?QOS_2) -> ?QOS_1.

send_will(#state{ will_msg = WillMsg }) ->
	?INFO("willmsg: ~p~n", [WillMsg]).

send_frame(Sock, Frame) ->
    erlang:port_command(Sock, emqtt_frame:serialise(Frame)).

%%----------------------------------------------------------------------------
network_error(_Reason,
              State = #state{ conn_name  = ConnStr}) ->
    ?INFO("MQTT detected network error for ~p~n", [ConnStr]),
    send_will(State),
    % todo: flush channel after publish
    stop({shutdown, conn_closed}, State).

run_socket(State = #state{ connection_state = blocked }) ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    async_recv(Sock, 0, infinity),
    State#state{ await_recv = true }.

control_throttle(State = #state{ connection_state = Flow,
                                 conserve         = Conserve }) ->
    case {Flow, Conserve} of
        {running,   true} -> State #state{ connection_state = blocked };
        {blocked,  false} -> run_socket(State #state{
                                                connection_state = running });
        {_,            _} -> run_socket(State)
    end.

stop(Reason, State ) ->
    {stop, Reason, State}.

valid_client_id(ClientId) ->
    ClientIdLen = size(ClientId),
    1 =< ClientIdLen andalso ClientIdLen =< ?CLIENT_ID_MAXLEN.

