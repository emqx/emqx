%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

%%Don't send amqp message

-module(emqtt_processor).

-export([info/2, initial_state/1,
         process_frame/2, send_will/1]).

-include("emqtt.hrl").
-include("emqtt_frame.hrl").

-record(proc_state, { socket,
                      subscriptions,
                      consumer_tags,
                      unacked_pubs,
                      awaiting_ack,
                      awaiting_seqno,
                      message_id,
                      client_id,
                      clean_sess,
                      will_msg,
                      channels,
                      connection,
                      exchange }).

-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).

initial_state(Socket) ->
    #proc_state{ unacked_pubs  = gb_trees:empty(),
                 awaiting_ack  = gb_trees:empty(),
                 message_id    = 1,
                 subscriptions = dict:new(),
                 consumer_tags = {undefined, undefined},
                 channels      = {undefined, undefined},
                 exchange      = emqtt_util:env(exchange),
                 socket        = Socket }.

info(client_id, #proc_state{ client_id = ClientId }) -> ClientId.

process_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState = #proc_state{ connection = undefined } )
  when Type =/= ?CONNECT ->
    {err, connect_expected, PState};

process_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState ) ->
    process_request(Type, Frame, PState).

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                          username   = Username,
                                          password   = Password,
                                          proto_ver  = ProtoVersion,
                                          clean_sess = CleanSess,
                                          client_id  = ClientId } = Var}, PState) ->
    {ReturnCode, PState1} =
        case {ProtoVersion =:= ?MQTT_PROTO_MAJOR,
              emqtt_util:valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, PState};
            {_, false} ->
                {?CONNACK_INVALID_ID, PState};
            _ ->
                case creds(Username, Password) of
                    nocreds ->
                        error_logger:error_msg("MQTT login failed - no credentials~n"),
                        {?CONNACK_CREDENTIALS, PState};
                    {UserBin, PassBin} ->
						{?CONNACK_ACCEPT,
						 PState #proc_state{ will_msg   = make_will_msg(Var),
											 client_id  = ClientId }}
                end
        end,
		send_client(#mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?CONNACK},
								 variable = #mqtt_frame_connack{
                                         return_code = ReturnCode }}, PState1),
    {ok, PState1};

process_request(?PUBACK,
                #mqtt_frame{
                  variable = #mqtt_frame_publish{ message_id = MessageId }},
                #proc_state{awaiting_ack = Awaiting } = PState) ->
    {ok, PState #proc_state{ awaiting_ack = gb_trees:delete( MessageId, Awaiting)}};

process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos = ?QOS_2 }}, PState) ->
    {err, qos2_not_supported, PState};

process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos    = Qos,
                                             retain = Retain,
                                             dup    = Dup },
                  variable = #mqtt_frame_publish{ topic_name = Topic,
                                                  message_id = MessageId },
                  payload = Payload }, PState) ->
	Msg =  #mqtt_msg{ retain     = Retain,
					 qos        = Qos,
					 topic      = Topic,
					 dup        = Dup,
					 message_id = MessageId,
					 payload    = Payload },
	emqtt_router:route(Msg),

	send_client(
	  #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
				   variable = #mqtt_frame_publish{ message_id = MessageId }},
              PState),
    {ok, PState};


process_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined },
                #proc_state{} = PState0) ->
    QosResponse =
	lists:foldl(fun (#mqtt_topic{ name = TopicName,
								   qos  = Qos }, QosList) ->
				   SupportedQos = supported_subs_qos(Qos),
				   [SupportedQos | QosList]
			   end, [], Topics),

	[emqtt_topic:insert(Name) || #mqtt_topic{name=Name} <- Topics],

	[emqtt_router:insert(#subscriber{topic=Name, pid=self()}) 
				|| #mqtt_topic{name=Name} <- Topics],
	
    send_client(#mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?SUBACK },
                             variable = #mqtt_frame_suback{
                                         message_id = MessageId,
                                         qos_table  = QosResponse }}, PState0),

    {ok, PState0};

process_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined }, #proc_state{ client_id     = ClientId,
                                                      subscriptions = Subs0} = PState) ->

	
	[emqtt_router:delete(#subscriber{topic=Name, pid=self()}) 
		|| #mqtt_topic{name=Name} <- Topics],
	
    send_client(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type       = ?UNSUBACK },
                             variable = #mqtt_frame_suback{ message_id = MessageId }},
                PState),

    {ok, PState #proc_state{ subscriptions = Subs0 }};

process_request(?PINGREQ, #mqtt_frame{}, PState) ->
    send_client(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PINGRESP }},
                PState),
    {ok, PState};

process_request(?DISCONNECT, #mqtt_frame{}, PState) ->
    {stop, PState}.



next_msg_id(PState = #proc_state{ message_id = 16#ffff }) ->
    PState #proc_state{ message_id = 1 };
next_msg_id(PState = #proc_state{ message_id = MsgId }) ->
    PState #proc_state{ message_id = MsgId + 1 }.

%% decide at which qos level to deliver based on subscription
%% and the message publish qos level. non-MQTT publishes are
%% assumed to be qos 1, regardless of delivery_mode.
delivery_qos(Tag, _Headers,  #proc_state{ consumer_tags = {Tag, _} }) ->
    {?QOS_0, ?QOS_0};
delivery_qos(Tag, Headers,   #proc_state{ consumer_tags = {_, Tag} }) ->
    case emqtt_util:table_lookup(Headers, <<"x-mqtt-publish-qos">>) of
        {byte, Qos} -> {lists:min([Qos, ?QOS_1]), ?QOS_1};
        undefined   -> {?QOS_1, ?QOS_1}
    end.

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

send_will(PState = #proc_state{ will_msg = WillMsg }) ->
	error_logger:info_msg("willmsg: ~p~n", [WillMsg]).


send_client(Frame, #proc_state{ socket = Sock }) ->
    erlang:port_command(Sock, emqtt_frame:serialise(Frame)).

