%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_coap_session).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_coap.hrl").

%% API
-export([new/0, transfer_result/3]).

-export([ received/3
        , reply/4
        , reply/5
        , ack/3
        , piggyback/4
        , deliver/3
        , timeout/3]).

-export_type([session/0]).

-record(session, { transport_manager :: emqx_coap_tm:manager()
                 , observe_manager :: emqx_coap_observe_res:manager()
                 , next_msg_id :: coap_message_id()
                 }).

-type session() :: #session{}.

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
-spec new() -> session().
new() ->
    _ = emqx_misc:rand_seed(),
    #session{ transport_manager = emqx_coap_tm:new()
            , observe_manager = emqx_coap_observe_res:new_manager()
            , next_msg_id = rand:uniform(?MAX_MESSAGE_ID)}.

%%%-------------------------------------------------------------------
%%% Process Message
%%%-------------------------------------------------------------------
received(#coap_message{type = ack} = Msg, Cfg, Session) ->
    handle_response(Msg, Cfg, Session);

received(#coap_message{type = reset} = Msg, Cfg, Session) ->
    handle_response(Msg, Cfg, Session);

received(#coap_message{method = Method} = Msg, Cfg, Session) when is_atom(Method) ->
    handle_request(Msg, Cfg, Session);

received(Msg, Cfg, Session) ->
    handle_response(Msg, Cfg, Session).

reply(Req, Method, Cfg, Session) ->
    reply(Req, Method, <<>>, Cfg, Session).

reply(Req, Method, Payload, Cfg, Session) ->
    Response = emqx_coap_message:response(Method, Payload, Req),
    handle_out(Response, Cfg, Session).

ack(Req, Cfg, Session) ->
    piggyback(Req, <<>>, Cfg, Session).

piggyback(Req, Payload, Cfg, Session) ->
    Response = emqx_coap_message:ack(Req),
    Response2 = emqx_coap_message:set_payload(Payload, Response),
    handle_out(Response2, Cfg, Session).

deliver(Delivers, Cfg, Session) ->
    Fun = fun({_, Topic, Message},
              #{out := OutAcc,
                session := #session{observe_manager = OM,
                                    next_msg_id = MsgId} = SAcc} = Acc) ->
                  case emqx_coap_observe_res:res_changed(Topic, OM) of
                      undefined ->
                          Acc;
                      {Token, SeqId, OM2} ->
                          Msg = mqtt_to_coap(Message, MsgId, Token, SeqId, Cfg),
                          SAcc2 = SAcc#session{next_msg_id = next_msg_id(MsgId),
                                               observe_manager = OM2},
                          #{out := Out} = Result = call_transport_manager(handle_out, Msg, Cfg, SAcc2),
                          Result#{out := [Out | OutAcc]}
                  end
          end,
    lists:foldl(Fun,
                #{out => [],
                  session => Session},
                Delivers).

timeout(Timer, Cfg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Timer, Cfg, Session).

transfer_result(From, Value, Result) ->
    ?TRANSFER_RESULT([out, subscribe], From, Value, Result).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
handle_request(Msg, Cfg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Cfg, Session).

handle_response(Msg, Cfg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Cfg, Session).

handle_out(Msg, Cfg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Cfg, Session).

call_transport_manager(Fun,
                       Msg,
                       Cfg,
                       #session{transport_manager = TM} = Session) ->
    try
        Result = emqx_coap_tm:Fun(Msg, Cfg, TM),
        {ok, _, Session2} = emqx_misc:pipeline([fun process_tm/2,
                                                fun process_subscribe/2],
                                               Result,
                                               Session),
        emqx_coap_channel:transfer_result(session, Session2, Result)
    catch Type:Reason:Stack ->
            ?ERROR("process transmission with, message:~p failed~n
Type:~p,Reason:~p~n,StackTrace:~p~n", [Msg, Type, Reason, Stack]),
            #{out => emqx_coap_message:response({error, internal_server_error}, Msg)}
    end.

process_tm(#{tm := TM}, Session) ->
    {ok, Session#session{transport_manager = TM}};
process_tm(_, Session) ->
    {ok, Session}.

process_subscribe(#{subscribe := Sub}, #session{observe_manager = OM} =  Session) ->
    case Sub of
        undefined ->
            {ok, Session};
        {Topic, Token} ->
            OM2 = emqx_coap_observe_res:insert(Topic, Token, OM),
            {ok, Session#session{observe_manager = OM2}};
        Topic ->
            OM2 = emqx_coap_observe_res:remove(Topic, OM),
            {ok, Session#session{observe_manager = OM2}}
    end;
process_subscribe(_, Session) ->
    {ok, Session}.

mqtt_to_coap(MQTT, MsgId, Token, SeqId, Cfg) ->
    #message{payload = Payload} = MQTT,
    #coap_message{type = get_notify_type(MQTT, Cfg),
                  method = {ok, content},
                  id = MsgId,
                  token = Token,
                  payload = Payload,
                  options = #{observe => SeqId,
                              max_age => get_max_age(MQTT)}}.

get_notify_type(#message{qos = Qos}, #{notify_type := Type}) ->
    case Type of
        qos ->
            case Qos of
                ?QOS_0 ->
                    non;
                _ ->
                    con
            end;
        Other ->
            Other
    end.

-spec get_max_age(#message{}) -> max_age().
get_max_age(#message{headers = #{properties := #{'Message-Expiry-Interval' := 0}}}) ->
    ?MAXIMUM_MAX_AGE;
get_max_age(#message{headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
                     timestamp = Ts}) ->
    Now = erlang:system_time(millisecond),
    Diff = (Now - Ts + Interval * 1000) / 1000,
    erlang:max(1, erlang:floor(Diff));
get_max_age(_) ->
    ?DEFAULT_MAX_AGE.

next_msg_id(MsgId) when MsgId >= ?MAX_MESSAGE_ID ->
    1;
next_msg_id(MsgId) ->
    MsgId + 1.
