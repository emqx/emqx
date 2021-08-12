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
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

%% API
-export([new/0, transfer_result/3]).

-export([ info/1
        , info/2
        , stats/1
        ]).

-export([ handle_request/3
        , handle_response/3
        , handle_out/3
        , deliver/3
        , timeout/3]).

-export_type([session/0]).

-record(session, { transport_manager :: emqx_coap_tm:manager()
                 , observe_manager :: emqx_coap_observe_res:manager()
                 , next_msg_id :: coap_message_id()
                 , created_at :: pos_integer()
                 }).

-type session() :: #session{}.

%% steal from emqx_session
-define(INFO_KEYS, [subscriptions,
                    upgrade_qos,
                    retry_interval,
                    await_rel_timeout,
                    created_at
                   ]).

-define(STATS_KEYS, [subscriptions_cnt,
                     subscriptions_max,
                     inflight_cnt,
                     inflight_max,
                     mqueue_len,
                     mqueue_max,
                     mqueue_dropped,
                     next_pkt_id,
                     awaiting_rel_cnt,
                     awaiting_rel_max
                    ]).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
-spec new() -> session().
new() ->
    _ = emqx_misc:rand_seed(),
    #session{ transport_manager = emqx_coap_tm:new()
            , observe_manager = emqx_coap_observe_res:new_manager()
            , next_msg_id = rand:uniform(?MAX_MESSAGE_ID)
            , created_at = erlang:system_time(millisecond)}.

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------
%% @doc Compatible with emqx_session
%% do we need use inflight and mqueue in here?
-spec(info(session()) -> emqx_types:infos()).
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(subscriptions, #session{observe_manager = OM}) ->
    emqx_coap_observe_res:subscriptions(OM);
info(subscriptions_cnt, #session{observe_manager = OM}) ->
    erlang:length(emqx_coap_observe_res:subscriptions(OM));
info(subscriptions_max, _) ->
    infinity;
info(upgrade_qos, _) ->
    ?QOS_0;
info(inflight, _) ->
    emqx_inflight:new();
info(inflight_cnt, _) ->
    0;
info(inflight_max, _) ->
    0;
info(retry_interval, _) ->
    infinity;
info(mqueue, _) ->
    emqx_mqueue:init(#{max_len => 0, store_qos0 => false});
info(mqueue_len, #session{transport_manager = TM}) ->
    maps:size(TM);
info(mqueue_max, _) ->
    0;
info(mqueue_dropped, _) ->
    0;
info(next_pkt_id, #session{next_msg_id = PacketId}) ->
    PacketId;
info(awaiting_rel, _) ->
    #{};
info(awaiting_rel_cnt, _) ->
    0;
info(awaiting_rel_max, _) ->
    infinity;
info(await_rel_timeout, _) ->
    infinity;
info(created_at, #session{created_at = CreatedAt}) ->
    CreatedAt.

%% @doc Get stats of the session.
-spec(stats(session()) -> emqx_types:stats()).
stats(Session) -> info(?STATS_KEYS, Session).

%%%-------------------------------------------------------------------
%%% Process Message
%%%-------------------------------------------------------------------
handle_request(Msg, Ctx, Session) ->
    call_transport_manager(?FUNCTION_NAME,
                           Msg,
                           Ctx,
                           [fun process_tm/3, fun process_subscribe/3],
                           Session).

handle_response(Msg, Ctx, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Ctx, [fun process_tm/3], Session).

handle_out(Msg, Ctx, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Ctx, [fun process_tm/3], Session).

deliver(Delivers, Ctx, Session) ->
    Fun = fun({_, Topic, Message},
              #{out := OutAcc,
                session := #session{observe_manager = OM,
                                    next_msg_id = MsgId,
                                    transport_manager = TM} = SAcc} = Acc) ->
                  case emqx_coap_observe_res:res_changed(Topic, OM) of
                      undefined ->
                          Acc;
                      {Token, SeqId, OM2} ->
                          Msg = mqtt_to_coap(Message, MsgId, Token, SeqId, Ctx),
                          SAcc2 = SAcc#session{next_msg_id = next_msg_id(MsgId, TM),
                                               observe_manager = OM2},
                          #{out := Out} = Result = handle_out(Msg, Ctx, SAcc2),
                          Result#{out := [Out | OutAcc]}
                  end
          end,
    lists:foldl(Fun,
                #{out => [], session => Session},
                lists:reverse(Delivers)).

timeout(Timer, Ctx, Session) ->
    call_transport_manager(?FUNCTION_NAME, Timer, Ctx, [fun process_tm/3], Session).

result_keys() ->
    [tm, subscribe] ++ emqx_coap_channel:result_keys().

transfer_result(From, Value, Result) ->
    ?TRANSFER_RESULT(From, Value, Result).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
call_transport_manager(Fun,
                       Msg,
                       Ctx,
                       Processor,
                       #session{transport_manager = TM} = Session) ->
    try
        Result = emqx_coap_tm:Fun(Msg, Ctx, TM),
        {ok, Result2, Session2} = pipeline(Processor,
                                           Result,
                                           Msg,
                                           Session),
        emqx_coap_channel:transfer_result(session, Session2, Result2)
    catch Type:Reason:Stack ->
            ?ERROR("process transmission with, message:~p failed~nType:~p,Reason:~p~n,StackTrace:~p~n",
                   [Msg, Type, Reason, Stack]),
            ?REPLY({error, internal_server_error}, Msg)
    end.

process_tm(#{tm := TM}, _, Session) ->
    {ok, Session#session{transport_manager = TM}};
process_tm(_, _, Session) ->
    {ok, Session}.

process_subscribe(#{subscribe := Sub} = Result,
                  Msg,
                  #session{observe_manager = OM} =  Session) ->
    case Sub of
        undefined ->
            {ok, Result, Session};
        {Topic, Token} ->
            {SeqId, OM2} = emqx_coap_observe_res:insert(Topic, Token, OM),
            Replay = emqx_coap_message:piggyback({ok, content}, Msg),
            Replay2 = Replay#coap_message{options = #{observe => SeqId}},
            {ok, Result#{reply => Replay2}, Session#session{observe_manager = OM2}};
        Topic ->
            OM2 = emqx_coap_observe_res:remove(Topic, OM),
            Replay = emqx_coap_message:piggyback({ok, nocontent}, Msg),
            {ok, Result#{reply => Replay}, Session#session{observe_manager = OM2}}
    end;
process_subscribe(Result, _, Session) ->
    {ok, Result, Session}.

mqtt_to_coap(MQTT, MsgId, Token, SeqId, Ctx) ->
    #message{payload = Payload} = MQTT,
    #coap_message{type = get_notify_type(MQTT, Ctx),
                  method = {ok, content},
                  id = MsgId,
                  token = Token,
                  payload = Payload,
                  options = #{observe => SeqId}}.

get_notify_type(#message{qos = Qos}, Ctx) ->
    case emqx_coap_channel:get_config(notify_type, Ctx) of
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

next_msg_id(MsgId, TM) ->
    next_msg_id(MsgId + 1, MsgId, TM).

next_msg_id(MsgId, MsgId, _) ->
    erlang:throw("too many message in delivering");
next_msg_id(MsgId, BeginId, TM) when MsgId >= ?MAX_MESSAGE_ID ->
    check_is_inused(1, BeginId, TM);
next_msg_id(MsgId, BeginId, TM) ->
    check_is_inused(MsgId, BeginId, TM).

check_is_inused(NewMsgId, BeginId, TM) ->
    case emqx_coap_tm:is_inused(out, NewMsgId, TM) of
        false ->
            NewMsgId;
        _ ->
            next_msg_id(NewMsgId + 1, BeginId, TM)
    end.

pipeline([Fun | T], Result, Msg, Session) ->
    case Fun(Result, Msg, Session) of
        {ok, Session2} ->
            pipeline(T, Result, Msg, Session2);
        {ok, Result2, Session2} ->
            pipeline(T, Result2, Msg, Session2)
    end;

pipeline([], Result, _, Session) ->
    {ok, Result, Session}.
