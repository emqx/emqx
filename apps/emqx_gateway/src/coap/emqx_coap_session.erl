%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("src/coap/include/emqx_coap.hrl").

%% API
-export([
    new/0,
    process_subscribe/4
]).

-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    handle_request/2,
    handle_response/2,
    handle_out/2,
    set_reply/2,
    deliver/3,
    timeout/2
]).

-export_type([session/0]).

-record(session, {
    transport_manager :: emqx_coap_tm:manager(),
    observe_manager :: emqx_coap_observe_res:manager(),
    created_at :: pos_integer()
}).

-type session() :: #session{}.

%% steal from emqx_session
-define(INFO_KEYS, [
    subscriptions,
    upgrade_qos,
    retry_interval,
    await_rel_timeout,
    created_at
]).

-define(STATS_KEYS, [
    subscriptions_cnt,
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

-import(emqx_coap_medium, [iter/3]).
-import(emqx_coap_channel, [metrics_inc/2]).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
-spec new() -> session().
new() ->
    _ = emqx_misc:rand_seed(),
    #session{
        transport_manager = emqx_coap_tm:new(),
        observe_manager = emqx_coap_observe_res:new_manager(),
        created_at = erlang:system_time(millisecond)
    }.

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------
%% @doc Compatible with emqx_session
%% do we need use inflight and mqueue in here?
-spec info(session()) -> emqx_types:infos().
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(subscriptions, #session{observe_manager = OM}) ->
    Topics = emqx_coap_observe_res:subscriptions(OM),
    lists:foldl(
        fun(T, Acc) -> Acc#{T => emqx_gateway_utils:default_subopts()} end,
        #{},
        Topics
    );
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
info(next_pkt_id, _) ->
    0;
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
-spec stats(session()) -> emqx_types:stats().
stats(Session) -> info(?STATS_KEYS, Session).

%%%-------------------------------------------------------------------
%%% Process Message
%%%-------------------------------------------------------------------
handle_request(Msg, Session) ->
    call_transport_manager(
        ?FUNCTION_NAME,
        Msg,
        Session
    ).

handle_response(Msg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Session).

handle_out(Msg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Session).

set_reply(Msg, #session{transport_manager = TM} = Session) ->
    TM2 = emqx_coap_tm:set_reply(Msg, TM),
    Session#session{transport_manager = TM2}.

deliver(
    Delivers,
    Ctx,
    #session{
        observe_manager = OM,
        transport_manager = TM
    } = Session
) ->
    Fun = fun({_, Topic, Message}, {OutAcc, OMAcc, TMAcc} = Acc) ->
        case emqx_coap_observe_res:res_changed(Topic, OMAcc) of
            undefined ->
                metrics_inc('delivery.dropped', Ctx),
                metrics_inc('delivery.dropped.no_subid', Ctx),
                Acc;
            {Token, SeqId, OM2} ->
                metrics_inc('messages.delivered', Ctx),
                Msg = mqtt_to_coap(Message, Token, SeqId),
                #{out := Out, tm := TM2} = emqx_coap_tm:handle_out(Msg, TMAcc),
                {Out ++ OutAcc, OM2, TM2}
        end
    end,
    {Outs, OM2, TM2} = lists:foldl(Fun, {[], OM, TM}, lists:reverse(Delivers)),

    #{
        out => lists:reverse(Outs),
        session => Session#session{
            observe_manager = OM2,
            transport_manager = TM2
        }
    }.

timeout(Timer, Session) ->
    call_transport_manager(?FUNCTION_NAME, Timer, Session).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
call_transport_manager(
    Fun,
    Msg,
    #session{transport_manager = TM} = Session
) ->
    Result = emqx_coap_tm:Fun(Msg, TM),
    iter(
        [tm, fun process_tm/4, fun process_session/3],
        Result,
        Session
    ).

process_tm(TM, Result, Session, Cursor) ->
    iter(Cursor, Result, Session#session{transport_manager = TM}).

process_session(_, Result, Session) ->
    Result#{session => Session}.

process_subscribe(
    Sub,
    Msg,
    Result,
    #session{observe_manager = OM} = Session
) ->
    case Sub of
        undefined ->
            Result;
        {Topic, Token} ->
            {SeqId, OM2} = emqx_coap_observe_res:insert(Topic, Token, OM),
            Replay = emqx_coap_message:piggyback({ok, content}, Msg),
            Replay2 = Replay#coap_message{options = #{observe => SeqId}},
            Result#{
                reply => Replay2,
                session => Session#session{observe_manager = OM2}
            };
        Topic ->
            OM2 = emqx_coap_observe_res:remove(Topic, OM),
            Replay = emqx_coap_message:piggyback({ok, nocontent}, Msg),
            Result#{
                reply => Replay,
                session => Session#session{observe_manager = OM2}
            }
    end.

mqtt_to_coap(MQTT, Token, SeqId) ->
    #message{payload = Payload} = MQTT,
    #coap_message{
        type = get_notify_type(MQTT),
        method = {ok, content},
        token = Token,
        payload = Payload,
        options = #{observe => SeqId}
    }.

get_notify_type(#message{qos = Qos}) ->
    case emqx_conf:get([gateway, coap, notify_qos], non) of
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
