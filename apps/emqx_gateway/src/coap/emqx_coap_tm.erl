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

%% transport manager
-module(emqx_coap_tm).

-export([ new/0
        , handle_request/3
        , handle_response/3
        , handle_out/3
        , timeout/3]).

-export_type([manager/0, event_result/2]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

-type direction() :: in | out.
-type transport_id() :: {direction(), non_neg_integer()}.

-record(transport, { id :: transport_id()
                   , state :: atom()
                   , timers :: maps:map()
                   , data :: any()}).
-type transport() :: #transport{}.

-type message_id() :: 0 .. ?MAX_MESSAGE_ID.

-type manager() :: #{message_id() => transport()}.

-type ttimeout() :: {state_timeout, pos_integer(), any()}
                  | {stop_timeout, pos_integer()}.

-type topic() :: binary().
-type token() :: binary().
-type sub_register() :: {topic(), token()} | topic().

-type event_result(State, Data) ::
        #{next => State,
          outgoing => emqx_coap_message(),
          timeouts => list(ttimeout()),
          has_sub  => undefined | sub_register(),
          data => Data}.

%%%===================================================================
%%% API
%%%===================================================================
new() ->
    #{}.

handle_request(#coap_message{id = MsgId} = Msg, TM, Cfg) ->
    Id = {in, MsgId},
    case maps:get(Id, TM, undefined) of
                 undefined ->
            Data = emqx_coap_transport:new(),
            Transport = new_transport(Id, Data),
            process_event(in, Msg, TM, Transport, Cfg);
        TP ->
            process_event(in, Msg, TM, TP, Cfg)
    end.

handle_response(#coap_message{type = Type, id = MsgId} = Msg, TM, Cfg) ->
    Id = {out, MsgId},
    case maps:get(Id, TM, undefined) of
        undefined ->
            case Type of
                reset ->
                    ?EMPTY_RESULT;
                _ ->
                    #{out => #coap_message{type = reset,
                                           id = MsgId}}
            end;
        TP ->
            process_event(in, Msg, TM, TP, Cfg)
    end.

handle_out(#coap_message{id = MsgId} = Msg, TM, Cfg) ->
    Id = {out, MsgId},
    case maps:get(Id, TM, undefined) of
        undefined ->
            Data = emqx_coap_transport:new(),
            Transport = new_transport(Id, Data),
            process_event(out, Msg, TM, Transport, Cfg);
        _ ->
            ?WARN("Repeat sending message with id:~p~n", [Id]),
            ?EMPTY_RESULT
    end.

timeout({Id, Type, Msg}, TM, Cfg) ->
    case maps:get(Id, TM, undefined) of
        undefined ->
            ?EMPTY_RESULT;
        #transport{timers = Timers} = TP ->
            %% maybe timer has been canceled
            case maps:is_key(Type, Timers) of
                true ->
                    process_event(Type, Msg, TM, TP, Cfg);
                _ ->
                    ?EMPTY_RESULT
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
new_transport(Id, Data) ->
    #transport{id = Id,
               state = idle,
               timers = #{},
               data = Data}.

process_event(stop_timeout,
              _,
              TM,
              #transport{id = Id,
                         timers = Timers},
              _) ->
    lists:foreach(fun({_, Ref}) ->
                          emqx_misc:cancel_timer(Ref)
                  end,
                  maps:to_list(Timers)),
    #{tm => maps:remove(Id, TM)};

process_event(Event,
              Msg,
              TM,
              #transport{id = Id,
                         state = State,
                         data = Data} = TP,
              Cfg) ->
    Result = emqx_coap_transport:State(Event, Msg, Data, Cfg),
    {ok, _, TP2} = emqx_misc:pipeline([fun process_state_change/2,
                                       fun process_data_change/2,
                                       fun process_timeouts/2],
                                      Result,
                                      TP),
    TM2 = TM#{Id => TP2},
    emqx_coap_session:transfer_result(Result, tm, TM2).

process_state_change(#{next := Next}, TP) ->
    {ok, cancel_state_timer(TP#transport{state = Next})};
process_state_change(_, TP) ->
    {ok, TP}.

cancel_state_timer(#transport{timers = Timers} = TP) ->
    case maps:get(state_timer, Timers, undefined) of
        undefined ->
            TP;
        Ref ->
            _ = emqx_misc:cancel_timer(Ref),
            TP#transport{timers = maps:remove(state_timer, Timers)}
    end.

process_data_change(#{data := Data}, TP) ->
    {ok, TP#transport{data = Data}};
process_data_change(_, TP) ->
    {ok, TP}.

process_timeouts(#{timeouts := []}, TP) ->
    {ok, TP};
process_timeouts(#{timeouts := Timeouts},
                 #transport{id = Id, timers = Timers} = TP) ->
    NewTimers = lists:foldl(fun({state_timeout, _, _} = Timer, Acc) ->
                                    process_timer(Id, Timer, Acc);
                               ({stop_timeout, I}, Acc) ->
                                    process_timer(Id, {stop_timeout, I, stop}, Acc)
                            end,
                            Timers,
                            Timeouts),
    {ok, TP#transport{timers = NewTimers}};

process_timeouts(_, TP) ->
    {ok, TP}.

process_timer(Id, {Type, Interval, Msg}, Timers) ->
    Ref = emqx_misc:start_timer(Interval, {transport, {Id, Type, Msg}}),
    Timers#{Type => Ref}.
