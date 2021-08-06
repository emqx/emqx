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

%% the transport state machine manager
-module(emqx_coap_tm).

-export([ new/0
        , handle_request/3
        , handle_response/3
        , handle_out/3
        , timeout/3
        , is_inused/3]).

-export_type([manager/0, event_result/1]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

-type direction() :: in | out.
-type state_machine_id() :: {direction(), non_neg_integer()}.

-record(state_machine, { id :: state_machine_id()
                       , state :: atom()
                       , timers :: maps:map()
                       , transport :: emqx_coap_transport:transport()}).
-type state_machine() :: #state_machine{}.

-type message_id() :: 0 .. ?MAX_MESSAGE_ID.

-type manager() :: #{message_id() => state_machine()}.

-type ttimeout() :: {state_timeout, pos_integer(), any()}
                  | {stop_timeout, pos_integer()}.

-type topic() :: binary().
-type token() :: binary().
-type sub_register() :: {topic(), token()} | topic().
-type event_result(State) ::
        #{next => State,
          outgoing => emqx_coap_message(),
          timeouts => list(ttimeout()),
          has_sub  => undefined | sub_register(),
          transport => emqx_coap_transport:transprot()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
new() ->
    #{}.

handle_request(#coap_message{id = MsgId} = Msg, Ctx, TM) ->
    Id = {in, MsgId},
    case maps:get(Id, TM, undefined) of
        undefined ->
            Transport = emqx_coap_transport:new(),
            Machine = new_state_machine(Id, Transport),
            process_event(in, Msg, TM, Ctx, Machine);
        Machine ->
            process_event(in, Msg, TM, Ctx, Machine)
    end.

handle_response(#coap_message{type = Type, id = MsgId} = Msg, Ctx, TM) ->
    Id = {out, MsgId},
    case maps:get(Id, TM, undefined) of
        undefined ->
            case Type of
                reset ->
                    ?EMPTY_RESULT;
                _ ->
                    ?RESET(Msg)
            end;
        Machine ->
            process_event(in, Msg, TM, Ctx, Machine)
    end.

handle_out(#coap_message{id = MsgId} = Msg, Ctx, TM) ->
    Id = {out, MsgId},
    case maps:get(Id, TM, undefined) of
        undefined ->
            Transport = emqx_coap_transport:new(),
            Machine = new_state_machine(Id, Transport),
            process_event(out, Msg, TM, Ctx, Machine);
        _ ->
            %% ignore repeat send
            ?EMPTY_RESULT
    end.

timeout({Id, Type, Msg}, Ctx, TM) ->
    case maps:get(Id, TM, undefined) of
        undefined ->
            ?EMPTY_RESULT;
        #state_machine{timers = Timers} = Machine ->
            %% maybe timer has been canceled
            case maps:is_key(Type, Timers) of
                true ->
                    process_event(Type, Msg, TM, Ctx, Machine);
                _ ->
                    ?EMPTY_RESULT
            end
    end.

-spec is_inused(direction(), message_id(), manager()) -> boolean().
is_inused(Dir, Msg, Manager) ->
    maps:is_key({Dir, Msg}, Manager).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
new_state_machine(Id, Transport) ->
    #state_machine{id = Id,
                   state = idle,
                   timers = #{},
                   transport = Transport}.

process_event(stop_timeout,
              _,
              TM,
              _,
              #state_machine{id = Id,
                             timers = Timers}) ->
    lists:foreach(fun({_, Ref}) ->
                          emqx_misc:cancel_timer(Ref)
                  end,
                  maps:to_list(Timers)),
    #{tm => maps:remove(Id, TM)};

process_event(Event,
              Msg,
              TM,
              Ctx,
              #state_machine{id = Id,
                             state = State,
                             transport = Transport} = Machine) ->
    Result = emqx_coap_transport:State(Event, Msg, Ctx, Transport),
    {ok, _, Machine2} = emqx_misc:pipeline([fun process_state_change/2,
                                            fun process_transport_change/2,
                                            fun process_timeouts/2],
                                           Result,
                                           Machine),
    TM2 = TM#{Id => Machine2},
    emqx_coap_session:transfer_result(tm, TM2, Result).

process_state_change(#{next := Next}, Machine) ->
    {ok, cancel_state_timer(Machine#state_machine{state = Next})};
process_state_change(_, Machine) ->
    {ok, Machine}.

cancel_state_timer(#state_machine{timers = Timers} = Machine) ->
    case maps:get(state_timer, Timers, undefined) of
        undefined ->
            Machine;
        Ref ->
            _ = emqx_misc:cancel_timer(Ref),
            Machine#state_machine{timers = maps:remove(state_timer, Timers)}
    end.

process_transport_change(#{transport := Transport}, Machine) ->
    {ok, Machine#state_machine{transport = Transport}};
process_transport_change(_, Machine) ->
    {ok, Machine}.

process_timeouts(#{timeouts := []}, Machine) ->
    {ok, Machine};
process_timeouts(#{timeouts := Timeouts},
                 #state_machine{id = Id, timers = Timers} = Machine) ->
    NewTimers = lists:foldl(fun({state_timeout, _, _} = Timer, Acc) ->
                                    process_timer(Id, Timer, Acc);
                               ({stop_timeout, I}, Acc) ->
                                    process_timer(Id, {stop_timeout, I, stop}, Acc)
                            end,
                            Timers,
                            Timeouts),
    {ok, Machine#state_machine{timers = NewTimers}};

process_timeouts(_, Machine) ->
    {ok, Machine}.

process_timer(Id, {Type, Interval, Msg}, Timers) ->
    Ref = emqx_misc:start_timer(Interval, {state_machine, {Id, Type, Msg}}),
    Timers#{Type => Ref}.
