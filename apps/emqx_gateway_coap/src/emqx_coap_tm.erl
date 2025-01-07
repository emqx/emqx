%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    new/0,
    handle_request/2,
    handle_response/2,
    handle_out/2,
    handle_out/3,
    set_reply/2,
    timeout/2
]).

-export_type([manager/0, event_result/1]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/logger.hrl").

-type direction() :: in | out.

-record(state_machine, {
    seq_id :: seq_id(),
    id :: state_machine_key(),
    token :: token() | undefined,
    observe :: 0 | 1 | undefined | observed,
    state :: atom(),
    timers :: map(),
    transport :: emqx_coap_transport:transport()
}).
-type state_machine() :: #state_machine{}.

-type message_id() :: 0..?MAX_MESSAGE_ID.
-type token_key() :: {token, token()}.
-type state_machine_key() :: {direction(), message_id()}.
-type seq_id() :: pos_integer().
-type manager_key() :: token_key() | state_machine_key() | seq_id().

-type manager() :: #{
    seq_id => seq_id(),
    next_msg_id => coap_message_id(),
    token_key() => seq_id(),
    state_machine_key() => seq_id(),
    seq_id() => state_machine()
}.

-type ttimeout() ::
    {state_timeout, pos_integer(), any()}
    | {stop_timeout, pos_integer()}.

-type topic() :: binary().
-type token() :: binary().
-type sub_register() :: {topic(), token()} | topic().

-type event_result(State) ::
    #{
        next => State,
        outgoing => coap_message(),
        timeouts => list(ttimeout()),
        has_sub => undefined | sub_register(),
        transport => emqx_coap_transport:transport()
    }.

-define(TOKEN_ID(T), {token, T}).

-import(emqx_coap_medium, [empty/0, iter/4, reset/1, proto_out/2]).

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> manager().
new() ->
    #{
        seq_id => 1,
        next_msg_id => rand:uniform(?MAX_MESSAGE_ID)
    }.

handle_request(#coap_message{id = MsgId} = Msg, TM) ->
    Id = {in, MsgId},
    case find_machine(Id, TM) of
        undefined ->
            {Machine, TM2} = new_in_machine(Id, TM),
            process_event(in, Msg, TM2, Machine);
        Machine ->
            process_event(in, Msg, TM, Machine)
    end.

%% client response
handle_response(#coap_message{type = Type, id = MsgId, token = Token} = Msg, TM) ->
    Id = {out, MsgId},
    TokenId = ?TOKEN_ID(Token),
    case find_machine_by_keys([Id, TokenId], TM) of
        undefined ->
            case Type of
                reset ->
                    empty();
                _ ->
                    reset(Msg)
            end;
        Machine ->
            process_event(in, Msg, TM, Machine)
    end.

%% send to a client, msg can be request/piggyback/separate/notify
handle_out({Ctx, Msg}, TM) ->
    handle_out(Msg, Ctx, TM);
handle_out(Msg, TM) ->
    handle_out(Msg, undefined, TM).

handle_out(#coap_message{token = Token} = MsgT, Ctx, TM) ->
    {MsgId, TM2} = alloc_message_id(TM),
    Msg = MsgT#coap_message{id = MsgId},
    Id = {out, MsgId},
    TokenId = ?TOKEN_ID(Token),
    %% TODO why find by token ?
    case find_machine_by_keys([Id, TokenId], TM2) of
        undefined ->
            {Machine, TM3} = new_out_machine(Id, Ctx, Msg, TM2),
            process_event(out, Msg, TM3, Machine);
        _ ->
            %% ignore repeat send
            empty()
    end.

set_reply(#coap_message{id = MsgId} = Msg, TM) ->
    Id = {in, MsgId},
    case find_machine(Id, TM) of
        undefined ->
            TM;
        #state_machine{
            transport = Transport,
            seq_id = SeqId
        } = Machine ->
            Transport2 = emqx_coap_transport:set_cache(Msg, Transport),
            Machine2 = Machine#state_machine{transport = Transport2},
            TM#{SeqId => Machine2}
    end.

timeout({SeqId, Type, Msg}, TM) ->
    case maps:get(SeqId, TM, undefined) of
        undefined ->
            empty();
        #state_machine{timers = Timers} = Machine ->
            %% maybe timer has been canceled
            case maps:is_key(Type, Timers) of
                true ->
                    process_event(Type, Msg, TM, Machine);
                _ ->
                    empty()
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
process_event(stop_timeout, _, TM, Machine) ->
    process_manager(stop, #{}, Machine, TM);
process_event(
    Event,
    Msg,
    TM,
    #state_machine{
        state = State,
        transport = Transport
    } = Machine
) ->
    Result = emqx_coap_transport:State(Event, Msg, Transport),
    iter(
        [
            proto,
            fun process_observe_response/5,
            next,
            fun process_state_change/5,
            transport,
            fun process_transport_change/5,
            timeouts,
            fun process_timeouts/5,
            fun process_manager/4
        ],
        Result,
        Machine,
        TM
    ).

process_observe_response(
    {response, {_, Msg}} = Response,
    Result,
    #state_machine{observe = 0} = Machine,
    TM,
    Iter
) ->
    Result2 = proto_out(Response, Result),
    case Msg#coap_message.method of
        {ok, _} ->
            iter(
                Iter,
                Result2#{next => observe},
                Machine#state_machine{observe = observed},
                TM
            );
        _ ->
            iter(Iter, Result2, Machine, TM)
    end;
process_observe_response(Proto, Result, Machine, TM, Iter) ->
    iter(Iter, proto_out(Proto, Result), Machine, TM).

process_state_change(Next, Result, Machine, TM, Iter) ->
    case Next of
        stop ->
            process_manager(stop, Result, Machine, TM);
        _ ->
            iter(
                Iter,
                Result,
                cancel_state_timer(Machine#state_machine{state = Next}),
                TM
            )
    end.

process_transport_change(Transport, Result, Machine, TM, Iter) ->
    iter(Iter, Result, Machine#state_machine{transport = Transport}, TM).

process_timeouts([], Result, Machine, TM, Iter) ->
    iter(Iter, Result, Machine, TM);
process_timeouts(
    Timeouts,
    Result,
    #state_machine{
        seq_id = SeqId,
        timers = Timers
    } = Machine,
    TM,
    Iter
) ->
    NewTimers = lists:foldl(
        fun
            ({state_timeout, _, _} = Timer, Acc) ->
                process_timer(SeqId, Timer, Acc);
            ({stop_timeout, I}, Acc) ->
                process_timer(SeqId, {stop_timeout, I, stop}, Acc)
        end,
        Timers,
        Timeouts
    ),
    iter(Iter, Result, Machine#state_machine{timers = NewTimers}, TM).

process_manager(stop, Result, #state_machine{seq_id = SeqId}, TM) ->
    Result#{tm => delete_machine(SeqId, TM)};
process_manager(_, Result, #state_machine{seq_id = SeqId} = Machine2, TM) ->
    Result#{tm => TM#{SeqId => Machine2}}.

cancel_state_timer(#state_machine{timers = Timers} = Machine) ->
    case maps:get(state_timer, Timers, undefined) of
        undefined ->
            Machine;
        Ref ->
            _ = emqx_utils:cancel_timer(Ref),
            Machine#state_machine{timers = maps:remove(state_timer, Timers)}
    end.

process_timer(SeqId, {Type, Interval, Msg}, Timers) ->
    Ref = emqx_utils:start_timer(Interval, {state_machine, {SeqId, Type, Msg}}),
    Timers#{Type => Ref}.

-spec delete_machine(manager_key(), manager()) -> manager().
delete_machine(Id, Manager) ->
    case find_machine(Id, Manager) of
        undefined ->
            Manager;
        #state_machine{
            seq_id = SeqId,
            id = MachineId,
            token = Token,
            timers = Timers
        } ->
            lists:foreach(
                fun({_, Ref}) ->
                    emqx_utils:cancel_timer(Ref)
                end,
                maps:to_list(Timers)
            ),
            maps:without([SeqId, MachineId, ?TOKEN_ID(Token)], Manager)
    end.

-spec find_machine(manager_key(), manager()) -> state_machine() | undefined.
find_machine({_, _} = Id, Manager) ->
    find_machine_by_seqid(maps:get(Id, Manager, undefined), Manager);
find_machine(SeqId, Manager) ->
    find_machine_by_seqid(SeqId, Manager).

-spec find_machine_by_seqid(seq_id() | undefined, manager()) ->
    state_machine() | undefined.
find_machine_by_seqid(SeqId, Manager) ->
    maps:get(SeqId, Manager, undefined).

-spec find_machine_by_keys(
    list(manager_key()),
    manager()
) -> state_machine() | undefined.
find_machine_by_keys([H | T], Manager) ->
    case H of
        ?TOKEN_ID(<<>>) ->
            find_machine_by_keys(T, Manager);
        _ ->
            case find_machine(H, Manager) of
                undefined ->
                    find_machine_by_keys(T, Manager);
                Machine ->
                    Machine
            end
    end;
find_machine_by_keys(_, _) ->
    undefined.

-spec new_in_machine(state_machine_key(), manager()) ->
    {state_machine(), manager()}.
new_in_machine(MachineId, #{seq_id := SeqId} = Manager) ->
    Machine = #state_machine{
        seq_id = SeqId,
        id = MachineId,
        state = idle,
        timers = #{},
        transport = emqx_coap_transport:new()
    },
    {Machine, Manager#{
        seq_id := SeqId + 1,
        SeqId => Machine,
        MachineId => SeqId
    }}.

-spec new_out_machine(state_machine_key(), any(), coap_message(), manager()) ->
    {state_machine(), manager()}.
new_out_machine(
    MachineId,
    Ctx,
    #coap_message{type = Type, token = Token, options = Opts},
    #{seq_id := SeqId} = Manager
) ->
    Observe = maps:get(observe, Opts, undefined),
    Machine = #state_machine{
        seq_id = SeqId,
        id = MachineId,
        token = Token,
        observe = Observe,
        state = idle,
        timers = #{},
        transport = emqx_coap_transport:new(Ctx)
    },

    Manager2 = Manager#{
        seq_id := SeqId + 1,
        SeqId => Machine,
        MachineId => SeqId
    },
    {Machine,
        if
            Token =:= <<>> ->
                Manager2;
            Observe =:= 1 ->
                TokenId = ?TOKEN_ID(Token),
                delete_machine(TokenId, Manager2);
            Type =:= con orelse Observe =:= 0 ->
                TokenId = ?TOKEN_ID(Token),
                case maps:get(TokenId, Manager, undefined) of
                    undefined ->
                        Manager2#{TokenId => SeqId};
                    _ ->
                        throw("token conflict")
                end;
            true ->
                Manager2
        end}.

alloc_message_id(#{next_msg_id := MsgId} = TM) ->
    alloc_message_id(MsgId, TM).

alloc_message_id(MsgId, TM) ->
    Id = {out, MsgId},
    case maps:get(Id, TM, undefined) of
        undefined ->
            {MsgId, TM#{next_msg_id => next_message_id(MsgId)}};
        _ ->
            alloc_message_id(next_message_id(MsgId), TM)
    end.

next_message_id(MsgId) ->
    Next = MsgId + 1,
    case Next >= ?MAX_MESSAGE_ID of
        true ->
            1;
        false ->
            Next
    end.
