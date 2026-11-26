%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_banned_tarpit).

-moduledoc """
Slots for the CONNECT tarpit of banned clients (`banned.tarpit`).

A connection process that holds a banned client in the tarpit takes one slot
with `enter/0`. It does not give the slot back. This process monitors each
holder and frees its slot when the holder exits, also when the holder exits
by an exit signal and runs no `terminate`.

The slot count is an `atomics` counter. `enter/0` increments it in the
caller, so a burst of banned clients cannot take more than `max` slots and
does not wait for this process. The counter is created again each time this
process starts: the holders of an earlier counter are not monitored any
more, and their slots are not counted.
""".

-behaviour(gen_server).

-export([start_link/0, enter/0, count/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(COUNTER_KEY, {?MODULE, counter}).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Take a tarpit slot for the calling process.

Return `{ok, Delay}` when the tarpit is enabled and a slot is free. Return
`false` when the tarpit is disabled or all `max` slots are taken.
""".
-spec enter() -> {ok, Delay :: pos_integer()} | false.
enter() ->
    case emqx_config:get([banned, tarpit], #{}) of
        #{delay := Delay, max := Max} when Delay > 0, Max > 0 ->
            take_slot(Delay, Max);
        _ ->
            false
    end.

-doc "Return the number of taken tarpit slots on this node.".
-spec count() -> non_neg_integer().
count() ->
    case persistent_term:get(?COUNTER_KEY, undefined) of
        undefined -> 0;
        Counter -> max(0, atomics:get(Counter, 1))
    end.

%% The counter is read before the owner pid. When the owner restarts between
%% the two reads, the new owner gets the old counter in the message and
%% ignores it.
take_slot(Delay, Max) ->
    Counter = persistent_term:get(?COUNTER_KEY, undefined),
    Owner = whereis(?MODULE),
    case Counter =/= undefined andalso is_pid(Owner) of
        true ->
            case atomics:add_get(Counter, 1, 1) =< Max of
                true ->
                    Owner ! {monitor, self(), Counter},
                    {ok, Delay};
                false ->
                    atomics:sub(Counter, 1, 1),
                    false
            end;
        false ->
            false
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Counter = atomics:new(1, [{signed, true}]),
    persistent_term:put(?COUNTER_KEY, Counter),
    {ok, Counter}.

handle_call(_Request, _From, Counter) ->
    {reply, ignored, Counter}.

handle_cast(_Msg, Counter) ->
    {noreply, Counter}.

handle_info({monitor, Pid, Counter}, Counter) ->
    _ = erlang:monitor(process, Pid),
    {noreply, Counter};
handle_info({monitor, _Pid, _OldCounter}, Counter) ->
    {noreply, Counter};
handle_info({'DOWN', _MRef, process, _Pid, _Reason}, Counter) ->
    atomics:sub(Counter, 1, 1),
    {noreply, Counter};
handle_info(_Info, Counter) ->
    {noreply, Counter}.
