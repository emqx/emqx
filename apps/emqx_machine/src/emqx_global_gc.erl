%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_global_gc).

-behaviour(gen_server).

-include_lib("emqx/include/emqx_cm.hrl").

-export([start_link/0, stop/0]).

-export([run/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Channels collected in one tick. This bounds the work of a single wakeup,
%% which is what keeps the collection off the CPU profile.
-define(CHUNK, 100).

%% Lower bound on the delay between ticks. On a node with so many channels that
%% `?CHUNK' per `?MIN_TICK' cannot cover the table within the configured
%% interval, one pass takes longer than the interval. That is deliberate: the
%% bound on per-tick work matters more than the length of the cycle.
-define(MIN_TICK, 10).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Collect every channel now, in one pass, and return the time (in
%% milliseconds) spent. The scheduled collection spreads the same work over the
%% configured interval; this call does not wait.
-spec run() -> {ok, non_neg_integer()}.
run() -> gen_server:call(?MODULE, run, infinity).

-spec stop() -> ok.
stop() -> gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Wait one interval before the first pass, so that collection never
    %% competes with node startup.
    {ok, idle(initial_state())}.

initial_state() ->
    #{timer => undefined, cursor => undefined, tick => ?MIN_TICK, started_at => 0}.

handle_call(run, _From, State) ->
    {Time, ok} = timer:tc(fun run_now/0),
    {reply, {ok, Time div 1000}, State, hibernate};
handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% `run' is the message the previous version of this module armed its timer
%% with. The clause is kept so that loading this beam into a running node is
%% enough on its own: a timer armed before the load still lands here, and the
%% state map built before the load is completed by `upgrade_state/1'. No
%% upgrade instruction and no `code_change/3' are needed.
handle_info({timeout, TRef, Msg}, State = #{timer := TRef}) when
    Msg =:= tick; Msg =:= run
->
    {noreply, tick(upgrade_state(State)), hibernate};
handle_info(_Info, State) ->
    {noreply, State}.

%% A state map written by the previous version carries only `timer'. Fill in
%% the keys this version reads, keeping whatever the old map already holds.
upgrade_state(State) ->
    maps:merge(initial_state(), State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal function
%%--------------------------------------------------------------------

%% A tick with no cursor begins a pass. The delay between the ticks of that
%% pass is derived from the number of channels, so one pass takes about
%% `global_gc_interval' whatever the size of the node.
tick(State = #{cursor := undefined}) ->
    case interval() of
        disabled ->
            State#{timer := undefined};
        Interval ->
            Ticks = max(1, ceil(channel_count() / ?CHUNK)),
            Tick = max(?MIN_TICK, Interval div Ticks),
            collect_chunk(State#{
                cursor := first(),
                tick := Tick,
                started_at := now_ms()
            })
    end;
tick(State) ->
    collect_chunk(State).

collect_chunk(State = #{cursor := Cursor, tick := Tick}) ->
    case collect(Cursor, ?CHUNK) of
        '$end_of_table' ->
            pass_done(State);
        Next ->
            schedule(Tick, State#{cursor := Next})
    end.

%% The pass is over. Wait out the rest of the interval, then start the next
%% one. On a node large enough that the pass already took longer than the
%% interval, there is nothing left to wait for and the next pass starts at
%% once, so every channel is still collected as often as the node can afford.
pass_done(State = #{started_at := StartedAt}) ->
    case interval() of
        disabled ->
            State#{timer := undefined, cursor := undefined};
        Interval ->
            Rest = Interval - (now_ms() - StartedAt),
            schedule(max(?MIN_TICK, Rest), State#{cursor := undefined})
    end.

idle(State) ->
    case interval() of
        disabled -> State#{timer := undefined, cursor := undefined};
        Interval -> schedule(Interval, State#{cursor := undefined})
    end.

schedule(Delay, State) ->
    State#{timer := emqx_utils:start_timer(Delay, tick)}.

collect('$end_of_table', _N) ->
    '$end_of_table';
collect(Key, 0) ->
    Key;
collect(Key, N) ->
    _ = gc_channel(Key),
    collect(next(Key), N - 1).

%% The key of the channel info table is `{ClientId, ChanPid}', so the pid needs
%% no table read. A channel that has gone since the cursor passed it is
%% skipped: `process_info/2' answers `undefined' for a dead pid.
gc_channel({_ClientId, Pid}) when is_pid(Pid) ->
    is_waiting(Pid) andalso garbage_collect(Pid);
gc_channel(_Key) ->
    false.

%% `ets:next/2' on an `ordered_set' accepts a key that has been deleted since
%% it was read, so the cursor survives a channel closing mid-pass.
first() ->
    case is_table_ready() of
        true -> ets:first(?CHAN_INFO_TAB);
        false -> '$end_of_table'
    end.

next(Key) ->
    case is_table_ready() of
        true -> ets:next(?CHAN_INFO_TAB, Key);
        false -> '$end_of_table'
    end.

is_table_ready() ->
    ets:info(?CHAN_INFO_TAB, size) =/= undefined.

channel_count() ->
    case ets:info(?CHAN_INFO_TAB, size) of
        undefined -> 0;
        Size -> Size
    end.

run_now() ->
    run_now(first()).

run_now('$end_of_table') ->
    ok;
run_now(Key) ->
    _ = gc_channel(Key),
    run_now(next(Key)).

now_ms() ->
    erlang:monotonic_time(millisecond).

interval() ->
    application:get_env(emqx_machine, global_gc_interval, disabled).

-compile({inline, [is_waiting/1]}).
is_waiting(Pid) ->
    {status, waiting} == process_info(Pid, status).
