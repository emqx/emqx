%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_cleanup).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_info/2, handle_call/3, handle_cast/2]).

-include("emqx_bcast.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Interval = cleanup_interval(),
    TimerRef = schedule(Interval),
    {ok, {Interval, TimerRef}}.

handle_info(cleanup, {Interval, _TimerRef}) ->
    case is_cleanup_leader() of
        true ->
            ok = emqx_bcast_storage:cleanup_expired();
        false ->
            %% A smaller-named core owns the cleanup; skip to avoid every
            %% core scanning the same mnesia tables at the same interval.
            ok
    end,
    TimerRef = schedule(Interval),
    {noreply, {Interval, TimerRef}};
handle_info(_Msg, State) ->
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(reschedule, {_Interval, TimerRef}) ->
    ok = emqx_bcast_utils:cancel_timer(TimerRef),
    Interval = cleanup_interval(),
    NewTimerRef = schedule(Interval),
    {noreply, {Interval, NewTimerRef}};
handle_cast(_Msg, State) ->
    {noreply, State}.

schedule(Interval) ->
    erlang:send_after(Interval, self(), cleanup).

cleanup_interval() ->
    emqx_bcast_config:get(cleanup_interval) * 1000.

%% Only the lexicographically smallest running core executes the cleanup
%% scan. Replicants never run the gen_server (see emqx_bcast_sup), and
%% among several cores one owner is enough: the tables are replicated, so a
%% scan on every core would repeat the same deletions and serialize on the
%% same locks.
is_cleanup_leader() ->
    case emqx_bcast:is_core() of
        false ->
            false;
        true ->
            case lists:sort(emqx_bcast:core_nodes()) of
                [Leader | _] -> Leader =:= node();
                [] -> false
            end
    end.
