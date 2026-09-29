%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_worker_waker).
-moduledoc """
This module implements a sidecar process used during replay of closed timer epochs.
It reads ahead to schedule timer wake-ups independently from the worker, which
reads the topic again just before executing timers.
""".

-behavior(gen_server).

%% API:
-export([start_link/4]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include("internals.hrl").

-define(replay_loop, replay_loop).

-record(cast_wake_up, {t :: integer()}).

-record(s, {
    parent :: pid(),
    stream :: emqx_ds:stream(),
    topic :: emqx_ds:topic(),
    time_delta :: integer(),
    it_next :: emqx_ds:iterator() | undefined,
    tail = [] :: [emqx_ds:ttv()]
}).

-spec start_link(pid(), emqx_ds:stream(), emqx_ds:topic(), integer()) -> {ok, pid()}.
start_link(Parent, Stream, Topic, DeltaT) ->
    gen_server:start_link(?MODULE, {Parent, Stream, Topic, DeltaT}, []).

init({Parent, Stream, Topic, DeltaT}) ->
    self() ! ?replay_loop,
    {ok, #s{
        parent = Parent,
        stream = Stream,
        topic = Topic,
        time_delta = DeltaT
    }}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(?replay_loop, S = #s{it_next = undefined}) ->
    case emqx_ds:make_iterator(?DB_GLOB, S#s.stream, S#s.topic, 0) of
        {ok, It} ->
            read_next(S#s{it_next = It});
        ?err_rec(Reason) ->
            retry(S, Reason)
    end;
handle_info(?replay_loop, S) ->
    read_next(S);
handle_info({timer, Time}, S = #s{parent = Parent, tail = Tail}) ->
    Parent ! #cast_wake_up{t = Time},
    case Tail of
        [{_, NextTime, _} | Rest] ->
            {noreply, schedule(NextTime, S#s{tail = Rest})};
        [] ->
            self() ! ?replay_loop,
            {noreply, S}
    end;
handle_info(_Info, S) ->
    {noreply, S}.

read_next(S = #s{it_next = It}) ->
    case emqx_ds:next(?DB_GLOB, It, emqx_durable_timer:cfg_batch_size()) of
        {ok, ItNext, []} ->
            S#s.parent ! replay_waker_finished,
            {stop, normal, S#s{it_next = ItNext}};
        {ok, ItNext, [{_, Time, _} | Rest]} ->
            ?tp(debug, ?tp_waker_scheduled, #{topic => S#s.topic, time => Time}),
            {noreply, schedule(Time, S#s{it_next = ItNext, tail = Rest})};
        ?err_rec(Reason) ->
            retry(S, Reason)
    end.

schedule(Time, S = #s{time_delta = Delta}) ->
    Delay = max(0, Time + Delta - emqx_durable_timer:now_ms()),
    _ = erlang:send_after(Delay, self(), {timer, Time}),
    S.

retry(S, _Reason) ->
    _ = erlang:send_after(
        emqx_durable_timer:cfg_replay_retry_interval(),
        self(),
        ?replay_loop
    ),
    {noreply, S}.
