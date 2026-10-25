%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_writer).
-moduledoc "Topic-affine QoS 0 batching writers.".
-behaviour(gen_server).
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").

-export([start_link/2, append/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2, terminate/2]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Index, Config) ->
    gen_server:start_link(name(Index), ?MODULE, Config, []).

append(Namespace, #message{topic = Topic, payload = Payload} = Msg, #{writer := Writer}) ->
    #{pool_size := Size, call_timeout := Timeout} = Writer,
    Index = erlang:phash2({Namespace, Topic}, Size),
    try
        gen_server:call(name(Index), {append, Namespace, Topic, Msg, byte_size(Payload)}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------

init(Config) ->
    %% Run `terminate/2` on supervisor shutdown so that buffered messages are flushed.
    process_flag(trap_exit, true),
    {ok, #{
        config => Config,
        batches => #{},
        count => 0,
        bytes => 0,
        idle => undefined,
        max => undefined
    }}.

handle_call(
    {append, Namespace, Topic, Message, Bytes},
    _From,
    #{batches := Batches, count := Count0, bytes := Total0, config := #{writer := Writer}} = State0
) ->
    Key = {Namespace, Topic},
    Batch = maps:get(Key, Batches, []),
    Count = Count0 + 1,
    Total = Total0 + Bytes,
    State = timers(
        State0#{batches := Batches#{Key => [Message | Batch]}, count := Count, bytes := Total},
        Writer
    ),
    #{flush_max_messages := MaxCount, flush_max_bytes := MaxBytes} = Writer,
    case Count >= MaxCount orelse Total >= MaxBytes of
        true -> {reply, ok, State, {continue, flush}};
        false -> {reply, ok, State}
    end.

handle_cast(_Request, State) -> {noreply, State}.

handle_continue(flush, State) -> {noreply, flush(State)}.

handle_info({timeout, Ref, flush}, #{idle := Ref} = State) -> {noreply, flush(State)};
handle_info({timeout, Ref, flush}, #{max := Ref} = State) -> {noreply, flush(State)};
handle_info(_Info, State) -> {noreply, State}.

terminate(Reason, State) when
    Reason =:= normal; Reason =:= shutdown; element(1, Reason) =:= shutdown
->
    _ = flush(State),
    ok;
terminate(_Reason, #{count := Count}) when Count > 0 ->
    %% The crashed flush may have appended part of the buffer. A second append would
    %% store those messages again at new offsets.
    emqx_offline_messages_pg:count(write_failed, Count),
    ?SLOG(error, #{msg => offline_messages_pg_buffer_dropped, count => Count}),
    ok;
terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

name(Index) -> {via, gproc, {n, l, {?MODULE, Index}}}.

timers(#{idle := Idle, max := Max0} = State, #{
    min_flush_timeout := MinTime, max_flush_timeout := MaxTime
}) ->
    cancel(Idle),
    Max =
        case Max0 of
            undefined -> erlang:start_timer(MaxTime, self(), flush);
            _ -> Max0
        end,
    State#{idle := erlang:start_timer(MinTime, self(), flush), max := Max}.

cancel(undefined) ->
    ok;
cancel(Ref) ->
    _ = erlang:cancel_timer(Ref),
    ok.

flush(#{batches := Batches, idle := Idle, max := Max, config := Config} = State) ->
    cancel(Idle),
    cancel(Max),
    maps:foreach(
        fun({Namespace, Topic}, Batch) ->
            Messages = lists:reverse(Batch),
            Operation =
                {append, Namespace, Topic, [emqx_omp_pg_store:encode(Msg) || Msg <- Messages]},
            case emqx_omp_pg_store:call(omp_pg_write, Topic, Operation, Config) of
                {ok, {Id, Offset}} ->
                    dispatch(Messages, {Namespace, Id}, Offset);
                {error, Reason} ->
                    emqx_offline_messages_pg:count(write_failed, length(Batch)),
                    ?SLOG_THROTTLE(error, #{
                        msg => offline_messages_pg_batch_failed, topic => Topic, reason => Reason
                    })
            end
        end,
        Batches
    ),
    State#{batches := #{}, count := 0, bytes := 0, idle := undefined, max := undefined}.

dispatch([], _Identity, _Offset) ->
    ok;
dispatch([Msg | Rest], Identity, Offset) ->
    try emqx_omp_pg_store:dispatch(Msg, Identity, Offset) of
        ok -> ok
    catch
        Class:Reason ->
            ?SLOG_THROTTLE(error, #{
                msg => offline_messages_pg_dispatch_failed, class => Class, reason => Reason
            })
    end,
    dispatch(Rest, Identity, Offset + 1).
