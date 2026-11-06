%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_streams_gc_slab_worker).
-moduledoc """
Delete expired data of last-value streams in one slab.

The worker reads the stream registry lazily and keeps at most `?MAX_QUEUES` streams in
deletion, with one asynchronous `del_batch` request in flight for each stream.

A failed request is sent again after `?RETRY_INTERVAL`. A stream is given up after
`?QUEUE_FAILURE_LIMIT` failures. Recoverable errors and stream lookup errors report that
the shard is unavailable. After `?SHARD_FAILURE_LIMIT` such failures in one run, the
worker stops. Successes and stream-specific errors do not reset this count.
""".
-behaviour(gen_server).
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/1, start_link/2]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-define(DB, ?STREAMS_MESSAGE_LASTVALUE_DB).
-define(MAX_QUEUES, 10).
-define(RETRY_INTERVAL, 1000).
-define(QUEUE_FAILURE_LIMIT, 3).
-define(SHARD_FAILURE_LIMIT, 5).

-type queue_entry() :: #{
    queue := emqx_streams_types:stream(),
    failures := non_neg_integer(),
    topic => emqx_ds:topic_filter(),
    cutoff => emqx_ds:time(),
    stream => emqx_ds:stream()
}.

-record(retry, {id :: emqx_streams_types:stream_id()}).
-record(st, {
    slab :: emqx_ds:slab(),
    source :: emqx_utils_stream:stream(emqx_streams_types:stream()),
    queues = #{} :: #{emqx_streams_types:stream_id() => queue_entry()},
    refs = #{} :: #{reference() => emqx_streams_types:stream_id()},
    shard_failures = 0 :: non_neg_integer()
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Start a worker that traverses the stream registry for this slab".
-spec start_link(emqx_ds:slab()) -> gen_server:start_ret().
start_link(Slab) -> start_link(Slab, emqx_streams_registry:list()).

-doc "Start a worker that takes streams from an explicit source".
-spec start_link(emqx_ds:slab(), emqx_utils_stream:stream(emqx_streams_types:stream())) ->
    gen_server:start_ret().
start_link(Slab, Source) -> gen_server:start_link(?MODULE, {Slab, Source}, []).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Slab, Source}) ->
    {ok, #st{slab = Slab, source = Source}, {continue, refill}}.

handle_continue(refill, State) ->
    refill(State).

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(?ds_tx_commit_reply(Ref, Reply), #st{slab = Slab, refs = Refs} = State) when
    is_map_key(Ref, Refs)
->
    Id = maps:get(Ref, Refs),
    Outcome = emqx_ds:del_batch_outcome(?DB, Ref, Reply),
    ?tp(streams_gc_batch_reply, #{slab => Slab, id => Id, result => Outcome}),
    outcome(Id, Outcome, State#st{refs = maps:remove(Ref, Refs)});
handle_info(#retry{id = Id}, #st{queues = Queues} = State) when is_map_key(Id, Queues) ->
    send_batch(Id, State);
handle_info(_Info, State) ->
    %% DS can deliver a late timeout reply after its request was cleaned up.
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% Take one queue from the source while there is room for it.
refill(#st{queues = Queues} = State) when map_size(Queues) >= ?MAX_QUEUES ->
    {noreply, State};
refill(#st{source = Source, queues = Queues} = State) ->
    case emqx_utils_stream:next(Source) of
        [] when map_size(Queues) =:= 0 ->
            {stop, normal, State};
        [] ->
            {noreply, State#st{source = []}};
        [StreamInfo | Rest] ->
            start_queue(StreamInfo, State#st{source = Rest})
    end.

start_queue(#{id := Id} = StreamInfo, #st{slab = Slab, queues = Queues} = State) ->
    ?tp(streams_gc_queue_taken, #{id => Id, slab => Slab}),
    case emqx_streams_prop:is_append_only(StreamInfo) of
        true ->
            {noreply, State, {continue, refill}};
        false ->
            Entry = #{queue => StreamInfo, failures => 0},
            case send_batch(Id, State#st{queues = Queues#{Id => Entry}}) of
                {noreply, State1} -> {noreply, State1, {continue, refill}};
                Result -> Result
            end
    end.

send_batch(Id, #st{slab = Slab, queues = Queues, refs = Refs} = State) ->
    case prepare(maps:get(Id, Queues), Slab) of
        skip ->
            done(Id, State);
        {error, Error, Entry} ->
            failure(Id, Error, State#st{queues = Queues#{Id => Entry}});
        {ok, #{topic := Topic, cutoff := Cutoff, stream := Stream} = Entry} ->
            State1 = State#st{queues = Queues#{Id => Entry}},
            Opts = #{sync => false, dirty => true},
            case emqx_ds:del_batch(?DB, Stream, Topic, 0, Cutoff, Opts) of
                {async, Ref} ->
                    ?tp(streams_gc_batch_sent, #{slab => Slab, id => Id}),
                    {noreply, State1#st{refs = Refs#{Ref => Id}}};
                Outcome ->
                    outcome(Id, Outcome, State1)
            end
    end.

%% Compute the cutoff and find the stream once, on the queue's first batch.
prepare(#{stream := _} = Entry, _Slab) ->
    {ok, Entry};
prepare(#{topic := Topic} = Entry, {Shard, _} = Slab) ->
    case emqx_ds:get_streams(?DB, Topic, 0, #{shard => Shard}) of
        {Streams, []} ->
            %% Each last-value message stream has one DS stream per slab.
            case [Stream || {S, Stream} <- Streams, S =:= Slab] of
                [] -> skip;
                [Stream] -> {ok, Entry#{stream => Stream}}
            end;
        {_, Errors} ->
            {error, {stream_lookup, Errors}, Entry}
    end;
prepare(#{queue := StreamInfo} = Entry, Slab) ->
    try emqx_streams_message_db:gc_info(StreamInfo, Slab) of
        skip -> skip;
        Info -> prepare(maps:merge(Entry, Info), Slab)
    catch
        Class:Reason -> {error, {Class, Reason}, Entry}
    end.

outcome(Id, {ok, true}, State) ->
    send_batch(Id, State);
outcome(Id, {ok, false}, State) ->
    done(Id, State);
outcome(Id, {error, unrecoverable, {storage_not_found, _}}, State) ->
    done(Id, State);
outcome(Id, Error, State) ->
    failure(Id, Error, State).

failure(Id, Error, #st{slab = Slab, queues = Queues} = State0) ->
    State = count_shard_failure(Error, State0),
    #{failures := Failures0} = Entry = maps:get(Id, Queues),
    Failures = Failures0 + 1,
    case State of
        #st{shard_failures = ShardFailures} when ShardFailures >= ?SHARD_FAILURE_LIMIT ->
            ?tp(warning, streams_gc_slab_failed, #{slab => Slab, reason => Error}),
            {stop, normal, State};
        _ when Failures >= ?QUEUE_FAILURE_LIMIT ->
            ?tp(warning, streams_gc_queue_failed, #{slab => Slab, id => Id, reason => Error}),
            done(Id, State);
        _ ->
            ?tp(streams_gc_queue_retry, #{slab => Slab, id => Id, failures => Failures}),
            erlang:send_after(?RETRY_INTERVAL, self(), #retry{id = Id}),
            {noreply, State#st{queues = Queues#{Id => Entry#{failures := Failures}}}}
    end.

count_shard_failure(Error, #st{shard_failures = N} = State) ->
    case is_shard_failure(Error) of
        true -> State#st{shard_failures = N + 1};
        false -> State
    end.

is_shard_failure({error, recoverable, _}) -> true;
is_shard_failure({stream_lookup, _}) -> true;
is_shard_failure(_) -> false.

done(Id, #st{queues = Queues} = State) ->
    {noreply, State#st{queues = maps:remove(Id, Queues)}, {continue, refill}}.
