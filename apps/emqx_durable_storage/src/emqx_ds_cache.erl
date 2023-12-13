%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_cache).

-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/1,
    stop/1,

    try_fetch_cache/4,

    add_cached_topic_filters/2,
    remove_cached_topic_filters/2,
    set_gc_interval/2
]).

%% for debugging/test
-export([get_info/1]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-ifdef(TEST).
-export([delete/2, delete/3]).
-endif.

-define(KEY(STREAM, LASTKEY), {STREAM, {LASTKEY}}).
-define(EOS_KEY(STREAM), {STREAM, []}).
-define(EOS, []).
-define(cache_ptkey(DB), {?MODULE, cache, DB}).
-define(cache(DB), (persistent_term:get(?cache_ptkey(DB)))).
-define(REF(DB), {via, gproc, {n, l, {?MODULE, DB}}}).

%% call/cast/info records
-record(add_cached_topic_filters, {topic_filters :: [emqx_ds:topic_filter()]}).
-record(remove_cached_topic_filters, {topic_filters :: [emqx_ds:topic_filter()]}).
-record(set_gc_interval, {gc_interval :: pos_integer()}).
-record(renew_streams, {}).
-record(pull, {}).
-record(gc, {}).
-record(get_info, {}).

-type start_opts() :: #{
    db := emqx_ds:db(),
    batch_size => pos_integer(),
    gc_interval => pos_integer(),
    topic_filters => [emqx_ds:topic_filter()]
}.

-type iterators() :: #{
    emqx_ds:topic_filter() => streams()
}.
-type streams() :: #{
    emqx_ds:stream() =>
        #{
            it := emqx_ds:iterator() | ?EOS,
            seqno := seqno() | undefined
        }
}.
-type state() :: #{
    db := emqx_ds:db(),
    batch_size := pos_integer(),
    gc_interval := pos_integer(),
    iterators := iterators(),
    start_time := emqx_ds:time()
}.

-type cache_fetch_result() ::
    false
    | {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}, ...]}
    | {ok, end_of_stream}.

-type seqno() :: non_neg_integer().
-type timestamp() :: integer().

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

-spec start_link(start_opts()) -> gen_server:start_ret().
start_link(Opts) ->
    #{db := DB} = Opts,
    gen_server:start_link(?REF(DB), ?MODULE, Opts, []).

-spec stop(emqx_ds:db()) -> ok.
stop(DB) ->
    gen_server:stop(?REF(DB)).

-spec try_fetch_cache(emqx_ds:db(), emqx_ds:stream(), emqx_ds:message_key(), pos_integer()) ->
    cache_fetch_result().
try_fetch_cache(DB, Stream, LastSeenKey, BatchSize) ->
    try
        case ets:member(?cache(DB), ?KEY(Stream, LastSeenKey)) of
            true ->
                fetch_cache(DB, Stream, LastSeenKey, BatchSize);
            false ->
                ?tp(ds_cache_miss, #{stream => Stream, last_seen_key => LastSeenKey}),
                false
        end
    catch
        error:badarg ->
            false
    end.

%% debug/test only
-spec get_info(emqx_ds:db()) -> map().
get_info(DB) ->
    gen_server:call(?REF(DB), #get_info{}).

-spec add_cached_topic_filters(emqx_ds:db(), [emqx_ds:topic_filter()]) -> ok.
add_cached_topic_filters(DB, TopicFilters) ->
    gen_server:cast(?REF(DB), #add_cached_topic_filters{topic_filters = TopicFilters}).

-spec remove_cached_topic_filters(emqx_ds:db(), [emqx_ds:topic_filter()]) -> ok.
remove_cached_topic_filters(DB, TopicFilters) ->
    gen_server:cast(?REF(DB), #remove_cached_topic_filters{topic_filters = TopicFilters}).

-spec set_gc_interval(emqx_ds:db(), pos_integer()) -> ok.
set_gc_interval(DB, GCInterval) ->
    gen_server:cast(?REF(DB), #set_gc_interval{gc_interval = GCInterval}).

%%--------------------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------------------

-spec init(start_opts()) -> {ok, state()}.
init(Opts) ->
    process_flag(trap_exit, true),
    DB = maps:get(db, Opts),
    Tid = create_table(),
    persistent_term:put(?cache_ptkey(DB), Tid),
    InitialTopicFilters = maps:get(topic_filters, Opts, []),
    BatchSize = maps:get(batch_size, Opts, 500),
    GCInterval = maps:get(gc_interval, Opts, timer:seconds(60)),
    case InitialTopicFilters of
        [] ->
            ok;
        _ ->
            ?MODULE:add_cached_topic_filters(DB, InitialTopicFilters)
    end,
    State = #{
        db => DB,
        batch_size => BatchSize,
        gc_interval => GCInterval,
        iterators => #{},
        start_time => now_ms()
    },
    start_timer(#renew_streams{}, 100),
    start_timer(#pull{}, 100),
    start_timer(#gc{}, GCInterval),
    {ok, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, #{db := DB}) ->
    persistent_term:erase(?cache_ptkey(DB)),
    ok.

handle_call(#get_info{}, _From, State) ->
    #{iterators := Iterators, gc_interval := GCInterval} = State,
    TopicFilters = [emqx_topic:join(TopicFilterWords) || TopicFilterWords <- maps:keys(Iterators)],
    Reply = #{
        gc_interval => GCInterval,
        topic_filters => TopicFilters
    },
    {reply, Reply, State};
handle_call(_Call, _From, State) ->
    {reply, error, State}.

handle_cast(#add_cached_topic_filters{topic_filters = TopicFilters}, State0) ->
    ?tp(ds_cache_add_cached, #{topic_filters => TopicFilters}),
    State = handle_add_cached_topic_filters(TopicFilters, State0),
    {noreply, State};
handle_cast(#remove_cached_topic_filters{topic_filters = TopicFilters}, State0) ->
    ?tp(ds_cache_remove_cached, #{topic_filters => TopicFilters}),
    State = handle_remove_cached_topic_filters(TopicFilters, State0),
    {noreply, State};
handle_cast(#set_gc_interval{gc_interval = GCInterval}, State0) ->
    ?tp(ds_cache_set_gc_interval, #{gc_interval => GCInterval}),
    State = State0#{gc_interval := GCInterval},
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#renew_streams{}, State0) ->
    State = handle_renew_streams(State0),
    start_timer(#renew_streams{}, 100),
    {noreply, State};
handle_info(#pull{}, State0) ->
    State = handle_pull(State0),
    start_timer(#pull{}, 100),
    {noreply, State};
handle_info(#gc{}, State) ->
    #{gc_interval := GCInterval} = State,
    handle_gc(State),
    start_timer(#gc{}, GCInterval),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------------------

-record(entry, {
    key,
    seqno,
    inserted_at,
    message
}).
-type entry() :: #entry{}.

-spec create_table() -> ets:tid().
create_table() ->
    ets:new(emqx_ds_cache, [
        public,
        ordered_set,
        {keypos, #entry.key},
        {read_concurrency, true}
    ]).

-spec fetch_cache(emqx_ds:db(), emqx_ds:stream(), undefined | emqx_ds:message_key(), pos_integer()) ->
    cache_fetch_result().
fetch_cache(DB, Stream, LastSeenKey, BatchSize) ->
    %% confirm that GC hasn't cleared entries in the meantime
    case ets:lookup(?cache(DB), ?KEY(Stream, LastSeenKey)) of
        [#entry{key = ?KEY(Stream, LastSeenKey) = Key, seqno = Seqno}] ->
            Next = ets:next(?cache(DB), Key),
            ?tp(ds_cache_will_fetch, #{
                key => Next,
                stream => Stream,
                expected_seqno => Seqno + 1,
                last_seen_key => LastSeenKey
            }),
            do_fetch_cache(Next, DB, Stream, Seqno + 1, BatchSize, _Acc = []);
        _ ->
            %% race condition: previous key doesn't match; GC may have run.
            false
    end.

-spec do_fetch_cache(
    '$end_of_table' | ?EOS_KEY(emqx_ds:stream()) | ?KEY(emqx_ds:stream(), emqx_ds:message_key()),
    emqx_ds:db(),
    emqx_ds:stream(),
    seqno(),
    _Remaining :: pos_integer(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    cache_fetch_result().
do_fetch_cache('$end_of_table' = Key, _DB, Stream, _ExpectedSeqno, _Remaining, Acc) ->
    with_last_key(Acc, Stream, Key);
do_fetch_cache(?EOS_KEY(Stream) = Key, _DB, Stream, _ExpectedSeqno, _Remaining, Acc) ->
    with_last_key(Acc, Stream, Key);
do_fetch_cache(Key, _DB, Stream, _ExpectedSeqno, Remaining, Acc) when Remaining =< 0 ->
    with_last_key(Acc, Stream, Key);
do_fetch_cache(Key, DB, Stream, ExpectedSeqno, Remaining, Acc) ->
    ?tp(ds_cache_lookup_enter, #{key => Key, stream => Stream, seqno => ExpectedSeqno}),
    case ets:lookup(?cache(DB), Key) of
        [#entry{key = ?KEY(Stream, DSKey), seqno = ExpectedSeqno, message = Message}] ->
            NextKey = ets:next(?cache(DB), Key),
            NewAcc = [{DSKey, Message} | Acc],
            do_fetch_cache(NextKey, DB, Stream, ExpectedSeqno + 1, Remaining - 1, NewAcc);
        _ ->
            %% Either:
            %%   i)   There's no entry;
            %%   ii)  The streams don't match;
            %%   iii) The seqnos don't match.
            with_last_key(Acc, Stream, Key)
    end.

-spec with_last_key(
    [{emqx_ds:message_key(), emqx_types:message()}],
    emqx_ds:stream(),
    '$end_of_table' | ?EOS_KEY(emqx_ds:stream()) | ?KEY(emqx_ds:stream(), emqx_ds:message_key())
) ->
    cache_fetch_result().
with_last_key(Acc, Stream, Key) ->
    case Acc of
        [{LastKey, _} | _] ->
            Batch = lists:reverse(Acc),
            ?tp(ds_cache_hit, #{stream => Stream, batch => Batch, last_key => LastKey}),
            {ok, LastKey, Batch};
        [] when Key =:= ?EOS_KEY(Stream) ->
            ?tp(ds_cache_eos_found, #{}),
            {ok, end_of_stream};
        [] ->
            %% Possible if entry was removed just before we started iterating, or if only
            %% the last seen key of the iterator is contained in the cache.
            ?tp(ds_cache_empty_result, #{}),
            false
    end.

-spec insert_end_of_stream(emqx_ds:db(), timestamp(), seqno(), emqx_ds:stream()) -> ok.
insert_end_of_stream(DB, Now, Seqno, Stream) ->
    Entry =
        #entry{
            key = ?EOS_KEY(Stream),
            seqno = Seqno,
            inserted_at = Now,
            message = ?EOS
        },
    ets:insert(?cache(DB), Entry),
    ?tp(ds_cache_eos_inserted, #{stream => Stream}),
    ok.

-spec store_batch(
    emqx_ds:db(),
    timestamp(),
    seqno(),
    emqx_ds:stream(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    seqno().
store_batch(DB, Now, Seqno, Stream, Batch = [_ | _]) ->
    {NewSeqno, Entries} = to_entries(Now, Seqno, Stream, Batch),
    ets:insert(?cache(DB), Entries),
    ?tp(ds_cache_stored_batch, #{stream => Stream, batch => Batch}),
    NewSeqno;
store_batch(_DB, _Now, Seqno, _Stream, _Batch = []) ->
    Seqno.

-spec to_entries(
    timestamp(),
    seqno(),
    emqx_ds:stream(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    {seqno(), [entry()]}.
to_entries(Now, Seqno, Stream, Batch) ->
    to_entries(Now, Seqno, Stream, Batch, []).

-spec to_entries(
    timestamp(),
    seqno(),
    emqx_ds:stream(),
    [{emqx_ds:message_key(), emqx_types:message()}],
    [entry()]
) ->
    {seqno(), [entry()]}.
to_entries(Now, Seqno, Stream, [{DSKey, Msg} | Rest], Acc) ->
    Entry = #entry{
        key = ?KEY(Stream, DSKey),
        seqno = Seqno,
        inserted_at = Now,
        message = Msg
    },
    to_entries(Now, Seqno + 1, Stream, Rest, [Entry | Acc]);
to_entries(_Now, Seqno, _Stream, [], Acc) ->
    {Seqno, lists:reverse(Acc)}.

-spec handle_renew_streams(state()) -> state().
handle_renew_streams(State0) ->
    #{db := DB, iterators := Iterators0, start_time := StartTime} = State0,
    Iterators = renew_streams(DB, StartTime, Iterators0),
    State0#{iterators := Iterators}.

-spec renew_streams(emqx_ds:db(), emqx_ds:time(), iterators()) -> iterators().
renew_streams(DB, StartTime, Iterators0) ->
    ?tp_span(
        ds_cache_renew_streams,
        #{start_time => StartTime},
        maps:fold(
            fun(TopicFilter, StreamsToIt, Acc) ->
                ExistingStreams = maps:keys(StreamsToIt),
                StreamsAndRanks = emqx_ds:get_streams(DB, TopicFilter, StartTime),
                Streams = [Stream || {_Rank, Stream} <- StreamsAndRanks],
                NewStreams = Streams -- ExistingStreams,
                Its =
                    lists:foldl(
                        fun(Stream, AccInner) ->
                            {ok, It} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
                            AccInner#{Stream => #{it => It, seqno => 0}}
                        end,
                        StreamsToIt,
                        NewStreams
                    ),
                lists:foreach(fun(Stream) -> mark_tracked(DB, Stream) end, NewStreams),
                Acc#{TopicFilter => Its}
            end,
            Iterators0,
            Iterators0
        )
    ).

-spec handle_pull(state()) -> state().
handle_pull(State0) ->
    #{
        db := DB,
        iterators := Iterators0,
        batch_size := BatchSize
    } = State0,
    Iterators =
        maps:fold(
            fun(TopicFilter, StreamsToIt0, Acc) ->
                StreamsToIt = do_pull(DB, BatchSize, StreamsToIt0),
                Acc#{TopicFilter := StreamsToIt}
            end,
            Iterators0,
            Iterators0
        ),
    State0#{iterators := Iterators}.

-spec do_pull(emqx_ds:db(), pos_integer(), streams()) -> streams().
do_pull(DB, BatchSize, StreamsToIt) ->
    maps:fold(
        fun
            (_Stream, #{it := ?EOS}, Acc) ->
                Acc;
            (Stream, #{it := It0, seqno := Seqno}, Acc) ->
                {It, NewSeqno} = do_pull1(DB, BatchSize, Stream, It0, Seqno),
                Acc#{Stream => #{it => It, seqno => NewSeqno}}
        end,
        StreamsToIt,
        StreamsToIt
    ).

-spec do_pull1(emqx_ds:db(), pos_integer(), emqx_ds:stream(), emqx_ds:iterator(), seqno()) ->
    {emqx_ds:iterator(), seqno()} | {?EOS, undefined}.
do_pull1(DB, BatchSize, Stream, It0, Seqno) ->
    case emqx_ds:next(DB, It0, BatchSize, #{use_cache => false}) of
        {ok, It, Batch} ->
            Now = now_ms(),
            NewSeqno = store_batch(DB, Now, Seqno, Stream, Batch),
            {It, NewSeqno};
        {ok, end_of_stream} ->
            Now = now_ms(),
            insert_end_of_stream(DB, Now, Seqno, Stream),
            {?EOS, undefined};
        {error, _Error} ->
            %% log?
            {It0, Seqno}
    end.

-spec handle_add_cached_topic_filters([emqx_ds:topic_filter()], state()) -> state().
handle_add_cached_topic_filters(TopicFilters, State0) ->
    #{db := DB, iterators := Iterators0} = State0,
    NewTopicFilters = TopicFilters -- maps:keys(Iterators0),
    NewIterators0 = maps:from_keys(NewTopicFilters, #{}),
    StartTime = now_ms(),
    NewIterators = renew_streams(DB, StartTime, NewIterators0),
    Iterators = maps:merge(Iterators0, NewIterators),
    State0#{iterators := Iterators}.

-spec handle_remove_cached_topic_filters([emqx_ds:topic_filter()], state()) -> state().
handle_remove_cached_topic_filters(TopicFilters, State0) ->
    #{iterators := Iterators0} = State0,
    Iterators = maps:without(TopicFilters, Iterators0),
    State0#{iterators := Iterators}.

-spec handle_gc(state()) -> ok.
handle_gc(State) ->
    #{db := DB, gc_interval := GCInterval} = State,
    Now = now_ms(),
    MS = gc_ms(Now, GCInterval),
    NumDeleted = ets:select_delete(?cache(DB), MS),
    ?tp(ds_cache_gc_ran, #{num_deleted => NumDeleted}),
    ok.

gc_ms(Now, Threshold) ->
    ets:fun2ms(fun(#entry{inserted_at = InsertedAt}) when
        InsertedAt + Threshold =< Now
    ->
        true
    end).

now_ms() ->
    erlang:system_time(millisecond).

start_timer(Event, Timeout) ->
    erlang:send_after(Timeout, self(), Event),
    ok.

-ifdef(TEST).
delete(DB, Key = ?KEY(_, _)) ->
    ets:delete(?cache(DB), Key),
    ok.

delete(DB, Stream, DSKey) ->
    delete(DB, ?KEY(Stream, DSKey)).
%% ifdef(TEST)
-endif.
