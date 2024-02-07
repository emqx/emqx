%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_cache_coordinator).

-behaviour(gen_server).

%% API
-export([
    start_link/1, start_link/2,

    is_cache_enabled/0,
    try_fetch_cache/4
]).

%% For testing
-export([renew_streams/1]).

%% API for cache workers
-export([mark_end_of_stream/2]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_ds_cache.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(APP, emqx_durable_storage).
-define(via(REC), {via, gproc, {n, l, REC}}).
-define(cache_coord, ?MODULE).
-define(cache_ptkey(DB), {?MODULE, cache, DB}).
-define(meta_cache_key(STREAM), {cache, STREAM}).

-record(?cache_coord, {db}).

-type seqno() :: non_neg_integer().

-type cache_opts() :: #{cache_prefetch_topic_filters => [emqx_ds:topic_filter()]}.
-type state() :: #{
    db := emqx_ds:db(),
    tab := ets:tid(),
    streams := streams()
}.

-type cache_fetch_result() ::
    false
    | {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}, ...]}
    | {ok, end_of_stream}.

-type streams() :: #{emqx_ds:topic_filter() => [emqx_ds:stream()]}.

%% call/cast/info events
-record(renew_streams, {}).
-record(mark_end_of_stream, {stream :: emqx_ds:stream()}).

%%================================================================================
%% API
%%================================================================================

-spec is_cache_enabled() -> boolean().
is_cache_enabled() ->
    application:get_env(?APP, cache_enabled, false).

-spec start_link(emqx_ds:db()) -> supervisor:startchild_ret().
start_link(DB) ->
    start_link(DB, _Opts = #{}).

-spec start_link(emqx_ds:db(), cache_opts()) -> supervisor:startchild_ret().
start_link(DB, CacheOpts) ->
    gen_server:start_link(?via(#?cache_coord{db = DB}), ?MODULE, {DB, CacheOpts}, _Opts = []).

-spec try_fetch_cache(
    emqx_ds:db(),
    emqx_ds:ds_specific_stream(),
    emqx_ds:ds_specific_iterator(),
    pos_integer()
) ->
    cache_fetch_result().
try_fetch_cache(DB, Stream, Iter, BatchSize) ->
    try
        case may_serve(DB, Stream, Iter) of
            {ok, StreamTid, TopicFilter, LastSeenKey} ->
                fetch_cache(StreamTid, DB, Stream, TopicFilter, LastSeenKey, BatchSize);
            false ->
                ?tp(ds_cache_miss, #{stream => Stream}),
                false
        end
    catch
        error:badarg ->
            false
    end.

%%================================================================================
%% API (used by cache worker)
%%================================================================================

-spec mark_end_of_stream(emqx_ds:db(), emqx_ds:stream()) -> ok.
mark_end_of_stream(DB, Stream) ->
    gen_server:cast(?via(#?cache_coord{db = DB}), #mark_end_of_stream{stream = Stream}).

%%================================================================================
%% API (testing)
%%================================================================================

-spec renew_streams(emqx_ds:db()) -> ok.
renew_streams(DB) ->
    gen_server:call(?via(#?cache_coord{db = DB}), #renew_streams{}, infinity).

%%================================================================================
%% `gen_server' API
%%================================================================================

-spec init({emqx_ds:db(), cache_opts()}) -> {ok, state()}.
init({DB, CacheOpts}) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{db => DB, domain => [ds, cache, coordinator]}),
    Tid = create_table(),
    persistent_term:put(?cache_ptkey(DB), Tid),
    TopicFilters =
        maps:get(
            cache_prefetch_topic_filters,
            CacheOpts,
            application:get_env(?APP, cache_prefetch_topic_filters, [])
        ),
    TopicFiltersToStreams = maps:from_keys(TopicFilters, []),
    State = #{
        db => DB,
        tab => Tid,
        streams => TopicFiltersToStreams
    },
    start_timer(#renew_streams{}, 0),
    {ok, State}.

-spec terminate(any(), state()) -> ok.
terminate(_Reason, #{db := DB}) ->
    persistent_term:erase(?cache_ptkey(DB)),
    ok.

handle_call(#renew_streams{}, _From, State0) ->
    %% This is just a sync call for tests.
    State = handle_renew_streams(State0),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(#mark_end_of_stream{stream = Stream}, State) ->
    handle_mark_end_of_stream(State, Stream),
    {reply, ok, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#renew_streams{}, State0) ->
    State = handle_renew_streams(State0),
    start_timer(#renew_streams{}, 5_000),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec create_table() -> ets:tid().
create_table() ->
    ets:new(?MODULE, [
        protected,
        ordered_set,
        {read_concurrency, true}
    ]).

-spec may_serve(emqx_ds:db(), emqx_ds:stream(), emqx_ds:iterator()) ->
    false | {ok, ets:tid(), emqx_ds:topic_filter(), emqx_ds:message_key()}.
may_serve(DB, Stream, Iter) ->
    case persistent_term:get(?cache_ptkey(DB), undefined) of
        undefined ->
            false;
        Tid ->
            do_may_serve(DB, Tid, Stream, Iter)
    end.

-spec do_may_serve(emqx_ds:db(), ets:tid(), emqx_ds:stream(), emqx_ds:iterator()) ->
    false | {ok, ets:tid(), emqx_ds:topic_filter(), emqx_ds:message_key()}.
do_may_serve(DB, MetaTid, Stream, Iter) ->
    case emqx_utils_ets:lookup_value(MetaTid, ?meta_cache_key(Stream)) of
        #{table := StreamTid, extractor_fn := ExtractorFn} ->
            #{
                last_seen_key := LastSeenKey,
                topic_filter := TopicFilter
            } = emqx_ds:extract_iterator_info(DB, Iter, ExtractorFn),
            case emqx_utils_ets:member(StreamTid, LastSeenKey) of
                true ->
                    {ok, StreamTid, TopicFilter, LastSeenKey};
                false ->
                    false
            end;
        _ ->
            false
    end.

-spec fetch_cache(
    ets:tid(),
    emqx_ds:db(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    undefined | emqx_ds:message_key(),
    pos_integer()
) ->
    cache_fetch_result().
fetch_cache(StreamTid, DB, Stream, TopicFilter, LastSeenKey, BatchSize) ->
    %% confirm that GC hasn't cleared entries in the meantime
    case ets:lookup(StreamTid, LastSeenKey) of
        [#cache_entry{key = LastSeenKey, seqno = Seqno}] ->
            Next = ets:next(StreamTid, LastSeenKey),
            ?tp(ds_cache_will_fetch, #{
                key => Next,
                stream => Stream,
                expected_seqno => Seqno + 1,
                last_seen_key => LastSeenKey
            }),
            do_fetch_cache(
                Next,
                StreamTid,
                DB,
                Stream,
                TopicFilter,
                LastSeenKey,
                Seqno + 1,
                BatchSize,
                _Acc = []
            );
        _ ->
            %% race condition: previous key doesn't match; GC may have run.
            false
    end.

-spec do_fetch_cache(
    '$end_of_table' | emqx_ds:message_key(),
    ets:tid(),
    emqx_ds:db(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:message_key(),
    seqno(),
    _Remaining :: pos_integer(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    cache_fetch_result().
do_fetch_cache(
    '$end_of_table',
    _StreamTid,
    DB,
    Stream,
    _TopicFilter,
    OrigLastSeenKey,
    _ExpectedSeqno,
    _Remaining,
    Acc
) ->
    with_last_key(Acc, DB, Stream, OrigLastSeenKey);
do_fetch_cache(
    _Key, _StreamTid, DB, Stream, _TopicFilter, OrigLastSeenKey, _ExpectedSeqno, Remaining, Acc
) when
    Remaining =< 0
->
    with_last_key(Acc, DB, Stream, OrigLastSeenKey);
do_fetch_cache(
    Key, StreamTid, DB, Stream, TopicFilter, OrigLastSeenKey, ExpectedSeqno, Remaining, Acc
) ->
    ?tp(ds_cache_lookup_enter, #{key => Key, stream => Stream, seqno => ExpectedSeqno}),
    case ets:lookup(StreamTid, Key) of
        [#cache_entry{key = DSKey, seqno = ExpectedSeqno, message = Message}] ->
            NextKey = ets:next(StreamTid, Key),
            #message{topic = Topic} = Message,
            case emqx_topic:match(emqx_topic:words(Topic), TopicFilter) of
                true ->
                    NewAcc = [{DSKey, Message} | Acc],
                    do_fetch_cache(
                        NextKey,
                        StreamTid,
                        DB,
                        Stream,
                        TopicFilter,
                        OrigLastSeenKey,
                        ExpectedSeqno + 1,
                        Remaining - 1,
                        NewAcc
                    );
                false ->
                    do_fetch_cache(
                        NextKey,
                        StreamTid,
                        DB,
                        Stream,
                        TopicFilter,
                        OrigLastSeenKey,
                        ExpectedSeqno + 1,
                        Remaining,
                        Acc
                    )
            end;
        _ ->
            %% Either:
            %%   i)   There's no entry;
            %%   ii)  The streams don't match;
            %%   iii) The seqnos don't match.
            with_last_key(Acc, DB, Stream, OrigLastSeenKey)
    end.

-spec with_last_key(
    [{emqx_ds:message_key(), emqx_types:message()}],
    emqx_ds:db(),
    emqx_ds:stream(),
    emqx_ds:message_key()
) ->
    cache_fetch_result().
with_last_key(Acc, DB, Stream, OrigLastSeenKey) ->
    case Acc of
        [{LastKey, _} | _] ->
            Batch = lists:reverse(Acc),
            ?tp(ds_cache_hit, #{stream => Stream, batch => Batch, last_key => LastKey}),
            {ok, LastKey, Batch};
        [] ->
            case is_end_of_stream(DB, Stream) of
                true ->
                    {ok, end_of_stream};
                false ->
                    %% Possible if entry was removed just before we started iterating, or if only
                    %% the last seen key of the iterator is contained in the cache.
                    ?tp(ds_cache_empty_result, #{}),
                    {ok, OrigLastSeenKey, []}
            end
    end.

-spec handle_renew_streams(state()) -> state().
handle_renew_streams(State0) ->
    TopicFiltersToStreams = do_renew_streams(State0),
    State0#{streams := TopicFiltersToStreams}.

-spec do_renew_streams(state()) -> streams().
do_renew_streams(State0) ->
    #{
        db := DB,
        tab := MetaTid,
        streams := TopicFiltersToStreams0
    } = State0,
    StartTime = now_ms(),
    ?tp_span(
        ds_cache_renew_streams,
        #{start_time => StartTime},
        maps:fold(
            fun(TopicFilter, KnownStreams, Acc) ->
                StreamsAndRanks = emqx_ds:get_streams(DB, TopicFilter, StartTime),
                Streams = [Stream || {_Rank, Stream} <- StreamsAndRanks],
                NewStreams = Streams -- KnownStreams,
                DeletedStreams = KnownStreams -- Streams,
                lists:foreach(
                    fun(NewStream) ->
                        handle_new_stream(DB, MetaTid, NewStream, TopicFilter, StartTime)
                    end,
                    NewStreams
                ),
                lists:foreach(
                    fun(DeletedStream) ->
                        handle_deleted_stream(DB, MetaTid, DeletedStream)
                    end,
                    DeletedStreams
                ),
                Acc#{TopicFilter => Streams}
            end,
            TopicFiltersToStreams0,
            TopicFiltersToStreams0
        )
    ).

-spec handle_new_stream(
    emqx_ds:db(),
    ets:tid(),
    emqx_ds:ds_specific_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> ok.
handle_new_stream(DB, MetaTid, NewStream, TopicFilter, StartTime) ->
    case emqx_ds:iterator_info_extractor(DB, NewStream) of
        {ok, ExtractorFn} ->
            ok = emqx_ds_builtin_db_sup:ensure_cache_worker_started(
                DB, NewStream, TopicFilter, StartTime
            ),
            StreamTid = emqx_ds_cache_worker:get_table(DB, NewStream),
            MetaValue = #{
                table => StreamTid,
                extractor_fn => ExtractorFn,
                end_of_stream => false
            },
            ets:insert(MetaTid, {?meta_cache_key(NewStream), MetaValue}),
            ?tp(ds_cache_new_stream, #{}),
            ok;
        undefined ->
            ok
    end.

-spec handle_deleted_stream(
    emqx_ds:db(),
    ets:tid(),
    emqx_ds:stream()
) -> ok.
handle_deleted_stream(DB, MetaTid, DeletedStream) ->
    ets:delete(MetaTid, ?meta_cache_key(DeletedStream)),
    ok = emqx_ds_builtin_db_sup:ensure_cache_worker_stopped(DB, DeletedStream),
    ok.

-spec handle_mark_end_of_stream(state(), emqx_ds:stream()) -> ok.
handle_mark_end_of_stream(State, Stream) ->
    #{tab := MetaTid} = State,
    case emqx_utils_ets:lookup_value(MetaTid, ?meta_cache_key(Stream)) of
        undefined ->
            ok;
        MetaValue0 ->
            MetaValue = MetaValue0#{end_of_stream := true},
            _ = ets:insert(MetaTid, {?meta_cache_key(Stream), MetaValue}),
            ?tp(ds_cache_end_of_stream, #{stream => Stream}),
            ok
    end.

is_end_of_stream(DB, Stream) ->
    case persistent_term:get(?cache_ptkey(DB), undefined) of
        undefined ->
            false;
        MetaTid ->
            case emqx_utils_ets:lookup_value(MetaTid, ?meta_cache_key(Stream)) of
                #{end_of_stream := EoS} ->
                    EoS;
                _ ->
                    false
            end
    end.

start_timer(Event, Timeout) ->
    erlang:send_after(Timeout, self(), Event),
    ok.

now_ms() ->
    erlang:system_time(millisecond).
