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

-module(emqx_ds_cache_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/4,

    get_table/2
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds_cache.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(APP, emqx_durable_storage).
-define(via(REC), {via, gproc, {n, l, REC}}).
-define(cache_worker, ?MODULE).

-record(?cache_worker, {db, stream}).

-type cache_entry() :: #cache_entry{}.
-type seqno() :: non_neg_integer().
-type timestamp() :: integer().

-type state() :: #{
    db := emqx_ds:db(),
    stream := emqx_ds:stream(),
    table := ets:tid(),
    iterator := ?EOS_MARKER | emqx_ds:iterator(),
    seqno := undefined | seqno()
}.

%% call/cast/info records
-record(get_table, {}).
-record(pull, {}).

%%================================================================================
%% API
%%================================================================================

-spec start_link(emqx_ds:db(), emqx_ds:stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    supervisor:startchild_ret().
start_link(DB, Stream, TopicFilter, StartTime) ->
    gen_server:start_link(
        ?via(#?cache_worker{db = DB, stream = Stream}),
        ?MODULE,
        {DB, Stream, TopicFilter, StartTime},
        _Opts = []
    ).

-spec get_table(emqx_ds:db(), emqx_ds:ds_specific_stream()) -> ets:tid().
get_table(DB, Stream) ->
    gen_server:call(?via(#?cache_worker{db = DB, stream = Stream}), #get_table{}, infinity).

%%================================================================================
%% `gen_server' API
%%================================================================================

-spec init({emqx_ds:db(), emqx_ds:stream(), emqx_ds:topic_filter(), emqx_ds:time()}) ->
    {ok, state()}.
init({DB, Stream, TopicFilter, StartTime}) ->
    logger:set_process_metadata(#{db => DB, stream => Stream, domain => [ds, cache, worker]}),
    ?tp(ds_cache_starting_worker, #{stream => Stream, start_time => StartTime}),
    Tid = create_table(),
    {ok, Iter} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    State =
        #{
            db => DB,
            stream => Stream,
            table => Tid,
            iterator => Iter,
            seqno => 0
        },
    start_timer(#pull{}, 0),
    {ok, State}.

handle_call(#get_table{}, _From, State) ->
    #{table := Tid} = State,
    {reply, Tid, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#pull{}, State0) ->
    {noreply, handle_pull(State0)};
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
        {keypos, #cache_entry.key},
        {read_concurrency, true}
    ]).

-spec handle_pull(state()) -> state().
handle_pull(State0 = #{iterator := ?EOS_MARKER}) ->
    State0;
handle_pull(State0) ->
    #{
        db := DB,
        stream := Stream,
        table := StreamTid,
        iterator := Iter0,
        seqno := Seqno0
    } = State0,
    {Iter, Seqno} = do_pull(DB, Stream, StreamTid, Iter0, Seqno0),
    case Seqno0 =:= Seqno of
        true ->
            start_timer(#pull{}, 100);
        false ->
            start_timer(#pull{}, 0)
    end,
    State0#{iterator := Iter, seqno := Seqno}.

-spec do_pull(emqx_ds:db(), emqx_ds:stream(), ets:tid(), emqx_ds:iterator(), seqno()) ->
    {emqx_ds:iterator(), seqno()} | {?EOS_MARKER, undefined}.
do_pull(DB, Stream, StreamTid, Iter0, Seqno) ->
    BatchSize = batch_size(),
    case emqx_ds:next(DB, Iter0, BatchSize, #{use_cache => false}) of
        {ok, It, Batch} ->
            Now = now_ms(),
            NewSeqno = store_batch(StreamTid, Now, Seqno, Batch),
            {It, NewSeqno};
        {ok, end_of_stream} ->
            emqx_ds_cache_coordinator:mark_end_of_stream(DB, Stream),
            {?EOS_MARKER, undefined};
        {error, _Error} ->
            %% log?
            {Iter0, Seqno}
    end.

-spec store_batch(
    ets:tid(),
    timestamp(),
    seqno(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    seqno().
store_batch(StreamTid, Now, Seqno, Batch = [_ | _]) ->
    {NewSeqno, Entries} = to_entries(Now, Seqno, Batch),
    ets:insert(StreamTid, Entries),
    ?tp(ds_cache_stored_batch, #{batch => Batch}),
    NewSeqno;
store_batch(_DB, _Now, Seqno, _Batch = []) ->
    Seqno.

-spec to_entries(
    timestamp(),
    seqno(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    {seqno(), [cache_entry()]}.
to_entries(Now, Seqno, Batch) ->
    to_entries(Now, Seqno, Batch, []).

-spec to_entries(
    timestamp(),
    seqno(),
    [{emqx_ds:message_key(), emqx_types:message()}],
    [cache_entry()]
) ->
    {seqno(), [cache_entry()]}.
to_entries(Now, Seqno, [{DSKey, Msg} | Rest], Acc) ->
    Entry = #cache_entry{
        key = DSKey,
        seqno = Seqno,
        message = Msg
    },
    to_entries(Now, Seqno + 1, Rest, [Entry | Acc]);
to_entries(_Now, Seqno, [], Acc) ->
    {Seqno, lists:reverse(Acc)}.

batch_size() ->
    application:get_env(?APP, cache_batch_size, 100).

now_ms() ->
    erlang:system_time(millisecond).

start_timer(Event, Timeout) ->
    erlang:send_after(Timeout, self(), Event),
    ok.
