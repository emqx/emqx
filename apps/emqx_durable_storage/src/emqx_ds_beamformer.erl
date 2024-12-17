%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This process is responsible for processing async poll requests
%% from the consumers. It is designed as a generic, drop-in "frontend"
%% that can be integrated into any DS backend (builtin or external).
%%
%% It serves as a pool for such requests, limiting the number of
%% queries running in parallel. In addition, it tries to group
%% "coherent" poll requests together, so they can be fulfilled as a
%% group ("coherent beam").
%%
%% By "coherent" we mean requests to scan overlapping key ranges of
%% the same DS stream. Grouping requests helps to reduce the number of
%% storage queries and conserve throughput of the EMQX backplane
%% network. This should help in the situations when the majority of
%% clients are up to date and simply wait for the new data.
%%
%% Beamformer works as following:
%%
%% - Initially, requests are added to the "pending" queue.
%%
%% - Beamformer process spins in a "fulfill loop" that takes requests
%% from the pending queue one at a time, and tries to fulfill them
%% normally by quering the storage.
%%
%% - When storage returns a non-empty batch, an unrecoverable error or
%% `end_of_stream', beamformer searches for pending poll requests that
%% may be coherent with the current one. All matching requests are
%% then packed into "beams" (one per destination node) and sent out
%% accodingly.
%%
%% - If the query returns an empty batch, beamformer moves the request
%% to the "wait" queue. Poll requests just linger there until they
%% time out, or until beamformer receives a matching stream event from
%% the storage.
%%
%% The storage backend can send events to the beamformer by calling
%% `shard_event' function.
%%
%% Storage event processing logic is following: if beamformer finds
%% waiting poll requests matching the event, it queries the storage
%% for a batch of data. If batch is non-empty, requests are served
%% exactly as described above. If batch is empty again, request is
%% moved back to the wait queue.

%% WARNING: beamformer makes some implicit assumptions about the
%% storage layout:
%%
%% - For each topic filter and stream, there's a bijection between
%% iterator and the message key
%%
%% - Quering a stream with non-wildcard topic-filter is equivalent to
%% quering it with a wildcard topic filter and dropping messages in
%% postprocessing, e.g.:
%%
%% ```
%% next("foo/bar", StartTime) ==
%%   filter(λ msg. msg.topic == "foo/bar",
%%         next("#", StartTime))
%% '''
-module(emqx_ds_beamformer).

-feature(maybe_expr, enable).

-behaviour(gen_statem).

%% API:
-export([start_link/2]).
-export([poll/5, subscribe/6, unsubscribe/2, shard_event/2, generation_event/1, suback/3]).
-export([unpack_iterator/3, update_iterator/4, scan_stream/6, high_watermark/3, fast_forward/4]).
-export([beams_init/4, beams_add/4, beams_conclude/3, beams_n_matched/1, split/1]).
-export([shard_metrics_id/1, reschedule/2, send_and_forget/3]).
-export([cfg_pending_request_limit/0, cfg_batch_size/0, cfg_housekeeping_interval/0]).

%% internal exports:
-export([do_dispatch/1]).
%% Testing/debugging:
-export([where/1, ls/1, lookup_sub/2, subtab/1]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

-export_type([
    dbshard/0,
    sub_id/0,
    opts/0,
    sub_state/0, sub_state/2,
    beam/2, beam/0,
    return_addr/1,
    unpack_iterator_result/1,
    event_topic/0,
    stream_scan_return/0,
    iterator_type/0,
    beam_maker/0
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds.hrl").
-include("emqx_ds_beamformer.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(idle, idle).
-define(busy, busy).
-define(recovering, recovering).

-type state() ::
    %% The shard is healthy, and there aren't any pending poll or
    %% subscribe requests:
    ?idle
    %% The shard is healthy, and there are some requests that need to
    %% be dispatched to the workers immediately:
    | ?busy
    %% Waiting for shard recovery. Request scheduling has been delayed
    %% and will be retried after a cooldown period:
    | ?recovering.

%% State timeout (for both `idle' and `recovering' states) that
%% triggers handover of subscriptions to the workers:
-define(schedule_timeout, schedule_timeout).

-type dbshard() :: {emqx_ds:db(), _Shard}.

-type sub_id() :: {_Shard, reference()}.

%% `event_topic' and `event_topic_filter' types are structurally (but
%% not semantically) equivalent to their `emqx_ds' counterparts.
%%
%% These types are only used for matching of events against waiting
%% poll requests. Values of these types are never exposed outside.
%% Hence the backend can, for example, compress topics used in the
%% events and iterators.
-type event_topic() :: emqx_ds:topic().
-type event_topic_filter() :: emqx_ds:topic_filter().

-type opts() :: #{
    n_workers := non_neg_integer()
}.

%% Request:

-type return_addr(ItKey) :: {reference(), ItKey} | {pid(), reference(), ItKey}.

-type poll_req_id() :: reference().

-type sub_state(ItKey, Iterator) ::
    #sub_state{
        req_id :: reference(),
        client :: pid(),
        mref :: reference(),
        %% Flow control:
        seqno :: emqx_ds:sub_seqno(),
        acked_seqno :: emqx_ds:sub_seqno(),
        max_unacked :: pos_integer(),
        %%
        rank :: emqx_ds:stream_rank(),
        stream :: _Stream,
        topic_filter :: emqx_ds:topic_filter(),
        start_key :: emqx_ds:message_key(),
        %% Information about the process that created the request:
        return_addr :: return_addr(ItKey),
        %% Iterator:
        it :: Iterator,
        %% Callback that filters messages that belong to the request:
        msg_matcher :: match_messagef(),
        opts :: emqx_ds:sub_opts(),
        deadline :: integer() | undefined
    }.

-type sub_state() :: sub_state(_, _).

%% Response:

-type dispatch_mask() :: bitstring().

-record(beam, {iterators, pack, misc = #{}}).

-opaque beam(ItKey, Iterator) ::
    #beam{
        iterators :: [{return_addr(ItKey), Iterator}],
        pack ::
            [{emqx_ds:message_key(), dispatch_mask(), emqx_types:message()}]
            | end_of_stream
            | emqx_ds:error(_),
        misc :: #{
            seqnos => #{reference() => emqx_ds:sub_seqno()}
        }
    }.

-type beam() :: beam(_ItKey, _Iterator).

-type stream_scan_return() ::
    {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}]}
    | {ok, end_of_stream}
    | emqx_ds:error(_).

-type iterator_type() :: committed | committed_fresh | future.

%% Beam constructor:

-type per_node_requests() :: #{poll_req_id() => sub_state()}.

-type dispatch_matrix() :: #{{poll_req_id(), non_neg_integer()} => _}.

-type filtered_batch() :: [{non_neg_integer(), {emqx_ds:message_key(), emqx_types:message()}}].

-record(beam_maker, {
    cbm :: module(),
    shard_id :: _Shard,
    queue_drop :: fun(),
    queue_update :: fun(),
    n = 0 :: non_neg_integer(),
    reqs = #{} :: #{node() => per_node_requests()},
    counts = #{} :: #{poll_req_id() => pos_integer()},
    matrix = #{} :: dispatch_matrix(),
    msgs = [] :: filtered_batch()
}).

-opaque beam_maker() :: #beam_maker{}.

-define(name(SHARD), {n, l, {?MODULE, SHARD}}).
-define(via(SHARD), {via, gproc, ?name(SHARD)}).

%%================================================================================
%% Callbacks
%%================================================================================

-type match_messagef() :: fun((emqx_ds:message_key(), emqx_types:message()) -> boolean()).

-type unpack_iterator_result(Stream) :: #{
    stream := Stream,
    topic_filter := event_topic_filter(),
    last_seen_key := emqx_ds:message_key(),
    message_matcher := match_messagef(),
    rank := emqx_ds:stream_rank()
}.

-callback unpack_iterator(dbshard(), _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error(_).

-callback update_iterator(dbshard(), Iterator, emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(Iterator).

-callback scan_stream(
    dbshard(), _Stream, event_topic_filter(), emqx_ds:message_key(), _BatchSize :: non_neg_integer()
) ->
    stream_scan_return().

-callback high_watermark(dbshard(), _Stream) -> {ok, emqx_ds:message_key()} | emqx_ds:error().

%% @doc This callback is used to safely advance the iterator to the position represented by the key.
-callback fast_forward(dbshard(), Iterator, emqx_ds:key()) ->
    {ok, Iterator} | {ok, end_of_stream} | emqx_ds:error().

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(dbshard(), module()) -> {ok, pid()}.
start_link(DBShard, CBM) ->
    gen_statem:start_link(?via(DBShard), ?MODULE, [DBShard, CBM], []).

%% @doc Submit a poll request
-spec poll(node(), return_addr(_ItKey), dbshard(), _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(Node, ReturnAddr, Shard, Iterator, Opts = #{timeout := Timeout}) ->
    CBM = emqx_ds_beamformer_sup:cbm(Shard),
    case CBM:unpack_iterator(Shard, Iterator) of
        #{
            stream := Stream,
            topic_filter := TF,
            last_seen_key := DSKey,
            message_matcher := MsgMatcher
        } ->
            Deadline = erlang:monotonic_time(millisecond) + Timeout,
            ReqId = make_ref(),
            ?tp(beamformer_poll, #{
                req_id => ReqId,
                shard => Shard,
                key => DSKey,
                timeout => Timeout,
                deadline => Deadline
            }),
            %% Make a request:
            Req = #sub_state{
                req_id = ReqId,
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                return_addr = ReturnAddr,
                it = Iterator,
                opts = Opts,
                deadline = Deadline,
                msg_matcher = MsgMatcher
            },
            emqx_ds_builtin_metrics:inc_poll_requests(shard_metrics_id(Shard), 1),
            ok = emqx_ds_beamformer_rt:enqueue(Shard, Req);
        Err = {error, _, _} ->
            Beam = #beam{
                iterators = [{ReturnAddr, Iterator}],
                pack = Err
            },
            send_out(undefined, Node, Beam)
    end.

%% @doc This internal API notifies the beamformer that new data is
%% available for reading in the storage.
%%
%% Backends should call this function after committing data to the
%% storage, so waiting long poll requests can be fulfilled.
%%
%% Types of arguments to this function are dependent on the backend.
%%
%% - `dbshard()': most DS backends have some notion of shard. Beamformer
%% uses this fact to partition poll requests, but otherwise it treats
%% shard as an opaque value.
%%
%% - `_Stream': backend- and layout-specific type of stream.
%% Beamformer uses exact matching on streams when it searches for
%% similar requests and when it matches events. Otherwise it's an
%% opaque type.
%%
%% - `event_topic()': When beamformer receives stream events, it
%% selects waiting events with matching stream AND
%% `event_topic_filter'.
-spec shard_event(dbshard(), [{_Stream, emqx_ds:message_key()}]) -> ok.
shard_event(Shard, Events) ->
    emqx_ds_beamformer_rt:shard_event(Shard, Events).

%% @doc Split beam into individual batches
-spec split(beam(ItKey, Iterator)) -> [{ItKey, emqx_ds:next_result(Iterator)}].
split(#beam{iterators = Its, pack = end_of_stream}) ->
    [{ItKey, {ok, end_of_stream}} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = {error, _, _} = Err}) ->
    [{ItKey, Err} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = Pack}) ->
    split(Its, Pack, 0, []).

%% @doc Create a beam that contains `end_of_stream' or unrecoverable
%% error, send it to the subscriptions, and forget about the
%% subscriptions.
-spec send_and_forget(dbshard(), {ok, end_of_stream} | {error, unrecoverable, _}, [
    sub_state()
]) ->
    ok.
send_and_forget(DBShard, Term, Reqs) ->
    ReqsByNode = maps:groups_from_list(
        fun(#sub_state{client = PID}) ->
            node(PID)
        end,
        Reqs
    ),
    %% Pack requests into beams and serve them:
    maps:foreach(
        fun(Node, Iterators) ->
            send_and_forget_node(DBShard, Term, Node, Iterators)
        end,
        ReqsByNode
    ),
    %% FIXME: drop subscriptions:
    ok.

send_and_forget_node(DBShard, Term, Node, Reqs) ->
    SubTab = subtab(DBShard),
    {Iterators, Seqnos} = lists:mapfoldl(
        fun(#sub_state{req_id = SubId, return_addr = RAddr, it = It}, Acc0) ->
            Acc =
                case RAddr of
                    {_, _} ->
                        %% This is a singleton poll request:
                        Acc0;
                    {_, _, _} ->
                        %% This is a subscription:
                        SeqNo = ets:update_counter(SubTab, SubId, {#sub_state.seqno, 1}),
                        Acc0#{SubId => SeqNo}
                end,
            ets:delete(SubTab, SubId),
            {{RAddr, It}, Acc}
        end,
        #{},
        Reqs
    ),
    Beam = #beam{
        pack = Term,
        iterators = Iterators,
        misc = #{seqnos => Seqnos}
    },
    send_out(DBShard, Node, Beam).

-spec beams_init(module(), dbshard(), fun(), fun()) -> beam_maker().
beams_init(CBM, DBShard, Drop, UpdateQueue) ->
    #beam_maker{
        cbm = CBM,
        shard_id = DBShard,
        queue_drop = Drop,
        queue_update = UpdateQueue
    }.

%% @doc Return the number of requests represented in the beam
-spec beams_n_matched(beam_maker()) -> non_neg_integer().
beams_n_matched(#beam_maker{reqs = Reqs}) ->
    maps:size(Reqs).

-spec beams_add(
    emqx_ds:message_key(),
    emqx_types:message(),
    [sub_state()],
    beam_maker()
) -> beam_maker().
beams_add(_, _, [], BM = #beam_maker{}) ->
    %% This message was not matched by any poll request, so we don't
    %% pack it into the beam:
    BM;
beams_add(
    MsgKey, Msg, NewReqs, S = #beam_maker{n = N, msgs = Msgs}
) ->
    {Reqs, Counts, Matrix} = lists:foldl(
        fun(Req = #sub_state{client = Client, req_id = Ref}, {Reqs1, Counts1, Matrix1}) ->
            %% Classify the request according to its source node:
            Node = node(Client),
            Reqs2 = maps:update_with(
                Node,
                fun(A) -> A#{Ref => Req} end,
                #{Ref => Req},
                Reqs1
            ),
            %% Update the sparse dispatch matrix:
            Matrix2 = Matrix1#{{Ref, N} => []},
            %% Increase per-request counters counters:
            Counts2 = maps:update_with(Ref, fun(I) -> I + 1 end, 1, Counts1),
            {Reqs2, Counts2, Matrix2}
        end,
        {S#beam_maker.reqs, S#beam_maker.counts, S#beam_maker.matrix},
        NewReqs
    ),
    S#beam_maker{
        n = N + 1,
        reqs = Reqs,
        counts = Counts,
        matrix = Matrix,
        msgs = [{MsgKey, Msg} | Msgs]
    }.

%% @doc Update seqnos of the subscriptions, send out beams, and return
%% the list of poll request keys that either don't have parent
%% subscription or have fallen too far behind on the ack:
-spec beams_conclude(
    dbshard(),
    emqx_ds:message_key(),
    beam_maker()
) -> ok.
beams_conclude(DBShard, NextKey, #beam_maker{reqs = Reqs} = BM) ->
    maps:foreach(
        fun(Node, NodeReqs) ->
            Beam = pack(DBShard, BM, NextKey, NodeReqs),
            send_out(DBShard, Node, Beam)
        end,
        Reqs
    ).

shard_metrics_id({DB, Shard}) ->
    emqx_ds_builtin_metrics:shard_metric_id(DB, Shard).

%% @doc In case this is a multishot subscription with a parent get
%% batch seqno and a whether to keep the request in the queue or
%% remove it:
-spec keep_and_seqno(ets:tid(), sub_state(), pos_integer()) ->
    {boolean(), reference(), emqx_ds:sub_seqno() | undefined} | boolean().
keep_and_seqno(_SubTab, #sub_state{deadline = Deadline}, _) when is_integer(Deadline) ->
    %% This is a one-time poll request, we never keep them after the first match:
    false;
keep_and_seqno(SubTab, #sub_state{req_id = Ref}, Nmsgs) ->
    try
        ets:update_counter(SubTab, Ref, [
            {#sub_state.seqno, Nmsgs},
            {#sub_state.acked_seqno, 0},
            {#sub_state.max_unacked, 0}
        ])
    of
        [SeqNo, Acked, Window] ->
            {is_sub_active(SeqNo, Acked, Window), Ref, SeqNo}
    catch
        error:badarg ->
            {false, Ref, undefined}
    end.

is_sub_active(SeqNo, Acked, Window) ->
    SeqNo - Acked < Window.

%% Dynamic config (currently it's global for all DBs):

cfg_pending_request_limit() ->
    application:get_env(emqx_durable_storage, poll_pending_request_limit, 100_000).

cfg_batch_size() ->
    application:get_env(emqx_durable_storage, poll_batch_size, 100).

cfg_housekeeping_interval() ->
    application:get_env(emqx_durable_storage, beamformer_housekeeping_interval, 1000).

%%================================================================================
%% behavior callback wrappers
%%================================================================================

-spec unpack_iterator(module(), dbshard(), _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error().
unpack_iterator(Mod, Shard, It) ->
    Mod:unpack_iterator(Shard, It).

-spec update_iterator(module(), dbshard(), _It, emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result().
update_iterator(Mod, Shard, It, Key) ->
    Mod:update_iterator(Shard, It, Key).

-spec scan_stream(
    module(), dbshard(), _Stream, event_topic_filter(), emqx_ds:message_key(), non_neg_integer()
) ->
    stream_scan_return().
scan_stream(Mod, Shard, Stream, TopicFilter, StartKey, BatchSize) ->
    Mod:scan_stream(Shard, Stream, TopicFilter, StartKey, BatchSize).

-spec high_watermark(module(), dbshard(), _Stream) -> {ok, emqx_ds:message_key()} | emqx_ds:error().
high_watermark(Mod, Shard, Stream) ->
    Mod:high_watermark(Shard, Stream).

-spec fast_forward(module(), dbshard(), Iterator, emqx_ds:message_key()) ->
    {ok, Iterator} | {error, unrecoverable, has_data | old_key} | emqx_ds:error().
fast_forward(Mod, Shard, It, Key) ->
    Mod:fast_forward(Shard, It, Key).

%%================================================================================
%% Internal subscription management API (RPC target)
%%================================================================================

-record(sub_req, {
    client :: pid(),
    handle :: emqx_ds:subscription_handle(),
    it :: emqx_ds:ds_specific_iterator(),
    it_key,
    opts :: emqx_ds:poll_opts()
}).
-record(unsub_req, {id :: reference()}).
-record(wakeup_sub_req, {id :: reference()}).
-record(reschedule_req, {ids :: [reference()]}).
-record(generation_event, {}).

%% @doc Get pid of a beamformer process serving the shard
-spec where(dbshard()) -> pid() | undefined.
where(DBShard) ->
    gproc:where(?name(DBShard)).

%% @doc List shards that have local beamformer process
-spec ls(emqx_ds:db()) -> [dbshard()].
ls(DB) ->
    MS = {{?name({DB, '$1'}), '_', '_'}, [], ['$1']},
    Shards = gproc:select({local, names}, [MS]),
    [{DB, I} || I <- Shards].

%% @doc Look up state of a subscription
-spec lookup_sub(emqx_ds:db(), sub_id()) -> [sub_state()].
lookup_sub(DB, {Shard, SubId}) ->
    ets:lookup(subtab({DB, Shard}), SubId).

-spec subscribe(
    pid(), pid(), sub_id(), emqx_ds:ds_specific_iterator(), _ItKey, emqx_ds:sub_opts()
) ->
    {ok, sub_id()} | emqx_ds:error().
subscribe(Server, Client, SubId, It, ItKey, Opts = #{max_unacked := MaxUnacked}) when
    is_integer(MaxUnacked), MaxUnacked > 0
->
    gen_statem:call(Server, #sub_req{
        client = Client, it = It, it_key = ItKey, opts = Opts, handle = SubId
    }).

-spec unsubscribe(dbshard(), sub_id()) -> boolean().
unsubscribe(DBShard, SubId) ->
    gen_statem:call(?via(DBShard), #unsub_req{id = SubId}).

%% @doc Ack batches up to the sequence number:
-spec suback(dbshard(), sub_id(), emqx_ds:sub_seqno()) -> ok | {error, _}.
suback(DBShard, SubId, Acked) ->
    try
        ets:update_counter(subtab(DBShard), SubId, [
            {#sub_state.seqno, 0}, {#sub_state.acked_seqno, 0}, {#sub_state.max_unacked, 0}
        ])
    of
        [SeqNo, OldAcked, Window] when Acked =< SeqNo ->
            ets:update_element(subtab(DBShard), SubId, {#sub_state.acked_seqno, Acked}),
            %% If this subscription ack changed state of the subscription,
            %% then request moving subscription back into the pool:
            case {is_sub_active(SeqNo, OldAcked, Window), is_sub_active(SeqNo, Acked, Window)} of
                {false, true} ->
                    gen_statem:call(?via(DBShard), #wakeup_sub_req{id = SubId});
                _ ->
                    ok
            end;
        _ ->
            {error, invalid_seqno}
    catch
        error:badarg ->
            %% This happens when subscription is not found:
            {error, subscripton_not_found}
    end.

%% @doc This internal API notifies the beamformer that generatins have
%% been added or removed.
-spec generation_event(dbshard()) -> ok.
generation_event(DBShard) ->
    gen_statem:cast(?via(DBShard), #generation_event{}).

%%================================================================================
%% gen_statem callbacks
%%================================================================================

-record(d, {
    dbshard :: dbshard(),
    tab :: ets:tid(),
    cbm :: module(),
    monitor_tab = ets:tid(),
    %% List of subscription IDs waiting for dispatching to the
    %% worker:
    pending = [] :: [reference()],
    %% Generation cache:
    generations :: #{emqx_ds:generation_rank() => emqx_ds:generation_info()}
}).

-type d() :: #d{}.

callback_mode() ->
    [handle_event_function, state_enter].

init([DBShard, CBM]) ->
    process_flag(trap_exit, true),
    SubTab = make_subtab(DBShard),
    gproc_pool:new(emqx_ds_beamformer_catchup:pool(DBShard), hash, [{auto_size, true}]),
    gproc_pool:new(emqx_ds_beamformer_rt:pool(DBShard), hash, [{auto_size, true}]),
    persistent_term:put(?ps_cbm(DBShard), CBM),
    logger:debug(#{
        msg => started_bf, shard => DBShard, cbm => emqx_ds_beamformer_sup:cbm(DBShard)
    }),
    MTab = ets:new(beamformer_monitor_tab, [private, set]),
    {ok, ?idle, #d{
        dbshard = DBShard,
        tab = SubTab,
        cbm = CBM,
        monitor_tab = MTab,
        generations = emqx_ds:list_generations_with_lifetimes(element(1, DBShard))
    }}.

-spec handle_event(gen_statem:event_type(), _Event, state(), d()) ->
    gen_statem:event_handler_result(state()).
%% Handle subscribe call:
handle_event(
    {call, From},
    #sub_req{client = Client, it = It, it_key = ItKey, opts = Opts, handle = SubId},
    State,
    D0 = #d{dbshard = Shard, tab = Tab, cbm = CBM, monitor_tab = MTab, pending = Pending}
) ->
    try CBM:unpack_iterator(Shard, It) of
        #{
            stream := Stream,
            topic_filter := TF,
            last_seen_key := DSKey,
            message_matcher := MsgMatcher,
            rank := Rank
        } ->
            MRef = monitor(process, Client),
            ?tp(beamformer_subscribe, #{
                sub_id => SubId,
                mref => MRef,
                shard => Shard,
                key => DSKey
            }),
            %% Update monitor table for automatic cleanup:
            ets:insert(MTab, {MRef, SubId}),
            #{max_unacked := MaxUnacked} = Opts,
            SubState = #sub_state{
                req_id = SubId,
                client = Client,
                mref = MRef,
                max_unacked = MaxUnacked,
                rank = Rank,
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                return_addr = {Client, SubId, ItKey},
                it = It,
                opts = Opts,
                msg_matcher = MsgMatcher
            },
            %% Insert subscription state into the table:
            ets:insert(Tab, SubState),
            %% Schedule dispaching to the worker:
            D = D0#d{pending = [SubId | Pending]},
            Reply = {reply, From, {ok, SubId}},
            case State of
                ?idle ->
                    {next_state, ?busy, D, Reply};
                _ ->
                    {keep_state, D, Reply}
            end;
        Err = {error, _, _} ->
            {keep_state_and_data, {reply, From, Err}}
    catch
        EC:Reason:Stack ->
            Error = {error, unrecoverable, {EC, Reason, Stack}},
            {keep_state_and_data, {reply, From, Error}}
    end;
%% Handle unsubscribe call:
handle_event(
    {call, From},
    #unsub_req{id = SubId},
    _State,
    D0
) ->
    {Ret, D} = remove_subscription(SubId, D0),
    {keep_state, D, {reply, From, Ret}};
%% Handle wakeup subscription call:
handle_event(
    {call, From},
    #wakeup_sub_req{id = SubId},
    State,
    D0 = #d{pending = Pending}
) ->
    D = D0#d{pending = [SubId | Pending]},
    Reply = {reply, From, ok},
    case State of
        ?idle ->
            {next_state, ?busy, D, Reply};
        _ ->
            {keep_state, D, Reply}
    end;
%% Handle redispatch call:
handle_event(
    {call, From},
    #reschedule_req{ids = Ids},
    _State,
    D = #d{pending = Pending}
) ->
    Reply = {reply, From, ok},
    {next_state, ?recovering, D#d{pending = Ids ++ Pending}, Reply};
%% Handle unknown call:
handle_event(
    {call, From},
    Call,
    State,
    Data
) ->
    ?tp(
        error,
        emqx_ds_beamformer_unknown_event,
        #{event_type => call, state => State, data => Data, from => From, event => Call}
    ),
    {keep_state_and_data, {reply, From, {error, unrecoverable, {unknown_call, Call}}}};
%% Handle down event:
handle_event(
    info,
    {'DOWN', MRef, process, _Pid, _Info},
    _State,
    D0 = #d{monitor_tab = MTab}
) ->
    case ets:take(MTab, MRef) of
        [{_, SubId}] ->
            %% One of the subscribers went down, remove subscription:
            {_Ret, D} = remove_subscription(SubId, D0),
            {keep_state, D};
        [] ->
            %% Some other process that we don't care about:
            keep_state_and_data
    end;
%% Set up state timeouts:
handle_event(enter, _OldState, ?busy, _D) ->
    %% In `busy' state we try to hand over subscriptions to the
    %% workers as soon as possible:
    {keep_state_and_data, {state_timeout, 0, ?schedule_timeout}};
handle_event(enter, _OldState, ?recovering, _D) ->
    %% In `recovering' state we delay handover to the worker to let
    %% the system stabilize:
    Cooldown = 1000,
    {keep_state_and_data, {state_timeout, Cooldown, ?schedule_timeout}};
handle_event(enter, _OldState, _NewState, _D) ->
    keep_state_and_data;
%% Handover subscriptions to the workers:
handle_event(state_timeout, ?schedule_timeout, _State, D0) ->
    {Success, D} = do_schedule(D0),
    NextState =
        case Success of
            true -> ?idle;
            false -> ?recovering
        end,
    {next_state, NextState, D};
%% Handle changes to the generations:
handle_event(
    cast,
    #generation_event{},
    _State,
    D = #d{dbshard = DBShard, generations = Gens0}
) ->
    {DB, _} = DBShard,
    %% Find generatins that have been sealed:
    Gens = emqx_ds:list_generations_with_lifetimes(DB),
    Sealed = diff_gens(DBShard, Gens0, Gens),
    %% Notify the RT workers:
    _ = [emqx_ds_beamformer_rt:seal_generation(DBShard, I) || I <- Sealed],
    {keep_state, D#d{generations = Gens}};
%% Unknown event:
handle_event(EventType, Event, State, Data) ->
    ?tp(
        error,
        emqx_ds_beamformer_unknown_event,
        #{event_type => EventType, state => State, data => Data, event => Event}
    ),
    keep_state_and_data.

terminate(_Reason, _State, #d{dbshard = DBShard}) ->
    persistent_term:erase(?ps_subtid(DBShard)),
    gproc_pool:force_delete(emqx_ds_beamformer_rt:pool(DBShard)),
    gproc_pool:force_delete(emqx_ds_beamformer_catchup:pool(DBShard)),
    persistent_term:erase(?ps_cbm(DBShard)).

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc Called by the worker processes when they are about to
%% terminate abnormally. This causes the FSM to enter `recovering'
%% where it will attempt to re-assign the subscriptions to the workers
%% after a cooldown interval:
-spec reschedule(dbshard(), [sub_id()]) -> ok.
reschedule(DBShard, SubIds) ->
    gen_statem:call(?via(DBShard), #reschedule_req{ids = SubIds}).

-spec subtab(dbshard()) -> ets:table().
subtab(DBShard) ->
    persistent_term:get(?ps_subtid(DBShard)).

%% @doc RPC target: split the beam and dispatch replies to local
%% consumers.
-spec do_dispatch(beam()) -> ok.
do_dispatch(Beam = #beam{misc = Misc}) ->
    SeqNos = maps:get(seqnos, Misc, #{}),
    lists:foreach(
        fun
            ({{Alias, ItKey}, Result}) ->
                %% Single poll:
                Alias ! #poll_reply{ref = Alias, userdata = ItKey, payload = Result};
            ({{PID, SubRef, ItKey}, Result}) ->
                %% Multi-poll (subscription):
                #{SubRef := SeqNo} = SeqNos,
                PID ! #poll_reply{ref = SubRef, userdata = ItKey, payload = Result, seqno = SeqNo}
        end,
        split(Beam)
    ).

%%================================================================================
%% Internal functions
%%================================================================================

diff_gens(_DBShard, Old, New) ->
    %% TODO: filter by shard
    maps:fold(
        fun(Rank, #{until := UOld}, Acc) ->
            case New of
                #{Rank := #{until := UNew}} when UOld =:= UNew ->
                    %% Unchanged:
                    Acc;
                _ ->
                    %% Changed or gone:
                    [Rank | Acc]
            end
        end,
        [],
        Old
    ).

do_schedule(D = #d{dbshard = DBShard, tab = Tab, pending = Pending}) ->
    Pool = emqx_ds_beamformer_catchup:pool(DBShard),
    %% Find the appropriate worker for the requests and group the requests:
    ByWorker = maps:groups_from_list(
        fun(SubId) ->
            case ets:lookup(Tab, SubId) of
                [#sub_state{stream = Stream}] ->
                    gproc_pool:pick_worker(Pool, Stream);
                [] ->
                    undefined
            end
        end,
        Pending
    ),
    %% Dispatch the events to the workers and collect the requests
    %% that couldn't be dispatched:
    Unmatched = maps:fold(
        fun
            (undefined, _Reqs, Unmatched) ->
                Unmatched;
            (Worker, Reqs, Unmatched) ->
                case emqx_ds_beamformer_catchup:enqueue(Worker, Reqs) of
                    ok ->
                        Unmatched;
                    Error ->
                        ?tp(
                            error,
                            emqx_ds_beamformer_delegate_error,
                            #{shard => DBShard, worker => Worker, error => Error}
                        ),
                        Reqs ++ Unmatched
                end
        end,
        [],
        ByWorker
    ),
    {Unmatched =:= [], D#d{pending = Unmatched}}.

remove_subscription(SubId, D = #d{tab = Tab, monitor_tab = MTab, pending = Pending}) ->
    case ets:take(Tab, SubId) of
        [#sub_state{mref = MRef}] ->
            demonitor(MRef, [flush]),
            ets:delete(MTab, MRef),
            %% TODO: remove from the workers:
            {true, D#d{pending = Pending -- [SubId]}};
        [] ->
            {false, D}
    end.

-spec pack(dbshard(), beam_maker(), emqx_ds:message_key(), per_node_requests()) ->
    beam(_ItKey, _Iterator).
pack(DBShard, BM, NextKey, Reqs) ->
    #beam_maker{
        matrix = Matrix,
        msgs = Msgs,
        queue_update = QueueUpdate,
        queue_drop = QueueDrop,
        cbm = CBM,
        shard_id = ShardId,
        counts = Counts,
        n = N
    } = BM,
    SubTab = subtab(DBShard),
    {SeqNos, Refs, UpdatedIterators} =
        maps:fold(
            fun(
                _,
                SubS = #sub_state{req_id = Ref, it = It0, return_addr = RAddr},
                {AccSeqNos0, AccRefs, Acc}
            ) ->
                ?tp(beamformer_fulfilled, #{req_id => Ref}),
                {ok, It} = update_iterator(CBM, ShardId, It0, NextKey),
                #{message_matcher := Matcher} = CBM:unpack_iterator(DBShard, It),
                #{Ref := Nmsgs} = Counts,
                AccSeqNos =
                    case keep_and_seqno(subtab(DBShard), SubS, Nmsgs) of
                        false ->
                            %% This is a singleton poll request. We
                            %% simply drop them upon completion:
                            ets:delete(SubTab, Ref),
                            QueueDrop(SubS),
                            AccSeqNos0;
                        {Active, SubRef, SeqNo} ->
                            %% This is a long-living subscription.
                            %% Update its state:
                            Existing = ets:update_element(
                                SubTab,
                                Ref,
                                [
                                    {#sub_state.it, It},
                                    {#sub_state.start_key, NextKey},
                                    {#sub_state.msg_matcher, Matcher}
                                ]
                            ),
                            case Existing and Active of
                                true ->
                                    %% Subscription is keeping up with
                                    %% the ack window. We update
                                    %% worker's queue state:
                                    QueueUpdate(
                                        SubS,
                                        SubS#sub_state{
                                            start_key = NextKey, it = It, msg_matcher = Matcher
                                        }
                                    );
                                false ->
                                    %% This subscription is not
                                    %% keeping up with the acks. We
                                    %% drop it from the worker's
                                    %% active queue:
                                    QueueDrop(SubS)
                            end,
                            AccSeqNos0#{SubRef => SeqNo}
                    end,
                {AccSeqNos, [Ref | AccRefs], [{RAddr, It} | Acc]}
            end,
            {#{}, [], []},
            Reqs
        ),
    Pack = do_pack(lists:reverse(Refs), Matrix, Msgs, [], N - 1),
    #beam{
        iterators = lists:reverse(UpdatedIterators),
        pack = Pack,
        misc = #{seqnos => SeqNos}
    }.

do_pack(_Refs, _Matrix, [], Acc, _N) ->
    %% Messages in the `beam_maker' record are reversed, so we don't
    %% have to reverse them here:
    Acc;
do_pack(Refs, Matrix, [{MsgKey, Msg} | Msgs], Acc, N) ->
    %% FIXME: it can include messages that are not needed for the
    %% node:
    DispatchMask = lists:foldl(
        fun(Ref, Bin) ->
            case Matrix of
                #{{Ref, N} := _} ->
                    <<Bin/bitstring, 1:1>>;
                #{} ->
                    <<Bin/bitstring, 0:1>>
            end
        end,
        <<>>,
        Refs
    ),
    do_pack(Refs, Matrix, Msgs, [{MsgKey, DispatchMask, Msg} | Acc], N - 1).

split([], _Pack, _N, Acc) ->
    Acc;
split([{ItKey, It} | Rest], Pack, N, Acc0) ->
    Msgs = [
        {MsgKey, Msg}
     || {MsgKey, Mask, Msg} <- Pack,
        check_mask(N, Mask)
    ],
    case Msgs of
        [] -> logger:warning("Empty batch ~p", [ItKey]);
        _ -> ok
    end,
    Acc = [{ItKey, {ok, It, Msgs}} | Acc0],
    split(Rest, Pack, N + 1, Acc).

-spec check_mask(non_neg_integer(), dispatch_mask()) -> boolean().
check_mask(N, Mask) ->
    <<_:N, Val:1, _/bitstring>> = Mask,
    Val =:= 1.

send_out(DBShard, Node, Beam) ->
    ?tp(debug, beamformer_out, #{
        dest_node => Node,
        beam => Beam
    }),
    %% FIXME: gen_rpc currently doesn't optimize local casts:
    emqx_ds_beamsplitter_proto_v2:dispatch(DBShard, Node, Beam).

%% subtab(Shard) ->
%%     persistent_term:get(?subtid(Shard)).

make_subtab(Shard) ->
    Tab = ets:new(emqx_ds_beamformer_sub_tab, [set, public, {keypos, #sub_state.req_id}]),
    persistent_term:put(?ps_subtid(Shard), Tab),
    Tab.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

chec_mask_test_() ->
    [
        ?_assert(check_mask(0, <<1:1>>)),
        ?_assertNot(check_mask(0, <<0:1>>)),
        ?_assertNot(check_mask(5, <<0:10>>)),

        ?_assert(check_mask(0, <<255:8>>)),
        ?_assert(check_mask(7, <<255:8>>)),

        ?_assertNot(check_mask(7, <<0:8, 1:1, 0:10>>)),
        ?_assert(check_mask(8, <<0:8, 1:1, 0:10>>)),
        ?_assertNot(check_mask(9, <<0:8, 1:1, 0:10>>))
    ].

pack_test_() ->
    Raddr = raddr,
    NextKey = <<"42">>,
    Req1 = #sub_state{
        req_id = make_ref(),
        return_addr = Raddr,
        it = {it1, <<"0">>}
    },
    Req2 = #sub_state{
        req_id = make_ref(),
        return_addr = Raddr,
        it = {it2, <<"1">>}
    },
    Req3 = #sub_state{
        req_id = make_ref(),
        return_addr = Raddr,
        it = {it3, <<"1">>}
    },
    %% Messages:
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {<<"2">>, #message{id = <<"2">>}},
    [
        ?_assertMatch(
            [],
            maps:to_list(
                pack_test_helper(
                    NextKey,
                    [
                        {M1, []},
                        {M2, []}
                    ]
                )
            )
        ),
        ?_assertMatch(
            #{
                undefined :=
                    #beam{
                        iterators = [
                            {Raddr, {it1, NextKey}}, {Raddr, {it2, NextKey}}
                        ],
                        pack = [
                            {<<"1">>, <<1:1, 0:1>>, #message{id = <<"1">>}},
                            {<<"2">>, <<0:1, 1:1>>, #message{id = <<"2">>}}
                        ]
                    }
            },
            pack_test_helper(
                NextKey,
                [
                    {M1, [Req1]},
                    {M2, [Req2]}
                ]
            )
        ),
        ?_assertMatch(
            #{
                undefined :=
                    #beam{
                        iterators = [
                            {Raddr, {it1, NextKey}}, {Raddr, {it2, NextKey}}
                        ],
                        pack = [
                            {<<"2">>, <<1:1, 1:1>>, #message{id = <<"2">>}}
                        ]
                    }
            },
            pack_test_helper(
                NextKey,
                [
                    {M1, []},
                    {M2, [Req1, Req2]}
                ]
            )
        ),
        ?_assertMatch(
            #{
                undefined :=
                    #beam{
                        iterators = [
                            {Raddr, {it1, NextKey}},
                            {Raddr, {it2, NextKey}},
                            {RAddr, {it3, NextKey}}
                        ],
                        pack = [
                            {<<"1">>, <<1:1, 0:1, 1:1>>, #message{id = <<"1">>}},
                            {<<"2">>, <<0:1, 1:1, 0:1>>, #message{id = <<"2">>}}
                        ]
                    }
            },
            pack_test_helper(
                NextKey,
                [
                    {M1, [Req1, Req3]},
                    {M2, [Req2]}
                ]
            )
        )
    ].

pack_test_helper(NextKey, L) ->
    Drop = Update = fun(_SubState) -> ok end,
    BeamMaker = lists:foldl(
        fun({{MsgKey, Message}, Reqs}, Acc) ->
            beams_add(MsgKey, Message, Reqs, Acc)
        end,
        beams_init(emqx_ds_beamformer_test_cbm, <<"test_shard">>, Drop, Update),
        L
    ),
    maps:map(
        fun(Node, NodeReqs) ->
            pack(my_shard, BeamMaker, NextKey, NodeReqs)
        end,
        BeamMaker#beam_maker.reqs
    ).

split_test_() ->
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {<<"2">>, #message{id = <<"2">>}},
    M3 = {<<"3">>, #message{id = <<"3">>}},
    Its = [{<<"it1">>, it1}, {<<"it2">>, it2}],
    Beam1 = #beam{
        iterators = Its,
        pack = [
            {<<"1">>, <<1:1, 0:1>>, element(2, M1)},
            {<<"2">>, <<0:1, 1:1>>, element(2, M2)},
            {<<"3">>, <<1:1, 1:1>>, element(2, M3)}
        ]
    },
    [{<<"it1">>, Result1}, {<<"it2">>, Result2}] = lists:sort(split(Beam1)),
    [
        ?_assertMatch(
            {ok, it1, [M1, M3]},
            Result1
        ),
        ?_assertMatch(
            {ok, it2, [M2, M3]},
            Result2
        )
    ].

-endif.
