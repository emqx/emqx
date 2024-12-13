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

-behaviour(gen_server).

%% API:
-export([start_link/2]).
-export([poll/5, subscribe/6, unsubscribe/2, shard_event/2, suback/3]).
-export([unpack_iterator/3, update_iterator/4, scan_stream/6, high_watermark/3, fast_forward/4]).
-export([beams_init/4, beams_add/4, beams_conclude/3, beams_n_matched/1, split/1]).
-export([shard_metrics_id/1, enqueue/3, send_out_term/2, cleanup_expired/4]).
-export([cfg_pending_request_limit/0, cfg_batch_size/0, cfg_housekeeping_interval/0]).

%% internal exports:
-export([do_dispatch/1]).
%% Testing/debugging:
-export([where/1, ls/1, lookup_sub/2, subtab/1]).

%% Behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

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

-type return_addr(ItKey) :: {reference(), ItKey} | {pid(), sub_id(), ItKey}.

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
        stream :: _Stream,
        topic_filter :: emqx_ds:topic_filter(),
        start_key :: emqx_ds:message_key(),
        %% Information about the process that created the request:
        return_addr :: return_addr(ItKey),
        %% Iterator:
        it :: Iterator,
        %% Callback that filters messages that belong to the request:
        msg_matcher :: emqx_beamformer:match_messagef(),
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
        misc :: #{}
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
    update_iterator :: fun((Iterator, emqx_ds:message_key()) -> Iterator),
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

-type match_messagef() :: fun(
    (emqx_ds:message_key(), emqx_ds:topic(), emqx_types:message()) -> boolean()
).

-type unpack_iterator_result(Stream) :: #{
    stream := Stream,
    topic_filter := event_topic_filter(),
    last_seen_key := emqx_ds:message_key(),
    message_matcher := match_messagef(),
    type => iterator_type()
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

-callback fast_forward(dbshard(), Iterator, emqx_ds:key()) ->
    {ok, Iterator} | {ok, end_of_stream} | emqx_ds:error().

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(dbshard(), module()) -> {ok, pid()}.
start_link(DBShard, CBM) ->
    gen_server:start_link(?via(DBShard), ?MODULE, [DBShard, CBM], []).

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
            emqx_ds_beamformer_rt:enqueue(Shard, Req, Timeout);
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

%% FIXME: include shard
-spec send_out_term({ok, end_of_stream} | emqx_ds:error(_), [sub_state()]) -> ok.
send_out_term(Term, Reqs) ->
    ReqsByNode = maps:groups_from_list(
        fun(#sub_state{client = PID}) -> node(PID) end,
        fun(#sub_state{return_addr = RAddr, it = It}) ->
            {RAddr, It}
        end,
        Reqs
    ),
    %% Pack requests into beams and serve them:
    maps:foreach(
        fun(Node, Its) ->
            Beam = #beam{
                pack = Term,
                iterators = Its
            },
            send_out(undefined, Node, Beam)
        end,
        ReqsByNode
    ).

-spec beams_init(module(), _Shard, fun(), fun()) -> beam_maker().
beams_init(CBM, ShardId, Drop, UpdateQueue) ->
    #beam_maker{
        cbm = CBM,
        shard_id = ShardId,
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

-record(unsub_req, {id :: sub_id()}).

-record(wakeup_sub_req, {id :: sub_id()}).

-spec where(dbshard()) -> pid() | undefined.
where(DBShard) ->
    gproc:where(?name(DBShard)).

-spec ls(emqx_ds:db()) -> [dbshard()].
ls(DB) ->
    MS = {{?name({DB, '$1'}), '_', '_'}, [], ['$1']},
    Shards = gproc:select({local, names}, [MS]),
    [{DB, I} || I <- Shards].

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
    %% FIXME sub ref should be generated by monitoring the beamformer, rather than other way around
    gen_server:call(Server, #sub_req{
        client = Client, it = It, it_key = ItKey, opts = Opts, handle = SubId
    }).

-spec unsubscribe(dbshard(), sub_id()) -> boolean().
unsubscribe(Shard, SubId) ->
    gen_server:call(?via(Shard), #unsub_req{id = SubId}).

%% @doc Ack batches up to sequence number:
-spec suback(dbshard(), sub_id(), emqx_ds:sub_seqno()) -> ok.
suback(DBShard, SubId, Acked) ->
    try
        ets:update_counter(subtab(DBShard), SubId, [
            {#sub_state.seqno, 0}, {#sub_state.acked_seqno, 0}, {#sub_state.max_unacked, 0}
        ])
    of
        [SeqNo, OldAcked, Window] ->
            ets:update_element(subtab(DBShard), SubId, {#sub_state.acked_seqno, Acked}),
            %% If this subscription ack changed state of the subscription,
            %% then request moving subscription back into the pool:
            not is_sub_active(SeqNo, OldAcked, Window) andalso is_sub_active(SeqNo, Acked, Window) andalso
                gen_server:call(?via(DBShard), #wakeup_sub_req{id = SubId}),
            ok
    catch
        error:badarg ->
            %% This happens when subscription is not found:
            {error, subscripton_not_found}
    end.

%%================================================================================
%% gen_server (responsible for subscriptions)
%%================================================================================

-record(s, {dbshard :: dbshard(), tab :: ets:tid(), cbm :: module(), monitor_tab = ets:tid()}).

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
    {ok, #s{dbshard = DBShard, tab = SubTab, cbm = CBM, monitor_tab = MTab}}.

handle_call(
    #sub_req{client = Client, it = It, it_key = ItKey, opts = Opts, handle = SubId},
    _From,
    S = #s{dbshard = Shard, tab = Tab, cbm = CBM, monitor_tab = MTab}
) ->
    try CBM:unpack_iterator(Shard, It) of
        #{
            stream := Stream,
            topic_filter := TF,
            last_seen_key := DSKey,
            message_matcher := MsgMatcher
        } ->
            MRef = monitor(process, Client),
            ?tp(beamformer_subscribe, #{
                sub_id => SubId,
                mref => MRef,
                shard => Shard,
                key => DSKey
            }),
            ets:insert(MTab, {MRef, SubId}),
            #{max_unacked := MaxUnacked} = Opts,
            SubState = #sub_state{
                req_id = SubId,
                client = Client,
                mref = MRef,
                max_unacked = MaxUnacked,
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                return_addr = {Client, SubId, ItKey},
                it = It,
                opts = Opts,
                msg_matcher = MsgMatcher
            },
            ets:insert(Tab, SubState),
            emqx_ds_beamformer_catchup:enqueue(Shard, SubState, 0),
            {reply, {ok, SubId}, S};
        Err = {error, _, _} ->
            {reply, Err, S}
    catch
        EC:Err:Stack ->
            {reply, {error, unrecoverable, {EC, Err, Stack}}, S}
    end;
handle_call(#unsub_req{id = Id}, _From, S = #s{tab = Tab, monitor_tab = MTab}) ->
    Ret =
        case ets:take(Tab, Id) of
            [#sub_state{mref = MRef}] ->
                demonitor(MRef, [flush]),
                ets:delete(MTab, MRef),
                true;
            [] ->
                false
        end,
    {reply, Ret, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'DOWN', MRef, process, _Pid, _Info}, S = #s{tab = Tab, monitor_tab = MTab}) ->
    case ets:take(MTab, MRef) of
        [{_, SubId}] ->
            %% FIXME: also delete it from the queues
            ets:delete(Tab, SubId);
        [] ->
            ok
    end,
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_, #s{dbshard = DBShard}) ->
    persistent_term:erase(?ps_subtid(DBShard)),
    gproc_pool:force_delete(emqx_ds_beamformer_rt:pool(DBShard)),
    gproc_pool:force_delete(emqx_ds_beamformer_catchup:pool(DBShard)),
    persistent_term:erase(?ps_cbm(DBShard)).

%%================================================================================
%% Internal exports
%%================================================================================

-spec subtab(dbshard()) -> ets:table().
subtab(DBShard) ->
    persistent_term:get(?ps_subtid(DBShard)).

%% enqueue(_, Req = #sub_state{hops = Hops}, _) when Hops > 3 ->
%% It seems that the poll request is being bounced between the
%% queues. After certain number of such hops we drop it to break
%% the infinite loop.
%%
%% Normally, the poll request is bounced between the queues in the
%% following cases:
%%
%% 1. The request was submitted to the RT queue, but its
%% `start_key' is older than the high watermark for the stream. In
%% this case RT queue will move it to the catchup queue.
%%
%% 2. Catchup queue tried to fulfill the poll request, but got an
%% empty batch. In this case, it moves the poll request to the RT
%% queue.
%%
%% Normally, the second case should not lead to the first one,
%% since catchup queue updates the start key before sending it to
%% the RT queue. However, the DS backend MAY misbehave and fail to
%% return the end key corresponding to the empty batch equal to
%% the high watermark.
%%
%% This is the probable cause of this warning. To avoid it the
%% backend must ensure the following property:
%%
%% If `scan_stream' callback returns `{ok, EndKey, []}', then
%% `EndKey' must be greater or equal to the `high_watermark' for
%% the stream at the moment.
%% ?tp(warning, beamformer_enqueue_max_hops, #{req => Req}),
%% ok;
enqueue(Pool, Req = #sub_state{stream = Stream}, Timeout) ->
    %% Currently we implement backpressure by ignoring transient
    %% errors (gen_server timeouts, `too_many_requests'), and just
    %% letting poll requests expire at the higher level. This should
    %% hold back the caller.
    Worker = gproc_pool:pick_worker(Pool, Stream),
    case Timeout of
        0 ->
            %% Avoid deadlock due to calling self:
            gen_server:cast(Worker, Req);
        _ ->
            try gen_server:call(Worker, Req, Timeout) of
                ok -> ok;
                {error, recoverable, too_many_requests} -> ok
            catch
                exit:{timeout, _} ->
                    ok
            end
    end.

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

%% @doc Semi-generic function for cleaning expired poll requests from
%% an ETS table that is organized as a collection of 2-tuples, where
%% `#poll_req{}' is the second element:
%%
%% `{_Key, #sub_state{...}}'
%%
%% This function does not affect subscriptions.
cleanup_expired(QueueType, _ParentSubTab, Metrics, Table) ->
    Now = erlang:monotonic_time(millisecond),
    maybe_report_expired(QueueType, Table, Now),
    %% Cleanup 1-shot poll requests:
    MS = {
        {'_', #sub_state{_ = '_', deadline = '$1'}},
        [{'<', '$1', Now}],
        [true]
    },
    NDeletedPolls = ets:select_delete(Table, [MS]),
    emqx_ds_builtin_metrics:inc_poll_requests_expired(Metrics, NDeletedPolls),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

%% In TEST profile we want to emit trace events for expired poll
%% reqests. In prod we just silently drop them.
-ifndef(TEST).
maybe_report_expired(_QueueType, _Tabl, _Now) ->
    ok.
-else.
maybe_report_expired(QueueType, Table, Now) ->
    MS = {{'_', #sub_state{_ = '_', deadline = '$1', req_id = '$2'}}, [{'<', '$1', Now}], ['$2']},
    [
        ?tp(beamformer_poll_expired, #{req_id => ReqId, queue => QueueType})
     || ReqId <- ets:select(Table, [MS])
    ],
    ok.
-endif.

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
                #{Ref := Nmsgs} = Counts,
                NewSubS = SubS#sub_state{start_key = NextKey, it = It},
                case keep_and_seqno(subtab(DBShard), SubS, Nmsgs) of
                    false ->
                        %% This is a singleton poll request. We drop
                        %% them upon completion:
                        ets:delete(SubTab, Ref),
                        QueueDrop(SubS),
                        AccSeqNo = AccSeqNos0;
                    {false, SubRef, SeqNo} ->
                        %% This request belongs to a subscription that
                        %% went for too long without ack. We drop it
                        %% from the active queue:
                        ets:insert(SubTab, NewSubS),
                        QueueDrop(SubS),
                        AccSeqNo = AccSeqNos0#{SubRef => SeqNo};
                    {true, SubRef, SeqNo} ->
                        %% This request belongs to a subscription that
                        %% keeps up with the ack window. We update its
                        %% state:
                        ets:insert(SubTab, NewSubS),
                        QueueUpdate(SubS, NewSubS),
                        AccSeqNo = AccSeqNos0#{SubRef => SeqNo}
                end,
                {AccSeqNo, [Ref | AccRefs], [{RAddr, It} | Acc]}
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
