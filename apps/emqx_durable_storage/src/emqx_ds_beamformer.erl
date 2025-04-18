%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([start_link/2, where/1]).
-export([poll/5, subscribe/5, unsubscribe/2, shard_event/2, generation_event/1, suback/3]).
-export([unpack_iterator/3, update_iterator/4, scan_stream/6, high_watermark/3, fast_forward/4]).
-export([
    make_subtab/1,
    take_ownership/3,
    sub_tab_update/2,
    sub_tab_delete/2,
    sub_tab_lookup/2,
    sub_tab_take/2,
    on_worker_down/2
]).
-export([owner_tab/1, handle_recoverable_error/2]).
-export([beams_init/6, beams_add/5, beams_conclude/3, beams_n_matched/1]).
-export([metrics_id/2, send_out_final_beam/4]).
-export([cfg_batch_size/0, cfg_housekeeping_interval/0, cfg_workers_per_shard/0]).

%% internal exports:
-export([do_dispatch/1]).
%% Debugging/monitoring:
-export([ls/0, ls/1, subscription_info/2]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

-export_type([
    dbshard/0,
    opts/0,
    sub_state/0, sub_state/1,
    beam/2, beam/0,
    return_addr/1,
    unpack_iterator_result/1,
    event_topic/0,
    stream_scan_return/0,
    beam_builder/0,
    sub_tab/0
]).

-include_lib("snabbkaffe/include/trace.hrl").

-include("emqx_ds.hrl").
-include("emqx_ds_beamformer.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% States:
-define(initializing, initializing).
-define(idle, idle).
-define(busy, busy).
-define(recovering, recovering).

%% When the worker terminates and it wants the parent to deal with the
%% subscriptions it owns, it gives away its ets table containing the
%% subscriptions. This token is used in the `ETS-TRANSFER':
-define(heir_info, reschedule_subs).

-type state() ::
    %% Waiting for the shard to start
    ?initializing
    %% The shard is healthy, and there aren't any pending poll or
    %% subscribe requests:
    | ?idle
    %% The shard is healthy, and there are some requests that need to
    %% be dispatched to the workers immediately:
    | ?busy
    %% Waiting for shard recovery. Request scheduling has been delayed
    %% and will be retried after a cooldown period:
    | ?recovering.

%% State timeout (for both `idle' and `recovering' states) that
%% triggers handover of subscriptions to the workers:
-define(schedule_timeout, schedule_timeout).
%% `initializing' state timeout that triggers getting of shard
%% metadata:
-define(init_timeout, init_timeout).

-type dbshard() :: {emqx_ds:db(), _Shard}.

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

-type flowcontrol() :: {pos_integer(), atomics:atomics_ref()}.
-define(fc_idx_seqno, 1).
-define(fc_idx_acked, 2).
-define(fc_idx_stuck, 3).

%-type poll_req_id() :: reference().

-type sub_state(Iterator) ::
    #sub_state{
        req_id :: reference(),
        client :: pid(),
        %% Flow control:
        flowcontrol :: flowcontrol(),
        %%
        rank :: emqx_ds:slab(),
        stream :: _Stream,
        topic_filter :: emqx_ds:topic_filter(),
        start_key :: emqx_ds:message_key(),
        %% Iterator:
        it :: Iterator
    }.

-type sub_state() :: sub_state(_).

%% Type of records stored in `fc_tab' table of gvar. This table
%% contains references to the subscription flow control atomics.
-record(fctab, {
    sub_ref :: emqx_ds:sub_ref(),
    max_unacked :: pos_integer(),
    aref :: atomics:atomics_ref()
}).

%% Type of records stored in `owner_tab' table of gvar. This table
%% contains pids of workers that currently own the subscription.
-record(owner_tab, {
    sub_ref :: emqx_ds:sub_ref(),
    %% Monitor reference the process owning the subscription on the
    %% client side:
    mref :: reference(),
    %% Pid of the worker owning the subscription on the DS side:
    owner :: pid() | undefined
}).

%% Response:

-type pack() ::
    [{emqx_ds:message_key(), emqx_types:message()}]
    | end_of_stream
    | emqx_ds:error(_).

%% obsolete
-record(beam, {iterators, pack, misc = #{}}).

-opaque beam(ItKey, Iterator) ::
    #beam{
        iterators :: [{return_addr(ItKey), Iterator}],
        pack :: pack(),
        misc :: #{
            seqnos => #{reference() => emqx_ds:sub_seqno()},
            lagging => boolean(),
            stuck => #{reference() => _}
        }
    }.

-type beam() :: beam(_ItKey, _Iterator).

-type stream_scan_return() ::
    {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}]}
    | {ok, end_of_stream}
    | emqx_ds:error(_).

-define(name(SHARD), {n, l, {?MODULE, SHARD}}).
-define(via(SHARD), {via, gproc, ?name(SHARD)}).

-type gvar() :: #{
    cbm := module(),
    fc_tab := ets:tid(),
    owner_tab := ets:tid(),
    stuck_sub_tab := sub_tab()
}.

%% Beam builder:

%% Per-subscription builder state:
-record(beam_builder_sub, {
    s :: sub_state(),
    matcher :: fun(),
    mask :: emqx_ds_dispatch_mask:enc(),
    n_msgs :: pos_integer()
}).

%% Per-node builder:
-record(beam_builder_node, {
    global_n_msgs :: non_neg_integer() | undefined,
    n_msgs = 0 :: non_neg_integer(),
    %% List of messages and keys, reversed:
    pack = [] :: [{emqx_ds:message_key(), emqx_types:message()}],
    subs = #{} :: #{reference() => #beam_builder_sub{}}
}).
-type bbn() :: #beam_builder_node{}.

%% Global builder:
-record(beam_builder, {
    cbm :: module(),
    sub_tab :: sub_tab(),
    lagging :: boolean(),
    shard_id :: _Shard,
    queue_drop :: fun(),
    queue_update :: fun(),
    global_n_msgs = 0 :: non_neg_integer(),
    per_node = #{} :: #{node() => #beam_builder_node{}}
}).

-opaque beam_builder() :: #beam_builder{}.

-opaque sub_tab() :: ets:tid().

%%================================================================================
%% Callbacks
%%================================================================================

-type unpack_iterator_result(Stream) :: #{
    stream := Stream,
    topic_filter := event_topic_filter(),
    last_seen_key := emqx_ds:message_key(),
    rank := emqx_ds:slab()
}.

-callback unpack_iterator(dbshard(), _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error(_).

-callback update_iterator(dbshard(), Iterator, emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(Iterator).

-callback scan_stream(
    dbshard(), _Stream, event_topic_filter(), emqx_ds:message_key(), _BatchSize :: non_neg_integer()
) ->
    stream_scan_return().

-callback high_watermark(dbshard(), _Stream) -> {ok, emqx_ds:message_key()} | emqx_ds:error(_).

%% @doc This callback is used to safely advance the iterator to the
%% position represented by the key.
-callback fast_forward(dbshard(), Iterator, emqx_ds:message_key()) ->
    {ok, Iterator} | {ok, end_of_stream} | emqx_ds:error(_).

%% @doc These two callbacks are used to create the dispatch matrix of
%% the beam.
%%
%% Let c_m := message_match_context(key, message),
%% c_i := iterator_match_context(iterator[client])
%%
%% then dispatch_matrix[message, client] = c_i(c_m)
-callback message_match_context(dbshard(), _Stream, emqx_ds:message_key(), emqx_types:message()) ->
    {ok, _MatchCtxMsg}.

-callback iterator_match_context(dbshard(), _Iterator) -> fun((_MatchCtxMsg) -> boolean()).

%%================================================================================
%% API functions
%%================================================================================

-record(sub_req, {
    client :: pid(),
    mref :: reference(),
    it :: emqx_ds:ds_specific_iterator(),
    opts :: emqx_ds:sub_opts()
}).
-record(wakeup_sub_req, {id :: reference()}).
-record(generation_event, {}).
-record(handle_recoverable_req, {sub_states :: [sub_state()]}).
-record(disown_stuck_req, {sub_state :: sub_state()}).

-spec start_link(dbshard(), module()) -> {ok, pid()}.
start_link(DBShard, CBM) ->
    gen_statem:start_link(?via(DBShard), ?MODULE, [DBShard, CBM], []).

%% @doc Display all `{DB, Shard}' pairs for beamformers running on the
%% node:
-spec ls() -> [dbshard()].
ls() ->
    MS = {{?name({'$1', '$2'}), '_', '_'}, [], [{{'$1', '$2'}}]},
    gproc:select({local, names}, [MS]).

%% @doc Display the hieararchy of beamformer workers for the database
%% (debug).
-spec ls(emqx_ds:db()) -> map().
ls(DB) ->
    MS = {{?name({DB, '$1'}), '_', '_'}, [], ['$1']},
    Shards = gproc:select({local, names}, [MS]),
    maps:from_list(
        [emqx_ds_beamformer_sup:info({DB, I}) || I <- Shards]
    ).

%% @obsolete Submit a poll request
-spec poll(node(), return_addr(_ItKey), dbshard(), _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(_Node, _ReturnAddr, _Shard, _Iterator, #{timeout := _Timeout}) ->
    ok.

%% @doc Create a local subscription registry
-spec make_subtab(dbshard()) -> sub_tab().
make_subtab(DBShard) ->
    ets:new(emqx_ds_beamformer_sub_tab, [
        set,
        public,
        {keypos, #sub_state.req_id},
        {heir, where(DBShard), ?heir_info}
    ]).

-spec take_ownership(dbshard(), sub_tab(), sub_state()) -> boolean().
take_ownership(DBShard, SubTab, SubS = #sub_state{req_id = SubRef}) ->
    sub_tab_update(SubTab, SubS),
    ets:update_element(owner_tab(DBShard), SubRef, {#owner_tab.owner, self()}).

-spec sub_tab_update(sub_tab(), sub_state()) -> ok.
sub_tab_update(SubTab, SubS = #sub_state{req_id = _SubRef}) ->
    ets:insert(SubTab, SubS),
    ok.

-spec sub_tab_delete(sub_tab(), emqx_ds:sub_ref()) -> ok.
sub_tab_delete(SubTab, SubRef) ->
    ets:delete(SubTab, SubRef),
    ok.

-spec sub_tab_lookup(sub_tab(), emqx_ds:sub_ref()) -> {ok, sub_state()} | undefined.
sub_tab_lookup(SubTab, SubRef) ->
    case ets:lookup(SubTab, SubRef) of
        [SubS] ->
            {ok, SubS};
        [] ->
            undefined
    end.

-spec sub_tab_take(sub_tab(), emqx_ds:sub_ref()) -> {ok, sub_state()} | undefined.
sub_tab_take(SubTab, SubRef) ->
    case ets:take(SubTab, SubRef) of
        [SubS] ->
            {ok, SubS};
        [] ->
            undefined
    end.

%% @doc This function must be called by the worker when it terminates.
-spec on_worker_down(sub_tab(), _ExitReason) -> ok.
on_worker_down(SubTab, Reason) ->
    case Reason of
        shutdown ->
            ets:delete(SubTab);
        _ ->
            ok
    end.

-spec handle_recoverable_error(dbshard(), [sub_state()]) -> ok.
handle_recoverable_error(DBShard, SubStates) ->
    gen_statem:call(?via(DBShard), #handle_recoverable_req{sub_states = SubStates}).

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
    %% FIXME: start order of processes
    try
        emqx_ds_beamformer_rt:shard_event(Shard, Events)
    catch
        _:_ -> ok
    end.

%% @doc Create a beam that contains `end_of_stream' or unrecoverable
%% error, and send it to the clients. Then delete the subscriptions.
-spec send_out_final_beam(
    dbshard(),
    sub_tab(),
    {ok, end_of_stream} | ?err_unrec(_),
    [sub_state()]
) ->
    ok.
send_out_final_beam(DBShard, SubTab, Term, Reqs) ->
    ReqsByNode = maps:groups_from_list(
        fun(#sub_state{client = Pid}) ->
            node(Pid)
        end,
        Reqs
    ),
    %% Pack requests into beams and serve them:
    maps:foreach(
        fun(Node, Iterators) ->
            send_out_final_term_to_node(DBShard, SubTab, Term, Node, Iterators)
        end,
        ReqsByNode
    ).

%% @doc Create an object that is used to incrementally build
%% destinations for the batch.
-spec beams_init(module(), dbshard(), sub_tab(), boolean(), fun(), fun()) -> beam_builder().
beams_init(CBM, DBShard, SubTab, Lagging, Drop, UpdateQueue) ->
    #beam_builder{
        cbm = CBM,
        sub_tab = SubTab,
        shard_id = DBShard,
        lagging = Lagging,
        queue_drop = Drop,
        queue_update = UpdateQueue
    }.

%% @doc Return the number of requests represented in the beam
-spec beams_n_matched(beam_builder()) -> non_neg_integer().
beams_n_matched(#beam_builder{per_node = PerNode}) ->
    maps:fold(
        fun(_SubId, #beam_builder_node{subs = Subscribers}, Acc) ->
            Acc + maps:size(Subscribers)
        end,
        0,
        PerNode
    ).

%% @doc Add a message to the beam builder.
%%
%% Arguments:
%% 1. Stream of the message
%%
%% 2. DS key of the message
%%
%% 3. The message itself
%%
%% 4. List of potential destinations: node where the subscriber is
%% located + subscription id.
%%
%% 5. Beam builder object to be updated.
%%
%% This function runs message matcher callback for each destination
%% and adds messsage to the per-node batch when at least one
%% subscriber matches the message.
-spec beams_add(
    _Stream,
    emqx_ds:message_key(),
    emqx_types:message(),
    [{node(), reference()}],
    beam_builder()
) -> beam_builder().
beams_add(
    Stream,
    Key,
    Msg,
    Candidates,
    BB = #beam_builder{
        cbm = Mod,
        shard_id = DBShard,
        sub_tab = SubTab,
        global_n_msgs = GlobalNMsgs0,
        per_node = PerNode0
    }
) ->
    %% 1. Update global message counter. It's used to check whether
    %% the message should be added to the per-node pack or it's
    %% already there.
    GlobalNMsgs = GlobalNMsgs0 + 1,
    %% 2. Create match context used to speed up updating of the
    %% dispatch matrix.
    {ok, MatchCtx} = Mod:message_match_context(DBShard, Stream, Key, Msg),
    %% 3. Iterate over candidates.
    PerNode = fold_with_groups(
        fun(SubRef, NodeS) ->
            beams_add_per_node(Mod, DBShard, SubTab, GlobalNMsgs, Key, Msg, MatchCtx, SubRef, NodeS)
        end,
        #beam_builder_node{},
        Candidates,
        PerNode0
    ),
    BB#beam_builder{
        per_node = PerNode,
        global_n_msgs = GlobalNMsgs
    }.

%% @doc Update states of the subscriptions, active queues of the
%% workers, and send out beams.
-spec beams_conclude(
    dbshard(),
    emqx_ds:message_key(),
    beam_builder()
) -> ok.
beams_conclude(DBShard, NextKey, BB = #beam_builder{per_node = PerNode}) ->
    maps:foreach(
        fun(Node, BeamMakerNode) ->
            beams_conclude_node(DBShard, NextKey, BB, Node, BeamMakerNode)
        end,
        PerNode
    ).

metrics_id({DB, Shard}, Type) ->
    emqx_ds_builtin_metrics:metric_id([{db, DB}, {shard, Shard}, {type, Type}]).

%% @doc In case this is a multishot subscription with a parent get
%% batch seqno and a whether to keep the request in the queue or
%% remove it:
-spec keep_and_seqno(sub_tab(), sub_state(), pos_integer()) ->
    {boolean(), emqx_ds:sub_seqno() | undefined}.
keep_and_seqno(_SubTab, #sub_state{flowcontrol = FC}, NMsgs) ->
    try
        {MaxUnacked, ARef} = FC,
        SeqNo = atomics:add_get(ARef, ?fc_idx_seqno, NMsgs),
        Acked = atomics:get(ARef, ?fc_idx_acked),
        IsActive = is_sub_active(SeqNo, Acked, MaxUnacked),
        (not IsActive) andalso
            atomics:put(ARef, ?fc_idx_stuck, 1),
        {IsActive, SeqNo}
    catch
        error:badarg ->
            {false, undefined}
    end.

is_sub_active(SeqNo, Acked, Window) ->
    SeqNo - Acked < Window.

%% Dynamic config (currently it's global for all DBs):

cfg_batch_size() ->
    application:get_env(emqx_durable_storage, poll_batch_size, 1000).

cfg_housekeeping_interval() ->
    application:get_env(emqx_durable_storage, beamformer_housekeeping_interval, 1000).

cfg_workers_per_shard() ->
    application:get_env(emqx_durable_storage, beamformer_workers_per_shard, 10).

%%================================================================================
%% behavior callback wrappers
%%================================================================================

-spec unpack_iterator(module(), dbshard(), _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error(_).
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

-spec high_watermark(module(), dbshard(), _Stream) ->
    {ok, emqx_ds:message_key()} | emqx_ds:error(_).
high_watermark(Mod, Shard, Stream) ->
    Mod:high_watermark(Shard, Stream).

-spec fast_forward(module(), dbshard(), Iterator, emqx_ds:message_key()) ->
    {ok, Iterator} | ?err_unrec(has_data | old_key) | emqx_ds:error(_).
fast_forward(Mod, Shard, It, Key) ->
    Mod:fast_forward(Shard, It, Key).

%%================================================================================
%% Internal subscription management API (RPC target)
%%================================================================================

%% @doc Get pid of a beamformer process serving the shard
-spec where(dbshard()) -> pid() | undefined.
where(DBShard) ->
    gproc:where(?name(DBShard)).

%% @doc Look up state of a subscription
-spec subscription_info(dbshard(), emqx_ds:sub_ref()) -> emqx_ds:sub_info() | undefined.
subscription_info(DBShard, SubId) ->
    case ets:lookup(fc_tab(DBShard), SubId) of
        [#fctab{max_unacked = MaxUnacked, aref = ARef}] ->
            SeqNo = atomics:get(ARef, ?fc_idx_seqno),
            Acked = atomics:get(ARef, ?fc_idx_acked),
            Stuck = atomics:get(ARef, ?fc_idx_stuck) =:= 1,
            case ets:lookup(owner_tab(DBShard), SubId) of
                [#owner_tab{owner = Owner}] ->
                    ok;
                [] ->
                    Owner = undefined
            end,
            #{seqno => SeqNo, acked => Acked, window => MaxUnacked, stuck => Stuck, owner => Owner};
        [] ->
            undefined
    end.

-spec subscribe(
    pid(), pid(), emqx_ds:sub_ref(), emqx_ds:ds_specific_iterator(), emqx_ds:sub_opts()
) ->
    {ok, emqx_ds:sub_ref()} | emqx_ds:error(_).
subscribe(Server, Client, SubId, It, Opts = #{max_unacked := MaxUnacked}) when
    is_integer(MaxUnacked), MaxUnacked > 0
->
    try
        gen_statem:call(Server, #sub_req{
            client = Client, it = It, opts = Opts, mref = SubId
        })
    catch
        exit:{noproc, _} ->
            {error, recoverable, beamformer_down}
    end.

-spec unsubscribe(dbshard(), emqx_ds:sub_ref()) -> boolean().
unsubscribe(DBShard, SubId) ->
    try
        gen_statem:call(?via(DBShard), #unsub_req{id = SubId})
    catch
        exit:{noproc, _} ->
            false
    end.

%% @doc Ack payloads up to a sequence number:
-spec suback(dbshard(), emqx_ds:sub_ref(), emqx_ds:sub_seqno()) -> ok | {error, _}.
suback(DBShard, SubId, Acked) ->
    case ets:lookup(fc_tab(DBShard), SubId) of
        [#fctab{max_unacked = MaxUnacked, aref = ARef}] ->
            atomics:put(ARef, ?fc_idx_acked, Acked),
            case atomics:get(ARef, ?fc_idx_stuck) of
                1 ->
                    %% We've been kicked out from the active queue for
                    %% being stuck:
                    SeqNo = atomics:get(ARef, ?fc_idx_seqno),
                    case is_sub_active(SeqNo, Acked, MaxUnacked) of
                        true ->
                            %% Subscription became active, notify the beamformer:
                            %% FIXME: add hysteresis
                            gen_statem:call(?via(DBShard), #wakeup_sub_req{id = SubId});
                        false ->
                            %% Still stuck:
                            ok
                    end;
                0 ->
                    %% Subscription is active:
                    ok
            end;
        [] ->
            {error, subscription_not_found}
    end.

%% @doc This internal API notifies the beamformer that generations
%% have been added or removed.
-spec generation_event(dbshard()) -> ok.
generation_event(DBShard) ->
    gen_statem:cast(?via(DBShard), #generation_event{}).

%%================================================================================
%% gen_statem callbacks
%%================================================================================

-record(d, {
    dbshard :: dbshard(),
    metrics_id,
    stuck_sub_tab :: ets:tid(),
    cbm :: module(),
    monitor_tab = ets:tid(),
    %% List of subscription states waiting for dispatching to the
    %% workers. Subscription requests can be retained in this queue
    %% while the workers are recovering from failure:
    pending = [] :: [sub_state()],
    %% Storage for the ongoing ownership transfers:
    inflight = gen_server:reqids_new() :: gen_server:request_id_collection(),
    %% Generation cache:
    generations :: #{emqx_ds:slab() => emqx_ds:slab_info()}
}).

-type d() :: #d{}.

callback_mode() ->
    [handle_event_function, state_enter].

init([DBShard = {DB, Shard}, CBM]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(#{dbshard => DBShard}),
    emqx_ds_builtin_metrics:init_for_beamformer(DB, Shard),
    %% For catchup pool we don't expect high degree of batch sharing.
    %% It's actually better to distribute subscriptions evenly.
    gproc_pool:new(emqx_ds_beamformer_catchup:pool(DBShard), round_robin, [{auto_size, true}]),
    %% RT pool uses hash of the stream to maximize batch sharing. It's
    %% critical to use hash-based strategy here:
    gproc_pool:new(emqx_ds_beamformer_rt:pool(DBShard), hash, [{auto_size, true}]),
    logger:debug(#{
        msg => started_bf, shard => DBShard, cbm => CBM
    }),
    StuckSubTab = ets:new(emqx_ds_beamformer_stuck_sub_tab, [
        set, public, {keypos, #sub_state.req_id}, {write_concurrency, true}
    ]),
    GVar = #{
        cbm => CBM,
        fc_tab => make_fctab(),
        owner_tab => make_owner_tab(),
        stuck_sub_tab => StuckSubTab
    },
    persistent_term:put(?pt_gvar(DBShard), GVar),
    MTab = ets:new(beamformer_monitor_tab, [private, set]),
    {ok, ?initializing, #d{
        dbshard = DBShard,
        metrics_id = emqx_ds_builtin_metrics:metric_id([{db, DB}]),
        cbm = CBM,
        monitor_tab = MTab,
        stuck_sub_tab = StuckSubTab,
        generations = #{}
    }}.

-spec handle_event(gen_statem:event_type(), _Event, state(), d()) ->
    gen_statem:event_handler_result(state()).
handle_event(ET, Event, State, D0) ->
    #d{dbshard = DBShard, inflight = Inflight0, pending = Pending} = D0,
    case gen_server:check_response(Event, Inflight0, true) of
        {Response, SubState, Inflight} ->
            %% The event is result of `enqueue':
            case Response of
                {reply, ok} ->
                    %% We've successfully delegated the subscription
                    %% to the worker.
                    D = D0#d{inflight = Inflight},
                    {keep_state, D};
                {error, {Reason, ServerRef}} ->
                    %% This can only happen when the worker is down.
                    %% Give it time to recover and reschedule request
                    %% later.
                    %%
                    %% TODO: currently this will pause dispatching of
                    %% subscriptions to *all* workers in the shard.
                    %% Ideally, error handling should be more
                    %% targeted.
                    ?tp(
                        error,
                        emqx_ds_beamformer_delegate_error,
                        #{shard => DBShard, worker => ServerRef, error => Reason}
                    ),
                    D = D0#d{inflight = Inflight, pending = [SubState | Pending]},
                    {next_state, ?recovering, D}
            end;
        _ ->
            %% Handle everything else:
            do_handle_event(ET, Event, State, D0)
    end.

%% Handle all events that are not async gen_server replies
-spec do_handle_event(gen_statem:event_type(), _Event, state(), d()) ->
    gen_statem:event_handler_result(state()).
%% Handle subscribe call:
do_handle_event(
    {call, From},
    #sub_req{client = Client, it = It, opts = Opts, mref = SubRef},
    State,
    D0 = #d{
        dbshard = Shard,
        cbm = CBM,
        monitor_tab = MTab,
        pending = Pending
    }
) ->
    try CBM:unpack_iterator(Shard, It) of
        #{
            stream := Stream,
            topic_filter := TF,
            last_seen_key := DSKey,
            rank := Rank
        } ->
            MRef = monitor(process, Client),
            ?tp(beamformer_subscribe, #{
                sub_id => SubRef,
                mref => MRef,
                shard => Shard,
                key => DSKey
            }),
            %% Update monitor reference table:
            ets:insert(MTab, {MRef, SubRef}),
            #{max_unacked := MaxUnacked} = Opts,
            %% Create the flow control object, storing subscription
            %% seqno, ack and active state. It's a triplet of atomic
            %% variables:
            FlowControl = atomics:new(3, []),
            SubState = #sub_state{
                req_id = SubRef,
                client = Client,
                flowcontrol = {MaxUnacked, FlowControl},
                rank = Rank,
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                it = It
            },
            %% Store another reference to the flow control object in a
            %% different table readable by the client during suback:
            ets:insert(fc_tab(Shard), #fctab{
                sub_ref = SubRef, max_unacked = MaxUnacked, aref = FlowControl
            }),
            %% Create an entry in the owner table:
            ets:insert(owner_tab(Shard), #owner_tab{sub_ref = SubRef, mref = MRef}),
            %% Schedule dispaching to the catchup worker:
            D = D0#d{pending = [SubState | Pending]},
            Reply = {reply, From, {ok, SubRef}},
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
            Error = ?err_unrec({EC, Reason, Stack}),
            {keep_state_and_data, {reply, From, Error}}
    end;
%% Handle unsubscribe call:
do_handle_event(
    {call, From},
    #unsub_req{id = SubId},
    _State,
    D0
) ->
    {Ret, D} = remove_subscription(SubId, D0),
    {keep_state, D, {reply, From, Ret}};
%% Handle disown request:
do_handle_event(
    cast,
    #disown_stuck_req{sub_state = SubState},
    _State,
    #d{dbshard = DBShard, metrics_id = Metrics}
) ->
    #sub_state{req_id = SubRef} = SubState,
    case ets:update_element(owner_tab(DBShard), SubRef, {#owner_tab.owner, undefined}) of
        true ->
            emqx_ds_builtin_metrics:inc_subs_stuck_total(Metrics),
            %% Subscription was found, insert it to the disowned
            %% table:
            true = ets:insert_new(disowned_sub_tab(DBShard), SubState),
            ok;
        false ->
            %% Subscription has been removed in the meantime. Ignore.
            ok
    end,
    keep_state_and_data;
%% Handle wakeup subscription call:
do_handle_event(
    {call, From},
    #wakeup_sub_req{id = SubId},
    State,
    D0 = #d{pending = Pending, stuck_sub_tab = Tab, metrics_id = Metrics}
) ->
    case ets:take(Tab, SubId) of
        [SubS = #sub_state{flowcontrol = {_MaxUnacked, ARef}}] ->
            atomics:put(ARef, ?fc_idx_stuck, 0),
            emqx_ds_builtin_metrics:inc_subs_unstuck_total(Metrics),
            Reply = {reply, From, ok},
            D = D0#d{pending = [SubS | Pending]},
            case State of
                ?idle ->
                    {next_state, ?busy, D, Reply};
                _ ->
                    {keep_state, D, Reply}
            end;
        [] ->
            Reply = {reply, From, {error, subscription_not_found}},
            {keep_state_and_data, Reply}
    end;
%% Handle worker crash:
do_handle_event(
    info,
    {'ETS-TRANSFER', ETS, FromPid, ?heir_info},
    _State,
    D
) ->
    %% One of the workers crashed and we inherited its subscription
    %% table. We'll sent DOWN messages to the subscribers on its behalf:
    ?tp(warning, emqx_ds_beamformer_worker_crash, #{pid => FromPid}),
    on_worker_down(ETS),
    ets:delete(ETS),
    {next_state, ?recovering, D};
%% Handle recoverable errors:
do_handle_event(
    {call, From},
    #handle_recoverable_req{sub_states = SubStates},
    _State,
    D = #d{pending = Pending}
) ->
    ?tp(info, emqx_ds_beamformer_handle_recoverable, #{pid => From}),
    {next_state, ?recovering, D#d{pending = SubStates ++ Pending}};
%% Handle unknown call:
do_handle_event(
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
    {keep_state_and_data, {reply, From, ?err_unrec({unknown_call, Call})}};
%% Handle down event:
do_handle_event(
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
do_handle_event(enter, _OldState, ?initializing, _D) ->
    {keep_state_and_data, {state_timeout, 0, ?init_timeout}};
do_handle_event(enter, _OldState, ?busy, _D) ->
    %% In `busy' state we try to hand over subscriptions to the
    %% workers as soon as possible:
    {keep_state_and_data, {state_timeout, 0, ?schedule_timeout}};
do_handle_event(enter, _OldState, ?recovering, _D) ->
    %% In `recovering' state we delay handover to the worker to let
    %% the system stabilize:
    Cooldown = 1000,
    {keep_state_and_data, {state_timeout, Cooldown, ?schedule_timeout}};
do_handle_event(enter, _OldState, _NewState, _D) ->
    keep_state_and_data;
%% Perform initialization:
do_handle_event(state_timeout, ?init_timeout, ?initializing, D = #d{dbshard = {DB, _}}) ->
    try emqx_ds:list_generations_with_lifetimes(DB) of
        Generations ->
            {next_state, ?busy, D#d{generations = Generations}}
    catch
        _:_ ->
            {keep_state_and_data, {state_timeout, 1000, ?init_timeout}}
    end;
do_handle_event(_Kind, _Event, ?initializing, _D) ->
    %% Ignore events until fulliy initialized:
    keep_state_and_data;
%% Handover subscriptions to the workers:
do_handle_event(state_timeout, ?schedule_timeout, _State, D0) ->
    D = do_schedule(D0),
    {next_state, ?idle, D};
%% Handle changes to the generations:
do_handle_event(
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
do_handle_event(EventType, Event, State, D) ->
    ?tp(
        error,
        emqx_ds_beamformer_unknown_event,
        #{event_type => EventType, state => State, data => D, event => Event}
    ),
    keep_state_and_data.

terminate(Reason, _State, #d{dbshard = DBShard}) ->
    gproc_pool:force_delete(emqx_ds_beamformer_rt:pool(DBShard)),
    gproc_pool:force_delete(emqx_ds_beamformer_catchup:pool(DBShard)),
    persistent_term:erase(?pt_gvar(DBShard)),
    emqx_ds_lib:terminate(?MODULE, Reason, #{}).

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc RPC target, obsolete. Kept for compatibility.
-spec do_dispatch(beam()) -> ok.
do_dispatch(_) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

-spec beams_add_per_node(
    module(),
    dbshard(),
    ets:tid(),
    non_neg_integer(),
    emqx_ds:message_key(),
    emqx_types:message(),
    _MatchCtx,
    emqx_ds:sub_ref(),
    bbn()
) ->
    bbn().
beams_add_per_node(Mod, DBShard, SubTab, GlobalNMsgs, Key, Msg, MatchCtx, SubRef, NodeS0) ->
    #beam_builder_node{subs = Subscribers, global_n_msgs = MyGlobalN, pack = Pack, n_msgs = NMsgs} =
        NodeS0,
    %% Lookup subscription's matching parameters (SubS) and
    %% per-subscription beam builder state (BBS0). If it's not found,
    %% set BBS0 to undefined. Note: variables bound by this expression
    %% are used later.
    case Subscribers of
        #{SubRef := BBS0 = #beam_builder_sub{matcher = Matcher, s = SubS}} ->
            ok;
        #{} ->
            {ok, SubS} = sub_tab_lookup(SubTab, SubRef),
            Matcher = Mod:iterator_match_context(DBShard, SubS#sub_state.it),
            BBS0 = undefined
    end,
    %% Check if the messages should be delivered to the subscriber:
    case Matcher(MatchCtx) of
        false ->
            %% Message was rejected, ignore:
            NodeS0;
        true ->
            %% Yes, this message is meant for the subscriber.
            %%
            %% 1. Ensure the message is added to the node's pack:
            case MyGlobalN =:= GlobalNMsgs of
                true ->
                    %% This message is already in the pack, as
                    %% indicated by MyGlobalN:
                    %%
                    %% 1.1 Do not update the pack/n_msgs:
                    NodeS1 = NodeS0,
                    %% 1.2 Index of this message is NMsgs - 1:
                    Idx = NMsgs - 1;
                false ->
                    %% This message is new for the node.
                    %%
                    %% 1.1 Add message to the pack and increase n_msgs:
                    NodeS1 = NodeS0#beam_builder_node{
                        global_n_msgs = GlobalNMsgs,
                        pack = [{Key, Msg} | Pack],
                        n_msgs = NMsgs + 1
                    },
                    %% 1.2 Index of this message in the pack = (NMsgs + 1) - 1:
                    Idx = NMsgs
            end,
            %% 2. Update subscriber's state:
            BBS =
                case BBS0 of
                    undefined ->
                        %% This is the first message for subscriber.
                        %% Initialize its state:
                        Mask = emqx_ds_dispatch_mask:enc_push_true(
                            Idx, emqx_ds_dispatch_mask:enc_make()
                        ),
                        #beam_builder_sub{
                            s = SubS,
                            matcher = Matcher,
                            mask = Mask,
                            n_msgs = 1
                        };
                    #beam_builder_sub{mask = Mask0, n_msgs = N} ->
                        %% Existing subscriber:
                        Mask = emqx_ds_dispatch_mask:enc_push_true(Idx, Mask0),
                        BBS0#beam_builder_sub{
                            mask = Mask,
                            n_msgs = N + 1
                        }
                end,
            %% 3. Update the accumulator:
            NodeS1#beam_builder_node{subs = Subscribers#{SubRef => BBS}}
    end.

-spec fc_tab(dbshard()) -> ets:tid().
fc_tab(DBShard) ->
    #{fc_tab := TID} = gvar(DBShard),
    TID.

-spec owner_tab(dbshard()) -> ets:tid().
owner_tab(DBShard) ->
    #{owner_tab := TID} = gvar(DBShard),
    TID.

%% @doc Transfer ownership over subscription to the parent. Called by
%% the worker when it drops the subscription from its active queue:
-spec disown_stuck_subscription(dbshard(), ets:tid(), sub_state()) -> ok.
disown_stuck_subscription(DBShard, SubTab, #sub_state{req_id = SubRef}) ->
    {ok, SubState} = sub_tab_take(SubTab, SubRef),
    gen_statem:cast(?via(DBShard), #disown_stuck_req{sub_state = SubState}).

-spec disowned_sub_tab(dbshard()) -> ets:tid().
disowned_sub_tab(DBShard) ->
    #{stuck_sub_tab := TID} = gvar(DBShard),
    TID.

beams_conclude_node(DBShard, NextKey, BeamMaker, Node, BeamMakerNode) ->
    #beam_builder{
        cbm = CBM,
        sub_tab = SubTab,
        lagging = IsLagging,
        queue_drop = QueueDrop,
        queue_update = QueueUpdate
    } = BeamMaker,
    #beam_builder_node{
        n_msgs = BatchSize,
        pack = PackRev,
        subs = Subscribers
    } = BeamMakerNode,
    Flags0 =
        case IsLagging of
            true -> ?DISPATCH_FLAG_LAGGING;
            false -> 0
        end,
    Destinations =
        maps:fold(
            fun(SubId, #beam_builder_sub{s = SubS0, mask = MaskEncoder, n_msgs = NMsgs}, Acc) ->
                #sub_state{client = Client, it = It0} = SubS0,
                ?tp(beamformer_fulfilled, #{sub_id => SubId}),
                %% Update the iterator, start key and message
                %% matcher for the subscription:
                {ok, It} = update_iterator(CBM, DBShard, It0, NextKey),
                %% Update the subscription state:
                SubS = SubS0#sub_state{it = It, start_key = NextKey},
                sub_tab_update(SubTab, SubS),
                %% Update sequence number and check if the
                %% subscription should remain active:
                {Active, SeqNo} = keep_and_seqno(SubTab, SubS0, NMsgs),
                %% Update the worker's queue:
                case Active of
                    true ->
                        %% Subscription is keeping up with the
                        %% acks. Update the worker's active
                        %% queue:
                        Flags = Flags0,
                        QueueUpdate(
                            SubS0,
                            SubS
                        );
                    false ->
                        %% This subscription is not keeping up
                        %% with the acks. Freeze the
                        %% subscription by dropping it from
                        %% the worker's active queue:
                        Flags = Flags0 bor ?DISPATCH_FLAG_STUCK,
                        disown_stuck_subscription(DBShard, SubTab, SubS),
                        QueueDrop(SubS0)
                end,
                DispatchMask = emqx_ds_dispatch_mask:enc_finalize(BatchSize + 1, MaskEncoder),
                [
                    ?DESTINATION(Client, SubId, SeqNo, DispatchMask, Flags, It)
                    | Acc
                ]
            end,
            [],
            Subscribers
        ),
    %% Send the beam to the destination node:
    send_out(DBShard, Node, lists:reverse(PackRev), Destinations).

send_out_final_term_to_node(DBShard, SubTab, Term, Node, Reqs) ->
    Mask = emqx_ds_dispatch_mask:encode([true]),
    Destinations = lists:map(
        fun(
            #sub_state{
                client = Client, req_id = SubId, it = It, flowcontrol = FC
            }
        ) ->
            {_MaxUnacked, ARef} = FC,
            SeqNo = atomics:add_get(ARef, ?fc_idx_seqno, 1),
            sub_tab_delete(SubTab, SubId),
            ?DESTINATION(Client, SubId, SeqNo, Mask, 0, It)
        end,
        Reqs
    ),
    send_out(DBShard, Node, Term, Destinations).

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

do_schedule(D = #d{dbshard = DBShard, pending = Pending, inflight = Inflight0}) ->
    Pool = emqx_ds_beamformer_catchup:pool(DBShard),
    %% Distribute new subscriptions from the pending queue to the workers:
    Inflight = lists:foldl(
        fun(#sub_state{} = SubState, Acc) ->
            %% Find the appropriate worker for the request.
            Worker = gproc_pool:pick_worker(Pool),
            emqx_ds_beamformer_catchup:enqueue(Worker, SubState, Acc)
        end,
        Inflight0,
        Pending
    ),
    D#d{pending = [], inflight = Inflight}.

remove_subscription(
    SubId, D = #d{dbshard = DBShard, stuck_sub_tab = SubTab, monitor_tab = MTab, pending = Pending}
) ->
    ets:delete(fc_tab(DBShard), SubId),
    sub_tab_delete(SubTab, SubId),
    case ets:take(owner_tab(DBShard), SubId) of
        [#owner_tab{owner = Owner, mref = MRef}] ->
            %% Remove client monitoring:
            demonitor(MRef, [flush]),
            ets:delete(MTab, MRef),
            %% Remove stuck subscription (if stuck):
            ets:delete(disowned_sub_tab(DBShard), SubId),
            %% Notify the worker (if not stuck):
            is_pid(Owner) andalso
                (Owner ! #unsub_req{id = SubId}),
            {true, D#d{pending = drop_from_pending(SubId, Pending)}};
        [] ->
            {false, D}
    end.

drop_from_pending(SubId, Pending) ->
    lists:filter(
        fun(#sub_state{req_id = Id}) -> Id =/= SubId end,
        Pending
    ).

send_out(DBShard = {DB, _}, Node, Pack, Destinations) ->
    ?tp(debug, beamformer_out, #{
        dest_node => Node,
        destinations => Destinations
    }),
    case node() of
        Node ->
            %% TODO: this may block the worker. Introduce a separate
            %% worker for local fanout?
            emqx_ds_beamsplitter:dispatch_v2(DB, Pack, Destinations, #{});
        _ ->
            emqx_ds_beamsplitter_proto_v2:dispatch(DBShard, Node, DB, Pack, Destinations, #{})
    end.

-compile({inline, gvar/1}).
-spec gvar(dbshard()) -> gvar().
gvar(DBShard) ->
    persistent_term:get(?pt_gvar(DBShard)).

make_fctab() ->
    ets:new(emqx_ds_beamformer_flow_control_tab, [
        set,
        protected,
        {keypos, #fctab.sub_ref},
        {read_concurrency, true}
    ]).

make_owner_tab() ->
    ets:new(emqx_ds_beamformer_owner_tab, [
        set,
        public,
        {keypos, #owner_tab.sub_ref},
        {write_concurrency, true}
    ]).

%% @doc Send fake monitor messages to all subscribers registered in
%% the table, nudging them to resubscribe.
on_worker_down(InheritedSubTab) ->
    MS = {#sub_state{client = '$1', req_id = '$2', _ = '_'}, [], [{{'$1', '$2'}}]},
    notify_down_loop(ets:select(InheritedSubTab, [MS], 100)).

notify_down_loop('$end_of_table') ->
    ok;
notify_down_loop({Matches, Cont}) ->
    _ = [Pid ! {'DOWN', Ref, process, self(), worker_crash} || {Pid, Ref} <- Matches],
    notify_down_loop(ets:select(Cont)).

%% @doc Version of `fold' that operates on a list of elements tagged
%% with a "group". Each group has its own accumulator.
%%
%% This function is optimizied for processing lists sorted by groups.
-spec fold_with_groups(
    fun((Elem, GroupState) -> GroupState),
    GroupState,
    [{Group, Elem}],
    #{Group => GroupState}
) ->
    #{Group => GroupState}.
fold_with_groups(_Fun, _InitialState, [], Acc) ->
    Acc;
fold_with_groups(Fun, InitialState, [{Group, Elem} | Rest], Acc) ->
    case Acc of
        #{Group := GroupS} ->
            fold_with_groups(
                Fun,
                InitialState,
                Group,
                Fun(Elem, GroupS),
                Rest,
                Acc
            );
        #{} ->
            fold_with_groups(
                Fun,
                InitialState,
                Group,
                Fun(Elem, InitialState),
                Rest,
                Acc
            )
    end.

-spec fold_with_groups(
    fun((Elem, GroupState) -> GroupState),
    GroupState,
    Group,
    GroupState,
    [{Group, Elem}],
    #{Group => GroupState}
) ->
    #{Group => GroupState}.
fold_with_groups(_Fun, _InitialState, CurrentGroup, CurrentGroupState, [], Acc) ->
    Acc#{CurrentGroup => CurrentGroupState};
fold_with_groups(Fun, InitialState, CurrentGroup, CurrentGroupState, [{Group, Elem} | Rest], Acc0) ->
    case Group of
        CurrentGroup ->
            fold_with_groups(
                Fun,
                InitialState,
                CurrentGroup,
                Fun(Elem, CurrentGroupState),
                Rest,
                Acc0
            );
        _ ->
            Acc = Acc0#{CurrentGroup => CurrentGroupState},
            case Acc of
                #{Group := GroupState} ->
                    fold_with_groups(
                        Fun,
                        InitialState,
                        Group,
                        Fun(Elem, GroupState),
                        Rest,
                        Acc
                    );
                #{} ->
                    fold_with_groups(
                        Fun,
                        InitialState,
                        Group,
                        Fun(Elem, InitialState),
                        Rest,
                        Acc
                    )
            end
    end.
