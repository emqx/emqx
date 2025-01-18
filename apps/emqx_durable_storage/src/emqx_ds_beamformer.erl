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
-export([start_link/2, where/1]).
-export([poll/5, subscribe/6, unsubscribe/2, shard_event/2, generation_event/1, suback/3]).
-export([unpack_iterator/3, update_iterator/4, scan_stream/6, high_watermark/3, fast_forward/4]).
-export([make_subtab/1, owner_tab/1, take_ownership/3, handle_recoverable_error/2]).
-export([beams_init/6, beams_add/4, beams_conclude/3, beams_n_matched/1]).
-export([shard_metrics_id/1, send_out_final_beam/4]).
-export([cfg_batch_size/0, cfg_housekeeping_interval/0, cfg_workers_per_shard/0]).

%% internal exports:
-export([do_dispatch/1]).
%% Testing/debugging:
-export([ls/1, subscription_info/2]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

-export_type([
    dbshard/0,
    opts/0,
    sub_state/0, sub_state/2,
    beam/2, beam/0,
    return_addr/1,
    unpack_iterator_result/1,
    event_topic/0,
    stream_scan_return/0,
    beam_builder/0
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

-type sub_state(UserData, Iterator) ::
    #sub_state{
        req_id :: reference(),
        client :: pid(),
        mref :: reference(),
        %% Flow control:
        flowcontrol :: flowcontrol(),
        %%
        rank :: emqx_ds:stream_rank(),
        stream :: _Stream,
        topic_filter :: emqx_ds:topic_filter(),
        start_key :: emqx_ds:message_key(),
        %% Information about the process that created the request:
        userdata :: UserData,
        %% Iterator:
        it :: Iterator,
        %% Callback that filters messages that belong to the request:
        msg_matcher :: match_messagef(),
        deadline :: integer() | undefined
    }.

-type sub_state() :: sub_state(_, _).

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
    stuck_sub_tab := ets:tid()
}.

%% Beam builder:

%% Per-subscription builder state:
-record(beam_builder_sub, {
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

%% Global builder:
-record(beam_builder, {
    cbm :: module(),
    sub_tab :: ets:tid(),
    lagging :: boolean(),
    shard_id :: _Shard,
    queue_drop :: fun(),
    queue_update :: fun(),
    global_n_msgs = 0 :: non_neg_integer(),
    per_node = #{} :: #{node => #beam_builder_node{}}
}).

-opaque beam_builder() :: #beam_builder{}.

%%================================================================================
%% Callbacks
%%================================================================================

-type match_messagef() :: fun(
    (
        _LastSeenKey :: emqx_ds:message_key(),
        _MessageKey :: emqx_ds:message_key(),
        _TopicTokens :: [binary()],
        emqx_types:message()
    ) -> boolean()
).

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

-callback high_watermark(dbshard(), _Stream) -> {ok, emqx_ds:message_key()} | emqx_ds:error(_).

%% @doc This callback is used to safely advance the iterator to the position represented by the key.
-callback fast_forward(dbshard(), Iterator, emqx_ds:message_key()) ->
    {ok, Iterator} | {ok, end_of_stream} | emqx_ds:error(_).

%%================================================================================
%% API functions
%%================================================================================

-record(sub_req, {
    client :: pid(),
    handle :: emqx_ds:subscription_handle(),
    it :: emqx_ds:ds_specific_iterator(),
    userdata,
    opts :: emqx_ds:sub_opts()
}).
-record(unsub_req, {id :: reference()}).
-record(wakeup_sub_req, {id :: reference()}).
-record(generation_event, {}).
-record(handle_recoverable_req, {sub_states :: [sub_state()]}).

-spec start_link(dbshard(), module()) -> {ok, pid()}.
start_link(DBShard, CBM) ->
    gen_statem:start_link(?via(DBShard), ?MODULE, [DBShard, CBM], []).

%% @obsolete Submit a poll request
-spec poll(node(), return_addr(_ItKey), dbshard(), _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(_Node, _ReturnAddr, _Shard, _Iterator, #{timeout := _Timeout}) ->
    ok.

%% @doc Create a local subscription registry
-spec make_subtab(dbshard()) -> ok.
make_subtab(DBShard) ->
    ets:new(emqx_ds_beamformer_sub_tab, [
        set,
        private,
        {keypos, #sub_state.req_id},
        {heir, where(DBShard), ?heir_info}
    ]).

-spec take_ownership(dbshard(), ets:tid(), sub_state()) -> ok.
take_ownership(DBShard, SubTab, SubS = #sub_state{req_id = SubRef}) ->
    true = ets:insert_new(SubTab, SubS),
    ets:update_element(owner_tab(DBShard), SubRef, {#owner_tab.owner, self()}).

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
    emqx_ds_beamformer_rt:shard_event(Shard, Events).

%% @doc Create a beam that contains `end_of_stream' or unrecoverable
%% error, and send it to the clients. Then delete the subscriptions.
-spec send_out_final_beam(
    dbshard(),
    ets:tid(),
    {ok, end_of_stream} | {error, unrecoverable, _},
    [sub_state()]
) ->
    ok.
send_out_final_beam(DBShard, SubTab, Term, Reqs) ->
    ReqsByNode = maps:groups_from_list(
        fun(#sub_state{client = PID}) ->
            node(PID)
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

-spec beams_init(module(), dbshard(), ets:tid(), boolean(), fun(), fun()) -> beam_builder().
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
        fun(_SubId, #beam_builder_node{subs = Subs}, Acc) ->
            Acc + maps:size(Subs)
        end,
        0,
        PerNode
    ).

-spec beams_add(
    emqx_ds:message_key(),
    emqx_types:message(),
    [reference()],
    beam_builder()
) -> beam_builder().
beams_add(
    Key,
    Msg = #message{topic = Topic},
    Candidates,
    BB = #beam_builder{sub_tab = SubTab, global_n_msgs = GlobalNMsgs0, per_node = PerNode0}
) ->
    GlobalNMsgs = GlobalNMsgs0 + 1,
    TopicTokens = emqx_topic:tokens(Topic),
    PerNode = lists:foldl(
        fun(SubId, Acc) ->
            case ets:lookup(SubTab, SubId) of
                [#sub_state{msg_matcher = Matcher, start_key = LastSeenKey, client = ClientPid}] ->
                    case Matcher(LastSeenKey, Key, TopicTokens, Msg) of
                        true ->
                            Node = node(ClientPid),
                            do_beams_add(GlobalNMsgs, Key, Msg, SubId, Node, Acc);
                        false ->
                            Acc
                    end;
                [] ->
                    Acc
            end
        end,
        PerNode0,
        Candidates
    ),
    BB#beam_builder{
        per_node = PerNode,
        global_n_msgs = GlobalNMsgs
    }.

do_beams_add(GlobalNMsgs, Key, Msg, SubId, Node, Acc) ->
    BBN0 =
        #beam_builder_node{n_msgs = NMsgs, subs = Subs} = maybe_update_pack(
            GlobalNMsgs, Node, Key, Msg, Acc
        ),
    %% Index of message within the per-node batch:
    Index = NMsgs - 1,
    %% Lookup or initialize per-subscription beam builder:
    case Subs of
        #{SubId := #beam_builder_sub{n_msgs = Count, mask = MaskEncoder}} ->
            ok;
        #{} ->
            Count = 0,
            MaskEncoder = emqx_ds_dispatch_mask:enc_make()
    end,
    %% Update the subscription's dispatch mask and increment
    %% its message counter:
    SubS = #beam_builder_sub{
        n_msgs = Count + 1,
        mask = emqx_ds_dispatch_mask:enc_push_true(Index, MaskEncoder)
    },
    %% Update the accumulator:
    BBN = BBN0#beam_builder_node{subs = Subs#{SubId => SubS}},
    Acc#{Node => BBN}.

maybe_update_pack(Global, Node, Key, Msg, PerNodeStates) ->
    case PerNodeStates of
        #{Node := BB = #beam_builder_node{global_n_msgs = Global}} ->
            %% This message has been already added to the node's pack:
            BB;
        #{Node := BB0 = #beam_builder_node{n_msgs = Idx, pack = Pack}} ->
            %% This message is new for the node:
            BB0#beam_builder_node{
                pack = [{Key, Msg} | Pack],
                n_msgs = Idx + 1,
                global_n_msgs = Global
            };
        #{} ->
            #beam_builder_node{
                pack = [{Key, Msg}],
                n_msgs = 1,
                global_n_msgs = Global
            }
    end.

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

shard_metrics_id({DB, Shard}) ->
    emqx_ds_builtin_metrics:shard_metric_id(DB, Shard).

%% @doc In case this is a multishot subscription with a parent get
%% batch seqno and a whether to keep the request in the queue or
%% remove it:
-spec keep_and_seqno(ets:tid(), sub_state(), pos_integer()) ->
    {boolean(), emqx_ds:sub_seqno() | undefined}.
keep_and_seqno(_SubTab, #sub_state{flowcontrol = FC}, Nmsgs) ->
    try
        {MaxUnacked, ARef} = FC,
        SeqNo = atomics:add_get(ARef, ?fc_idx_seqno, Nmsgs),
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
    {ok, Iterator} | {error, unrecoverable, has_data | old_key} | emqx_ds:error(_).
fast_forward(Mod, Shard, It, Key) ->
    Mod:fast_forward(Shard, It, Key).

%%================================================================================
%% Internal subscription management API (RPC target)
%%================================================================================

%% @doc Get pid of a beamformer process serving the shard
-spec where(dbshard()) -> pid() | undefined.
where(DBShard) ->
    gproc:where(?name(DBShard)).

%% @doc List shards that have local beamformer process
-spec ls(emqx_ds:db()) -> [dbshard()].
ls(DB) ->
    MS = {{?name({DB, '$1'}), '_', '_'}, [], ['$1']},
    Shards = gproc:select({local, names}, [MS]),
    maps:from_list(
        [emqx_ds_beamformer_sup:info({DB, I}) || I <- Shards]
    ).

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
    pid(), pid(), emqx_ds:sub_ref(), emqx_ds:ds_specific_iterator(), _ItKey, emqx_ds:sub_opts()
) ->
    {ok, emqx_ds:sub_ref()} | emqx_ds:error(_).
subscribe(Server, Client, SubId, It, ItKey, Opts = #{max_unacked := MaxUnacked}) when
    is_integer(MaxUnacked), MaxUnacked > 0
->
    gen_statem:call(Server, #sub_req{
        client = Client, it = It, userdata = ItKey, opts = Opts, handle = SubId
    }).

-spec unsubscribe(dbshard(), emqx_ds:sub_ref()) -> boolean().
unsubscribe(DBShard, SubId) ->
    gen_statem:call(?via(DBShard), #unsub_req{id = SubId}).

%% @doc Ack batches up to the sequence number:
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
    stuck_sub_tab :: ets:tid(),
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
    gproc_pool:new(emqx_ds_beamformer_catchup:pool(DBShard), hash, [{auto_size, true}]),
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
        cbm = CBM,
        monitor_tab = MTab,
        stuck_sub_tab = StuckSubTab,
        generations = #{}
    }}.

-spec handle_event(gen_statem:event_type(), _Event, state(), d()) ->
    gen_statem:event_handler_result(state()).
%% Handle subscribe call:
handle_event(
    {call, From},
    #sub_req{client = Client, it = It, userdata = Userdata, opts = Opts, handle = SubId},
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
            %% Create the flow control. It's a triplet of atomic
            %% variables:
            FlowControl = atomics:new(3, []),
            SubState = #sub_state{
                req_id = SubId,
                client = Client,
                mref = MRef,
                flowcontrol = {MaxUnacked, FlowControl},
                rank = Rank,
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                userdata = Userdata,
                it = It,
                msg_matcher = MsgMatcher
            },
            %% Insert subscription state into the tables:
            ets:insert(owner_tab(Shard), #owner_tab{sub_ref = SubId}),
            ets:insert(fc_tab(Shard), #fctab{
                sub_ref = SubId, max_unacked = MaxUnacked, aref = FlowControl
            }),
            %% Schedule dispaching to the worker:
            D = D0#d{pending = [SubState | Pending]},
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
    D0 = #d{pending = Pending, stuck_sub_tab = Tab}
) ->
    case ets:take(Tab, SubId) of
        [SubS = #sub_state{flowcontrol = {_MaxUnacked, ARef}}] ->
            atomics:put(ARef, ?fc_idx_stuck, 0),
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
handle_event(
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
handle_event(
    {call, From},
    #handle_recoverable_req{sub_states = SubStates},
    _State,
    D = #d{pending = Pending}
) ->
    ?tp(info, emqx_ds_beamformer_handle_recoverable, #{pid => From}),
    {next_state, ?recovering, D#d{pending = SubStates ++ Pending}};
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
handle_event(enter, _OldState, ?initializing, _D) ->
    {keep_state_and_data, {state_timeout, 0, ?init_timeout}};
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
%% Perform initialization:
handle_event(state_timeout, ?init_timeout, ?initializing, D = #d{dbshard = {DB, _}}) ->
    try emqx_ds:list_generations_with_lifetimes(DB) of
        Generations ->
            {next_state, ?busy, D#d{generations = Generations}}
    catch
        _:_ ->
            {keep_state_and_data, {state_timeout, 1000, ?init_timeout}}
    end;
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
    gproc_pool:force_delete(emqx_ds_beamformer_rt:pool(DBShard)),
    gproc_pool:force_delete(emqx_ds_beamformer_catchup:pool(DBShard)),
    persistent_term:erase(?pt_gvar(DBShard)).

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
    [SubState] = ets:take(SubTab, SubRef),
    ets:update_element(owner_tab(DBShard), SubRef, {#owner_tab.owner, undefined}),
    true = ets:insert_new(disowned_sub_tab(DBShard), SubState).

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
        subs = Subs
    } = BeamMakerNode,
    Flags0 =
        case IsLagging of
            true -> ?DISPATCH_FLAG_LAGGING;
            false -> 0
        end,
    Destinations =
        maps:fold(
            fun(SubId, #beam_builder_sub{mask = MaskEncoder, n_msgs = NMsgs}, Acc) ->
                case ets:lookup(SubTab, SubId) of
                    [SubS0 = #sub_state{client = Client, userdata = UserData, it = It0}] ->
                        ?tp(beamformer_fulfilled, #{sub_id => SubId}),
                        %% Update the iterator, start key and message
                        %% matcher for the subscription:
                        {ok, It} = update_iterator(CBM, DBShard, It0, NextKey),
                        %% Update the subscription state:
                        SubS = SubS0#sub_state{it = It, start_key = NextKey},
                        ets:insert(SubTab, SubS),
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
                        DispatchMask = emqx_ds_dispatch_mask:enc_finalize(BatchSize, MaskEncoder),
                        [
                            ?DESTINATION(Client, SubId, UserData, SeqNo, DispatchMask, Flags, It)
                            | Acc
                        ];
                    [] ->
                        %% Subscription has been removed in the
                        %% meantime:
                        Acc
                end
            end,
            [],
            Subs
        ),
    %% Send the beam to the destination node:
    send_out(DBShard, Node, lists:reverse(PackRev), Destinations).

send_out_final_term_to_node(DBShard, SubTab, Term, Node, Reqs) ->
    Mask = emqx_ds_dispatch_mask:encode([true]),
    Destinations = lists:map(
        fun(
            #sub_state{
                client = Client, req_id = SubId, userdata = UserData, it = It, flowcontrol = FC
            }
        ) ->
            {_MaxUnacked, ARef} = FC,
            SeqNo = atomics:add_get(ARef, ?fc_idx_seqno, 1),
            ets:delete(SubTab, SubId),
            ?DESTINATION(Client, SubId, UserData, SeqNo, Mask, 0, It)
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

do_schedule(D = #d{dbshard = DBShard, pending = Pending}) ->
    Pool = emqx_ds_beamformer_catchup:pool(DBShard),
    %% Find the appropriate worker for the requests and group the requests:
    ByWorker = maps:groups_from_list(
        fun(#sub_state{stream = Stream}) ->
            gproc_pool:pick_worker(Pool, Stream)
        end,
        Pending
    ),
    %% Dispatch the events to the workers and collect the requests
    %% that couldn't be dispatched:
    Unmatched = maps:fold(
        fun(Worker, Reqs, Unmatched) ->
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

remove_subscription(
    SubId, D = #d{dbshard = DBShard, stuck_sub_tab = SubTab, monitor_tab = _MTab, pending = Pending}
) ->
    ets:delete(fc_tab(DBShard), SubId),
    ets:delete(SubTab, SubId),
    case ets:take(owner_tab(DBShard), SubId) of
        [#owner_tab{owner = _Owner}] ->
            %% FIXME:
            %% demonitor(MRef, [flush]),
            %% ets:delete(MTab, MRef),
            %% FIXME: notify owner
            {true, D#d{pending = Pending -- [SubId]}};
        [] ->
            {false, D}
    end.

send_out(DBShard, Node, Pack, Destinations) ->
    ?tp(debug, beamformer_out, #{
        dest_node => Node,
        destinations => Destinations
    }),
    case node() of
        Node ->
            %% TODO: this may block the worker. Introduce a separate
            %% worker for local fanout?
            emqx_ds_beamsplitter:dispatch_v2(Pack, Destinations);
        _ ->
            emqx_ds_beamsplitter_proto_v2:dispatch(DBShard, Node, Pack, Destinations)
    end.

-compile({inline, gvar/1}).
-spec gvar(dbshard()) -> gvar().
gvar(DBShard) ->
    persistent_term:get(?pt_gvar(DBShard)).

%% subtab(Shard) ->
%%     persistent_term:get(?subtid(Shard)).

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
    [Pid ! {'DOWN', Ref, process, self(), worker_crash} || {Pid, Ref} <- Matches],
    notify_down_loop(ets:select(Cont)).
