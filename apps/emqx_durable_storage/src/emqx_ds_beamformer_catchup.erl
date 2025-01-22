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

%% @doc This type of beamformer worker takes care of fulfilling the
%% poll requests data committed long time ago. It utilizes the
%% internal DS indexes.
-module(emqx_ds_beamformer_catchup).

-behavior(gen_server).

%% API:
-export([start_link/4, pool/1, enqueue/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds_beamformer.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(s, {
    module :: module(),
    metrics_id,
    shard_id,
    sub_tab :: ets:table(),
    name,
    queue :: ets:table(),
    batch_size :: non_neg_integer()
}).

-type s() :: #s{}.

-record(enqueue_req, {sub_states}).
-define(fulfill_loop, beamformer_fulfill_loop).
-define(housekeeping_loop, housekeeping_loop).

-define(queue_elem(STREAM, TF, STARTKEY, DESTNODE, SUBREF),
    {STREAM, TF, STARTKEY, DESTNODE, SUBREF}
).

%%================================================================================
%% API functions
%%================================================================================

-spec enqueue(pid(), [emqx_ds_beamformer:sub_state()]) ->
    ok | {error, unrecoverable, stale} | emqx_ds:error(_).
enqueue(Worker, SubStates) ->
    try
        gen_server:call(Worker, #enqueue_req{sub_states = SubStates}, infinity)
    catch
        exit:{timeout, _} ->
            {error, recoverable, timeout}
    end.

-spec start_link(module(), _Shard, integer(), emqx_ds_beamformer:opts()) -> {ok, pid()}.
start_link(Mod, ShardId, Name, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name, Opts], []).

%% @doc Pool of catchup beamformers
pool(Shard) ->
    {emqx_ds_beamformer_catchup, Shard}.

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, ShardId, Name, _Opts]) ->
    process_flag(trap_exit, true),
    %% Attach this worker to the pool:
    gproc_pool:add_worker(pool(ShardId), Name),
    gproc_pool:connect_worker(pool(ShardId), Name),
    S = #s{
        module = CBM,
        shard_id = ShardId,
        sub_tab = emqx_ds_beamformer:make_subtab(ShardId),
        metrics_id = emqx_ds_beamformer:shard_metrics_id(ShardId),
        name = Name,
        queue = queue_new(),
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(#enqueue_req{sub_states = SubStates}, _From, S) ->
    do_enqueue(SubStates, S),
    {reply, ok, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(?fulfill_loop, S0) ->
    put(?fulfill_loop, false),
    S = fulfill(S0),
    {noreply, S};
handle_info(
    ?housekeeping_loop, S0 = #s{}
) ->
    %% Reload configuration according from environment variables:
    S = S0#s{
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    erlang:send_after(emqx_ds_beamformer:cfg_housekeeping_interval(), self(), ?housekeeping_loop),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(Reason, #s{sub_tab = SubTab, shard_id = ShardId, name = Name}) ->
    gproc_pool:disconnect_worker(pool(ShardId), Name),
    gproc_pool:remove_worker(pool(ShardId), Name),
    %% Should the master to deal with the remaining subscriptions?
    case Reason of
        shutdown ->
            ets:delete(SubTab);
        _ ->
            ok
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%% Temporary remove requests from the active queue and notify the
%% parent about reschedule.
handle_recoverable(#s{sub_tab = SubTab, queue = Queue, shard_id = DBShard}, Subs) ->
    ok = emqx_ds_beamformer:handle_recoverable_error(DBShard, Subs),
    lists:foreach(
        fun(SubS) -> drop(Queue, SubTab, SubS) end,
        Subs
    ).

do_enqueue(SubStates, #s{shard_id = DBShard, sub_tab = SubTab, queue = Queue, metrics_id = Metrics}) ->
    lists:foreach(
        fun(SubS) ->
            queue_push(Queue, SubS),
            emqx_ds_beamformer:take_ownership(DBShard, SubTab, SubS)
        end,
        SubStates
    ),
    emqx_ds_builtin_metrics:set_pendingq_len(Metrics, ets:info(Queue, size)),
    ensure_fulfill_loop().

fulfill(S = #s{queue = Queue}) ->
    %% debug_pending(S),
    %% TODO: rotate through streams, try to be more fair.
    case find_older_request(Queue) of
        undefined ->
            S;
        {Stream, TopicFilter, StartKey} ->
            ?tp(emqx_ds_beamformer_fulfill_old, #{
                stream => Stream, topic => TopicFilter, start_key => StartKey
            }),
            do_fulfill(S, Stream, TopicFilter, StartKey),
            ensure_fulfill_loop(),
            S
    end.

do_fulfill(
    S = #s{
        shard_id = DBShard,
        sub_tab = SubTab,
        module = CBM,
        batch_size = BatchSize,
        queue = Queue,
        metrics_id = Metrics
    },
    Stream,
    TopicFilter,
    StartKey
) ->
    case emqx_ds_beamformer:scan_stream(CBM, DBShard, Stream, TopicFilter, StartKey, BatchSize) of
        {ok, EndKey, []} ->
            %% Empty batch? Try to move request to the RT queue:
            move_to_realtime(S, Stream, TopicFilter, StartKey, EndKey);
        {ok, EndKey, Batch} ->
            fulfill_batch(S, Stream, TopicFilter, StartKey, EndKey, Batch);
        {error, recoverable, Err} ->
            ?tp(
                warning,
                emqx_ds_beamformer_fulfill_fail,
                #{shard => DBShard, recoverable => true, error => Err}
            ),
            handle_recoverable(S, lookup_subs(S, Stream, TopicFilter, StartKey));
        Other ->
            Pack =
                case Other of
                    {ok, end_of_stream} ->
                        end_of_stream;
                    {error, unrecoverable, _} ->
                        Other
                end,
            MatchReqs = lookup_subs(S, Stream, TopicFilter, StartKey),
            NFulfilled = length(MatchReqs),
            report_metrics(Metrics, NFulfilled),
            %% Pack requests into beams and send out:
            emqx_ds_beamformer:send_out_final_beam(DBShard, SubTab, Pack, MatchReqs),
            %% Remove requests from the queue to avoid repeated failure:
            queue_drop_all(Queue, Stream, TopicFilter, StartKey)
    end.

-spec move_to_realtime(
    s(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:message_key(),
    emqx_ds:message_key()
) ->
    ok.
move_to_realtime(
    S = #s{shard_id = DBShard, sub_tab = SubTab, queue = Queue},
    Stream,
    TopicFilter,
    StartKey,
    EndKey
) ->
    Reqs = lookup_subs(S, Stream, TopicFilter, StartKey),
    lists:foreach(
        fun(Req) ->
            ?tp(debug, beamformer_move_to_rt, #{
                shard => DBShard,
                stream => Stream,
                tf => TopicFilter,
                start_key => StartKey,
                end_key => EndKey,
                req_id => Req#sub_state.req_id
            }),
            case emqx_ds_beamformer_rt:enqueue(DBShard, Req) of
                ok ->
                    drop(Queue, SubTab, Req);
                {error, unrecoverable, stale} ->
                    %% Race condition: new data has been added. Keep
                    %% this request in the catchup queue, so
                    %% fulfillment of this requst is retried
                    %% immediately (is it a good idea?):
                    ok;
                {error, recoverable, Error} ->
                    ?tp(
                        warning,
                        emqx_ds_beamformer_move_to_rt_fail,
                        #{shard => DBShard, recoverable => true, req => Req, error => Error}
                    ),
                    handle_recoverable(S, [Req]);
                Err = {error, unrecoverable, Reason} ->
                    ?tp(
                        error,
                        emqx_ds_beamformer_move_to_rt_fail,
                        #{shard => DBShard, recoverable => false, req => Req, error => Reason}
                    ),
                    emqx_ds_beamformer:send_out_final_beam(DBShard, SubTab, Err, [Req]),
                    queue_drop(Queue, Req)
            end
        end,
        Reqs
    ).

-spec fulfill_batch(
    s(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:message_key(),
    emqx_ds:message_key(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) -> ok.
fulfill_batch(
    S = #s{shard_id = ShardId, sub_tab = SubTab, metrics_id = Metrics, module = CBM, queue = Queue},
    Stream,
    TopicFilter,
    StartKey,
    EndKey,
    Batch
) ->
    %% Find iterators that match the start message of the batch (to
    %% handle iterators freshly created by `emqx_ds:make_iterator'):
    Candidates = queue_lookup(S, Stream, TopicFilter, StartKey),
    %% Search for iterators where `last_seen_key' is equal to key of
    %% any message in the batch:
    BeamMaker =
        process_batch(
            S,
            Stream,
            TopicFilter,
            Batch,
            Candidates,
            emqx_ds_beamformer:beams_init(
                CBM,
                ShardId,
                SubTab,
                true,
                fun(Req) -> queue_drop(Queue, Req) end,
                fun(OldReq, Req) -> queue_update(Queue, OldReq, Req) end
            )
        ),
    report_metrics(Metrics, emqx_ds_beamformer:beams_n_matched(BeamMaker)),
    %% Send out the replies:
    ok = emqx_ds_beamformer:beams_conclude(ShardId, EndKey, BeamMaker).

-spec process_batch(
    s(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    [{emqx_ds:message_key(), emqx_types:message()}],
    [#sub_state{}],
    emqx_ds_beamformer:beam_builder()
) -> emqx_ds_beamformer:beam_builder().
process_batch(_S, _Stream, _TopicFilter, [], _Candidates, Beams) ->
    Beams;
process_batch(S, Stream, TopicFilter, [{Key, Msg} | Rest], Candidates0, Beams0) ->
    Candidates = queue_lookup(S, Stream, TopicFilter, Key) ++ Candidates0,
    Beams = emqx_ds_beamformer:beams_add(Stream, Key, Msg, Candidates, Beams0),
    process_batch(S, Stream, TopicFilter, Rest, Candidates, Beams).

%% It's always worth trying to fulfill the oldest requests first,
%% because they have a better chance of producing a batch that
%% overlaps with other pending requests.
%%
%% This function implements a heuristic that tries to find such poll
%% request. It simply compares the keys (and nothing else) within a
%% small sample of pending polls, and picks request with the smallest
%% key as the starting point.
-spec find_older_request(ets:tid()) ->
    {emqx_ds:stream(), emqx_ds:topic_filter(), emqx_ds:message_key()} | undefined.
find_older_request(Tab) ->
    case ets:first(Tab) of
        '$end_of_table' ->
            undefined;
        ?queue_elem(Stream, TF, StartKey, _Node, _Ref) ->
            {Stream, TF, StartKey}
    end.

-spec ensure_fulfill_loop() -> ok.
ensure_fulfill_loop() ->
    case get(?fulfill_loop) of
        true ->
            ok;
        _ ->
            put(?fulfill_loop, true),
            self() ! ?fulfill_loop,
            ok
    end.

queue_new() ->
    ets:new(old_polls, [ordered_set, private, {keypos, 1}]).

queue_push(
    Queue, #sub_state{
        req_id = Ref, stream = Stream, topic_filter = TF, start_key = Key, client = Pid
    }
) ->
    ?tp(beamformer_push_replay, #{req_id => Ref}),
    ets:insert(Queue, {?queue_elem(Stream, TF, Key, node(Pid), Ref)}).

%% @doc Get all requests that await data with given stream, topic
%% filter (exact) and start key.
queue_lookup(#s{queue = Queue}, Stream, TopicFilter, StartKey) ->
    MS = {{?queue_elem(Stream, TopicFilter, StartKey, '$1', '$2')}, [], [{{'$1', '$2'}}]},
    ets:select(Queue, [MS]).

%% @doc Lookup requests and enrich them with the data from the
%% subscription registry:
lookup_subs(S = #s{sub_tab = Subs}, Stream, TopicFilter, StartKey) ->
    lists:flatmap(
        fun({_Node, ReqId}) ->
            ets:lookup(Subs, ReqId)
        end,
        queue_lookup(S, Stream, TopicFilter, StartKey)
    ).

queue_drop_all(Queue, Stream, TopicFilter, StartKey) ->
    ets:match_delete(Queue, {?queue_elem(Stream, TopicFilter, StartKey, '_', '_')}).

drop(Queue, SubTab, Sub = #sub_state{req_id = SubRef}) ->
    ets:delete(SubTab, SubRef),
    queue_drop(Queue, Sub).

queue_drop(
    Queue, #sub_state{
        req_id = SubRef, stream = Stream, topic_filter = TF, start_key = Key, client = Pid
    }
) ->
    %% logger:warning(#{drop_req => {Stream, TF, Key, Ref}, tab => ets:tab2list(Queue)}),
    ets:delete(Queue, ?queue_elem(Stream, TF, Key, node(Pid), SubRef)).

queue_update(Queue, OldReq, Req) ->
    %% logger:warning(#{old => OldReq, new => Req}),
    queue_drop(Queue, OldReq),
    queue_push(Queue, Req).

report_metrics(_Metrics, 0) ->
    ok;
report_metrics(Metrics, NFulfilled) ->
    emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
    emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled).
