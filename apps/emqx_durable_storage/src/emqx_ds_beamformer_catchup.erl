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
    parent_sub_tab :: ets:tid(),
    name,
    queue :: ets:tid(),
    batch_size :: non_neg_integer()
}).

-type s() :: #s{}.

-record(enqueue_req, {sub_ids}).
-define(fulfill_loop, beamformer_fulfill_loop).
-define(housekeeping_loop, housekeeping_loop).

%%================================================================================
%% API functions
%%================================================================================

-spec enqueue(pid(), [emqx_ds:sub_ref()]) ->
    ok | {error, unrecoverable, stale} | emqx_ds:error(_).
enqueue(Worker, SubIds) ->
    try
        gen_server:call(Worker, #enqueue_req{sub_ids = SubIds})
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
        parent_sub_tab = emqx_ds_beamformer:subtab(ShardId),
        metrics_id = emqx_ds_beamformer:shard_metrics_id(ShardId),
        name = Name,
        queue = queue_new(),
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(#enqueue_req{sub_ids = SubIds}, _From, S) ->
    do_enqueue(SubIds, S),
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

terminate(Reason, #s{shard_id = ShardId, name = Name, queue = Queue}) ->
    gproc_pool:disconnect_worker(pool(ShardId), Name),
    gproc_pool:remove_worker(pool(ShardId), Name),
    %% In case of abnormal shutdown we notify the parent about dropped
    %% subscription states:
    Reason =:= shutdown orelse
        emqx_ds_beamformer:reschedule(ShardId, queue_all_reqs(Queue)).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%% Temporary remove requests from the active queue and notify the
%% parent about reschedule.
reschedule(#s{queue = Queue, shard_id = DBShard}, Reqs) ->
    ok = emqx_ds_beamformer:reschedule(DBShard, [SubId || #sub_state{req_id = SubId} <- Reqs]),
    lists:foreach(
        fun(Req) -> queue_drop(Queue, Req) end,
        Reqs
    ).

do_enqueue(SubIds, #s{parent_sub_tab = SubTab, queue = Queue, metrics_id = Metrics}) ->
    _ = [queue_push(Queue, Req) || SubId <- SubIds, Req <- ets:lookup(SubTab, SubId)],
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
            reschedule(S, queue_lookup(S, Stream, TopicFilter, StartKey));
        Other ->
            Pack =
                case Other of
                    {ok, end_of_stream} ->
                        end_of_stream;
                    {error, unrecoverable, _} ->
                        Other
                end,
            MatchReqs = queue_lookup(S, Stream, TopicFilter, StartKey),
            NFulfilled = length(MatchReqs),
            report_metrics(Metrics, NFulfilled),
            %% Pack requests into beams and send out:
            emqx_ds_beamformer:send_out_final_beam(DBShard, Pack, MatchReqs),
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
    S = #s{shard_id = DBShard, queue = Queue},
    Stream,
    TopicFilter,
    StartKey,
    EndKey
) ->
    Reqs = queue_lookup(S, Stream, TopicFilter, StartKey),
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
                    queue_drop(Queue, Req);
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
                    reschedule(S, [Req]);
                Err = {error, unrecoverable, Reason} ->
                    ?tp(
                        error,
                        emqx_ds_beamformer_move_to_rt_fail,
                        #{shard => DBShard, recoverable => false, req => Req, error => Reason}
                    ),
                    emqx_ds_beamformer:send_out_final_beam(DBShard, Err, [Req]),
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
    S = #s{shard_id = ShardId, metrics_id = Metrics, module = CBM, queue = Queue},
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
    MatchingReqs = lists:filter(
        fun(#sub_state{msg_matcher = Matcher}) ->
            Matcher(Key, Msg)
        end,
        Candidates
    ),
    Beams = emqx_ds_beamformer:beams_add(Key, Msg, MatchingReqs, Beams0),
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
        {Stream, TF, StartKey, _Ref} ->
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

-define(queue_elem(STREAM, TF, STARTKEY, SUBID),
    {{STREAM, TF, STARTKEY, SUBID}}
).

queue_push(
    Queue, #sub_state{req_id = Ref, stream = Stream, topic_filter = TF, start_key = Key}
) ->
    ?tp(beamformer_push_replay, #{req_id => Ref}),
    ets:insert(Queue, ?queue_elem(Stream, TF, Key, Ref)).

%% @doc Get all requests that await data with given stream, topic
%% filter (exact) and start key.
queue_lookup(#s{queue = Queue, parent_sub_tab = Subs}, Stream, TopicFilter, StartKey) ->
    MS = {?queue_elem(Stream, TopicFilter, StartKey, '$1'), [], ['$1']},
    ReqIds = ets:select(Queue, [MS]),
    lists:flatmap(
        fun(ReqId) ->
            ets:lookup(Subs, ReqId)
        end,
        ReqIds
    ).

queue_drop_all(Queue, Stream, TopicFilter, StartKey) ->
    ets:match_delete(Queue, ?queue_elem(Stream, TopicFilter, StartKey, '_')).

queue_drop(
    Queue, #sub_state{req_id = Ref, stream = Stream, topic_filter = TF, start_key = Key}
) ->
    %% logger:warning(#{drop_req => {Stream, TF, Key, Ref}, tab => ets:tab2list(Queue)}),
    ets:delete(Queue, {Stream, TF, Key, Ref}).

queue_all_reqs(Queue) ->
    MS = {?queue_elem('_', '_', '_', '$1'), [], ['$1']},
    ets:match(Queue, MS).

queue_update(Queue, OldReq, Req) ->
    %% logger:warning(#{old => OldReq, new => Req}),
    queue_drop(Queue, OldReq),
    queue_push(Queue, Req).

report_metrics(_Metrics, 0) ->
    ok;
report_metrics(Metrics, NFulfilled) ->
    emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
    emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled).
