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

%% @doc This type of beamformer worker serves lagging subscriptions.
%% It utilizes the internal DS indexes.
-module(emqx_ds_beamformer_catchup).

-behaviour(gen_server).

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

-record(enqueue_req, {sub_states}).
-define(fulfill_loop, beamformer_fulfill_loop).
-define(housekeeping_loop, housekeeping_loop).

-define(queue_key(STREAM, TF, STARTKEY, DESTNODE, SUBREF),
    {STREAM, TF, STARTKEY, DESTNODE, SUBREF}
).

-type queue_key() :: ?queue_key(
    emqx_ds:stream(), emqx_ds:topic_filter(), emqx_ds:message_key(), node(), emqx_ds:sub_ref()
).

-record(s, {
    module :: module(),
    metrics_id,
    shard_id,
    sub_tab :: emqx_ds_beamformer:sub_tab(),
    name,
    queue :: ets:table(),
    batch_size :: non_neg_integer(),
    %% This field points at the approximate position of the last
    %% fulfilled request. It's used to rotate through subscriptions to
    %% make fulfillment more fair:
    last_served :: ?queue_key(emqx_ds:stream(), _TF, _Key, _Node, _SubRef) | undefined,
    pending_handovers = gen_server:reqids_new() :: gen_server:request_id_collection()
}).

-type s() :: #s{}.

%%================================================================================
%% API functions
%%================================================================================

-spec enqueue(pid(), [emqx_ds_beamformer:sub_state()]) ->
    ok | {error, unrecoverable, stale} | emqx_ds:error(_).
enqueue(Worker, SubStates) ->
    gen_server:call(Worker, #enqueue_req{sub_states = SubStates}, infinity).

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
    logger:update_process_metadata(#{dbshard => ShardId, name => Name}),
    %% Attach this worker to the pool:
    gproc_pool:add_worker(pool(ShardId), Name),
    gproc_pool:connect_worker(pool(ShardId), Name),
    S = #s{
        module = CBM,
        shard_id = ShardId,
        sub_tab = emqx_ds_beamformer:make_subtab(ShardId),
        metrics_id = emqx_ds_beamformer:metrics_id(ShardId, catchup),
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
    ?housekeeping_loop,
    S0 = #s{metrics_id = Metrics, name = Name, queue = Queue}
) ->
    %% Reload configuration:
    S = S0#s{
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    %% Update metrics:
    emqx_ds_builtin_metrics:set_subs_count(Metrics, Name, ets:info(Queue, size)),
    erlang:send_after(emqx_ds_beamformer:cfg_housekeeping_interval(), self(), ?housekeeping_loop),
    {noreply, S};
handle_info(#unsub_req{id = SubId}, S = #s{sub_tab = SubTab, queue = Queue}) ->
    case emqx_ds_beamformer:sub_tab_take(SubTab, SubId) of
        undefined ->
            ok;
        {ok, SubState} ->
            queue_drop(Queue, SubState)
    end,
    {noreply, S};
handle_info(Info, S0 = #s{pending_handovers = Handovers0, sub_tab = SubTab}) ->
    case gen_server:check_response(Info, Handovers0, true) of
        {Response, SubRef, Handovers} ->
            S = S0#s{pending_handovers = Handovers},
            case emqx_ds_beamformer:sub_tab_lookup(SubTab, SubRef) of
                {ok, SubState} ->
                    handover_complete(Response, SubState, S);
                undefined ->
                    ok
            end,
            {noreply, S};
        no_request ->
            {noreply, S0};
        no_reply ->
            {noreply, S0}
    end.

terminate(Reason, #s{sub_tab = SubTab, shard_id = ShardId, name = Name}) ->
    gproc_pool:disconnect_worker(pool(ShardId), Name),
    gproc_pool:remove_worker(pool(ShardId), Name),
    emqx_ds_beamformer:on_worker_down(SubTab, Reason),
    emqx_ds_lib:terminate(?MODULE, Reason, #{}).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%% Temporary remove requests from the active queue and notify the
%% parent about reschedule.
handle_recoverable(#s{sub_tab = SubTab, queue = Queue, shard_id = DBShard}, Subscribers) ->
    ok = emqx_ds_beamformer:handle_recoverable_error(DBShard, Subscribers),
    lists:foreach(
        fun(SubS) -> drop(Queue, SubTab, SubS) end,
        Subscribers
    ).

do_enqueue(SubStates, #s{
    shard_id = DBShard, sub_tab = SubTab, queue = Queue
}) ->
    lists:foreach(
        fun(SubS) ->
            queue_push(Queue, SubS),
            emqx_ds_beamformer:take_ownership(DBShard, SubTab, SubS)
        end,
        SubStates
    ),
    ensure_fulfill_loop().

fulfill(S0 = #s{queue = Queue, metrics_id = Metrics}) ->
    %% debug_pending(S),
    case find_older_request(Queue, S0) of
        undefined ->
            S0#s{last_served = undefined};
        ?queue_key(Stream, TopicFilter, StartKey, _DestNode, _SubRef) ->
            ?tp(emqx_ds_beamformer_fulfill_old, #{
                stream => Stream, topic => TopicFilter, start_key => StartKey
            }),
            T0 = erlang:monotonic_time(microsecond),
            S = do_fulfill(S0, Stream, TopicFilter, StartKey),
            T1 = erlang:monotonic_time(microsecond),
            emqx_ds_builtin_metrics:observe_beamformer_fulfill_time(Metrics, T1 - T0),
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
    T0 = erlang:monotonic_time(microsecond),
    ScanResult = emqx_ds_beamformer:scan_stream(
        CBM, DBShard, Stream, TopicFilter, StartKey, BatchSize
    ),
    emqx_ds_builtin_metrics:observe_beamformer_scan_time(
        Metrics, erlang:monotonic_time(microsecond) - T0
    ),
    case ScanResult of
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
            handle_recoverable(S, lookup_subs(S, Stream, TopicFilter, StartKey)),
            S;
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
            queue_drop_all(Queue, Stream, TopicFilter, StartKey),
            S
    end.

-spec move_to_realtime(
    s(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:message_key(),
    emqx_ds:message_key()
) ->
    s().
move_to_realtime(
    S = #s{shard_id = DBShard, queue = Queue, pending_handovers = Handovers0},
    Stream,
    TopicFilter,
    StartKey,
    EndKey
) ->
    Reqs = lookup_subs(S, Stream, TopicFilter, StartKey),
    Handovers = lists:foldl(
        fun(Req, HandoversAcc) ->
            ?tp(debug, beamformer_move_to_rt, #{
                shard => DBShard,
                stream => Stream,
                tf => TopicFilter,
                start_key => StartKey,
                end_key => EndKey,
                req_id => Req#sub_state.req_id
            }),
            %% Remove request the active queue, but keep it in the sub
            %% table for now:
            queue_drop(Queue, Req),
            %% Send async handover request to the RT worker:
            emqx_ds_beamformer_rt:enqueue(DBShard, Req, HandoversAcc)
        end,
        Handovers0,
        Reqs
    ),
    S#s{pending_handovers = Handovers}.

handover_complete(
    Reply,
    SubState = #sub_state{req_id = SubRef},
    S = #s{shard_id = DBShard, sub_tab = SubTab, queue = Queue}
) ->
    case Reply of
        {reply, Result} ->
            ok;
        {error, Err} ->
            Result = {error, recoverable, Err}
    end,
    case Result of
        ok ->
            %% Handover has been successful. Remove the request from
            %% the subscription table as well:
            emqx_ds_beamformer:sub_tab_delete(SubTab, SubRef);
        ?err_unrec(stale) ->
            %% Race condition: new data has been added. Add the
            %% request back to the active queue, so it can be
            %% retried:
            queue_push(Queue, SubState);
        ?err_rec(Reason) ->
            ?tp(
                warning,
                emqx_ds_beamformer_move_to_rt_fail,
                #{shard => DBShard, recoverable => true, req => SubState, error => Reason}
            ),
            handle_recoverable(S, [SubState]);
        ?err_unrec(Reason) ->
            ?tp(
                error,
                emqx_ds_beamformer_move_to_rt_fail,
                #{shard => DBShard, recoverable => false, req => SubState, error => Reason}
            ),
            emqx_ds_beamformer:send_out_final_beam(DBShard, SubTab, ?err_unrec(Reason), [SubState]),
            queue_drop(Queue, SubState)
    end.

-spec fulfill_batch(
    s(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:message_key(),
    emqx_ds:message_key(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) -> s().
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
    ok = emqx_ds_beamformer:beams_conclude(ShardId, EndKey, BeamMaker),
    %% Update last served. Note: topic filter is a list; according to
    %% the Erlang term order, any binary is greater than the list, so
    %% the following construct forces the worker to move to the next
    %% stream:
    S#s{last_served = ?queue_key(Stream, <<>>, undefined, undefined, undefined)}.

-spec process_batch(
    s(),
    emqx_ds:stream(),
    emqx_ds:topic_filter(),
    [{emqx_ds:message_key(), emqx_types:message()}],
    [emqx_ds_beamformer:sub_state()],
    emqx_ds_beamformer:beam_builder()
) -> emqx_ds_beamformer:beam_builder().
process_batch(_S, _Stream, _TopicFilter, [], _Candidates, Beams) ->
    Beams;
process_batch(S, Stream, TopicFilter, [{Key, Msg} | Rest], Candidates0, Beams0) ->
    Candidates = queue_lookup(S, Stream, TopicFilter, Key) ++ Candidates0,
    Beams = emqx_ds_beamformer:beams_add(Stream, Key, Msg, Candidates, Beams0),
    process_batch(S, Stream, TopicFilter, Rest, Candidates, Beams).

%% It's always worth trying to fulfill the most laggy subscriptions
%% first, because they have a better chance of producing a batch
%% overlapping with other similar subscriptions.
%%
%% This function implements a heuristic that tries to find such
%% subscription. It picks elements with the smallest key (and,
%% incidentally, the smallest stream, topic filter, etc., which is
%% irrelevent here) as the starting point.
-spec find_older_request(ets:tid(), s()) ->
    queue_key() | undefined.
find_older_request(Tab, #s{last_served = undefined}) ->
    case ets:first(Tab) of
        '$end_of_table' ->
            undefined;
        Key ->
            Key
    end;
find_older_request(Tab, S = #s{last_served = PrevKey}) ->
    case ets:next(Tab, PrevKey) of
        '$end_of_table' ->
            find_older_request(Tab, S#s{last_served = undefined});
        Key ->
            Key
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

queue_push(Queue, SubState) ->
    ?tp(beamformer_push_catchup, #{req_id => SubState#sub_state.req_id}),
    ets:insert(Queue, {queue_key(SubState)}).

%% @doc Get all requests that await data with given stream, topic
%% filter (exact) and start key.
queue_lookup(#s{queue = Queue}, Stream, TopicFilter, StartKey) ->
    MS = {{?queue_key(Stream, TopicFilter, StartKey, '$1', '$2')}, [], [{{'$1', '$2'}}]},
    ets:select(Queue, [MS]).

%% @doc Lookup requests and enrich them with the data from the
%% subscription registry:
lookup_subs(S = #s{sub_tab = SubTab}, Stream, TopicFilter, StartKey) ->
    lists:map(
        fun({_Node, ReqId}) ->
            {ok, SubState} = emqx_ds_beamformer:sub_tab_lookup(SubTab, ReqId),
            SubState
        end,
        queue_lookup(S, Stream, TopicFilter, StartKey)
    ).

queue_drop_all(Queue, Stream, TopicFilter, StartKey) ->
    ets:match_delete(Queue, {?queue_key(Stream, TopicFilter, StartKey, '_', '_')}).

drop(Queue, SubTab, Sub = #sub_state{req_id = SubRef}) ->
    emqx_ds_beamformer:sub_tab_delete(SubTab, SubRef),
    queue_drop(Queue, Sub).

queue_drop(Queue, SubState) ->
    ets:delete(Queue, queue_key(SubState)).

queue_update(Queue, OldReq, Req) ->
    %% logger:warning(#{old => OldReq, new => Req}),
    queue_drop(Queue, OldReq),
    queue_push(Queue, Req).

report_metrics(_Metrics, 0) ->
    ok;
report_metrics(Metrics, NFulfilled) ->
    emqx_ds_builtin_metrics:inc_beams_sent(Metrics, NFulfilled),
    emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled).

-compile({inline, queue_key/1}).
queue_key(#sub_state{
    req_id = SubRef, stream = Stream, topic_filter = TF, start_key = Key, client = Pid
}) ->
    ?queue_key(Stream, TF, Key, node(Pid), SubRef).
