%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a "realtime" beamformer worker serving
%% subscriptions that reached end of the stream.
-module(emqx_ds_beamformer_rt).

-feature(maybe_expr, enable).

-behaviour(gen_server).

%% API:
-export([pool/1, start_link/4, enqueue/3, shard_event/2, seal_generation/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([process_stream_event/4]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds_beamformer.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(MAX_RETRIES, 10).

%% Whenever we receive a notification that a stream has new data, we
%% add this stream to this table. Events are deduplicated.
-record(active_streams, {
    %% Table of streams
    t :: ets:tid(),
    %% Last stream that we processed. It is kept to process streams in
    %% a round robin fashion
    last_key :: term()
}).

-type active_streams() :: #active_streams{}.

-record(s, {
    module :: module(),
    metrics_id,
    shard,
    sub_tab :: emqx_ds_beamformer:sub_tab(),
    name,
    high_watermark :: ets:tid(),
    queue :: emqx_ds_beamformer_waitq:t(),
    batch_size :: non_neg_integer(),
    worker :: pid() | undefined,
    worker_ref :: reference() | undefined,
    %% Table of streams that has new data:
    active_streams = active_streams_new() :: active_streams(),
    %% Queue of pending commands (such as subscribe/unsubscribe) saved
    %% while the temporary worker is active:
    cmd_queue = [] :: [fun((S) -> S)]
}).

-define(housekeeping_loop, housekeeping_loop).

-record(shard_event, {stream, retries}).
-record(seal_event, {rank}).

%%================================================================================
%% API functions
%%================================================================================

pool(DBShard) ->
    {emqx_ds_beamformer_rt, DBShard}.

-spec enqueue(_Shard, emqx_ds_beamformer:sub_state(), gen_server:request_id_collection()) ->
    gen_server:request_id_collection().
enqueue(Shard, Req = #sub_state{req_id = SubId, stream = Stream}, ReqIdCollection) ->
    ?tp(debug, beamformer_enqueue, #{req_id => SubId, queue => rt}),
    Worker = gproc_pool:pick_worker(pool(Shard), Stream),
    gen_server:send_request(Worker, Req, SubId, ReqIdCollection).

-spec start_link(module(), _Shard, integer(), emqx_ds_beamformer:opts()) -> {ok, pid()}.
start_link(Mod, ShardId, Name, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name, Opts], []).

shard_event(Shard, Events) ->
    Pool = pool(Shard),
    lists:foreach(
        fun(Stream) ->
            Worker = gproc_pool:pick_worker(Pool, Stream),
            Worker ! stream_event(Stream, ?MAX_RETRIES)
        end,
        Events
    ).

seal_generation(DBShard, Rank) ->
    ?tp(debug, beamformer_seal_generation, #{shard => DBShard, rank => Rank}),
    Workers = gproc_pool:active_workers(pool(DBShard)),
    lists:foreach(
        fun({_WorkerId, Pid}) ->
            gen_server:cast(Pid, #seal_event{rank = Rank})
        end,
        Workers
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, DBShard = {DB, _}, Name, _Opts]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(#{dbshard => DBShard, name => Name}),
    Pool = pool(DBShard),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    #{batch_size := BatchSize} = emqx_ds_beamformer:runtime_config(CBM, DB),
    S = #s{
        module = CBM,
        shard = DBShard,
        sub_tab = emqx_ds_beamformer:make_subtab(DBShard),
        metrics_id = emqx_ds_beamformer:metrics_id(DBShard, rt),
        name = Name,
        high_watermark = ets:new(high_watermark, [ordered_set, public]),
        queue = emqx_ds_beamformer_waitq:new(),
        batch_size = BatchSize
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(Req = #sub_state{}, From, S0) ->
    Fun = fun(S) ->
        do_enqueue(Req, From, S)
    end,
    push_cmd(S0, Fun);
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(#seal_event{rank = Rank}, S0) ->
    Fun = fun(S = #s{queue = Queue}) ->
        %% Currently generation seal events are treated as stream events,
        %% for each known stream that has the matching rank:
        Streams = emqx_ds_beamformer_waitq:streams_of_rank(Rank, Queue),
        %% TODO: pass it through the event queue too
        _ = [process_stream_event(self(), 0, Stream, S) || Stream <- Streams],
        S
    end,
    push_cmd(S0, Fun);
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(E = #shard_event{}, S = #s{shard = Shard, active_streams = Q}) ->
    ?tp(debug, beamformer_rt_event, #{event => E, shard => Shard}),
    active_streams_push(Q, E),
    {noreply, maybe_dispatch_event(S)};
handle_info({Ref, Result}, S0 = #s{worker_ref = Ref}) ->
    ok = Result,
    S = exec_pending_cmds(S0#s{worker = undefined, worker_ref = undefined}),
    {noreply, maybe_dispatch_event(S)};
handle_info(
    ?housekeeping_loop,
    S0 = #s{module = CBM, shard = {DB, _}, name = Name, metrics_id = Metrics, queue = Queue}
) ->
    %% Reload configuration:
    #{
        batch_size := BatchSize,
        housekeeping_interval := HouseKeepingInterval
    } = emqx_ds_beamformer:runtime_config(CBM, DB),
    S = S0#s{
        batch_size = BatchSize
    },
    %% Report metrics:
    PQLen = emqx_ds_beamformer_waitq:size(Queue),
    emqx_ds_builtin_metrics:set_subs_count(Metrics, Name, PQLen),
    %% Continue the loop:
    erlang:send_after(HouseKeepingInterval, self(), ?housekeeping_loop),
    {noreply, S};
handle_info(
    #unsub_req{id = SubId}, S0
) ->
    Fun = fun(S = #s{sub_tab = SubTab, queue = Queue}) ->
        case emqx_ds_beamformer:sub_tab_take(SubTab, SubId) of
            undefined ->
                ok;
            {ok, SubS} ->
                queue_drop(Queue, SubS)
        end,
        S
    end,
    push_cmd(S0, Fun);
handle_info(_Info, S) ->
    {noreply, S}.

terminate(Reason, #s{shard = ShardId, name = Name, sub_tab = SubTab, worker = Worker}) ->
    case Worker of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            unlink(Pid),
            erlang:exit(Pid, shutdown)
    end,
    Pool = pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    emqx_ds_beamformer:on_worker_down(SubTab, Reason),
    emqx_ds_lib:terminate(?MODULE, Reason, #{}).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

do_enqueue(
    Req = #sub_state{
        stream = Stream,
        topic_filter = TF,
        req_id = ReqId,
        start_key = Key,
        it = It0,
        rank = Rank,
        client = Client
    },
    From,
    S = #s{shard = Shard, queue = Queue, module = CBM, sub_tab = SubTab, batch_size = BatchSize}
) ->
    Reply =
        case high_watermark(Stream, S) of
            {ok, HighWatermark} ->
                ?tp(beamformer_push_rt, #{
                    req_id => ReqId, stream => Stream, key => Key, it => It0
                }),
                emqx_ds_beamformer_waitq:insert(
                    Stream, TF, {node(Client), ReqId}, Rank, Queue
                ),
                emqx_ds_beamformer:take_ownership(Shard, SubTab, Req),
                FFRes = emqx_ds_beamformer:fast_forward(CBM, Shard, It0, HighWatermark, BatchSize),
                complete_takeover(S, Req, FFRes);
            undefined ->
                ?tp(
                    warning,
                    beamformer_push_rt_unknown_stream,
                    #{poll_req => Req}
                ),
                {error, unrecoverable, unknown_stream}
        end,
    ok = gen_server:reply(From, Reply),
    S.

-doc """
This function is called during hand-over from catch-up to realtime worker.

If more data was published while the subscription was in hand-over
state, RT worker uses this function to send it to the client.
""".
complete_takeover(_, _, {ok, _, _, []}) ->
    ok;
complete_takeover(
    S, #sub_state{client = Client, req_id = ReqId, stream = Stream}, {ok, PTrans, EndKey, Batch}
) ->
    #s{
        shard = DBShard,
        module = CBM,
        queue = Queue,
        sub_tab = SubTab,
        metrics_id = Metrics
    } = S,
    BB0 = emqx_ds_beamformer:beams_init(
        CBM,
        DBShard,
        PTrans,
        SubTab,
        true,
        fun(SubS) -> queue_drop(Queue, SubS) end,
        fun(_OldSubState, _SubState) -> ok end
    ),
    Node = node(Client),
    BB = lists:foldl(
        fun({Key, Msg}, BB1) ->
            emqx_ds_beamformer:beams_add(Stream, Key, Msg, [{Node, ReqId}], BB1)
        end,
        BB0,
        Batch
    ),
    emqx_ds_builtin_metrics:inc_beams_sent(Metrics, 1),
    emqx_ds_builtin_metrics:observe_sharing(Metrics, 1),
    emqx_ds_beamformer:beams_conclude(DBShard, EndKey, BB),
    ok;
complete_takeover(
    S,
    Req = #sub_state{client = Client, req_id = ReqId, stream = Stream, topic_filter = TF},
    Pack = {error, _, _}
) ->
    #s{
        shard = DBShard,
        sub_tab = SubTab,
        queue = Queue
    } = S,
    emqx_ds_beamformer:send_out_final_beam(DBShard, SubTab, Pack, [Req]),
    emqx_ds_beamformer_waitq:delete(Stream, TF, {node(Client), ReqId}, Queue),
    ok.

process_stream_event(Parent, RetriesOnEmpty, Stream, S) ->
    T0 = erlang:monotonic_time(microsecond),
    do_process_stream_event(Parent, RetriesOnEmpty, Stream, S),
    emqx_ds_builtin_metrics:observe_beamformer_fulfill_time(
        S#s.metrics_id,
        erlang:monotonic_time(microsecond) - T0
    ),
    ok.

do_process_stream_event(
    Parent,
    RetriesOnEmpty,
    Stream,
    S = #s{
        shard = DBShard,
        metrics_id = Metrics,
        module = CBM,
        batch_size = BatchSize,
        queue = Queue,
        sub_tab = SubTab
    }
) ->
    {ok, StartKey} = high_watermark(Stream, S),
    T0 = erlang:monotonic_time(microsecond),
    ScanResult = emqx_ds_beamformer:scan_stream(CBM, DBShard, Stream, ['#'], StartKey, BatchSize),
    emqx_ds_builtin_metrics:observe_beamformer_scan_time(
        Metrics, erlang:monotonic_time(microsecond) - T0
    ),
    case ScanResult of
        {ok, _PTrans, LastKey, []} ->
            ?tp(beamformer_rt_batch, #{
                shard => DBShard, from => StartKey, to => LastKey, stream => Stream, empty => true
            }),
            %% Potential race condition: event arrived before the data
            %% became available. Alternatively, the backend was not
            %% careful: it added some data _before_ the stream high
            %% watermark and send the event. Ideally, such events
            %% should be avoided. Retry later:
            RetriesOnEmpty > 0 andalso
                begin
                    ?tp(debug, beamformer_rt_retry_event, #{
                        stream => Stream, retries => RetriesOnEmpty
                    }),
                    erlang:send_after(100, Parent, stream_event(Stream, RetriesOnEmpty - 1))
                end,
            set_high_watermark(Stream, LastKey, S);
        {ok, PTrans, LastKey, Batch} ->
            ?tp(beamformer_rt_batch, #{
                shard => DBShard, from => StartKey, to => LastKey, stream => Stream
            }),
            Beams = emqx_ds_beamformer:beams_init(
                CBM,
                DBShard,
                PTrans,
                SubTab,
                false,
                fun(SubS) -> queue_drop(Queue, SubS) end,
                fun(_OldSubState, _SubState) -> ok end
            ),
            process_batch(Stream, LastKey, Batch, S, Beams),
            set_high_watermark(Stream, LastKey, S),
            do_process_stream_event(Parent, 0, Stream, S);
        {error, recoverable, _Err} ->
            %% FIXME:
            exit(retry);
        Other ->
            Pack =
                case Other of
                    {ok, end_of_stream} ->
                        end_of_stream;
                    {error, unrecoverable, _} ->
                        Other
                end,
            Ids = emqx_ds_beamformer_waitq:matching_keys(Stream, ['#'], Queue),
            MatchReqs = lists:map(
                fun({_Node, SubId}) ->
                    {ok, SubState} = emqx_ds_beamformer:sub_tab_lookup(SubTab, SubId),
                    SubState
                end,
                Ids
            ),
            emqx_ds_beamformer:send_out_final_beam(DBShard, SubTab, Pack, MatchReqs),
            emqx_ds_beamformer_waitq:del_stream(Stream, Queue)
    end.

process_batch(
    _Stream,
    EndKey,
    [],
    #s{metrics_id = Metrics, shard = ShardId},
    Beams
) ->
    %% Report metrics:
    NFulfilled = emqx_ds_beamformer:beams_n_matched(Beams),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_beams_sent(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Send data:
    emqx_ds_beamformer:beams_conclude(ShardId, EndKey, Beams);
process_batch(Stream, EndKey, [{Key, Msg} | Rest], S, Beams0) ->
    Candidates = queue_search(S, Stream, Key, Msg),
    Beams = emqx_ds_beamformer:beams_add(Stream, Key, Msg, Candidates, Beams0),
    process_batch(Stream, EndKey, Rest, S, Beams).

queue_search(#s{queue = Queue}, Stream, _MsgKey, {Topic, _Time, _Value}) ->
    emqx_ds_beamformer_waitq:matching_keys(Stream, Topic, Queue);
queue_search(#s{queue = Queue}, Stream, _MsgKey, #message{topic = TopicBin}) ->
    Topic = emqx_topic:tokens(TopicBin),
    emqx_ds_beamformer_waitq:matching_keys(Stream, Topic, Queue).

queue_drop(Queue, #sub_state{stream = Stream, topic_filter = TF, req_id = ID, client = Client}) ->
    emqx_ds_beamformer_waitq:delete(Stream, TF, {node(Client), ID}, Queue).

high_watermark(Stream, S = #s{high_watermark = Tab}) ->
    case ets:lookup(Tab, Stream) of
        [{_, HWM}] ->
            {ok, HWM};
        [] ->
            update_high_watermark(Stream, S)
    end.

update_high_watermark(Stream, S = #s{module = CBM, shard = Shard}) ->
    maybe
        {ok, HWM} ?= emqx_ds_beamformer:high_watermark(CBM, Shard, Stream),
        set_high_watermark(Stream, HWM, S),
        {ok, HWM}
    else
        Other ->
            ?tp(
                warning,
                beamformer_rt_failed_to_update_high_watermark,
                #{shard => Shard, stream => Stream, reason => Other}
            ),
            undefined
    end.

set_high_watermark(Stream, LastSeenKey, #s{high_watermark = Tab}) ->
    ?tp(debug, beamformer_rt_set_high_watermark, #{stream => Stream, key => LastSeenKey}),
    ets:insert(Tab, {Stream, LastSeenKey}).

-compile({inline, stream_event/2}).
stream_event(Stream, Retries) ->
    #shard_event{stream = Stream, retries = Retries}.

%% Add a callback to the pending command queue. It will be executed
%% when the worker process is not running.
push_cmd(S0 = #s{cmd_queue = Q0, worker_ref = W}, Fun) ->
    S = S0#s{cmd_queue = [Fun | Q0]},
    case W of
        undefined ->
            {noreply, exec_pending_cmds(S)};
        _ when is_reference(W) ->
            {noreply, S}
    end.

exec_pending_cmds(S0 = #s{cmd_queue = Q}) ->
    T0 = erlang:monotonic_time(microsecond),
    S = lists:foldr(
        fun(Fun, Acc) ->
            Fun(Acc)
        end,
        S0#s{cmd_queue = []},
        Q
    ),
    emqx_ds_builtin_metrics:observer_beamformer_cmds_time(
        S#s.metrics_id,
        erlang:monotonic_time(microsecond) - T0
    ),
    S.

active_streams_new() ->
    #active_streams{
        t = ets:new(evt_queue, [private, ordered_set, {keypos, #shard_event.stream}])
    }.

maybe_dispatch_event(S = #s{worker_ref = W}) when is_reference(W) ->
    S;
maybe_dispatch_event(S = #s{active_streams = AS0, worker_ref = undefined, queue = Queue}) ->
    case active_streams_pop(AS0) of
        undefined ->
            S;
        {ok, #shard_event{stream = Stream, retries = Retries}, AS} ->
            case emqx_ds_beamformer_waitq:has_candidates(Stream, Queue) of
                true ->
                    {ok, Pid, Ref} = emqx_ds_lib:with_worker(
                        ?MODULE, process_stream_event, [self(), Retries, Stream, S]
                    ),
                    S#s{worker = Pid, worker_ref = Ref, active_streams = AS};
                false ->
                    %% Even if we don't have any poll requests for the stream,
                    %% it's still necessary to update the high watermark to
                    %% avoid full scans of very old data:
                    _ = update_high_watermark(Stream, S),
                    S#s{active_streams = AS}
            end
    end.

active_streams_push(#active_streams{t = T}, #shard_event{stream = Key, retries = Retries}) ->
    Op = {#shard_event.retries, Retries, ?MAX_RETRIES, ?MAX_RETRIES},
    _ = ets:update_counter(T, Key, Op, #shard_event{retries = 0}),
    ok.

active_streams_pop(Q = #active_streams{t = T, last_key = K0}) ->
    case ets:next(T, K0) of
        '$end_of_table' ->
            case ets:first(T) of
                '$end_of_table' ->
                    undefined;
                Key ->
                    [Event] = ets:take(T, Key),
                    {ok, Event, Q#active_streams{last_key = Key}}
            end;
        Key ->
            [Event] = ets:take(T, Key),
            {ok, Event, Q#active_streams{last_key = Key}}
    end.
