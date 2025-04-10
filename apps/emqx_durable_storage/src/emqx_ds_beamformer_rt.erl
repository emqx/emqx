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
    shard,
    sub_tab :: emqx_ds_beamformer:sub_tab(),
    name,
    high_watermark :: ets:tid(),
    queue :: emqx_ds_beamformer_waitq:t(),
    batch_size :: non_neg_integer()
}).

-define(housekeeping_loop, housekeeping_loop).

-record(shard_event, {event}).
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
            Worker ! stream_event(Stream)
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

init([CBM, DBShard, Name, _Opts]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(#{dbshard => DBShard, name => Name}),
    Pool = pool(DBShard),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    S = #s{
        module = CBM,
        shard = DBShard,
        sub_tab = emqx_ds_beamformer:make_subtab(DBShard),
        metrics_id = emqx_ds_beamformer:metrics_id(DBShard, rt),
        name = Name,
        high_watermark = ets:new(high_watermark, [ordered_set, private]),
        queue = emqx_ds_beamformer_waitq:new(),
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(Req = #sub_state{}, _From, S) ->
    Reply = do_enqueue(Req, S),
    {reply, Reply, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(#seal_event{rank = Rank}, S = #s{queue = Queue}) ->
    %% Currently generation seal events are treated as stream events,
    %% for each known stream that has the matching rank:
    Streams = emqx_ds_beamformer_waitq:streams_of_rank(Rank, Queue),
    _ = [process_stream_event(false, Stream, S) || Stream <- Streams],
    {noreply, S};
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(E = #shard_event{event = Event}, S = #s{shard = Shard, queue = Queue}) ->
    ?tp(info, beamformer_rt_event, #{event => Event, shard => Shard}),
    %% Before processing an event, clear the mailbox from the matching
    %% events to avoid repeatedly hammering the DB:
    flush_similar_events(E),
    %% TODO: make a proper event wrapper?
    Stream = Event,
    _ =
        case emqx_ds_beamformer_waitq:has_candidates(Stream, Queue) of
            true ->
                process_stream_event(true, Stream, S);
            false ->
                %% Even if we don't have any poll requests for the stream,
                %% it's still necessary to update the high watermark to
                %% avoid full scans of very old data:
                update_high_watermark(Stream, S)
        end,
    {noreply, S};
handle_info(
    ?housekeeping_loop, S0 = #s{name = Name, metrics_id = Metrics, queue = Queue}
) ->
    %% Reload configuration according from environment variables:
    S = S0#s{
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    %% Report metrics:
    PQLen = emqx_ds_beamformer_waitq:size(Queue),
    emqx_ds_builtin_metrics:set_subs_count(Metrics, Name, PQLen),
    %% Continue the loop:
    erlang:send_after(emqx_ds_beamformer:cfg_housekeeping_interval(), self(), ?housekeeping_loop),
    {noreply, S};
handle_info(
    #unsub_req{id = SubId}, S = #s{sub_tab = SubTab, queue = Queue}
) ->
    case emqx_ds_beamformer:sub_tab_take(SubTab, SubId) of
        undefined ->
            ok;
        {ok, SubS} ->
            queue_drop(Queue, SubS)
    end,
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(Reason, #s{shard = ShardId, name = Name, sub_tab = SubTab}) ->
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
    S = #s{shard = Shard, queue = Queue, module = CBM, sub_tab = SubTab}
) ->
    case high_watermark(Stream, S) of
        {ok, HighWatermark} ->
            case emqx_ds_beamformer:fast_forward(CBM, Shard, It0, HighWatermark) of
                {ok, It} ->
                    ?tp(beamformer_push_rt, #{
                        req_id => ReqId, stream => Stream, key => Key, it => It
                    }),
                    emqx_ds_beamformer_waitq:insert(Stream, TF, {node(Client), ReqId}, Rank, Queue),
                    emqx_ds_beamformer:take_ownership(Shard, SubTab, Req),
                    ok;
                {error, unrecoverable, has_data} ->
                    ?tp(info, beamformer_push_rt_downgrade, #{
                        req_id => ReqId, stream => Stream, key => Key
                    }),
                    {error, unrecoverable, stale};
                Error = {error, _, _} ->
                    ?tp(
                        error,
                        beamformer_rt_cannot_fast_forward,
                        #{
                            it => It0,
                            reason => Error
                        }
                    ),
                    Error
            end;
        undefined ->
            ?tp(
                warning,
                beamformer_push_rt_unknown_stream,
                #{poll_req => Req}
            ),
            {error, unrecoverable, unknown_stream}
    end.

process_stream_event(RetryOnEmpty, Stream, S) ->
    T0 = erlang:monotonic_time(microsecond),
    do_process_stream_event(RetryOnEmpty, Stream, S),
    emqx_ds_builtin_metrics:observe_beamformer_fulfill_time(
        S#s.metrics_id,
        erlang:monotonic_time(microsecond) - T0
    ).

do_process_stream_event(
    RetryOnEmpty,
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
        {ok, LastKey, []} ->
            ?tp(beamformer_rt_batch, #{
                shard => DBShard, from => StartKey, to => LastKey, stream => Stream, empty => true
            }),
            %% Race condition: event arrived before the data became
            %% available. Retry later:
            RetryOnEmpty andalso
                begin
                    ?tp(debug, beamformer_rt_retry_event, #{stream => Stream}),
                    erlang:send_after(10, self(), stream_event(Stream))
                end,
            set_high_watermark(Stream, LastKey, S);
        {ok, LastKey, Batch} ->
            ?tp(beamformer_rt_batch, #{
                shard => DBShard, from => StartKey, to => LastKey, stream => Stream
            }),
            Beams = emqx_ds_beamformer:beams_init(
                CBM,
                DBShard,
                SubTab,
                false,
                fun(SubS) -> queue_drop(Queue, SubS) end,
                fun(_OldSubState, _SubState) -> ok end
            ),
            process_batch(Stream, LastKey, Batch, S, Beams),
            set_high_watermark(Stream, LastKey, S),
            do_process_stream_event(false, Stream, S);
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

queue_search(#s{queue = Queue}, Stream, _MsgKey, Msg) ->
    Topic = emqx_topic:tokens(Msg#message.topic),
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

flush_similar_events(E = #shard_event{}) ->
    receive
        E -> flush_similar_events(E)
    after 0 ->
        ok
    end.

-compile({inline, stream_event/1}).
stream_event(Stream) ->
    #shard_event{event = Stream}.
