%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Async delivery pool facade.
%%
%% The BatchPub API handler never delivers messages inline: it resolves
%% device pids/topics synchronously (cheap ETS reads), then submits chunks
%% of pre-resolved targets to this pool. Workers build MQTT messages and
%% hand them to channel pids via `!` (remote pids included, Erlang
%% distribution routes them).
%%
%% Backpressure: an atomics counter tracks queued-but-not-started tasks.
%% When the counter exceeds `delivery_queue_max`, submit returns
%% {error, overloaded} and the API responds 429.
-module(emqx_bcast_deliver).

-export([
    submit_targets/2,
    submit_targets_reserved/2,
    submit_task/1,
    try_reserve/1,
    release_reservation/1,
    has_capacity/1,
    queue_depth/0
]).
-export([deliver_chunk/2]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(POOL, emqx_bcast_deliver_pool).
-define(DEPTH_KEY, {?APP, deliver_queue_depth}).
-define(CHUNK_SIZE, 200).

%% Targets :: [{DeviceName, Pid, Topic, SubQos}]
%% Ctx :: #{qos := 0|1, payload := binary(),
%%          delivery_id => binary(), product_key => binary(),
%%          force_upgrade_qos => boolean()}
submit_targets(Targets, Ctx) ->
    submit_chunks(chunk(Targets), fun submit_task/1, Ctx).

%% Submit after try_reserve/1 succeeded: the queue slots are already counted,
%% so tasks are handed to the pool without another capacity check.
submit_targets_reserved(Targets, Ctx) ->
    submit_chunks(chunk(Targets), fun submit_task_reserved/1, Ctx).

submit_chunks([], _Submit, _Ctx) ->
    ok;
submit_chunks([Chunk | Rest], Submit, Ctx) ->
    case Submit(fun() -> deliver_chunk(Chunk, Ctx) end) of
        ok ->
            submit_chunks(Rest, Submit, Ctx);
        {error, overloaded} = Error ->
            ?SLOG(warning, #{
                msg => "bcast_delivery_submit_overloaded",
                remaining_chunks => length(Rest) + 1
            }),
            Error
    end.

submit_task(Fun) when is_function(Fun, 0) ->
    Max = queue_max(),
    Counter = counter(),
    Depth = atomics:add_get(Counter, 1, 1),
    ok = emqx_bcast_metrics:delivery_queue_depth(Depth),
    case Depth =< Max of
        true ->
            submit_counted(Counter, Fun);
        false ->
            atomics:sub(Counter, 1, 1),
            ok = emqx_bcast_metrics:delivery_queue_depth(atomics:get(Counter, 1)),
            emqx_bcast_metrics:delivery_submit_rejected(),
            {error, overloaded}
    end.

%% Queue slots were reserved upfront by try_reserve/1; hand the task to the
%% pool directly, the counter already accounts for it.
submit_task_reserved(Fun) when is_function(Fun, 0) ->
    submit_counted(counter(), Fun).

submit_counted(Counter, Fun) ->
    Wrapped = fun() ->
        atomics:sub(Counter, 1, 1),
        ok = emqx_bcast_metrics:delivery_queue_depth(atomics:get(Counter, 1)),
        Fun()
    end,
    emqx_pool:async_submit_to_pool(?POOL, Wrapped, []),
    ok.

%% Atomically reserve queue slots for TargetCount targets. Callers that need
%% a 200-only-on-success guarantee (QoS=1) must reserve before writing any
%% storage record and submit with submit_targets_reserved/2 afterwards; a
%% failed reservation releases nothing (the counter was never bumped past the
%% limit) and a storage failure after reservation must call
%% release_reservation/1.
try_reserve(TargetCount) ->
    Counter = counter(),
    ChunkCount = needed_slots(TargetCount),
    Depth = atomics:add_get(Counter, 1, ChunkCount),
    ok = emqx_bcast_metrics:delivery_queue_depth(Depth),
    case Depth =< queue_max() of
        true ->
            ok;
        false ->
            atomics:sub(Counter, ChunkCount, 1),
            ok = emqx_bcast_metrics:delivery_queue_depth(atomics:get(Counter, 1)),
            {error, overloaded}
    end.

release_reservation(TargetCount) ->
    Counter = counter(),
    ChunkCount = needed_slots(TargetCount),
    atomics:sub(Counter, ChunkCount, 1),
    ok = emqx_bcast_metrics:delivery_queue_depth(atomics:get(Counter, 1)).

%% Whether submitting tasks for this many targets would exceed the queue
%% limit. Used by the QoS=0 API handler to reject with 429 before delivery.
has_capacity(TargetCount) ->
    atomics:get(counter(), 1) + needed_slots(TargetCount) =< queue_max().

needed_slots(TargetCount) ->
    (TargetCount + ?CHUNK_SIZE - 1) div ?CHUNK_SIZE.

chunk(Targets) ->
    chunk(Targets, ?CHUNK_SIZE, []).

chunk([], _Size, Acc) ->
    lists:reverse(Acc);
chunk(Targets, Size, Acc) ->
    {Head, Tail} = split(Size, Targets, []),
    chunk(Tail, Size, [Head | Acc]).

split(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
split(_N, [], Acc) ->
    {lists:reverse(Acc), []};
split(N, [H | T], Acc) ->
    split(N - 1, T, [H | Acc]).

queue_depth() ->
    atomics:get(counter(), 1).

queue_max() ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(delivery_queue_max, Config, 10000).

counter() ->
    case persistent_term:get(?DEPTH_KEY, undefined) of
        undefined ->
            Ref = atomics:new(1, [{signed, true}]),
            persistent_term:put(?DEPTH_KEY, Ref),
            Ref;
        Ref ->
            Ref
    end.

deliver_chunk(Targets, #{qos := 0, payload := Payload}) ->
    lists:foreach(
        fun({DN, Pid, Topic, _SubQos}) ->
            emqx_bcast_metrics:qos0_delivered(),
            Msg = emqx_message:make(DN, ?QOS_0, Topic, Payload),
            Pid ! #deliver{topic = Topic, message = Msg}
        end,
        Targets
    );
deliver_chunk(Targets, #{
    qos := 1,
    payload := Payload,
    delivery_id := DeliveryId,
    product_key := ProductKey,
    force_upgrade_qos := ForceUpgrade
}) ->
    lists:foreach(
        fun({DN, Pid, Topic, SubQos}) ->
            case ForceUpgrade orelse SubQos >= 1 of
                true ->
                    emqx_bcast_metrics:qos1_delivered_inline(),
                    Msg = emqx_message:make(
                        DeliveryId,
                        DN,
                        ?QOS_1,
                        Topic,
                        Payload,
                        #{},
                        #{
                            ?BCAST_DELIVERY_ID => DeliveryId,
                            ?BCAST_PRODUCT_KEY => ProductKey
                        }
                    ),
                    Pid ! #deliver{topic = Topic, message = Msg};
                false ->
                    Msg = emqx_message:make(DN, ?QOS_0, Topic, Payload),
                    Pid ! #deliver{topic = Topic, message = Msg},
                    case emqx_bcast_storage:process_ack(ProductKey, DN, DeliveryId) of
                        counted -> emqx_bcast_metrics:qos1_acked();
                        _ -> ok
                    end
            end
        end,
        Targets
    ).
