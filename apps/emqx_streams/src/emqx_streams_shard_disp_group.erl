%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_shard_disp_group).

-moduledoc """
The module is a draft, not integrated yet.
It is subject to change.
""".

-include("emqx_streams_internal.hrl").

-export([
    provision/4,
    provision_takeovers/4,
    current_allocation/2
]).

-export([
    new/0,
    n_leases/1,
    lookup_lease/2,
    announce/4,
    progress/6,
    release/5,
    handle_tx_reply/6
]).

-export_type([st/0, streamgroup/0]).

-type consumer() :: emqx_types:clientid().
-type streamgroup() :: binary().
-type shard() :: binary().
-type offset() :: non_neg_integer().
-type heartbeat() :: non_neg_integer().

-type st() :: #{
    {shard, shard()} := shard_st(),
    %% Piggyback
    _ => _
}.

-type shard_st() :: #{
    lease := boolean() | releasing,
    offset_last := offset(),
    offset_committed := offset() | undefined,
    heartbeat_last := heartbeat(),
    %% Introspection:
    offset_committed_last => offset(),
    offset_committed_max => offset()
}.

-define(N_ANNOUNCE_SLOTS, 8).

%% Protocol interaction

-spec new() -> st().
new() ->
    #{}.

-spec provision(consumer(), streamgroup(), [shard()], heartbeat()) -> [{lease | release, shard()}].
provision(Consumer, SGroup, Shards, HBWatermark) ->
    {Consumers, Leases} = emqx_streams_state_db:shard_leases_dirty(SGroup),
    LiveConsumers = maps:keys(maps:filter(fun(_C, HB) -> HB >= HBWatermark end, Consumers)),
    Alloc0 = current_allocation_(Leases, Shards),
    Alloc1 = lists:foldl(
        fun emqx_streams_allocation:add_member/2,
        Alloc0,
        [Consumer | LiveConsumers]
    ),
    Allocations = emqx_streams_allocation:allocate([], Alloc1),
    ProvisionalShards = phash_order(Consumer, [Shard || {Shard, C} <- Allocations, C == Consumer]),
    case ProvisionalShards of
        [_ | _] ->
            [{lease, Shard} || Shard <- ProvisionalShards];
        [] ->
            Rebalances = emqx_streams_allocation:rebalance([], Alloc1),
            [{release, Shard} || {Shard, C, _} <- Rebalances, C == Consumer]
    end.

provision_takeovers(Consumer, SGroup, Shards, HBWatermark) ->
    {Consumers, Leases} = emqx_streams_state_db:shard_leases_dirty(SGroup),
    Alloc0 = current_allocation_(Leases, Shards),
    Alloc1 = emqx_streams_allocation:add_member(Consumer, Alloc0),
    DeadConsumers = maps:filter(fun(_C, HB) -> HB < HBWatermark end, Consumers),
    case maps:size(DeadConsumers) of
        0 ->
            [];
        _ ->
            DeadCost = fun(_Shard, C) ->
                HeartbeatLast = maps:get(C, DeadConsumers, HBWatermark),
                max(0, HBWatermark - HeartbeatLast)
            end,
            Rebalances = emqx_streams_allocation:rebalance([DeadCost], Alloc1),
            [
                {takeover, Shard, DeadC, maps:get(DeadC, DeadConsumers)}
             || {Shard, DeadC, C} <- Rebalances, C == Consumer
            ]
    end.

% -spec current_allocation(streamgroup(), [shard()]) -> emqx_streams_allocation:t(consumer(), shard()).
current_allocation(SGroup, Shards) ->
    {_Consumers, Leases} = emqx_streams_state_db:shard_leases_dirty(SGroup),
    current_allocation_(Leases, Shards).

current_allocation_(Leases, Shards) ->
    maps:fold(
        fun(Shard, Consumer, Alloc0) ->
            Alloc = emqx_streams_allocation:add_member(Consumer, Alloc0),
            emqx_streams_allocation:occupy_resource(Shard, Consumer, Alloc)
        end,
        emqx_streams_allocation:new(Shards, []),
        Leases
    ).

phash_order(Consumer, Shards) ->
    %% TODO describe rand purpose
    [
        Shard
     || {_, Shard} <- lists:sort([{erlang:phash2([Consumer | S]), S} || S <- Shards])
    ].

-spec announce(consumer(), streamgroup(), heartbeat(), st()) ->
    st() | emqx_ds:error(_).
announce(Consumer, SGroup, Heartbeat, St) ->
    Slot = erlang:phash2(Consumer, ?N_ANNOUNCE_SLOTS),
    case emqx_streams_state_db:announce_consumer(SGroup, Slot, Consumer, Heartbeat) of
        ok ->
            St;
        Error ->
            Error
    end.

-spec n_leases(st()) -> non_neg_integer().
n_leases(St) ->
    maps:fold(
        fun
            ({shard, _}, #{leased := true}, N) -> N + 1;
            ({shard, _}, #{leased := releasing}, N) -> N + 1;
            (_, _, N) -> N
        end,
        0,
        St
    ).

-spec lookup_lease(shard(), st()) -> shard_st() | undefined.
lookup_lease(Shard, St) ->
    case St of
        #{{shard, Shard} := ShardSt} ->
            ShardSt;
        #{} ->
            undefined
    end.

-spec progress(consumer(), streamgroup(), shard(), offset(), heartbeat(), st()) ->
    st()
    | {tx, reference(), _Ctx, st()}
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
progress(Consumer, SGroup, Shard, Offset, Heartbeat, St) ->
    case St of
        #{{shard, Shard} := ShardSt0 = #{lease := true}} ->
            Result = progress_shard(Consumer, SGroup, Shard, Offset, Heartbeat, ShardSt0);
        #{{shard, Shard} := #{lease := releasing}} ->
            Result = {invalid, releasing};
        #{} ->
            % St = remove_proposed_lease(Consumer, SGroup, Shard, St0),
            Result = lease_shard(Consumer, SGroup, Shard, Offset, Heartbeat)
    end,
    case Result of
        {tx, Ref, Ret, ShardSt} ->
            Ctx = {Ret, Shard, Offset, Heartbeat},
            {tx, Ref, {progress, Ctx}, St#{{shard, Shard} => ShardSt}};
        ShardRet ->
            return_update_shard(Shard, ShardRet, St)
    end.

lease_shard(Consumer, SGroup, Shard, Offset, HB) ->
    case emqx_streams_state_db:lease_shard_async(SGroup, Shard, Consumer, Offset, HB) of
        ok ->
            #{
                lease => true,
                offset_last => Offset,
                offset_committed => Offset,
                heartbeat_last => HB
            };
        {async, Ref, Ret} ->
            {tx, Ref, Ret, #{
                lease => false,
                offset_last => Offset,
                offset_committed => undefined,
                heartbeat_last => HB
            }};
        Other ->
            Other
    end.

progress_shard(Consumer, SGroup, Shard, Offset, HB, St) ->
    #{offset_last := OffsetLast} = St,
    case OffsetLast =< Offset of
        true ->
            case emqx_streams_state_db:progress_shard_async(SGroup, Shard, Consumer, Offset, HB) of
                ok ->
                    {ok, St#{
                        lease := true,
                        heartbeat_last := HB,
                        offset_last := Offset,
                        offset_committed := Offset
                    }};
                {async, Ref, Ret} ->
                    {tx, Ref, Ret, St#{
                        offset_last := Offset,
                        heartbeat_last := HB
                    }};
                Other ->
                    Other
            end;
        false ->
            {invalid, {offset_going_backwards, OffsetLast}}
    end.

-spec handle_tx_reply(consumer(), streamgroup(), reference(), _Reply, _Ctx, st()) ->
    st()
    | {tx, reference(), _Ctx, st()}
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
handle_tx_reply(Consumer, SGroup, Ref, Reply, {progress, Ctx}, St) ->
    handle_progress_tx(Consumer, SGroup, Ref, Reply, Ctx, St);
handle_tx_reply(_Consumer, _SGroup, Ref, Reply, {release, Ctx}, St) ->
    handle_release_tx(Ref, Reply, Ctx, St).

-spec handle_progress_tx(consumer(), streamgroup(), reference(), _Reply, _Ctx, st()) ->
    st()
    | {tx, reference(), _Ctx, st()}
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
handle_progress_tx(Consumer, SGroup, Ref, Reply, Ctx, St) ->
    {Ret, Shard, Offset, Heartbeat} = Ctx,
    #{{shard, Shard} := ShardSt0} = St,
    Result = emqx_streams_state_db:progress_shard_tx_result(Ret, Ref, Reply),
    case handle_shard_progress(Result, Consumer, Offset, Heartbeat, ShardSt0) of
        {restart, ShardSt} ->
            %% FIXME logging context
            progress(Consumer, SGroup, Shard, Offset, Heartbeat, St#{{shard, Shard} := ShardSt});
        ShardRet ->
            return_update_shard(Shard, ShardRet, St)
    end.

handle_shard_progress(Result, Consumer, Offset, Heartbeat, St) ->
    #{
        offset_committed := OffsetCommitted,
        heartbeat_last := HBLast
    } = St,
    case Result of
        ok ->
            St#{
                lease := true,
                heartbeat_last := max(Heartbeat, emqx_maybe:define(HBLast, Heartbeat)),
                offset_committed := max(Offset, emqx_maybe:define(OffsetCommitted, Offset))
            };
        {invalid, {leased, Consumer}} ->
            undefined = OffsetCommitted,
            {restart, St#{lease := true}};
        {invalid, Reason = {leased, _}} ->
            {invalid, Reason, St#{lease := false}};
        {invalid, Reason} ->
            {invalid, Reason, handle_invalid(Reason, St)};
        Other ->
            Other
    end.

release(Consumer, SGroup, Shard, Offset, St) ->
    case St of
        #{{shard, Shard} := ShardSt0 = #{lease := true, heartbeat_last := HBLast}} ->
            case release_shard(Consumer, SGroup, Shard, Offset, HBLast, ShardSt0) of
                {tx, Ref, Ret, ShardSt} ->
                    Ctx = {Ret, Shard, Offset},
                    {tx, Ref, {release, Ctx}, St#{{shard, Shard} => ShardSt}};
                ShardRet ->
                    return_update_shard(Shard, ShardRet, St)
            end;
        #{{shard, Shard} := #{lease := releasing}} ->
            {invalid, releasing};
        #{{shard, Shard} := #{lease := false}} ->
            %% TODO if offset updated?
            St;
        #{} ->
            St
    end.

release_shard(Consumer, SGroup, Shard, Offset, HBLast, St) ->
    #{offset_last := OffsetLast} = St,
    case OffsetLast =< Offset of
        true ->
            case
                emqx_streams_state_db:release_shard_async(SGroup, Shard, Consumer, Offset, HBLast)
            of
                ok ->
                    {ok, St#{
                        lease := false,
                        offset_last := Offset,
                        offset_committed := Offset
                    }};
                {async, Ref, Ret} ->
                    {tx, Ref, Ret, St#{
                        lease := releasing,
                        offset_last := Offset
                    }};
                Other ->
                    Other
            end;
        false ->
            {invalid, {offset_going_backwards, OffsetLast}}
    end.

-spec handle_release_tx(reference(), _Reply, _Ctx, st()) ->
    st()
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
handle_release_tx(Ref, Reply, Ctx, St) ->
    {Ret, Shard, Offset} = Ctx,
    #{{shard, Shard} := ShardSt} = St,
    Result = emqx_streams_state_db:progress_shard_tx_result(Ret, Ref, Reply),
    return_update_shard(Shard, handle_shard_release(Result, Offset, ShardSt), St).

handle_shard_release(Result, Offset, St = #{offset_committed := OffsetCommitted}) ->
    case Result of
        ok ->
            St#{
                lease := false,
                offset_committed := max(Offset, emqx_maybe:define(OffsetCommitted, Offset))
            };
        {invalid, {leased, _DifferentConsumer}} ->
            St#{
                lease := false,
                offset_committed := undefined,
                offset_committed_last => OffsetCommitted
            };
        {invalid, Reason} ->
            {invalid, Reason, handle_invalid(Reason, St#{lease := true})};
        Other ->
            Other
    end.

return_update_shard(Shard, ShardSt = #{}, St) ->
    St#{{shard, Shard} := ShardSt};
return_update_shard(Shard, {invalid, Reason, ShardSt}, St) ->
    {invalid, Reason, St#{{shard, Shard} := ShardSt}};
return_update_shard(_Shard, {invalid, Reason}, St) ->
    {invalid, Reason, St};
return_update_shard(_Shard, Error, _St) ->
    Error.

handle_invalid({offset_ahead, Offset}, St) ->
    St#{offset_committed_max => Offset};
handle_invalid(_, St) ->
    St.
