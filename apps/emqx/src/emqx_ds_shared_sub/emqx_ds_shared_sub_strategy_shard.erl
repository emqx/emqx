%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_shared_sub_strategy_shard).
-moduledoc """
This module implements a shared durable subscription strategy that
allocates streams to borrowers according to the DS shard.

The reasoning behind this strategy is the following: volume of data
contained in DS streams can differ: some streams may serve only a
small number of low-traffic topics, while others may correspond to a
wildcard high-traffic volume. There's no practical way to know that in
advance. However, distribution of messages to different DS shards
should be more even, since it happens by hashing of the publisher's
clientID. We rely on this fact and make each borrower "responsible"
for one or few DS shards.
""".

%% behavior callbacks:
-export([stream_reallocation_strategy/3]).

-include("emqx_persistent_message.hrl").

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec stream_reallocation_strategy(
    emqx_ds_shared_sub_dl:t(),
    emqx_ds_shared_sub_leader:borrowers(),
    emqx_ds_shared_sub_leader:allocations()
) ->
    emqx_ds_shared_sub_leader:alloc_plan().
stream_reallocation_strategy(S, Borrowers, _Allocs) ->
    NBorrowers = maps:size(Borrowers),
    case NBorrowers of
        0 ->
            [];
        _ ->
            Shards = emqx_ds:list_shards(?PERSISTENT_MESSAGE_DB),
            BorrowerMap = assign_shards_to_borrowers(Borrowers, Shards),
            %% Assign everything to one client:
            emqx_ds_shared_sub_dl:fold_stream_states(
                fun(Slab = {Shard, _}, Stream, _, Acc) ->
                    #{Shard := BorrowerId} = BorrowerMap,
                    [{Slab, Stream, BorrowerId} | Acc]
                end,
                [],
                S
            )
    end.

%%================================================================================
%% Internal functions
%%================================================================================

%% TODO: currently there's no load spread in the shard.
-spec assign_shards_to_borrowers(emqx_ds_shared_sub_leader:borrowers(), [emqx_ds:shard()]) ->
    #{emqx_ds:shard() => emqx_ds_shared_sub_proto:borrower_id()}.
assign_shards_to_borrowers(Borrowers, Shards) ->
    %% Assert to avoid infinite loop:
    true = maps:size(Borrowers) > 0,
    L = assign_shards_to_borrowers(Borrowers, maps:iterator(Borrowers), Shards, []),
    maps:from_list(L).

assign_shards_to_borrowers(_Borrowers, _Iter, [], Acc) ->
    Acc;
assign_shards_to_borrowers(Borrowers, Iter0, [Shard | Rest], Acc) ->
    case maps:next(Iter0) of
        {BorrowerId, _, Iter} ->
            assign_shards_to_borrowers(Borrowers, Iter, Rest, [{Shard, BorrowerId} | Acc]);
        none ->
            assign_shards_to_borrowers(Borrowers, maps:iterator(Borrowers), [Shard | Rest], Acc)
    end.

%%================================================================================
%% Tests
%%================================================================================

%% TODO
