%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_ack_shard).

%% Sharded ack accumulator: replaces the single ack_aggregator gen_server
%% with one FIFO per client partition, so acks of a claim batch (which all
%% originate from one pull_shard partition) stay together instead of being
%% time-sliced into unrelated worker batches on a shared mailbox. Every
%% shard flushes on the same budget as before (100 acks or 10ms) and hands
%% the batch to pull_server_pool's bounded ack workers, which apply the
%% core accounting and route the counted confirmation back to the origin
%% pull shards. Index-side accounting, the ack_in_flight marker and the
%% counted-exactly-once semantics are untouched - only the accumulation
%% point is sharded.

-behaviour(gen_server).

-export([start_link/1, shard_count/0, shard_name/1, shard_of/2]).
-export([ack/3, client_down/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").

%% Ack partitions follow the pull_shard partition function (one per
%% scheduler) so every ack a pull_shard forwards lands in the matching
%% ack shard on the same node.

-record(state, {
    shard :: non_neg_integer(),
    acks = [] :: [{binary(), binary(), binary()}],
    timer :: reference() | undefined
}).

-spec start_link(non_neg_integer()) -> gen_server:start_ret().
start_link(Shard) ->
    gen_server:start_link({local, shard_name(Shard)}, ?MODULE, [Shard], []).

-spec shard_count() -> pos_integer().
shard_count() ->
    erlang:system_info(schedulers_online).

-spec shard_name(non_neg_integer()) -> atom().
shard_name(Shard) ->
    list_to_atom("emqx_bcast_ack_shard_" ++ integer_to_list(Shard)).

-spec shard_of(binary(), binary()) -> non_neg_integer().
shard_of(ProductKey, ClientId) ->
    erlang:phash2({ProductKey, ClientId}, shard_count()).

-spec ack(binary(), binary(), binary()) -> ok.
ack(ProductKey, ClientId, DeliveryId) ->
    gen_server:cast(
        shard_name(shard_of(ProductKey, ClientId)), {ack, ClientId, DeliveryId, ProductKey}
    ).

-spec client_down(binary(), binary()) -> ok.
client_down(ProductKey, ClientId) ->
    gen_server:cast(shard_name(shard_of(ProductKey, ClientId)), {client_down, ClientId}).

init([Shard]) ->
    {ok, #state{shard = Shard}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ack, ClientId, DeliveryId, ProductKey}, State) ->
    %% emqx_bcast_pull_shard is the ack entry point: it matches the local
    %% buffer first (setting the ack-in-flight marker before this ack can be
    %% applied at core). Here we only accumulate for batched core accounting.
    Acks = [{ProductKey, ClientId, DeliveryId} | State#state.acks],
    State1 = State#state{acks = Acks},
    {noreply, maybe_flush(State1)};
handle_cast({client_down, ClientId}, State) ->
    %% A client went down; flush only its already-acked-but-unreported acks
    %% (spec 4.6). The remaining clients' acks stay batched and flush on the
    %% timer.
    {Mine, Rest} = lists:partition(
        fun({_ProductKey, C, _DeliveryId}) -> C =:= ClientId end,
        State#state.acks
    ),
    ok = send_acks(Mine),
    {noreply, State#state{acks = Rest}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_acks, State) ->
    {noreply, flush(State#state{timer = undefined})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Ack flush cadence is deliberately faster than the old 10ms shared
%% budget: with per-device windows the ack-to-window-release latency is
%% the per-device drain rate limiter, so 2ms/200 keeps acks moving
%% without spawning a worker per handful of acks.
maybe_flush(State = #state{acks = Acks}) ->
    Timer =
        emqx_bcast_utils:maybe_batch_flush(length(Acks), State#state.timer, flush_acks, 2, 200),
    State#state{timer = Timer}.

flush(State = #state{acks = []}) ->
    State;
flush(State = #state{acks = Acks}) ->
    ok = emqx_bcast_utils:cancel_timer(State#state.timer),
    ok = send_acks(Acks),
    State#state{acks = [], timer = undefined}.

%% Hand the batch to pull_server_pool's bounded ack workers (core side).
%% Acks are grouped by the index shard that owns each {PK, DN} and sent
%% directly to the core hosting that shard, so the core-side ack_batch only
%% ever makes LOCAL device-shard calls - the old random-core hop plus the
%% second delivery-shard fan-out are both gone.
send_acks([]) ->
    ok;
send_acks(Acks) ->
    Origin = node(),
    lists:foreach(
        fun({Core, Sub}) ->
            case Core =:= Origin of
                true ->
                    emqx_bcast_pull_server_pool:ack_batch(Sub, Origin);
                false ->
                    emqx_rpc:cast(Core, emqx_bcast_pull_server_pool, ack_batch, [Sub, Origin])
            end
        end,
        group_acks_by_shard_owner(Acks)
    ),
    ok.

group_acks_by_shard_owner(Acks) ->
    lists:foldl(
        fun(Ack = {PK, DN, _Did}, Acc) ->
            Core = emqx_bcast_index_owner:shard_owner(
                emqx_bcast_index_owner:shard_of({PK, DN})
            ),
            case lists:keyfind(Core, 1, Acc) of
                {Core, List} -> lists:keyreplace(Core, 1, Acc, {Core, [Ack | List]});
                false -> [{Core, [Ack]} | Acc]
            end
        end,
        [],
        Acks
    ).
