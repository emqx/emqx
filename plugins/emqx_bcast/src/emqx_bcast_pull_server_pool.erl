%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pull_server_pool).

-behaviour(gen_server).

-export([
    start_link/0,
    want_next/1,
    want_next/2,
    ack_batch/1,
    ack_batch/2,
    qos0_broadcast/4,
    qos1_trigger/3,
    qos0_fanout_nodes/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").

%% Ack workers are bounded by the number of schedulers (like the plugin's
%% other pools: delivery_pool_size=0 means one per scheduler), not a hard
%% fixed constant. Acks beyond the cap queue in the gen_server and drain as
%% workers finish; per-client ordering is still guaranteed by the index
%% shard's own mailbox.

%% Worker pool that executes want_next claims off this gen_server's
%% mailbox, so concurrent claims run in parallel instead of serializing on
%% one process per core (per-device ordering stays with the index shards).
-define(SERVER_WORKER_POOL, bcast_pull_server_worker_pool).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Local call used both by local pull_pool workers and by remote pull_pool
%% workers through emqx_rpc:call; pull batches pick a random core.
want_next(Entries) ->
    want_next(Entries, node()).

want_next(Entries, Origin) ->
    gen_server:call(?MODULE, {want_next, Entries, Origin}, ?BCAST_RPC_CALL_TIMEOUT_MS).

ack_batch(Acks) ->
    ack_batch(Acks, node()).

ack_batch(Acks, Origin) ->
    gen_server:cast(?MODULE, {ack_batch, Origin, Acks}).

%% QoS0 / PubBroadcast: one-shot delivery. Core broadcasts full deliver data
%% to the nodes that host the target devices; each local pull_pool checks
%% online + subscription and drops silently when either check fails.
%% DeviceNames narrows the fanout to the BatchPub target list; undefined
%% means product-wide (PubBroadcast).
%% BatchPub with an explicit DeviceNames list no longer broadcasts the
%% full payload + list to EVERY node - the channel registry is global, so
%% the target nodes are known up front and the payload travels only to
%% them (PubBroadcast stays all-node: its targets cannot be precomputed).
qos0_broadcast(ProductKey, DeviceNames, TopicTemplate, Payload) ->
    broadcast_to_pull_pools_on(
        qos0_fanout_nodes(DeviceNames),
        {qos0_deliver_local, [ProductKey, DeviceNames, TopicTemplate, Payload]}
    ).

%% QoS1 BatchPub: pure trigger signal (no payload). Each pull_pool checks
%% online + subscription and turns the trigger into a want_next batch entry.
qos1_trigger(ProductKey, DeviceNames, TopicTemplate) ->
    broadcast_to_pull_pools({qos1_core_trigger_local, [ProductKey, DeviceNames, TopicTemplate]}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ensure_core_copies(),
    _ = erlang:send_after(?BCAST_ENSURE_COPIES_MS, self(), ensure_core_copies),
    AckCap = max(1, erlang:system_info(schedulers_online)),
    {ok, #{in_flight => 0, pending_acks => [], ack_cap => AckCap}}.

handle_call({want_next, Entries, Origin}, From, State) ->
    %% Run the claim in the server worker pool so concurrent want_next
    %% calls execute in parallel instead of serializing on this gen_server
    %% (a burst of claims used to queue ahead of every other call on the
    %% same mailbox). Each claim still fans out to the index shards in
    %% parallel and replies to From; per-device ordering is preserved by
    %% the shard mailboxes. Origin records the node whose pull shard holds
    %% the client buffer (the claim holder for node-down reclaim).
    case
        emqx_bcast_utils:submit_pool(?SERVER_WORKER_POOL, fun() ->
            gen_server:reply(
                From, emqx_bcast_storage:claim_want_next_batch(Entries, Origin)
            )
        end)
    of
        ok ->
            {noreply, State};
        {error, _Reason} ->
            %% Pool unavailable: fall back to inline so the caller's
            %% window=1 is not stalled.
            Results = emqx_bcast_storage:claim_want_next_batch(Entries, Origin),
            {reply, Results, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ack_batch, Origin, Acks}, State) ->
    %% Handle acks OFF the gen_server mailbox. A burst of ack batches
    %% used to pile up ahead of every want_next call (cast-then-call on the
    %% same mailbox), stalling window=1 claims cluster-wide. Spawning keeps
    %% the cast O(1); the ack work (index shard call + meta decrement) runs
    %% in a short-lived worker, and per-client ordering is still guaranteed
    %% by the index shard's own mailbox. Bounded concurrency. Each queued
    %% item carries the origin node so the core-applied confirmation can be
    %% routed back to the node whose pull shard owns the client buffer.
    case maps:get(in_flight, State) < maps:get(ack_cap, State) of
        true ->
            spawn_ack_worker(Acks, Origin),
            {noreply, State#{in_flight => maps:get(in_flight, State) + 1}};
        false ->
            Pending = maps:get(pending_acks, State),
            {noreply, State#{pending_acks => Pending ++ [{Origin, Acks}]}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ack_batch_done, Pid}, State) ->
    %% The worker finished; drain one queued batch if any (fire-and-forget
    %% by design, but receiving keeps the mailbox from accumulating DOWNs).
    _ = Pid,
    case maps:get(pending_acks, State) of
        [] ->
            {noreply, State#{in_flight => maps:get(in_flight, State) - 1}};
        [{Origin, Next} | Rest] ->
            spawn_ack_worker(Next, Origin),
            {noreply, State#{pending_acks => Rest}}
    end;
handle_info(ensure_core_copies, State) ->
    _ = ensure_core_copies(),
    _ = erlang:send_after(?BCAST_ENSURE_COPIES_MS, self(), ensure_core_copies),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

spawn_ack_worker(Acks, Origin) ->
    Parent = self(),
    _ = spawn(fun() ->
        Results = emqx_bcast_storage:process_ack_batch(Acks),
        %% Route the acks that were actually applied (counted) back to the
        %% origin node's pull shards: there the ack-in-flight marker is
        %% cleared, acked is counted exactly once, and the client's next
        %% want_next is unblocked. Duplicates/not_found produce no
        %% confirmation and therefore no count.
        route_notify(Origin, counted_pairs(Results, Acks)),
        Parent ! {ack_batch_done, self()}
    end),
    ok.

%% [{result, {ProductKey, ClientId, DeliveryId}}] -> confirmed triples
counted_pairs(Results, Acks) ->
    lists:filtermap(
        fun
            ({counted, {ProductKey, ClientId, DeliveryId}}) ->
                {true, {ClientId, ProductKey, DeliveryId}};
            (_) ->
                false
        end,
        lists:zip(Results, Acks)
    ).

route_notify(_Origin, []) ->
    ok;
route_notify(Origin, Notify) when Origin =:= node() ->
    emqx_bcast_pull_pool:ack_applied(Notify);
route_notify(Origin, Notify) ->
    emqx_rpc:cast(Origin, emqx_bcast_pull_pool, ack_applied, [Notify]).

broadcast_to_pull_pools({Fun, Args}) ->
    broadcast_to_pull_pools_on(emqx:running_nodes(), {Fun, Args}).

broadcast_to_pull_pools_on(Nodes0, {Fun, Args}) ->
    Nodes =
        case lists:member(node(), Nodes0) of
            true -> Nodes0;
            false -> [node() | Nodes0]
        end,
    lists:foreach(
        fun(Node) ->
            case Node =:= node() of
                true ->
                    apply(emqx_bcast_pull_pool, local_cast_fun(Fun), Args);
                false ->
                    emqx_rpc:cast(Node, emqx_bcast_pull_pool, local_cast_fun(Fun), Args)
            end
        end,
        Nodes
    ).

%% Target nodes for a QoS0 fanout. undefined (PubBroadcast) = every
%% running node; an explicit DeviceNames list = the union of nodes hosting
%% the devices' channels (emqx_cm:lookup_channels/1 is the global
%% registry, so a core node sees channels on all nodes).
%% The global session registry can be disabled
%% (enable_session_registry=false); lookup_channels/1 then degrades to
%% node-local, and a targeted fanout would silently MISS devices on other
%% nodes. Fall back to the all-node broadcast in that configuration -
%% correctness over fanout savings.
%% (Known constraint, EMQX semantics): channels that connect while the
%% global registry is disabled are not re-registered when it is re-enabled;
%% a targeted fanout still misses those devices even with is_enabled()
%% true. Documented as a platform constraint, not a plugin bug.
qos0_fanout_nodes(undefined) ->
    emqx:running_nodes();
qos0_fanout_nodes(DeviceNames) ->
    case emqx_cm_registry:is_enabled() of
        true ->
            Nodes = lists:usort(
                lists:append([
                    [node(Pid) || Pid <- emqx_cm:lookup_channels(DN)]
                 || DN <- DeviceNames
                ])
            ),
            case lists:member(node(), Nodes) of
                true -> Nodes;
                false -> [node() | Nodes]
            end;
        false ->
            emqx:running_nodes()
    end.

local_cast_fun(qos0_deliver_local) -> qos0_deliver_local;
local_cast_fun(qos1_core_trigger_local) -> qos1_core_trigger_local.

ensure_core_copies() ->
    try
        emqx_bcast:ensure_core_copies()
    catch
        _:_ -> ok
    end.
