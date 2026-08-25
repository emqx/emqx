%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pull_pool).

-behaviour(gen_server).

%% API for hooks and sibling pools.
-export([
    start_link/0,
    client_connected/3,
    client_disconnected/2,
    subscribe/4,
    unsubscribe/4,
    ping/4,
    ack/3,
    qos0_deliver_local/4,
    qos1_core_trigger_local/3,
    deliver_results/1
]).

%% Worker tasks.
-export([
    do_want_next/2,
    do_find_qos0_targets/4,
    do_find_trigger_devices/3,
    do_deliver_pending/1,
    do_deliver_qos0/1,
    do_deliver_qos0_and_ack/6,
    do_release_claim/3
]).

%% gen_server callbacks.
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_bcast.hrl").

-define(TAB_A, bcast_buffer_a).
-define(TAB_B, bcast_buffer_b).
-define(TAB_BUF3, bcast_buffer3).
-define(TAB_INFLIGHT, bcast_pull_inflight).
-define(WORKER_POOL, emqx_bcast_pull_worker_pool).

-record(state, {
    active = ?TAB_A,
    flush_timer = undefined,
    mons = #{}
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

client_connected(ClientId, Pid, ProductKey) ->
    gen_server:cast(?MODULE, {client_connected, ClientId, Pid, ProductKey}).

client_disconnected(ClientId, Pid) ->
    gen_server:cast(?MODULE, {client_disconnected, ClientId, Pid}).

subscribe(ClientId, Pid, ProductKey, Topics) ->
    gen_server:cast(?MODULE, {subscribe, ClientId, Pid, ProductKey, Topics}).

unsubscribe(ClientId, Pid, ProductKey, Topics) ->
    gen_server:cast(?MODULE, {unsubscribe, ClientId, Pid, ProductKey, Topics}).

ping(ClientId, Pid, ProductKey, Topics) ->
    gen_server:cast(?MODULE, {ping, ClientId, Pid, ProductKey, Topics}).

ack(ClientId, DeliveryId, ProductKey) ->
    gen_server:cast(?MODULE, {ack, ClientId, DeliveryId, ProductKey}).

qos0_deliver_local(ProductKey, DeviceNames, TopicTemplate, Payload) ->
    gen_server:cast(?MODULE, {qos0_deliver, ProductKey, DeviceNames, TopicTemplate, Payload}).

qos1_core_trigger_local(ProductKey, DeviceNames, TopicTemplate) ->
    gen_server:cast(?MODULE, {qos1_core_trigger, ProductKey, DeviceNames, TopicTemplate}).

deliver_results(Results) ->
    gen_server:cast(?MODULE, {deliver_results, Results}).

%%--------------------------------------------------------------------
%% Worker tasks
%%--------------------------------------------------------------------

do_want_next(Core, Entries) ->
    %% Subscription filters are resolved here, in the worker, by reading
    %% EMQX's own subscription tables for each channel pid. The topics
    %% travel with the claim request so the core claim tx can skip entries
    %% the device is not subscribed to. An empty subscription (or a dead
    %% pid) yields no topics and the claim will return no_more.
    Entries1 = [
        M#{topics => subscription_topics(maps:get(pid, M))}
     || M <- Entries
    ],
    Results =
        case Core =:= node() of
            true ->
                try
                    emqx_bcast_pull_server_pool:want_next(Entries1)
                catch
                    _:_ -> []
                end;
            false ->
                try
                    emqx_rpc:call(
                        ?MODULE,
                        Core,
                        emqx_bcast_pull_server_pool,
                        want_next,
                        [Entries1],
                        15000
                    )
                of
                    R when is_list(R) -> R;
                    _ -> []
                catch
                    _:_ -> []
                end
        end,
    %% ClientIds always travel along so the in-flight marks can be cleared
    %% even when the claim RPC failed and Results is empty.
    ClientIds = [maps:get(clientid, M) || M <- Entries],
    gen_server:cast(?MODULE, {deliver_results, Results, ClientIds}).

%% [{TopicFilter, Qos}] from EMQX's own subscription tables for a channel
%% pid. This is the single source of truth; no plugin-side mirror exists.
subscription_topics(Pid) ->
    case is_process_alive(Pid) of
        false ->
            [];
        true ->
            [
                {Filter, maps:get(qos, SubOpts, 0)}
             || {Filter, SubOpts} <- emqx_broker:subscriptions(Pid)
            ]
    end.

do_deliver_pending(Entries) ->
    lists:foreach(
        fun(
            #bcast_buffer_entry{
                clientid = ClientId,
                delivery_id = DeliveryId,
                product_key = ProductKey,
                topic_template = TopicTemplate,
                payload = Payload,
                pid = Pid
            }
        ) ->
            case is_process_alive(Pid) of
                true ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, ClientId),
                    Msg = emqx_message:make(
                        DeliveryId,
                        ClientId,
                        ?QOS_1,
                        Topic,
                        Payload,
                        #{},
                        #{
                            ?BCAST_DELIVERY_ID => DeliveryId,
                            ?BCAST_PRODUCT_KEY => ProductKey
                        }
                    ),
                    Pid ! #deliver{topic = Topic, message = Msg},
                    emqx_bcast_metrics:qos1_delivered();
                false ->
                    %% Client already gone: do not count a delivery that cannot
                    %% reach anyone. Remove the buffer entry (it would sit
                    %% unacked forever, blocking window=1) and release the
                    %% claim so the delivery becomes claimable again.
                    gen_server:cast(?MODULE, {deliver_failed, ClientId, DeliveryId}),
                    do_release_claim(ProductKey, ClientId, DeliveryId)
            end
        end,
        Entries
    ).

do_deliver_qos0(Targets) ->
    lists:foreach(
        fun({Pid, ClientId, Topic, Payload}) ->
            Msg = emqx_message:make(ClientId, ?QOS_0, Topic, Payload),
            Pid ! #deliver{topic = Topic, message = Msg}
        end,
        Targets
    ).

%% Resolve QoS0 fanout targets in a worker: for each target device, look up
%% its channel pid and check the subscription by reading EMQX's own
%% subscription tables (emqx_broker:subscriptions/1) instead of a plugin
%% mirror. DeviceNames = undefined means product-wide (PubBroadcast).
do_find_qos0_targets(ProductKey, DeviceNames, TopicTemplate, Payload) ->
    Devices =
        case DeviceNames of
            undefined ->
                emqx_bcast:lookup_devices_by_product(ProductKey);
            _ ->
                lists:filtermap(
                    fun(DeviceName) ->
                        case emqx_bcast:lookup_device({ProductKey, DeviceName}) of
                            {ok, Pid} -> {true, {DeviceName, Pid}};
                            {error, not_found} -> false
                        end
                    end,
                    DeviceNames
                )
        end,
    lists:filtermap(
        fun({DeviceName, Pid}) ->
            Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DeviceName),
            case sub_match(Pid, Topic) of
                {ok, _SubQos} ->
                    {true, {Pid, DeviceName, Topic, Payload}};
                false ->
                    false
            end
        end,
        Devices
    ).

%% Resolve which devices a QoS1 trigger applies to, in a worker. Returns
%% [{DeviceName, Pid}] for devices that are online and subscribed.
do_find_trigger_devices(ProductKey, DeviceNames, TopicTemplate) ->
    lists:filtermap(
        fun(DeviceName) ->
            case emqx_bcast:lookup_device({ProductKey, DeviceName}) of
                {ok, Pid} ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DeviceName),
                    case sub_match(Pid, Topic) of
                        {ok, _} -> {true, {DeviceName, Pid}};
                        false -> false
                    end;
                {error, not_found} ->
                    false
            end
        end,
        DeviceNames
    ).

%% Subscription check against EMQX's own subscription state. Returns
%% {ok, Qos} with the highest matching subscription QoS, or false.
sub_match(Pid, Topic) ->
    case is_process_alive(Pid) of
        false ->
            false;
        true ->
            Subs = emqx_broker:subscriptions(Pid),
            case
                lists:foldl(
                    fun({Filter, SubOpts}, Acc) ->
                        Qos = maps:get(qos, SubOpts, 0),
                        case emqx_topic:match(Topic, Filter) of
                            true -> max(Acc, Qos);
                            false -> Acc
                        end
                    end,
                    -1,
                    Subs
                )
            of
                -1 -> false;
                Qos -> {ok, Qos}
            end
    end.

do_deliver_qos0_and_ack(ClientId, Pid, Topic, Payload, DeliveryId, ProductKey) ->
    Msg = emqx_message:make(ClientId, ?QOS_0, Topic, Payload),
    Pid ! #deliver{topic = Topic, message = Msg},
    emqx_bcast_ack_pool:ack(ClientId, DeliveryId, ProductKey).

do_release_claim(ProductKey, ClientId, DeliveryId) ->
    _ = emqx_bcast:rpc_core(emqx_bcast_storage, release_claim, [ProductKey, ClientId, DeliveryId]),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = ensure_buffer_table(?TAB_A, #bcast_buffer_entry.clientid),
    ok = ensure_buffer_table(?TAB_B, #bcast_buffer_entry.clientid),
    ok = ensure_buffer_table(?TAB_BUF3, #bcast_buffer3.clientid),
    ok = ensure_buffer_table(?TAB_INFLIGHT, 1),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({client_connected, ClientId, Pid, ProductKey}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    Ref = monitor(process, Pid),
    Mons = maps:put(Ref, {Pid, ClientId}, State#state.mons),
    {noreply, State#state{mons = Mons}};
handle_cast({client_disconnected, ClientId, Pid}, State) ->
    cleanup_client(ClientId, Pid),
    {noreply, State};
handle_cast({subscribe, ClientId, Pid, ProductKey}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    {noreply, trigger_want_next(ClientId, Pid, ProductKey, State)};
handle_cast({unsubscribe, ClientId, Pid, ProductKey}, State) ->
    _ = {ClientId, Pid, ProductKey},
    {noreply, State};
handle_cast({ping, ClientId, Pid, ProductKey}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    {noreply, trigger_want_next(ClientId, Pid, ProductKey, State)};
handle_cast({ack, ClientId, DeliveryId, ProductKey}, State) ->
    {Pid, State0} =
        case take_pending(ClientId, DeliveryId, State) of
            {ok, PendingPid, S1} ->
                {PendingPid, S1};
            none ->
                case emqx_bcast:lookup_device({ProductKey, ClientId}) of
                    {ok, CurrentPid} -> {CurrentPid, State};
                    {error, not_found} -> {undefined, State}
                end
        end,
    State2 =
        case Pid of
            undefined ->
                State0;
            _ ->
                trigger_want_next(ClientId, Pid, ProductKey, State0)
        end,
    {noreply, State2};
handle_cast({qos0_deliver, ProductKey, DeviceNames, TopicTemplate, Payload}, State) ->
    %% The per-device online + subscription check runs in a worker: it reads
    %% emqx_broker:subscriptions(Pid) per device and must not block the
    %% gen_server on a large fanout.
    submit_to_worker(fun() ->
        Targets = do_find_qos0_targets(ProductKey, DeviceNames, TopicTemplate, Payload),
        case Targets of
            [] ->
                ok;
            _ ->
                emqx_bcast_metrics:qos0_delivery_count(length(Targets)),
                do_deliver_qos0(Targets)
        end
    end),
    {noreply, State};
handle_cast({qos1_core_trigger, ProductKey, DeviceNames, TopicTemplate}, State) ->
    %% Same: resolve which devices are online and subscribed in a worker,
    %% then funnel only the resulting want_next updates through this process.
    submit_to_worker(fun() ->
        Triggers = do_find_trigger_devices(ProductKey, DeviceNames, TopicTemplate),
        gen_server:cast(?MODULE, {qos1_triggers, ProductKey, Triggers})
    end),
    {noreply, State};
handle_cast({qos1_triggers, ProductKey, Triggers}, State) ->
    State1 = lists:foldl(
        fun({DeviceName, Pid}, AccState) ->
            trigger_want_next(DeviceName, Pid, ProductKey, AccState)
        end,
        State,
        Triggers
    ),
    {noreply, State1};
handle_cast({deliver_results, Results, ClientIds}, State) ->
    [ets:delete(?TAB_INFLIGHT, C) || C <- ClientIds],
    {NewEntries, State1} = process_deliver_results(Results, State),
    {noreply, fill_and_deliver(NewEntries, State1)};
handle_cast({deliver_failed, ClientId, DeliveryId}, State) ->
    %% The deliver worker found the channel pid dead: drop the buffer entry
    %% (if it is still the same delivery) so it does not sit in the active
    %% buffer as an unacked tombstone blocking window=1 forever.
    Active = State#state.active,
    case ets:lookup(Active, ClientId) of
        [#bcast_buffer_entry{delivery_id = DeliveryId}] ->
            ets:delete(Active, ClientId);
        _ ->
            ok
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_buffer3, State) ->
    ok = emqx_bcast_utils:cancel_timer(State#state.flush_timer),
    Entries = ets:tab2list(?TAB_BUF3),
    ets:delete_all_objects(?TAB_BUF3),
    case Entries of
        [] ->
            {noreply, State#state{flush_timer = undefined}};
        _ ->
            Maps = [buffer3_to_map(E) || E <- Entries],
            %% Mark every flushed client as claim-in-flight. Cleared when the
            %% deliver_results (or an RPC failure) comes back.
            [ets:insert(?TAB_INFLIGHT, {maps:get(clientid, M)}) || M <- Maps],
            Groups = lists:foldr(
                fun(M, Acc) ->
                    Core = emqx_bcast:core_for(maps:get(clientid, M)),
                    case lists:keyfind(Core, 1, Acc) of
                        {Core, List} ->
                            lists:keyreplace(Core, 1, Acc, {Core, [M | List]});
                        false ->
                            [{Core, [M]} | Acc]
                    end
                end,
                [],
                Maps
            ),
            [
                submit_to_worker(fun() -> do_want_next(Core, Group) end)
             || {Core, Group} <- Groups
            ],
            {noreply, State#state{flush_timer = undefined}}
    end;
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:take(Ref, State#state.mons) of
        {{Pid, ClientId}, Mons} ->
            cleanup_client(ClientId, Pid),
            gen_server:cast(emqx_bcast_ack_pool, {client_down, ClientId}),
            {noreply, State#state{mons = Mons}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal: buffer management
%%--------------------------------------------------------------------

ensure_buffer_table(Name, KeyPos) ->
    case ets:info(Name) of
        undefined ->
            _ = ets:new(Name, [
                named_table,
                public,
                set,
                {keypos, KeyPos},
                {read_concurrency, true},
                {write_concurrency, true}
            ]),
            ok;
        _ ->
            ok
    end.

flip(?TAB_A) -> ?TAB_B;
flip(?TAB_B) -> ?TAB_A.

trigger_want_next(ClientId, Pid, ProductKey, State) ->
    Active = State#state.active,
    case ets:lookup(Active, ClientId) of
        [_ | _] ->
            %% Window = 1: an unacked QoS1 delivery is already pending.
            State;
        [] ->
            case ets:member(?TAB_INFLIGHT, ClientId) of
                true ->
                    %% Window = 1: a want_next claim for this client is still
                    %% in flight (buffer3 flushed, deliver_results pending).
                    %% Without this guard a second claim would take the NEXT
                    %% delivery (the first is pending on core), and the two
                    %% results racing through fill_and_deliver drop one of
                    %% them (fill_drop), breaking the ack/take_pending
                    %% association.
                    State;
                false ->
                    case ets:lookup(?TAB_BUF3, ClientId) of
                        [_ | _] ->
                            %% buffer3 dedup: one want_next per client per batch.
                            State;
                        [] ->
                            ets:insert(?TAB_BUF3, #bcast_buffer3{
                                clientid = ClientId,
                                product_key = ProductKey,
                                pid = Pid
                            }),
                            maybe_flush_buffer3(State)
                    end
            end
    end.

maybe_flush_buffer3(State) ->
    Timer = emqx_bcast_utils:maybe_batch_flush(
        ets:info(?TAB_BUF3, size), State#state.flush_timer, flush_buffer3
    ),
    State#state{flush_timer = Timer}.

buffer3_to_map(#bcast_buffer3{
    clientid = ClientId,
    product_key = ProductKey,
    pid = Pid
}) ->
    #{clientid => ClientId, product_key => ProductKey, pid => Pid}.

cleanup_client(ClientId, Pid) ->
    release_pending(?TAB_A, ClientId, Pid),
    release_pending(?TAB_B, ClientId, Pid),
    cleanup_buffer3(ClientId, Pid),
    ets:delete(?TAB_INFLIGHT, ClientId),
    emqx_bcast:unregister_device(ClientId, Pid).

%% The client went away with an unacked delivery in flight: drop the buffer
%% entry and release its claim so the delivery becomes claimable again
%% (e.g. after a reconnect) without waiting for the pending lease.
release_pending(Tab, ClientId, Pid) ->
    case ets:lookup(Tab, ClientId) of
        [
            #bcast_buffer_entry{
                pid = Pid, product_key = ProductKey, delivery_id = DeliveryId
            }
        ] ->
            ets:delete(Tab, ClientId),
            submit_to_worker(fun() -> do_release_claim(ProductKey, ClientId, DeliveryId) end);
        _ ->
            ok
    end.

cleanup_buffer3(ClientId, Pid) ->
    case ets:lookup(?TAB_BUF3, ClientId) of
        [#bcast_buffer3{pid = Pid}] -> ets:delete(?TAB_BUF3, ClientId);
        _ -> ok
    end.

take_pending(ClientId, DeliveryId, State) ->
    Active = State#state.active,
    case ets:lookup(Active, ClientId) of
        [#bcast_buffer_entry{delivery_id = DeliveryId, pid = Pid}] ->
            ets:delete(Active, ClientId),
            {ok, Pid, State};
        _ ->
            none
    end.

%%--------------------------------------------------------------------
%% Internal: deliver-results processing and AB filling
%%--------------------------------------------------------------------

process_deliver_results(Results, State) ->
    lists:foldr(
        fun({ClientId, Result}, {Entries, AccState}) ->
            case Result of
                no_more ->
                    {Entries, AccState};
                {ok, DeliverMap} ->
                    case prepare_delivery(ClientId, DeliverMap) of
                        {pending, Entry} ->
                            {[Entry | Entries], AccState};
                        {qos0, ClientId0, Pid, Topic, Payload, DeliveryId, ProductKey} ->
                            submit_to_worker(fun() ->
                                do_deliver_qos0_and_ack(
                                    ClientId0, Pid, Topic, Payload, DeliveryId, ProductKey
                                )
                            end),
                            {Entries, AccState};
                        {no_match, ProductKey, ClientId0, DeliveryId} ->
                            submit_to_worker(fun() ->
                                do_release_claim(ProductKey, ClientId0, DeliveryId)
                            end),
                            {Entries, AccState};
                        {offline, ProductKey, ClientId0, DeliveryId} ->
                            %% Claimed on core but the device is not connected
                            %% here: release so the entry does not stay
                            %% pending until its lease expires.
                            submit_to_worker(fun() ->
                                do_release_claim(ProductKey, ClientId0, DeliveryId)
                            end),
                            {Entries, AccState}
                    end
            end
        end,
        {[], State},
        Results
    ).

prepare_delivery(ClientId, #{
    delivery_id := DeliveryId,
    product_key := ProductKey,
    topic_template := TopicTemplate,
    payload := Payload
}) ->
    case emqx_bcast:lookup_device({ProductKey, ClientId}) of
        {error, not_found} ->
            {offline, ProductKey, ClientId, DeliveryId};
        {ok, Pid} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                false ->
                    {offline, ProductKey, ClientId, DeliveryId};
                true ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, ClientId),
                    case sub_match(Pid, Topic) of
                        false ->
                            {no_match, ProductKey, ClientId, DeliveryId};
                        {ok, SubQos} ->
                            case SubQos >= 1 of
                                true ->
                                    {pending, #bcast_buffer_entry{
                                        clientid = ClientId,
                                        delivery_id = DeliveryId,
                                        product_key = ProductKey,
                                        topic_template = TopicTemplate,
                                        payload = Payload,
                                        pid = Pid
                                    }};
                                false ->
                                    {qos0, ClientId, Pid, Topic, Payload, DeliveryId, ProductKey}
                            end
                    end
            end
    end.

fill_and_deliver(NewEntries, State) ->
    Active = State#state.active,
    Inactive = flip(Active),
    ets:delete_all_objects(Inactive),
    %% Fill bufferB with freshly pulled deliveries.
    lists:foreach(
        fun(Entry) -> ets:insert(Inactive, Entry) end,
        NewEntries
    ),
    %% Merge unacked bufferA entries into bufferB before switching.
    lists:foreach(
        fun(Entry = #bcast_buffer_entry{clientid = ClientId}) ->
            case ets:lookup(Inactive, ClientId) of
                [] -> ets:insert(Inactive, Entry);
                [_] -> ok
            end
        end,
        ets:tab2list(Active)
    ),
    ets:delete_all_objects(Active),
    NewState = State#state{active = flip(State#state.active)},
    case NewEntries of
        [] ->
            ok;
        _ ->
            submit_to_worker(fun() -> do_deliver_pending(NewEntries) end)
    end,
    NewState.

submit_to_worker(Fun) ->
    emqx_bcast_utils:submit_pool(?WORKER_POOL, Fun).
