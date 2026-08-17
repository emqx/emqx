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
    qos0_deliver_local/3,
    qos1_core_trigger_local/3,
    deliver_results/1
]).

%% Worker tasks.
-export([
    do_want_next/3,
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
-define(WORKER_POOL, emqx_bcast_pull_worker_pool).
-define(FLUSH_MS, 50).
-define(FLUSH_COUNT, 100).

-record(state, {
    active = a,
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

qos0_deliver_local(ProductKey, TopicTemplate, Payload) ->
    gen_server:cast(?MODULE, {qos0_deliver, ProductKey, TopicTemplate, Payload}).

qos1_core_trigger_local(ProductKey, DeviceNames, TopicTemplate) ->
    gen_server:cast(?MODULE, {qos1_core_trigger, ProductKey, DeviceNames, TopicTemplate}).

deliver_results(Results) ->
    gen_server:cast(?MODULE, {deliver_results, Results}).

%%--------------------------------------------------------------------
%% Worker tasks
%%--------------------------------------------------------------------

do_want_next(Core, Node, Entries) ->
    Results =
        case Core =:= node() of
            true ->
                try
                    emqx_bcast_pull_server_pool:want_next(Node, Entries)
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
                        [Node, Entries],
                        15000
                    )
                of
                    R when is_list(R) -> R;
                    _ -> []
                catch
                    _:_ -> []
                end
        end,
    gen_server:cast(?MODULE, {deliver_results, Results}).

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
            emqx_bcast_metrics:qos1_delivered()
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
    emqx_bcast_subscription:init(),
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
handle_cast({subscribe, ClientId, Pid, ProductKey, Topics}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    emqx_bcast_subscription:replace(ClientId, Pid, Topics),
    {noreply, trigger_want_next(ClientId, Pid, ProductKey, Topics, State)};
handle_cast({unsubscribe, ClientId, Pid, ProductKey, Topics}, State) ->
    _ = ProductKey,
    emqx_bcast_subscription:replace(ClientId, Pid, Topics),
    {noreply, State};
handle_cast({ping, ClientId, Pid, ProductKey, Topics}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    emqx_bcast_subscription:replace(ClientId, Pid, Topics),
    {noreply, trigger_want_next(ClientId, Pid, ProductKey, Topics, State)};
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
                Topics = emqx_bcast_subscription:topics(ClientId),
                trigger_want_next(ClientId, Pid, ProductKey, Topics, State0)
        end,
    {noreply, State2};
handle_cast({qos0_deliver, ProductKey, TopicTemplate, Payload}, State) ->
    Devices = emqx_bcast:lookup_devices_by_product(ProductKey),
    Targets = lists:filtermap(
        fun({DeviceName, Pid}) ->
            Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DeviceName),
            case emqx_bcast_subscription:match(DeviceName, Topic) of
                {ok, _SubQos} ->
                    {true, {Pid, DeviceName, Topic, Payload}};
                false ->
                    false
            end
        end,
        Devices
    ),
    case Targets of
        [] ->
            ok;
        _ ->
            emqx_bcast_metrics:qos0_delivery_count(length(Targets)),
            submit_to_worker(fun() -> do_deliver_qos0(Targets) end)
    end,
    {noreply, State};
handle_cast({qos1_core_trigger, ProductKey, DeviceNames, TopicTemplate}, State) ->
    State1 = lists:foldl(
        fun(DeviceName, AccState) ->
            case emqx_bcast:lookup_device({ProductKey, DeviceName}) of
                {ok, Pid} ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DeviceName),
                    case emqx_bcast_subscription:match(DeviceName, Topic) of
                        {ok, _} ->
                            Topics = emqx_bcast_subscription:topics(DeviceName),
                            trigger_want_next(DeviceName, Pid, ProductKey, Topics, AccState);
                        false ->
                            AccState
                    end;
                {error, not_found} ->
                    AccState
            end
        end,
        State,
        DeviceNames
    ),
    {noreply, State1};
handle_cast({deliver_results, Results}, State) ->
    {NewEntries, State1} = process_deliver_results(Results, State),
    {noreply, fill_and_deliver(NewEntries, State1)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_buffer3, State) ->
    ok = cancel_flush_timer(State#state.flush_timer),
    Entries = ets:tab2list(?TAB_BUF3),
    ets:delete_all_objects(?TAB_BUF3),
    case Entries of
        [] ->
            {noreply, State#state{flush_timer = undefined}};
        _ ->
            Core = emqx_bcast:random_core(),
            Maps = [buffer3_to_map(E) || E <- Entries],
            submit_to_worker(fun() -> do_want_next(Core, node(), Maps) end),
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

active_tab(#state{active = a}) -> ?TAB_A;
active_tab(#state{active = b}) -> ?TAB_B.

inactive_tab(#state{active = a}) -> ?TAB_B;
inactive_tab(#state{active = b}) -> ?TAB_A.

trigger_want_next(_ClientId, _Pid, _ProductKey, [], State) ->
    State;
trigger_want_next(ClientId, Pid, ProductKey, Topics, State) ->
    Active = active_tab(State),
    case ets:lookup(Active, ClientId) of
        [_ | _] ->
            %% Window = 1: an unacked QoS1 delivery is already pending.
            State;
        [] ->
            case ets:lookup(?TAB_BUF3, ClientId) of
                [_ | _] ->
                    %% buffer3 dedup: one want_next per client per batch.
                    State;
                [] ->
                    ets:insert(?TAB_BUF3, #bcast_buffer3{
                        clientid = ClientId,
                        product_key = ProductKey,
                        topics = Topics,
                        pid = Pid
                    }),
                    maybe_flush_buffer3(State)
            end
    end.

maybe_flush_buffer3(State) ->
    case ets:info(?TAB_BUF3, size) >= ?FLUSH_COUNT of
        true ->
            ok = cancel_flush_timer(State#state.flush_timer),
            self() ! flush_buffer3,
            State#state{flush_timer = undefined};
        false ->
            ensure_flush_timer(State)
    end.

ensure_flush_timer(State = #state{flush_timer = undefined}) ->
    TRef = erlang:send_after(?FLUSH_MS, self(), flush_buffer3),
    State#state{flush_timer = TRef};
ensure_flush_timer(State) ->
    State.

cancel_flush_timer(undefined) ->
    ok;
cancel_flush_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.

buffer3_to_map(#bcast_buffer3{
    clientid = ClientId,
    product_key = ProductKey,
    topics = Topics
}) ->
    #{clientid => ClientId, product_key => ProductKey, topics => Topics}.

cleanup_client(ClientId, Pid) ->
    cleanup_buffer(?TAB_A, ClientId, Pid),
    cleanup_buffer(?TAB_B, ClientId, Pid),
    cleanup_buffer3(ClientId, Pid),
    emqx_bcast_subscription:clear(ClientId, Pid),
    emqx_bcast:unregister_device(ClientId, Pid).

cleanup_buffer(Tab, ClientId, Pid) ->
    case ets:lookup(Tab, ClientId) of
        [#bcast_buffer_entry{pid = Pid}] -> ets:delete(Tab, ClientId);
        _ -> ok
    end.

cleanup_buffer3(ClientId, Pid) ->
    case ets:lookup(?TAB_BUF3, ClientId) of
        [#bcast_buffer3{pid = Pid}] -> ets:delete(?TAB_BUF3, ClientId);
        _ -> ok
    end.

take_pending(ClientId, DeliveryId, State) ->
    Active = active_tab(State),
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
                        offline ->
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
            offline;
        {ok, Pid} ->
            Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, ClientId),
            case emqx_bcast_subscription:match(ClientId, Topic) of
                false ->
                    {no_match, ProductKey, ClientId, DeliveryId};
                {ok, SubQos} ->
                    Config = persistent_term:get({?APP, config}, #{}),
                    ForceUpgrade = maps:get(force_upgrade_qos, Config, true),
                    case ForceUpgrade orelse SubQos >= 1 of
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
    end.

fill_and_deliver(NewEntries, State) ->
    Inactive = inactive_tab(State),
    Active = active_tab(State),
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
    NewState = State#state{active = inactive_side(State)},
    case NewEntries of
        [] ->
            ok;
        _ ->
            submit_to_worker(fun() -> do_deliver_pending(NewEntries) end)
    end,
    NewState.

inactive_side(#state{active = a}) -> b;
inactive_side(#state{active = b}) -> a.

submit_to_worker(Fun) ->
    try
        emqx_pool:async_submit_to_pool(?WORKER_POOL, Fun)
    catch
        _:_ -> Fun()
    end.
