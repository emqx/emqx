%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast).

-export([
    hook/0,
    unhook/0,
    init_tables/0,
    rebuild_index/0,
    register_device/3,
    unregister_device/2,
    lookup_device/1,
    lookup_devices_by_product/1,
    on_client_connected/2,
    on_client_disconnected/3,
    on_client_subscribe/3,
    on_client_unsubscribe/3,
    on_session_resumed/2,
    on_message_acked/2,
    on_delivery_completed/2
]).

-include("emqx_bcast.hrl").

-define(TAB_MSG, bcast_message).
-define(TAB_MSG_API_ID, bcast_message_api_id).
-define(TAB_MSG_HASH, bcast_message_hash).
-define(TAB_MSG_REC, bcast_msg).
-define(TAB_MSG_IDX, bcast_msg_index).
-define(TAB_DEV_SUB, bcast_device_sub).
-define(TAB_DEV_CLIENT, bcast_device_client).

hook() ->
    ok = emqx_hooks:put('client.connected', {?MODULE, on_client_connected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.disconnected', {?MODULE, on_client_disconnected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.subscribe', {?MODULE, on_client_subscribe, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.unsubscribe', {?MODULE, on_client_unsubscribe, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('message.acked', {?MODULE, on_message_acked, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_HIGHEST).

unhook() ->
    ok = emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    ok = emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    ok = emqx_hooks:del('client.subscribe', {?MODULE, on_client_subscribe}),
    ok = emqx_hooks:del('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
    ok = emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    ok = emqx_hooks:del('message.acked', {?MODULE, on_message_acked}),
    ok = emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}).

init_tables() ->
    ok = create_mnesia_tables(),
    ok = create_ets_tables().

create_mnesia_tables() ->
    Tables = [
        {?TAB_MSG, bcast_message, record_info(fields, bcast_message)},
        {?TAB_MSG_API_ID, bcast_message_api_id, record_info(fields, bcast_message_api_id)},
        {?TAB_MSG_HASH, bcast_message_hash, record_info(fields, bcast_message_hash)},
        {?TAB_MSG_REC, bcast_msg, record_info(fields, bcast_msg)}
    ],
    lists:foreach(
        fun({Tab, RecordName, Attributes}) ->
            ok = mria:create_table(Tab, [
                {rlog_shard, ?BCAST_SHARD},
                {storage, disc_copies},
                {record_name, RecordName},
                {attributes, Attributes},
                {type, set}
            ])
        end,
        Tables
    ),
    ok = mria_rlog:ensure_shard(?BCAST_SHARD),
    ok = mria:wait_for_tables([Tab || {Tab, _, _} <- Tables]).

create_ets_tables() ->
    ensure_ets(?TAB_DEV_SUB, [
        named_table, public, set, {keypos, #bcast_device_sub.key}, {read_concurrency, true}
    ]),
    ensure_ets(?TAB_DEV_CLIENT, [
        named_table, public, set, {keypos, #bcast_device_client.clientid}, {read_concurrency, true}
    ]),
    ensure_ets(?TAB_MSG_IDX, [
        named_table,
        public,
        set,
        {keypos, #bcast_msg_index.key},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    emqx_bcast_subscription:init(),
    ok.

ensure_ets(Name, Opts) ->
    case ets:info(Name) of
        undefined -> ets:new(Name, Opts);
        _ -> ok
    end.

register_device(ProductKey, DeviceName, Pid) ->
    ensure_ets(?TAB_DEV_SUB, [
        named_table, public, set, {keypos, #bcast_device_sub.key}, {read_concurrency, true}
    ]),
    ensure_ets(?TAB_DEV_CLIENT, [
        named_table, public, set, {keypos, #bcast_device_client.clientid}, {read_concurrency, true}
    ]),
    ClientId = DeviceName,
    ets:insert(?TAB_DEV_SUB, #bcast_device_sub{
        key = {ProductKey, DeviceName}, clientid = ClientId, pid = Pid
    }),
    ets:insert(?TAB_DEV_CLIENT, #bcast_device_client{
        clientid = ClientId, pk_dn = {ProductKey, DeviceName}, pid = Pid
    }).

unregister_device(ClientId, Pid) ->
    case ets:lookup(?TAB_DEV_CLIENT, ClientId) of
        [#bcast_device_client{pid = Pid} = Entry] ->
            ets:delete(?TAB_DEV_SUB, Entry#bcast_device_client.pk_dn),
            ets:delete(?TAB_DEV_CLIENT, ClientId);
        [#bcast_device_client{}] ->
            %% stale disconnect: a newer connection has taken over
            ok;
        [] ->
            ok
    end.

lookup_device({ProductKey, DeviceName}) ->
    case ets:info(?TAB_DEV_SUB) of
        undefined ->
            {error, not_found};
        _ ->
            case ets:lookup(?TAB_DEV_SUB, {ProductKey, DeviceName}) of
                [#bcast_device_sub{pid = Pid}] -> {ok, Pid};
                [] -> {error, not_found}
            end
    end.

lookup_devices_by_product(ProductKey) ->
    case ets:info(?TAB_DEV_SUB) of
        undefined ->
            [];
        _ ->
            ets:match(?TAB_DEV_SUB, #bcast_device_sub{
                key = {ProductKey, '_'}, clientid = '$1', pid = '$2', _ = '_'
            })
    end.

on_client_connected(ClientInfo, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        DeviceName = ClientId,
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast:register_device(ProductKey, DeviceName, Pid)
    catch
        _E:_R:_ST ->
            ok
    end,
    {ok, ClientInfo}.

on_client_disconnected(ClientInfo, _Reason, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        emqx_bcast:unregister_device(ClientId, Pid),
        emqx_bcast_subscription:clear(ClientId, Pid),
        erlang:erase({?MODULE, replayed})
    catch
        _E:_R:_ST ->
            ok
    end.

on_client_subscribe(ClientInfo, _Properties, TopicFilters) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        lists:foreach(
            fun({TopicFilter, Opts}) ->
                Qos = maps:get(qos, Opts, 0),
                emqx_bcast_subscription:add(ClientId, Pid, {TopicFilter, Qos})
            end,
            TopicFilters
        ),
        ProductKey = get_product_key(ClientInfo),
        replay_pending(ProductKey, ClientId, Pid)
    catch
        _E:_R:_ST ->
            ok
    end,
    TopicFilters.

on_client_unsubscribe(ClientInfo, _Properties, TopicFilters) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        lists:foreach(
            fun({TopicFilter, Opts}) ->
                Qos = maps:get(qos, Opts, 0),
                emqx_bcast_subscription:remove(ClientId, Pid, {TopicFilter, Qos})
            end,
            TopicFilters
        )
    catch
        _E:_R:_ST ->
            ok
    end,
    TopicFilters.

on_session_resumed(ClientInfo, SessionInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        Subscriptions = maps:get(subscriptions, SessionInfo, #{}),
        emqx_bcast_subscription:backfill(ClientId, Pid, Subscriptions),
        ProductKey = get_product_key(ClientInfo),
        replay_pending(ProductKey, ClientId, Pid)
    catch
        _E:_R:_ST ->
            ok
    end,
    ok.

%% Replay each pending delivery at most once per connection. Track already
%% replayed delivery ids in the process dictionary so that a client that
%% subscribes, unsubscribes and resubscribes (or sends several SUBSCRIBE
%% packets on connect) does not receive the same offline message twice.
replay_pending(ProductKey, ClientId, Pid) ->
    {ok, DeliveryIds} = emqx_bcast_storage:get_device_deliveries({ProductKey, ClientId}),
    lists:foreach(
        fun(DeliveryId) ->
            maybe_replay(DeliveryId, ProductKey, ClientId, Pid)
        end,
        DeliveryIds
    ).

%% Replay a delivery unless it was already replayed in this connection, or
%% the client is not subscribed to its topic.
maybe_replay(DeliveryId, ProductKey, ClientId, Pid) ->
    case lists:member(DeliveryId, replayed()) of
        true ->
            ok;
        false ->
            case subscription_qos(DeliveryId, ProductKey, ClientId) of
                {ok, SubQos} ->
                    replay_delivery(Pid, ProductKey, ClientId, DeliveryId, SubQos),
                    mark_replayed(DeliveryId);
                skip ->
                    ok
            end
    end.

%% The QoS the client would receive the delivery with, or skip when the
%% delivery is gone or no subscription matches.
subscription_qos(DeliveryId, ProductKey, ClientId) ->
    case mnesia:dirty_read(bcast_msg, DeliveryId) of
        [#bcast_msg{topic_template = Template}] ->
            Topic = emqx_bcast_utils:expand_topic(Template, ProductKey, ClientId),
            case emqx_bcast_subscription:match(ClientId, Topic) of
                {ok, SubQos} -> {ok, SubQos};
                false -> skip
            end;
        [] ->
            skip
    end.

replayed() ->
    case erlang:get({?MODULE, replayed}) of
        undefined -> [];
        Replayed -> Replayed
    end.

mark_replayed(DeliveryId) ->
    erlang:put({?MODULE, replayed}, [DeliveryId | replayed()]).

on_message_acked(ClientInfo, Msg) ->
    case emqx_message:get_header(?BCAST_DELIVERY_ID, Msg, undefined) of
        undefined ->
            ok;
        DeliveryId ->
            #{clientid := DeviceName} = ClientInfo,
            ProductKey = get_product_key(ClientInfo),
            count_ack(ProductKey, DeviceName, DeliveryId),
            ok
    end.

on_delivery_completed(Msg, #{clientid := ClientId}) ->
    case emqx_message:get_header(?BCAST_DELIVERY_ID, Msg, undefined) of
        undefined ->
            ok;
        DeliveryId ->
            ProductKey = emqx_message:get_header(?BCAST_PRODUCT_KEY, Msg, undefined),
            count_ack(ProductKey, ClientId, DeliveryId),
            ok
    end.

count_ack(undefined, _DeviceName, _DeliveryId) ->
    ok;
count_ack(ProductKey, DeviceName, DeliveryId) ->
    case emqx_bcast_storage:process_ack(ProductKey, DeviceName, DeliveryId) of
        counted -> emqx_bcast_metrics:qos1_acked();
        _ -> ok
    end.

replay_delivery(Pid, ProductKey, DeviceName, DeliveryId, SubQos) ->
    case pending_payload(DeliveryId) of
        {ok, Payload, Template} ->
            Topic = emqx_bcast_utils:expand_topic(Template, ProductKey, DeviceName),
            deliver_replay(Pid, ProductKey, DeviceName, DeliveryId, Topic, Payload, SubQos);
        not_found ->
            ok
    end.

%% Deliver the replay at QoS 1 when force_upgrade_qos is enabled or the client
%% subscribed at QoS >= 1, QoS 0 otherwise. A QoS 0 replay is completed
%% immediately since no PUBACK will ever arrive.
deliver_replay(Pid, ProductKey, DeviceName, DeliveryId, Topic, Payload, SubQos) ->
    case should_upgrade(SubQos) of
        true ->
            Msg = emqx_message:make(
                DeliveryId,
                DeviceName,
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
            emqx_bcast_metrics:qos1_replayed();
        false ->
            Msg = emqx_message:make(DeviceName, ?QOS_0, Topic, Payload),
            Pid ! #deliver{topic = Topic, message = Msg},
            count_ack(ProductKey, DeviceName, DeliveryId)
    end.

should_upgrade(SubQos) ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(force_upgrade_qos, Config, true) orelse SubQos >= 1.

%% The payload and topic template of a pending delivery, or not_found when
%% the delivery or its message is gone.
pending_payload(DeliveryId) ->
    case mnesia:dirty_read(bcast_msg, DeliveryId) of
        [#bcast_msg{msg_id = MsgId, topic_template = Template}] ->
            case emqx_bcast_storage:lookup_message(MsgId) of
                {ok, #bcast_message{payload = Payload}} -> {ok, Payload, Template};
                {error, not_found} -> not_found
            end;
        [] ->
            not_found
    end.

get_product_key(#{client_attrs := #{<<"tns">> := Tns}}) -> Tns;
get_product_key(_ClientInfo) -> <<"default">>.

rebuild_index() ->
    ensure_ets(?TAB_MSG_IDX, [
        named_table,
        public,
        set,
        {keypos, #bcast_msg_index.key},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ets:delete_all_objects(?TAB_MSG_IDX),
    Deliveries = mnesia:dirty_match_object(bcast_msg, #bcast_msg{_ = '_'}),
    lists:foreach(
        fun(#bcast_msg{delivery_id = Did, product_key = PK, device_names = DNs}) ->
            lists:foreach(
                fun(DN) ->
                    Key = {PK, DN},
                    case ets:lookup(?TAB_MSG_IDX, Key) of
                        [#bcast_msg_index{delivery_ids = Ids}] ->
                            ets:insert(?TAB_MSG_IDX, #bcast_msg_index{
                                key = Key, delivery_ids = [Did | Ids]
                            });
                        [] ->
                            ets:insert(?TAB_MSG_IDX, #bcast_msg_index{
                                key = Key, delivery_ids = [Did]
                            })
                    end
                end,
                DNs
            )
        end,
        Deliveries
    ),
    ok.
