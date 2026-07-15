%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot).

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
    on_message_acked/2
]).

-include("emqx_iot.hrl").

-define(TAB_MSG, iot_mq_message).
-define(TAB_MSG_API_ID, iot_mq_message_api_id).
-define(TAB_MSG_HASH, iot_mq_message_hash).
-define(TAB_MSG_REC, iot_mq_msg).
-define(TAB_MSG_IDX, iot_mq_msg_index).
-define(TAB_DEV_SUB, iot_mq_device_sub).
-define(TAB_DEV_CLIENT, iot_mq_device_client).

hook() ->
    ok = emqx_hooks:put('client.connected', {?MODULE, on_client_connected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.disconnected', {?MODULE, on_client_disconnected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('message.acked', {?MODULE, on_message_acked, []}, ?HP_HIGHEST).

unhook() ->
    ok = emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    ok = emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    ok = emqx_hooks:del('message.acked', {?MODULE, on_message_acked}).

init_tables() ->
    ok = create_mnesia_tables(),
    ok = create_ets_tables().

create_mnesia_tables() ->
    Tables = [
        {?TAB_MSG, [
            {record_name, iot_mq_message},
            {attributes, record_info(fields, iot_mq_message)},
            {disc_copies, [node()]},
            {type, set}
        ]},
        {?TAB_MSG_API_ID, [
            {record_name, iot_mq_message_api_id},
            {attributes, record_info(fields, iot_mq_message_api_id)},
            {disc_copies, [node()]},
            {type, set}
        ]},
        {?TAB_MSG_HASH, [
            {record_name, iot_mq_message_hash},
            {attributes, record_info(fields, iot_mq_message_hash)},
            {disc_copies, [node()]},
            {type, set}
        ]},
        {?TAB_MSG_REC, [
            {record_name, iot_mq_msg},
            {attributes, record_info(fields, iot_mq_msg)},
            {disc_copies, [node()]},
            {type, set}
        ]}
    ],
    [
        mnesia:create_table(Tab, Opts)
     || {Tab, Opts} <- Tables, mnesia:create_table(Tab, Opts) =/= {aborted, {already_exists, Tab}}
    ],
    ok.

create_ets_tables() ->
    ensure_ets(?TAB_DEV_SUB, [
        named_table, public, set, {keypos, #iot_mq_device_sub.key}, {read_concurrency, true}
    ]),
    ensure_ets(?TAB_DEV_CLIENT, [
        named_table, public, set, {keypos, #iot_mq_device_client.clientid}, {read_concurrency, true}
    ]),
    ensure_ets(?TAB_MSG_IDX, [
        named_table, public, set, {keypos, #iot_mq_msg_index.key}, {read_concurrency, true}, {write_concurrency, true}
    ]),
    ok.

ensure_ets(Name, Opts) ->
    case ets:info(Name) of
        undefined -> ets:new(Name, Opts);
        _ -> ok
    end.

register_device(ProductKey, DeviceName, Pid) ->
    ensure_ets(?TAB_DEV_SUB, [
        named_table, public, set, {keypos, #iot_mq_device_sub.key}, {read_concurrency, true}
    ]),
    ensure_ets(?TAB_DEV_CLIENT, [
        named_table, public, set, {keypos, #iot_mq_device_client.clientid}, {read_concurrency, true}
    ]),
    ClientId = DeviceName,
    ets:insert(?TAB_DEV_SUB, #iot_mq_device_sub{
        key = {ProductKey, DeviceName}, clientid = ClientId, pid = Pid
    }),
    ets:insert(?TAB_DEV_CLIENT, #iot_mq_device_client{
        clientid = ClientId, pk_dn = {ProductKey, DeviceName}, pid = Pid
    }).

unregister_device(ClientId, Pid) ->
    case ets:lookup(?TAB_DEV_CLIENT, ClientId) of
        [#iot_mq_device_client{pid = Pid} = Entry] ->
            ets:delete(?TAB_DEV_SUB, Entry#iot_mq_device_client.pk_dn),
            ets:delete(?TAB_DEV_CLIENT, ClientId);
        [#iot_mq_device_client{}] ->
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
                [#iot_mq_device_sub{pid = Pid}] -> {ok, Pid};
                [] -> {error, not_found}
            end
    end.

lookup_devices_by_product(ProductKey) ->
    case ets:info(?TAB_DEV_SUB) of
        undefined ->
            [];
        _ ->
            ets:match(?TAB_DEV_SUB, #iot_mq_device_sub{
                key = {ProductKey, '_'}, clientid = '$1', pid = '$2', _ = '_'
            })
    end.

on_client_connected(ClientInfo, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        DeviceName = ClientId,
        ProductKey = get_product_key(ClientInfo),
        emqx_iot:register_device(ProductKey, DeviceName, Pid),
        {ok, DeliveryIds} = emqx_iot_storage:get_device_deliveries({ProductKey, DeviceName}),
        lists:foreach(
            fun(DeliveryId) ->
                replay_delivery(Pid, ProductKey, DeviceName, DeliveryId)
            end,
            DeliveryIds
        )
    catch
        _E:_R:_ST ->
            ok
    end,
    {ok, ClientInfo}.

on_client_disconnected(ClientInfo, _Reason, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        emqx_iot:unregister_device(ClientId, self())
    catch
        _E:_R:_ST ->
            ok
    end,
    {ok, ClientInfo}.

on_message_acked(ClientInfo, Msg) ->
    case emqx_message:get_header(?IOT_DELIVERY_ID, Msg, undefined) of
        undefined ->
            ok;
        DeliveryId ->
            #{clientid := DeviceName} = ClientInfo,
            ProductKey = get_product_key(ClientInfo),
            _ = emqx_iot_storage:process_ack(ProductKey, DeviceName, DeliveryId),
            emqx_iot_metrics:inc_msg_acked(),
            ok
    end.

replay_delivery(Pid, ProductKey, DeviceName, DeliveryId) ->
    case mnesia:dirty_read(iot_mq_msg, DeliveryId) of
        [#iot_mq_msg{msg_id = MsgId, topic_template = Template}] ->
            case emqx_iot_storage:lookup_message(MsgId) of
                {ok, #iot_mq_message{payload = Payload}} ->
                    Topic = emqx_iot_utils:expand_topic(Template, ProductKey, DeviceName),
                    Msg = emqx_message:make(
                        DeliveryId,
                        DeviceName,
                        ?QOS_1,
                        Topic,
                        Payload,
                        #{},
                        #{?IOT_DELIVERY_ID => DeliveryId}
                    ),
                    Pid ! #deliver{topic = Topic, message = Msg},
                    emqx_iot_metrics:inc_msg_replayed(),
                    ok;
                {error, not_found} ->
                    ok
            end;
        [] ->
            ok
    end.

get_product_key(#{client_attrs := #{<<"tns">> := Tns}}) -> Tns;
get_product_key(_ClientInfo) -> <<"default">>.

rebuild_index() ->
    ensure_ets(?TAB_MSG_IDX, [
        named_table, public, set, {keypos, #iot_mq_msg_index.key}, {read_concurrency, true}, {write_concurrency, true}
    ]),
    ets:delete_all_objects(?TAB_MSG_IDX),
    Deliveries = mnesia:dirty_match_object(iot_mq_msg, #iot_mq_msg{_ = '_'}),
    lists:foreach(
        fun(#iot_mq_msg{delivery_id = Did, product_key = PK, device_names = DNs}) ->
            lists:foreach(
                fun(DN) ->
                    Key = {PK, DN},
                    case ets:lookup(?TAB_MSG_IDX, Key) of
                        [#iot_mq_msg_index{delivery_ids = Ids}] ->
                            ets:insert(?TAB_MSG_IDX, #iot_mq_msg_index{key = Key, delivery_ids = [Did | Ids]});
                        [] ->
                            ets:insert(?TAB_MSG_IDX, #iot_mq_msg_index{key = Key, delivery_ids = [Did]})
                    end
                end,
                DNs
            )
        end,
        Deliveries
    ),
    ok.
