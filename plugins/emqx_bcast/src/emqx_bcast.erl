%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast).

-export([
    hook/0,
    unhook/0,
    init_tables/0,
    is_core/0,
    core_nodes/0,
    random_core/0,
    rpc_core/3,
    rpc_core/4,
    register_device/3,
    unregister_device/2,
    lookup_device/1,
    lookup_devices_by_product/1,
    on_client_connected/2,
    on_client_disconnected/3,
    on_client_subscribe/3,
    on_client_unsubscribe/3,
    on_session_resumed/2,
    on_client_ping/2,
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

-define(SUBS_PD, {?MODULE, subscriptions}).

%%--------------------------------------------------------------------
%% Role helpers
%%--------------------------------------------------------------------

is_core() ->
    try mria_config:whoami() =/= replicant
    catch
        _:_ -> true
    end.

core_nodes() ->
    try mria_membership:running_core_nodelist() of
        [] -> fallback_core_nodes();
        Nodes -> Nodes
    catch
        _:_ -> fallback_core_nodes()
    end.

fallback_core_nodes() ->
    try emqx:running_nodes() of
        [] -> [node()];
        Nodes -> Nodes
    catch
        _:_ -> [node()]
    end.

random_core() ->
    Nodes = core_nodes(),
    lists:nth(erlang:phash2(erlang:unique_integer(), length(Nodes)) + 1, Nodes).

rpc_core(Mod, Fun, Args) ->
    rpc_core(Mod, Fun, Args, 15000).

rpc_core(Mod, Fun, Args, Timeout) ->
    case is_core() of
        true ->
            apply(Mod, Fun, Args);
        false ->
            Core = random_core(),
            case Core =:= node() of
                true -> apply(Mod, Fun, Args);
                false -> emqx_rpc:call(?MODULE, Core, Mod, Fun, Args, Timeout)
            end
    end.


%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

hook() ->
    ok = emqx_hooks:put('client.connected', {?MODULE, on_client_connected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.disconnected', {?MODULE, on_client_disconnected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.subscribe', {?MODULE, on_client_subscribe, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.unsubscribe', {?MODULE, on_client_unsubscribe, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.ping', {?MODULE, on_client_ping, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('message.acked', {?MODULE, on_message_acked, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_HIGHEST).

unhook() ->
    ok = emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    ok = emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    ok = emqx_hooks:del('client.subscribe', {?MODULE, on_client_subscribe}),
    ok = emqx_hooks:del('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
    ok = emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    ok = emqx_hooks:del('client.ping', {?MODULE, on_client_ping}),
    ok = emqx_hooks:del('message.acked', {?MODULE, on_message_acked}),
    ok = emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}).

%%--------------------------------------------------------------------
%% Tables
%%--------------------------------------------------------------------

init_tables() ->
    ok = create_mnesia_tables(),
    ok = create_ets_tables().

create_mnesia_tables() ->
    case is_core() of
        false ->
            ok;
        true ->
            Tables = [
                {?TAB_MSG, bcast_message, record_info(fields, bcast_message)},
                {?TAB_MSG_API_ID, bcast_message_api_id, record_info(fields, bcast_message_api_id)},
                {?TAB_MSG_HASH, bcast_message_hash, record_info(fields, bcast_message_hash)},
                {?TAB_MSG_REC, bcast_msg, record_info(fields, bcast_msg)},
                {?TAB_MSG_IDX, bcast_msg_index, record_info(fields, bcast_msg_index)}
            ],
            lists:foreach(
                fun({Tab, RecordName, Attributes}) ->
                    ok = create_mnesia_table(Tab, RecordName, Attributes)
                end,
                Tables
            ),
            mnesia:wait_for_tables([Tab || {Tab, _, _} <- Tables], 15000)
    end.

create_mnesia_table(Tab, RecordName, Attributes) ->
    try mnesia:create_table(Tab, [
        {disc_copies, [node()]},
        {type, set},
        {record_name, RecordName},
        {attributes, Attributes}
    ]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Tab}} -> ok;
        {aborted, Reason} -> erlang:error({create_table_failed, Tab, Reason})
    catch
        error:{aborted, {already_exists, Tab}} -> ok;
        throw:{aborted, {already_exists, Tab}} -> ok
    end.

create_ets_tables() ->
    ensure_ets(?TAB_DEV_SUB, [
        named_table, public, set, {keypos, #bcast_device_sub.key}, {read_concurrency, true}
    ]),
    emqx_bcast_subscription:init(),
    ok.

ensure_ets(Name, Opts) ->
    case ets:info(Name) of
        undefined -> ets:new(Name, Opts);
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% Device table helpers (node-local ETS, replicant + core)
%%--------------------------------------------------------------------

register_device(ProductKey, DeviceName, Pid) ->
    ensure_ets(?TAB_DEV_SUB, [
        named_table, public, set, {keypos, #bcast_device_sub.key}, {read_concurrency, true}
    ]),
    ets:insert(?TAB_DEV_SUB, #bcast_device_sub{
        key = {ProductKey, DeviceName}, clientid = DeviceName, pid = Pid
    }).

unregister_device(ClientId, Pid) ->
    case ets:info(?TAB_DEV_SUB) of
        undefined ->
            ok;
        _ ->
            case ets:match_object(?TAB_DEV_SUB, #bcast_device_sub{
                clientid = ClientId, pid = Pid, _ = '_'
            }) of
                [] -> ok;
                Entries ->
                    lists:foreach(
                        fun(#bcast_device_sub{key = Key}) -> ets:delete(?TAB_DEV_SUB, Key) end,
                        Entries
                    )
            end
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
            [{DeviceName, Pid} || [DeviceName, _ClientId, Pid] <- ets:match(
                ?TAB_DEV_SUB,
                #bcast_device_sub{key = {ProductKey, '$1'}, clientid = '$2', pid = '$3', _ = '_'}
            )]
    end.

%%--------------------------------------------------------------------
%% Client hooks: all hooks only cast into local pools (never block the
%% channel process), except the process-dictionary subscription cache which
%% is deliberately process-local.
%%--------------------------------------------------------------------

on_client_connected(ClientInfo, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        gen_server:cast(emqx_bcast_pull_pool, {client_connected, ClientId, Pid, ProductKey})
    catch
        _E:_R:_ST ->
            ok
    end,
    {ok, ClientInfo}.

on_client_disconnected(ClientInfo, _Reason, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        gen_server:cast(emqx_bcast_pull_pool, {client_disconnected, ClientId, Pid}),
        gen_server:cast(emqx_bcast_ack_pool, {client_down, ClientId}),
        erase_subs()
    catch
        _E:_R:_ST ->
            ok
    end,
    ok.

on_client_subscribe(ClientInfo, _Properties, TopicFilters) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        Full = store_subs(TopicFilters, add),
        ProductKey = get_product_key(ClientInfo),
        gen_server:cast(emqx_bcast_pull_pool, {subscribe, ClientId, Pid, ProductKey, Full})
    catch
        _E:_R:_ST ->
            ok
    end,
    TopicFilters.

on_client_unsubscribe(ClientInfo, _Properties, TopicFilters) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        Full = store_subs(TopicFilters, remove),
        ProductKey = get_product_key(ClientInfo),
        gen_server:cast(emqx_bcast_pull_pool, {unsubscribe, ClientId, Pid, ProductKey, Full})
    catch
        _E:_R:_ST ->
            ok
    end,
    TopicFilters.

on_session_resumed(ClientInfo, SessionInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        Full = subscriptions_from_session(SessionInfo),
        put_subs(Full),
        ProductKey = get_product_key(ClientInfo),
        gen_server:cast(emqx_bcast_pull_pool, {subscribe, ClientId, Pid, ProductKey, Full})
    catch
        _E:_R:_ST ->
            ok
    end,
    ok.

on_client_ping(ClientInfo, _ConnInfo) ->
    try
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        Full = get_subs(),
        gen_server:cast(emqx_bcast_pull_pool, {ping, ClientId, Pid, ProductKey, Full})
    catch
        _E:_R:_ST ->
            ok
    end,
    ok.

on_message_acked(ClientInfo, Msg) ->
    case emqx_message:get_header(?BCAST_DELIVERY_ID, Msg, undefined) of
        undefined ->
            ok;
        DeliveryId ->
            #{clientid := DeviceName} = ClientInfo,
            ProductKey =
                case emqx_message:get_header(?BCAST_PRODUCT_KEY, Msg, undefined) of
                    undefined -> get_product_key(ClientInfo);
                    PK -> PK
                end,
            emqx_bcast_ack_pool:ack(DeviceName, DeliveryId, ProductKey),
            ok
    end.

on_delivery_completed(Msg, #{clientid := ClientId}) ->
    case emqx_message:get_header(?BCAST_DELIVERY_ID, Msg, undefined) of
        undefined ->
            ok;
        DeliveryId ->
            case emqx_message:get_header(?BCAST_PRODUCT_KEY, Msg, undefined) of
                undefined ->
                    ok;
                ProductKey ->
                    emqx_bcast_ack_pool:ack(ClientId, DeliveryId, ProductKey),
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% Process dictionary subscription cache
%%--------------------------------------------------------------------

store_subs(TopicFilters, add) ->
    Current = get_subs(),
    Updated =
        case add of
            add -> lists:foldl(fun add_sub/2, Current, TopicFilters);
            remove -> lists:foldl(fun remove_sub/2, Current, TopicFilters)
        end,
    put_subs(Updated),
    Updated.

add_sub({TopicFilter, Opts}, Acc) ->
    Qos = maps:get(qos, Opts, 0),
    [{TopicFilter, Qos} | lists:keydelete(TopicFilter, 1, Acc)].

remove_sub({TopicFilter, _Opts}, Acc) ->
    lists:keydelete(TopicFilter, 1, Acc).

put_subs(Subs) ->
    erlang:put(?SUBS_PD, Subs).

get_subs() ->
    case erlang:get(?SUBS_PD) of
        undefined -> [];
        Subs -> Subs
    end.

erase_subs() ->
    erlang:erase(?SUBS_PD),
    ok.

subscriptions_from_session(SessionInfo) ->
    maps:fold(
        fun(Filter, Opts, Acc) -> [{Filter, maps:get(qos, Opts, 0)} | Acc] end,
        [],
        maps:get(subscriptions, SessionInfo, #{})
    ).

%%--------------------------------------------------------------------
%% Misc
%%--------------------------------------------------------------------

get_product_key(#{client_attrs := #{<<"tns">> := Tns}}) -> Tns;
get_product_key(_ClientInfo) -> <<"default">>.
