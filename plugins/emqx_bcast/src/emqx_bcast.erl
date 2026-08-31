%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast).

-export([
    hook/0,
    unhook/0,
    init_tables/0,
    ensure_core_copies/0,
    is_core/0,
    core_nodes/0,
    random_core/0,
    core_for/1,
    rpc_core/3,
    rpc_core/4,
    register_device/3,
    unregister_device/3,
    lookup_device/1,
    lookup_devices_by_product/1,
    on_client_connected/2,
    on_client_disconnected/3,
    on_client_subscribe/3,
    on_client_unsubscribe/3,
    on_session_resumed/2,
    on_client_ping/3,
    on_message_acked/2
]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% Role helpers
%%--------------------------------------------------------------------

-spec is_core() -> boolean().
is_core() ->
    try
        mria_config:whoami() =/= replicant
    catch
        Error:Reason ->
            %% mria is not ready yet: assume replicant (read-only) until the
            %% role can be determined. The previous default of true could
            %% create storage tables, start core-only workers and serve
            %% writes on a replicant during startup.
            ?SLOG(warning, #{
                msg => "bcast_role_check_failed_default_replicant",
                exception => Error,
                reason => Reason
            }),
            false
    end.

-spec core_nodes() -> [node()].
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

-spec random_core() -> node().
random_core() ->
    Nodes = core_nodes(),
    lists:nth(erlang:phash2(erlang:unique_integer(), length(Nodes)) + 1, Nodes).

%% Deterministic core for a client: all want_next claims for the same client
%% land on the same core, which keeps the per-client claim load stable. The
%% node list is sorted so the mapping is stable regardless of discovery order.
-spec core_for(binary()) -> node().
core_for(ClientId) ->
    Nodes = lists:sort(core_nodes()),
    lists:nth(erlang:phash2(ClientId, length(Nodes)) + 1, Nodes).

-spec rpc_core(module(), atom(), [term()]) -> term().
rpc_core(Mod, Fun, Args) ->
    rpc_core(Mod, Fun, Args, ?BCAST_RPC_CALL_TIMEOUT_MS).

-spec rpc_core(module(), atom(), [term()], timeout()) -> term().
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

-spec hook() -> ok.
hook() ->
    ok = emqx_hooks:put('client.connected', {?MODULE, on_client_connected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.disconnected', {?MODULE, on_client_disconnected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.subscribe', {?MODULE, on_client_subscribe, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.unsubscribe', {?MODULE, on_client_unsubscribe, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.ping', {?MODULE, on_client_ping, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('message.acked', {?MODULE, on_message_acked, []}, ?HP_HIGHEST).

-spec unhook() -> ok.
unhook() ->
    ok = emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    ok = emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    ok = emqx_hooks:del('client.subscribe', {?MODULE, on_client_subscribe}),
    ok = emqx_hooks:del('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
    ok = emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    ok = emqx_hooks:del('client.ping', {?MODULE, on_client_ping}),
    ok = emqx_hooks:del('message.acked', {?MODULE, on_message_acked}).

%%--------------------------------------------------------------------
%% Tables
%%--------------------------------------------------------------------

-spec init_tables() -> ok.
init_tables() ->
    ok = create_mnesia_tables(),
    ok = create_ets_tables(),
    ok = ensure_core_copies().

create_mnesia_tables() ->
    case is_core() of
        false ->
            ok;
        true ->
            ok = migrate_legacy_tables(),
            Tables = [
                {?TAB_MSG, bcast_message, record_info(fields, bcast_message)},
                {?TAB_MSG_API_ID, bcast_message_api_id, record_info(fields, bcast_message_api_id)},
                {?TAB_MSG_HASH, bcast_message_hash, record_info(fields, bcast_message_hash)},
                {?TAB_MSG_REC, bcast_msg, record_info(fields, bcast_msg)},
                {?TAB_MSG_META, bcast_msg_meta, record_info(fields, bcast_msg_meta)},
                {?TAB_MSG_IDX, bcast_msg_index, record_info(fields, bcast_msg_index)},
                {?TAB_QUOTA, bcast_quota, record_info(fields, bcast_quota)}
            ],
            lists:foreach(
                fun({Tab, RecordName, Attributes}) ->
                    ok = create_mnesia_table(Tab, RecordName, Attributes)
                end,
                Tables
            ),
            ok = mria:wait_for_tables([Tab || {Tab, _, _} <- Tables]),
            ok = initialize_quota_count()
    end.

%% 0.1.x (and early 0.2.0) installed tables with an older attribute layout.
%% transform_table preserves every row; new tables are created by
%% create_mnesia_table below. Run before table creation so an existing old
%% table is never used with the new record shape.
migrate_legacy_tables() ->
    Migrations = [
        {?TAB_MSG, bcast_message, record_info(fields, bcast_message), fun fix_legacy_message/1},
        {?TAB_MSG_REC, bcast_msg, record_info(fields, bcast_msg), fun fix_legacy_delivery/1},
        {?TAB_MSG_IDX, bcast_msg_index, record_info(fields, bcast_msg_index),
            fun fix_legacy_index/1}
    ],
    lists:foreach(
        fun({Tab, RecordName, ExpectedAttrs, FixFun}) ->
            migrate_legacy_table(Tab, RecordName, ExpectedAttrs, FixFun)
        end,
        Migrations
    ).

migrate_legacy_table(Tab, RecordName, ExpectedAttrs, FixFun) ->
    case lists:member(Tab, mnesia:system_info(tables)) of
        false ->
            ok;
        true ->
            try mnesia:table_info(Tab, attributes) of
                ExpectedAttrs ->
                    ok;
                CurrentAttrs ->
                    ?SLOG(info, #{
                        msg => "bcast_migrating_legacy_mnesia_table",
                        table => Tab,
                        old_attributes => CurrentAttrs,
                        new_attributes => ExpectedAttrs
                    }),
                    {atomic, ok} = mnesia:transform_table(Tab, FixFun, ExpectedAttrs, RecordName),
                    ok
            catch
                Error:Reason ->
                    ?SLOG(error, #{
                        msg => "bcast_migrate_legacy_mnesia_table_failed",
                        table => Tab,
                        exception => Error,
                        reason => Reason
                    }),
                    erlang:error({bcast_migrate_legacy_table_failed, Tab, Reason})
            end
    end.

fix_legacy_message({bcast_message, MsgId, ApiMsgId, Hash, Payload, CreatedAt, ExpiresAt}) ->
    {bcast_message, MsgId, ApiMsgId, Hash, Payload, 0, CreatedAt, ExpiresAt};
fix_legacy_message(Record) ->
    Record.

fix_legacy_delivery(
    {bcast_msg, DeliveryId, MsgId, ProductKey, TopicTemplate, TargetAckCount, Counter, DeviceNames,
        CreatedAt, ExpiresAt, _ResponseTopicTemplate}
) ->
    {bcast_msg, DeliveryId, MsgId, ProductKey, TopicTemplate, TargetAckCount, Counter, DeviceNames,
        CreatedAt, ExpiresAt};
fix_legacy_delivery(Record) ->
    Record.

fix_legacy_index({bcast_msg_index, Key, Deliveries}) when is_list(Deliveries) ->
    Entries = normalize_legacy_index_entries(Deliveries),
    {bcast_msg_index, Key, Entries, length(Entries)};
fix_legacy_index({bcast_msg_index, Key, Deliveries, _OldCount}) when is_list(Deliveries) ->
    Entries = normalize_legacy_index_entries(Deliveries),
    {bcast_msg_index, Key, Entries, length(Entries)};
fix_legacy_index(Record) ->
    Record.

normalize_legacy_index_entries([{DeliveryId, _State} = Entry | Rest]) when is_binary(DeliveryId) ->
    [Entry | normalize_legacy_index_entries(Rest)];
normalize_legacy_index_entries([DeliveryId | Rest]) when is_binary(DeliveryId) ->
    [{DeliveryId, stored} | normalize_legacy_index_entries(Rest)];
normalize_legacy_index_entries([_Invalid | Rest]) ->
    normalize_legacy_index_entries(Rest);
normalize_legacy_index_entries([]) ->
    [].

%% bcast_quota did not exist in the legacy layout. Rebuild its global count
%% from the migrated index rows so pending-delivery quotas start from the
%% real backlog instead of zero.
initialize_quota_count() ->
    Count = lists:sum([
        Index#bcast_msg_index.count
     || Index <- mnesia:dirty_match_object(#bcast_msg_index{_ = '_'})
    ]),
    case mnesia:dirty_read(?TAB_QUOTA, global) of
        [] ->
            ok = mnesia:dirty_write(#bcast_quota{key = global, count = Count});
        [#bcast_quota{count = 0}] ->
            ok = mnesia:dirty_write(#bcast_quota{key = global, count = Count});
        [_] ->
            ok
    end.

%% Storage tables are mria ram_copies: pending deliveries are accepted
%% into memory on both core nodes (SLO: in-memory acceptance; the
%% subscriber's PUBACK is the final confirmation). Nothing is written to
%% disk, so a full cluster restart drops pending deliveries.
create_mnesia_table(Tab, RecordName, Attributes) ->
    try
        mria:create_table(Tab, [
            {rlog_shard, ?BCAST_SHARD},
            {type, set},
            {storage, ram_copies},
            {record_name, RecordName},
            {attributes, Attributes}
        ])
    of
        ok -> ok;
        {atomic, ok} -> ok;
        {aborted, {already_exists, Tab}} -> ok;
        {error, {already_exists, Tab}} -> ok;
        Other -> erlang:error({create_table_failed, Tab, Other})
    catch
        error:{already_exists, Tab} -> ok;
        error:{aborted, {already_exists, Tab}} -> ok;
        throw:{aborted, {already_exists, Tab}} -> ok
    end.

create_ets_tables() ->
    %% emqx_bcast_sup already owns and creates this table in its init/1;
    %% register_device deliberately has no create-on-demand fallback, so a
    %% channel process can never become the owner and destroy the registry.
    emqx_bcast_utils:ensure_ets(?TAB_DEV_REGISTRY, ?BCAST_DEV_REGISTRY_OPTS),
    ok.

%% Every core node needs a local ram copy of the storage tables so that
%% transactions (create, claim, ack) execute locally instead of being
%% shipped to whichever core created the table first. Tables created by
%% older builds hold disc_copies; on upgrade those are converted to
%% ram_copies (in-memory SLO). The periodic retry in
%% emqx_bcast_pull_server_pool covers nodes whose plugin started before
%% the cluster fully formed.
-spec ensure_core_copies() -> ok.
ensure_core_copies() ->
    case is_core() of
        false ->
            ok;
        true ->
            Tables = [
                ?TAB_MSG,
                ?TAB_MSG_API_ID,
                ?TAB_MSG_HASH,
                ?TAB_MSG_REC,
                ?TAB_MSG_IDX,
                ?TAB_QUOTA
            ],
            lists:foreach(
                fun(Tab) ->
                    case lists:member(node(), mnesia:table_info(Tab, ram_copies)) of
                        true ->
                            ok;
                        false ->
                            case lists:member(node(), mnesia:table_info(Tab, disc_copies)) of
                                true ->
                                    catch mnesia:change_table_copy_type(Tab, node(), ram_copies);
                                false ->
                                    catch mnesia:add_table_copy(Tab, node(), ram_copies)
                            end
                    end
                end,
                Tables
            ),
            ok
    end.

%%--------------------------------------------------------------------
%% Device table helpers (node-local ETS, replicant + core)
%%--------------------------------------------------------------------

-spec register_device(binary(), binary(), pid()) -> true.
%% Idempotent registration. The ping hook fires on every keepalive
%% (90k clients / 60s keepalive = 1500 writes/s); skip the write when the
%% entry already holds this pid.
register_device(ProductKey, DeviceName, Pid) ->
    Key = {ProductKey, DeviceName},
    case ets:info(?TAB_DEV_REGISTRY) of
        undefined ->
            ok;
        _ ->
            case ets:lookup(?TAB_DEV_REGISTRY, Key) of
                [#bcast_device_registry{pid = Pid}] ->
                    ok;
                _ ->
                    ets:insert(?TAB_DEV_REGISTRY, #bcast_device_registry{
                        key = Key, clientid = DeviceName, pid = Pid
                    })
            end
    end.

%% Keyed delete. The disconnect path knows the ProductKey (client
%% attrs), so delete by {PK, ClientId} directly instead of a match_object
%% full scan over the registry (90k devices x disconnect storm = O(devices
%% x disconnects)).
-spec unregister_device(binary(), binary(), pid()) -> ok.
unregister_device(ProductKey, ClientId, Pid) ->
    case ets:info(?TAB_DEV_REGISTRY) of
        undefined ->
            ok;
        _ ->
            case ets:lookup(?TAB_DEV_REGISTRY, {ProductKey, ClientId}) of
                [#bcast_device_registry{pid = Pid}] ->
                    ets:delete(?TAB_DEV_REGISTRY, {ProductKey, ClientId});
                _ ->
                    %% Not the current holder (takeover) or gone: leave it.
                    ok
            end
    end.

-spec lookup_device({binary(), binary()}) -> {ok, pid()} | {error, not_found}.
lookup_device({ProductKey, DeviceName}) ->
    case ets:info(?TAB_DEV_REGISTRY) of
        undefined ->
            {error, not_found};
        _ ->
            case ets:lookup(?TAB_DEV_REGISTRY, {ProductKey, DeviceName}) of
                [#bcast_device_registry{pid = Pid}] -> {ok, Pid};
                [] -> {error, not_found}
            end
    end.

-spec lookup_devices_by_product(binary()) -> [{binary(), pid()}].
lookup_devices_by_product(ProductKey) ->
    case ets:info(?TAB_DEV_REGISTRY) of
        undefined ->
            [];
        _ ->
            [
                {DeviceName, Pid}
             || [DeviceName, _ClientId, Pid] <- ets:match(
                    ?TAB_DEV_REGISTRY,
                    #bcast_device_registry{
                        key = {ProductKey, '$1'}, clientid = '$2', pid = '$3', _ = '_'
                    }
                )
            ]
    end.

%%--------------------------------------------------------------------
%% Client hooks: all hooks only cast into local pools (never block the
%% channel process), except the process-dictionary subscription cache which
%% is deliberately process-local.
%%--------------------------------------------------------------------

-spec on_client_connected(map(), term()) -> {ok, map()}.
on_client_connected(ClientInfo, _ConnInfo) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_pool:cast_client(ClientId, {client_connected, ClientId, Pid, ProductKey})
    end),
    {ok, ClientInfo}.

-spec on_client_disconnected(map(), term(), term()) -> ok.
on_client_disconnected(ClientInfo, _Reason, _ConnInfo) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_pool:cast_client(
            ClientId, {client_disconnected, ClientId, Pid, ProductKey}
        ),
        gen_server:cast(emqx_bcast_ack_pool, {client_down, ClientId})
    end),
    ok.

-spec on_client_subscribe(map(), term(), term()) -> term().
on_client_subscribe(ClientInfo, _Properties, TopicFilters) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_pool:cast_client(ClientId, {subscribe, ClientId, Pid, ProductKey})
    end),
    TopicFilters.

-spec on_client_unsubscribe(map(), term(), term()) -> term().
on_client_unsubscribe(ClientInfo, _Properties, TopicFilters) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_pool:cast_client(ClientId, {unsubscribe, ClientId, Pid, ProductKey})
    end),
    TopicFilters.

-spec on_session_resumed(map(), term()) -> ok.
on_session_resumed(ClientInfo, _SessionInfo) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        %% Completeness: the pools are sharded by clientid; casting to
        %% the old single registered name would hit a non-existent process
        %% and silently drop the subscribe signal (session resume would
        %% never re-arm a want_next).
        emqx_bcast_pull_pool:cast_client(ClientId, {subscribe, ClientId, Pid, ProductKey})
    end),
    ok.

-spec on_client_ping(map(), term(), term()) -> term().
on_client_ping(ClientInfo, _ConnInfo, Acc) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_pool:cast_client(ClientId, {ping, ClientId, Pid, ProductKey})
    end),
    Acc.

-spec on_message_acked(map(), emqx_types:message()) -> ok.
on_message_acked(ClientInfo, Msg) ->
    safe_hook(fun() ->
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
        end
    end),
    ok.

%% Hooks must never take down the EMQX hook runner. Failures are logged at
%% warning so a swallowing hook is visible under the default log level.
safe_hook(Fun) ->
    try Fun() of
        _ -> ok
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "bcast_hook_callback_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.

%%--------------------------------------------------------------------
%% Misc
%%--------------------------------------------------------------------

get_product_key(#{client_attrs := #{<<"tns">> := Tns}}) -> Tns;
get_product_key(_ClientInfo) -> <<"default">>.
