%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast).

-export([
    hook/0,
    unhook/0,
    init_tables/0,
    init_role/0,
    ensure_core_copies/0,
    is_core/0,
    storage_tables/0,
    core_nodes/0,
    random_core/0,
    rpc_core/3,
    rpc_core/4,
    rpc_core_cast/3,
    register_device/3,
    unregister_device/3,
    lookup_device/1,
    lookup_devices_by_product/1,
    on_client_connected/2,
    on_client_disconnected/3,
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_session_resumed/2,
    on_client_ping/3,
    on_message_acked/2,
    msg_epoch/1,
    bump_msg_epoch/1,
    epoch_bump_timeout_ms/0,
    bump_msg_epoch_everywhere/1
]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

%% Startup role probe: mria may publish the node's role a moment after the
%% plugin is loaded, so `init_role/0` retries briefly before it commits to a
%% layout. Only the startup path waits; the per-request callers of `is_core/0`
%% keep the immediate fallback.
-define(ROLE_PROBE_ATTEMPTS, 5).
-define(ROLE_PROBE_RETRY_MS, 200).

%%--------------------------------------------------------------------
%% Role helpers
%%--------------------------------------------------------------------

-spec is_core() -> boolean().
is_core() ->
    probe_role().

%% Resolve the role at the start of the plugin's startup path. The role is
%% fixed for the lifetime of the node, but a transient miss must not be read as
%% a stable "replicant": that layout skips every core-only table and worker,
%% and a node that owns index shards would leave those partitions without an
%% owner until the plugin is restarted.
-spec init_role() -> boolean().
init_role() ->
    probe_role(?ROLE_PROBE_ATTEMPTS).

probe_role() ->
    probe_role(1).

probe_role(Attempts) ->
    try
        mria_config:whoami() =/= replicant
    catch
        Error:Reason ->
            case Attempts > 1 of
                true ->
                    timer:sleep(?ROLE_PROBE_RETRY_MS),
                    probe_role(Attempts - 1);
                false ->
                    %% mria is still not answering: assume replicant
                    %% (read-only) until the role can be determined. The
                    %% previous default of true could create storage tables,
                    %% start core-only workers and serve writes on a replicant
                    %% during startup.
                    ?SLOG(warning, #{
                        msg => "bcast_role_check_failed_default_replicant",
                        exception => Error,
                        reason => Reason
                    }),
                    false
            end
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

%% One of this node's candidate cores, picked at random: used for the cluster
%% calls that any core can serve (metrics scrape, epoch bump). The per-client
%% claim routing is deterministic instead - the pull shard groups claim entries
%% by the shard owner that has to execute them, so a client always lands on the
%% same core (see emqx_bcast_pull_shard:flush_buffer3/1).
-spec random_core() -> node().
random_core() ->
    Nodes = core_nodes(),
    lists:nth(erlang:phash2(erlang:unique_integer(), length(Nodes)) + 1, Nodes).

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

%% Fire-and-forget variant used by release paths. The storage release
%% functions route to an index shard gen_server; running them inside a pull
%% worker blocked the whole worker pool on gen_server:call/RPC whenever
%% releases came in bursts. This casts the work to a core (or spawns it
%% locally on a core) so the pull worker returns immediately.
-spec rpc_core_cast(module(), atom(), [term()]) -> ok.
rpc_core_cast(Mod, Fun, Args) ->
    case is_core() of
        true ->
            _ = spawn(fun() -> apply(Mod, Fun, Args) end),
            ok;
        false ->
            Core = random_core(),
            case Core =:= node() of
                true ->
                    _ = spawn(fun() -> apply(Mod, Fun, Args) end),
                    ok;
                false ->
                    emqx_rpc:cast(Core, Mod, Fun, Args)
            end
    end.

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

-spec hook() -> ok.
hook() ->
    ok = emqx_hooks:put('client.connected', {?MODULE, on_client_connected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.disconnected', {?MODULE, on_client_disconnected, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put(
        'session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_HIGHEST
    ),
    ok = emqx_hooks:put('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('client.ping', {?MODULE, on_client_ping, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('message.acked', {?MODULE, on_message_acked, []}, ?HP_HIGHEST).

-spec unhook() -> ok.
unhook() ->
    ok = emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    ok = emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    ok = emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    ok = emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    ok = emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    ok = emqx_hooks:del('client.ping', {?MODULE, on_client_ping}),
    ok = emqx_hooks:del('message.acked', {?MODULE, on_message_acked}).

%%--------------------------------------------------------------------
%% Tables
%%--------------------------------------------------------------------

-spec init_tables() -> ok.
init_tables() ->
    _ = init_role(),
    ok = create_mnesia_tables(),
    ok = create_ets_tables(),
    ok = ensure_core_copies(),
    ok = log_table_baseline().

%% Startup baseline. These tables are ram_copies, so a node or plugin
%% restart starts them empty and mria re-loads them afterwards; logging the
%% per-table size (with the copy type) makes that - and any later reset -
%% visible in the logs instead of looking like rows vanishing.
log_table_baseline() ->
    ?SLOG(warning, #{
        msg => "bcast_tables_ready",
        node => node(),
        is_core => is_core(),
        tables => table_sizes()
    }),
    ok.

table_sizes() ->
    [
        {Tab, mnesia:table_info(Tab, size), mnesia:table_info(Tab, ram_copies)}
     || Tab <- storage_tables(),
        lists:member(Tab, mnesia:system_info(tables))
    ].

%% The plugin's storage tables, in creation order: the single source of truth
%% for what is created (`create_mnesia_tables/0`), what the startup baseline
%% logs (`table_sizes/0`) and what gets a local copy repaired on every core
%% (`ensure_core_copies/0`). Keeping one list means a new table cannot be
%% created without also being repaired: `mria:create_table` answers
%% `already_exists` on a core that starts after the table exists in the cluster
%% schema, and that core only gets its local copy from the repair pass.
-spec mnesia_table_specs() -> [{atom(), atom(), [atom()], atom()}].
mnesia_table_specs() ->
    [
        {?TAB_MSG, bcast_message, record_info(fields, bcast_message), set},
        {?TAB_MSG_API_ID, bcast_message_api_id, record_info(fields, bcast_message_api_id), set},
        {?TAB_MSG_HASH, bcast_message_hash, record_info(fields, bcast_message_hash), set},
        {?TAB_MSG_ORDER, bcast_message_order, record_info(fields, bcast_message_order),
            ordered_set},
        {?TAB_MSG_REG, bcast_message_reg, record_info(fields, bcast_message_reg), set},
        {?TAB_MSG_REC, bcast_msg, record_info(fields, bcast_msg), set},
        {?TAB_MSG_META, bcast_msg_meta, record_info(fields, bcast_msg_meta), set},
        {?TAB_MSG_META_CNT, bcast_msg_meta_counter, record_info(fields, bcast_msg_meta_counter),
            set},
        {?TAB_MSG_ACKED, bcast_msg_acked, record_info(fields, bcast_msg_acked), bag}
    ].

%% Table names only, in creation order (see `mnesia_table_specs/0`).
-spec storage_tables() -> [atom()].
storage_tables() ->
    [Tab || {Tab, _, _, _} <- mnesia_table_specs()].

create_mnesia_tables() ->
    case is_core() of
        false ->
            ok;
        true ->
            ok = migrate_legacy_tables(),
            Tables = mnesia_table_specs(),
            lists:foreach(
                fun({Tab, RecordName, Attributes, Type}) ->
                    ok = create_mnesia_table(Tab, RecordName, Attributes, Type)
                end,
                Tables
            ),
            ok = mria:wait_for_tables([Tab || {Tab, _, _, _} <- Tables]),
            ok = ensure_query_indexes(),
            ok = initialize_quota_count()
    end.

%% Management reads must not scan a whole table per request. The message
%% detail GET counts the outstanding deliveries of one message, and a full scan
%% of the delivery table for that does not scale - it is the kind of read that
%% eventually outlives the API budget and answers 503. The secondary index is
%% local to this node (the table is ram_copies) and is rebuilt from the rows on
%% every start, so it needs no migration.
ensure_query_indexes() ->
    case mnesia:add_table_index(?TAB_MSG_REC, msg_id) of
        ok ->
            ok;
        %% mnesia reports the existing index by position (and, on some paths,
        %% by attribute name): both mean the index is already there.
        {aborted, {already_exists, ?TAB_MSG_REC, _Index}} ->
            ok;
        Other ->
            ?SLOG(warning, #{
                msg => "bcast_table_index_failed",
                table => ?TAB_MSG_REC,
                index => msg_id,
                reason => Other
            }),
            ok
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
            fun fix_legacy_index/1},
        {?TAB_MSG_ACKED, bcast_msg_acked, record_info(fields, bcast_msg_acked),
            fun fix_legacy_acked/1}
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

%% 0.4.1 dev builds stored one ack marker row per device
%% ({bcast_msg_acked, Did, DeviceName}); the table now stores one row per
%% delivery per flush tick ({bcast_msg_acked, Did, [DeviceName]}).
fix_legacy_acked({bcast_msg_acked, Did, DN}) when is_binary(DN) ->
    {bcast_msg_acked, Did, [DN]};
fix_legacy_acked(Record) ->
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
%% Legacy migration only: bcast_msg_index / bcast_quota existed in old
%% builds; new installs no longer create them. Rebuild the legacy global
%% count when the tables are present and skip when they are absent.
initialize_quota_count() ->
    case lists:member(?TAB_MSG_IDX, mnesia:system_info(tables)) of
        false ->
            ok;
        true ->
            Count = lists:sum([
                Index#bcast_msg_index.count
             || Index <- mnesia:dirty_match_object(#bcast_msg_index{_ = '_'})
            ]),
            case lists:member(?TAB_QUOTA, mnesia:system_info(tables)) of
                false ->
                    ok;
                true ->
                    case mnesia:dirty_read(?TAB_QUOTA, global) of
                        [] ->
                            ok = mnesia:dirty_write(#bcast_quota{key = global, count = Count});
                        [#bcast_quota{count = 0}] ->
                            ok = mnesia:dirty_write(#bcast_quota{key = global, count = Count});
                        [_] ->
                            ok
                    end
            end
    end.

%% Storage tables are mria ram_copies: pending deliveries are accepted
%% into memory on both core nodes (SLO: in-memory acceptance; the
%% subscriber's PUBACK is the final confirmation). Nothing is written to
%% disk, so a full cluster restart drops pending deliveries.
create_mnesia_table(Tab, RecordName, Attributes, Type) ->
    try
        mria:create_table(Tab, [
            {rlog_shard, ?BCAST_SHARD},
            {type, Type},
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
    %% emqx_bcast_sup already owns and creates device registry in its init/1;
    %% register_device deliberately has no create-on-demand fallback, so a
    %% channel process can never become the owner and destroy the registry.
    emqx_bcast_utils:ensure_ets(?TAB_DEV_REGISTRY, ?BCAST_DEV_REGISTRY_OPTS),
    emqx_bcast_utils:ensure_ets(?TAB_MSG_EPOCH, ?BCAST_MSG_EPOCH_OPTS),
    ok.

%% Node-local delete epoch of one content hash. Admission stamps the epoch
%% it observed on the intake entry; promotion drops an entry whose stamped
%% epoch is older than the current one. That is what makes Delete Message
%% linearize against entries that were already sitting in an intake queue,
%% including a hash group that mixes entries admitted before and after the
%% delete (the decision is per entry, not per group).
-spec msg_epoch(binary()) -> non_neg_integer().
msg_epoch(Hash) ->
    try ets:lookup_element(?TAB_MSG_EPOCH, Hash, 2) of
        N when is_integer(N) -> N
    catch
        _:_ -> 0
    end.

-spec bump_msg_epoch(binary()) -> non_neg_integer().
bump_msg_epoch(Hash) ->
    ets:update_counter(?TAB_MSG_EPOCH, Hash, {2, 1}, {Hash, 0}).

%% Bump the epoch on every running core before a message's rows are removed.
%% An unreachable core could still promote an entry admitted before the
%% delete, so the delete is aborted instead of proceeding without it.
%%
%% Accepted trade-off: this fan-out is not atomic with the row removal. If a
%% later step of the delete fails, the delete reports an error with every row
%% still present, but the epochs are already advanced, so the promoter drops
%% the entries admitted before it for that hash as stale and releases their
%% admission. Orphan repair and TTL do NOT recover those intake entries -
%% they only reconcile residual index and storage rows. The exposure is
%% bounded by the delete's own duration and only affects requests that were
%% in flight against that content at that moment.
-spec bump_msg_epoch_everywhere(binary()) -> ok | {error, term()}.
bump_msg_epoch_everywhere(Hash) ->
    Nodes = lists:usort([node() | core_nodes()]),
    %% Every leg runs concurrently and each one is bounded by the request
    %% budget (emqx_bcast_utils:api_rpc_timeout_ms/0), not by the generic 15s
    %% RPC timeout: this runs on the synchronous management-delete path, where
    %% the plugin framework kills the callback (and answers 503) once its
    %% budget expires - so a sequential fan-out with a 15s leg would let the
    %% framework, not the plugin, decide the outcome, after some cores had
    %% already advanced their epoch for this hash.
    Results = parallel_legs(fun(Node) -> bump_msg_epoch_on(Node, Hash) end, Nodes),
    case [N || {N, Result} <- Results, Result =/= true] of
        [] -> ok;
        Failed -> {error, {epoch_bump_failed, Failed}}
    end.

bump_msg_epoch_on(Node, Hash) when Node =:= node() ->
    _ = bump_msg_epoch(Hash),
    true;
bump_msg_epoch_on(Node, Hash) ->
    Timeout = epoch_bump_timeout_ms(),
    try emqx_rpc:call(?MODULE, Node, ?MODULE, bump_msg_epoch, [Hash], Timeout) of
        {badrpc, _} -> false;
        _ -> true
    catch
        _:_ -> false
    end.

%% One epoch-bump leg. The fan-out runs inside a management API request, so a
%% leg is bounded by that request's budget (emqx_bcast_utils:api_rpc_timeout_ms/0)
%% and never by the generic 15s RPC timeout: an unreachable core must not let
%% the framework kill the callback (and answer 503) before the plugin answers
%% with a reason - and must not leave the request to die after advancing some
%% cores' epochs.
-spec epoch_bump_timeout_ms() -> pos_integer().
epoch_bump_timeout_ms() ->
    min(?BCAST_RPC_CALL_TIMEOUT_MS, emqx_bcast_utils:api_rpc_timeout_ms()).

%% Run one fun per item concurrently and pair each result with its item. A leg
%% that dies (or sends nothing before exiting) counts as a failure rather than
%% taking the caller down: the callers here are API paths that have to answer.
parallel_legs(Fun, Items) ->
    Parent = self(),
    Waiting = [
        begin
            Ref = make_ref(),
            Pid = spawn(fun() -> Parent ! {Ref, catch Fun(Item)} end),
            {Item, Ref, Pid}
        end
     || Item <- Items
    ],
    [{Item, leg_result(Ref, Pid)} || {Item, Ref, Pid} <- Waiting].

leg_result(Ref, Pid) ->
    MRef = monitor(process, Pid),
    receive
        {Ref, Result} ->
            demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Pid, _Reason} ->
            %% The result may already be queued behind the DOWN.
            receive
                {Ref, Result} -> Result
            after 0 -> false
            end
    end.

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
            Tables = storage_tables(),
            lists:foreach(fun ensure_core_copy/1, Tables),
            ok
    end.

%% Bring one table's local copy to ram_copies. Both operations drop and
%% re-load the local copy, so the attempt and the outcome are logged: a bare
%% catch used to swallow every failure, leaving the table empty or missing
%% on this node with no trace - which looks exactly like rows vanishing.
ensure_core_copy(Tab) ->
    case lists:member(node(), mnesia:table_info(Tab, ram_copies)) of
        true ->
            ok;
        false ->
            SizeBefore = mnesia:table_info(Tab, size),
            {Op, Do} =
                case lists:member(node(), mnesia:table_info(Tab, disc_copies)) of
                    true ->
                        {change_table_copy_type, fun() ->
                            mnesia:change_table_copy_type(Tab, node(), ram_copies)
                        end};
                    false ->
                        {add_table_copy, fun() ->
                            mnesia:add_table_copy(Tab, node(), ram_copies)
                        end}
                end,
            ?SLOG(warning, #{
                msg => "bcast_table_copy_type_change",
                table => Tab,
                operation => Op,
                size_before => SizeBefore,
                node => node()
            }),
            try Do() of
                Result ->
                    ?SLOG(warning, #{
                        msg => "bcast_table_copy_type_changed",
                        table => Tab,
                        operation => Op,
                        result => Result,
                        size_after => mnesia:table_info(Tab, size)
                    }),
                    ok
            catch
                Error:Reason ->
                    ?SLOG(error, #{
                        msg => "bcast_table_copy_type_change_failed",
                        table => Tab,
                        operation => Op,
                        size_before => SizeBefore,
                        exception => Error,
                        reason => Reason
                    }),
                    ok
            end
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
%% channel process). The per-client subscription filters live in the pull
%% shard's client-state row (fed by the subscribe/unsubscribe/resume casts
%% below).
%%--------------------------------------------------------------------

-spec on_client_connected(map(), term()) -> {ok, map()}.
on_client_connected(ClientInfo, _ConnInfo) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_shard:cast_client(
            ProductKey, ClientId, {client_connected, ClientId, Pid, ProductKey}
        )
    end),
    {ok, ClientInfo}.

-spec on_client_disconnected(map(), term(), term()) -> ok.
on_client_disconnected(ClientInfo, _Reason, _ConnInfo) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_shard:cast_client(
            ProductKey, ClientId, {client_disconnected, ClientId, Pid, ProductKey}
        ),
        emqx_bcast_ack_shard:client_down(ProductKey, ClientId)
    end),
    ok.

-spec on_session_subscribed(map(), emqx_types:topic() | emqx_types:share(), map()) -> ok.
on_session_subscribed(ClientInfo, TopicFilter, SubOpts) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        %% A subscription whose options the caller did not provide is not a
        %% QoS 0 subscription: see emqx_bcast_utils:sub_qos/1. Leave the cached
        %% filter as it is (the claim path then keeps using the last QoS it
        %% knew) instead of recording a 0 that would send a QoS=1 delivery out
        %% as QoS 0.
        case emqx_bcast_utils:sub_qos(SubOpts) of
            {ok, Qos} ->
                emqx_bcast_pull_shard:cast_client(
                    ProductKey,
                    ClientId,
                    {topic_added, ClientId, Pid, ProductKey, TopicFilter, Qos}
                );
            unknown ->
                ?SLOG(warning, #{
                    msg => "bcast_subscription_qos_unknown",
                    clientid => ClientId,
                    product_key => ProductKey,
                    topic => TopicFilter,
                    subopts => SubOpts
                }),
                ok
        end
    end),
    ok.

-spec on_session_unsubscribed(map(), emqx_types:topic() | emqx_types:share(), map()) -> ok.
on_session_unsubscribed(ClientInfo, TopicFilter, _SubOpts) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_shard:cast_client(
            ProductKey,
            ClientId,
            {topic_removed, ClientId, Pid, ProductKey, TopicFilter}
        )
    end),
    ok.

-spec on_session_resumed(map(), term()) -> ok.
on_session_resumed(ClientInfo, _SessionInfo) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        %% Completeness: the pools are sharded by clientid; casting to
        %% the old single registered name would hit a non-existent process
        %% and silently drop the resume signal (session resume would never
        %% re-arm a want_next). Resume restores the session's
        %% subscriptions without re-firing session.subscribed, so the pull
        %% shard re-syncs its cached filters here.
        emqx_bcast_pull_shard:cast_client(
            ProductKey, ClientId, {resume, ClientId, Pid, ProductKey}
        )
    end),
    ok.

-spec on_client_ping(map(), term(), term()) -> term().
on_client_ping(ClientInfo, _ConnInfo, Acc) ->
    safe_hook(fun() ->
        #{clientid := ClientId} = ClientInfo,
        Pid = self(),
        ProductKey = get_product_key(ClientInfo),
        emqx_bcast_pull_shard:cast_client(ProductKey, ClientId, {ping, ClientId, Pid, ProductKey})
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
                %% Route through pull_shard first: it matches the local buffer
                %% and sets the ack-in-flight marker BEFORE this ack can be
                %% applied at core, then forwards to the client's ack shard for the batched
                %% core accounting.
                emqx_bcast_pull_shard:cast_client(
                    ProductKey, DeviceName, {ack, DeviceName, DeliveryId, ProductKey}
                ),
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
