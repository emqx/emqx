%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_registry).
-moduledoc """
Owns the durable state for the v2 topic-metrics feature.

The cluster-wide source of truth is a `mria` `disc_copies` table
(`?MRIA_TAB`) — there is NO HOCON config root. The mnesia key is the
tuple `{OwnerNs, BinName}`, so the same bin-name can coexist in
different namespaces without clashing.

Each node also maintains two ETS tables for the hot path:

  * `?REGISTRY_TAB` (ordered_set) — `{Name, #{topic_filter,
    counter_ref, create_time}}`. The counter_ref is a per-node
    `counters` atomic and is NOT replicated.
  * `?INDEX_TAB` (ordered_set) — `emqx_topic_index` overlay used by
    `emqx_topic_metrics_hooks` for wildcard matching. The record body
    is the counter_ref; the namespace is encoded in the Name tuple so
    the publish hook can finish with a single ETS round-trip and
    filter by namespace at lookup time.

Writes (`persist_register/3`, `persist_deregister/1`,
`persist_deregister_all_owned_by/1`) go through
`emqx_cluster_rpc:multicall` so every node serially applies the same
operation; on each node, `do_install_local/*` / `do_uninstall_local/*`
/ `do_reset_local/*` write to mria and update the local ETS overlay
in lockstep. New nodes joining the cluster pick up the durable mria
rows automatically and re-hydrate their local ETS at boot via
`rehydrate/0`.

Reset is intentionally not persisted: counters live in per-node
atomics that are zeroed at every boot anyway, so a stored "reset
time" would be misleading. Each node emits its own audit log entry
from `do_reset_local/2` so the cluster history of resets is visible
without making mria responsible for state that already disappears on
restart.
""".

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_topic_metrics.hrl").

-export([start_link/0]).

-export([create_tables/0]).

%% Durable persistence operations that touch mria. These run ONLY
%% on the initiator (called from the facade in emqx_topic_metrics2)
%% and are wrapped in a mria transaction so the cap / duplicate
%% check is atomic with the write. The mria layer replicates the
%% resulting row to every node's local replica.
-export([
    persist_register/3,
    persist_deregister/1,
    persist_deregister_all_owned_by/1
]).

%% Cluster-coordinated LOCAL side-effect operations, fanned out via
%% emqx_cluster_rpc:multicall AFTER the durable mria write/delete has
%% already happened on the initiator. These callbacks NEVER touch
%% mria — they only read it (when convenient) and write to the local
%% ETS overlay tables.
%%
%% Two arities of each function exist for the same reason
%% `emqx:update_config/3' and `/4' coexist: bpapi static checks
%% verify that `M:F/length(Args)' exists for whatever the proto
%% module passes to `emqx_cluster_rpc:multicall'. At runtime,
%% `emqx_cluster_rpc:apply_mfa/3' appends a `#{kind => initiate |
%% replicate}' map to the arg list, so the function actually invoked
%% is the +1 arity.
-export([
    do_install_local/3, do_install_local/4,
    do_uninstall_local/1, do_uninstall_local/2,
    do_reset_local/1, do_reset_local/2,
    do_uninstall_all_local/1, do_uninstall_all_local/2
]).

%% Local read paths (lock-free ETS).
-export([
    lookup/1,
    list/1,
    matches/1,
    matches_with_record/2
]).

%% gen_server callbacks.
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type owner_ns() :: ?global_ns | binary().
-type bin_name() :: binary().
-type name() :: {owner_ns(), bin_name()}.
-type record() :: #{
    name := name(),
    owner_ns := owner_ns(),
    bin_name := bin_name(),
    topic_filter := binary(),
    counter_ref := counters:counters_ref(),
    create_time := binary()
}.

-export_type([owner_ns/0, bin_name/0, name/0, record/0]).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    Options = [
        {type, ordered_set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, topic_metric},
        {attributes, record_info(fields, topic_metric)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ],
    ok = mria:create_table(?MRIA_TAB, Options),
    [?MRIA_TAB].

%%--------------------------------------------------------------------
%% Lifecycle
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Persistence — initiator-only, transactional mria writes
%%
%% Each `persist_*' helper runs INSIDE a `mria:transaction', so the
%% cap / duplicate check is atomic with the row write across the
%% cluster. None of the cluster_rpc callbacks touch mria — they read
%% it at most. Splitting persistence from the local overlay update
%% keeps responsibilities cleanly separated: durable cluster state
%% is the facade's job, local ETS / counter_ref management is the
%% callback's job.
%%
%% Ownership is encoded in the key (`{OwnerNs, BinName}'), so there
%% is no separate ownership check parameter — a caller that can
%% build the key has the right to operate on the row.
%%--------------------------------------------------------------------

-spec persist_register(name(), binary(), binary()) ->
    {ok, new | existing}
    | {error, quota_exceeded | term()}.
persist_register({OwnerNs, BinName} = Name, TopicFilter, CreateTime) when
    is_binary(BinName),
    is_binary(TopicFilter),
    is_binary(CreateTime),
    ?IS_NAMESPACE(OwnerNs)
->
    Rec = #topic_metric{
        name = Name,
        topic_filter = TopicFilter,
        create_time = CreateTime
    },
    Fun = fun() ->
        case mnesia:read(?MRIA_TAB, Name, write) of
            [#topic_metric{}] ->
                %% Idempotent — keep the existing row untouched so
                %% topic_filter and create_time are never silently
                %% overwritten by a re-register.
                existing;
            [] ->
                case mnesia:table_info(?MRIA_TAB, size) >= ?MAX_COLLECTIONS of
                    true ->
                        mnesia:abort(quota_exceeded);
                    false ->
                        ok = mnesia:write(?MRIA_TAB, Rec, write),
                        new
                end
        end
    end,
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, new} -> {ok, new};
        {atomic, existing} -> {ok, existing};
        {aborted, Reason} -> {error, Reason}
    end.

-spec persist_deregister(name()) -> {ok, gone} | {error, not_found}.
persist_deregister({OwnerNs, BinName} = Name) when
    is_binary(BinName), ?IS_NAMESPACE(OwnerNs)
->
    Fun = fun() ->
        case mnesia:read(?MRIA_TAB, Name, write) of
            [] ->
                absent;
            [#topic_metric{}] ->
                ok = mnesia:delete({?MRIA_TAB, Name}),
                gone
        end
    end,
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, gone} -> {ok, gone};
        {atomic, absent} -> {error, not_found};
        {aborted, Reason} -> {error, Reason}
    end.

%% Scope: `all_ns' bulk-deletes the whole table; a namespace value
%% (`?global_ns' or a binary tenant) limits the sweep to rows owned
%% by that namespace.
-spec persist_deregister_all_owned_by(owner_ns() | all_ns) -> {ok, [name()]}.
persist_deregister_all_owned_by(OwnerNs) ->
    Fun = fun() ->
        Names = lists:filtermap(
            fun(#topic_metric{name = N}) ->
                case scope_match(OwnerNs, N) of
                    true ->
                        ok = mnesia:delete({?MRIA_TAB, N}),
                        {true, N};
                    false ->
                        false
                end
            end,
            mnesia:foldl(fun(R, Acc) -> [R | Acc] end, [], ?MRIA_TAB)
        ),
        Names
    end,
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, Names} -> {ok, Names};
        {aborted, _Reason} -> {ok, []}
    end.

scope_match(all_ns, _Name) -> true;
scope_match(OwnerNs, {OwnerNs, _Bin}) -> true;
scope_match(_, _) -> false.

%%--------------------------------------------------------------------
%% Local overlay update — cluster_rpc callbacks
%%
%% These run on EVERY cluster node (the initiator first, then every
%% follower) via emqx_cluster_rpc:multicall. They NEVER write mria;
%% the durable row is already there by the time the multicall fires
%% (see persist_*/N above). Each callback receives the data it needs
%% via its arg list, so the follower does not have to wait for mria
%% replication to complete.
%%--------------------------------------------------------------------

do_install_local(Name, TopicFilter, CreateTime) ->
    do_install_local(Name, TopicFilter, CreateTime, #{}).

do_uninstall_local(Name) ->
    do_uninstall_local(Name, #{}).

do_reset_local(Name) ->
    do_reset_local(Name, #{}).

do_uninstall_all_local(Names) ->
    do_uninstall_all_local(Names, #{}).

-spec do_install_local(name(), binary(), binary(), map()) -> ok.
do_install_local({OwnerNs, BinName} = Name, TopicFilter, CreateTime, _Ctx) when
    is_binary(BinName),
    is_binary(TopicFilter),
    is_binary(CreateTime),
    ?IS_NAMESPACE(OwnerNs)
->
    Rec = #topic_metric{
        name = Name,
        topic_filter = TopicFilter,
        create_time = CreateTime
    },
    install_local(Rec),
    ok.

-spec do_uninstall_local(name(), map()) -> ok.
do_uninstall_local({OwnerNs, BinName} = Name, _Ctx) when
    is_binary(BinName), ?IS_NAMESPACE(OwnerNs)
->
    uninstall_local(Name),
    ok.

%% Zero this node's counters for the collection. Each node emits its
%% own audit log entry so the cluster-wide history of resets is
%% visible even though the operation isn't persisted in mria. The
%% cluster_rpc `kind` (initiate | replicate) is included so an
%% operator can distinguish the node that originally received the
%% request from the followers that applied the side-effect.
-spec do_reset_local(name(), map()) -> ok.
do_reset_local({OwnerNs, BinName} = Name, Ctx) when
    is_binary(BinName), ?IS_NAMESPACE(OwnerNs)
->
    ok = clear_local_counters(Name),
    ok = emit_reset_audit(Name, Ctx),
    ok.

-spec do_uninstall_all_local([name()], map()) -> ok.
do_uninstall_all_local(Names, _Ctx) when is_list(Names) ->
    lists:foreach(fun uninstall_local/1, Names),
    ok.

%%--------------------------------------------------------------------
%% Local read paths
%%--------------------------------------------------------------------

-spec lookup(name()) -> {ok, record()} | {error, not_found}.
lookup({_OwnerNs, _BinName} = Name) ->
    case ets:lookup(?REGISTRY_TAB, Name) of
        [{Name, Rec}] -> {ok, expand(Name, Rec)};
        [] -> {error, not_found}
    end.

%% `list/1' is filter-by-owner: `all_ns' shows everything, `?global_ns'
%% shows only global-owned rows, a binary shows only that namespace.
%% The "global admin sees everything" admin-policy decision lives one
%% layer up (see `emqx_topic_metrics2_api:list_scope/1').
-spec list(owner_ns() | all_ns) -> [record()].
list(all_ns) ->
    [expand(Name, Rec) || {Name, Rec} <- ets:tab2list(?REGISTRY_TAB)];
list(Scope) ->
    [
        expand(Name, Rec)
     || {{O, _Bin} = Name, Rec} <- ets:tab2list(?REGISTRY_TAB),
        O =:= Scope
    ].

-spec matches(emqx_types:topic()) -> [name()].
matches(Topic) ->
    Matches = emqx_topic_index:matches(Topic, ?INDEX_TAB, [unique]),
    [emqx_topic_index:get_id(M) || M <- Matches].

%% Returns only the rows whose owner namespace matches the publisher's
%% namespace. A `?global_ns'-owned collection matches every publisher;
%% a namespaced collection matches only publishers in the same
%% namespace. This is the same logic the publish hook used to do in a
%% post-filter — pushing it down here means the hot path skips
%% counters that can't possibly be touched.
-spec matches_with_record(emqx_types:topic(), owner_ns()) ->
    [{name(), counters:counters_ref()}].
matches_with_record(Topic, PublisherNs) ->
    Matches = emqx_topic_index:matches(Topic, ?INDEX_TAB, [unique]),
    lists:foldl(
        fun(M, Acc) ->
            Name = emqx_topic_index:get_id(M),
            case ns_owner_matches(Name, PublisherNs) of
                true ->
                    case emqx_topic_index:get_record(M, ?INDEX_TAB) of
                        [CRef] -> [{Name, CRef} | Acc];
                        _ -> Acc
                    end;
                false ->
                    Acc
            end
        end,
        [],
        Matches
    ).

%% Global collections (`?global_ns') count every publisher. A
%% namespaced collection counts only same-namespace publishers. A
%% publisher with no `tns' attribute (`?global_ns') only matches
%% global collections.
ns_owner_matches({?global_ns, _Bin}, _Publisher) -> true;
ns_owner_matches({Owner, _Bin}, Owner) when is_binary(Owner) -> true;
ns_owner_matches(_, _) -> false.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%
%% The gen_server's sole job is to own the local ETS tables (so they
%% die with it on a clean shutdown) and to rehydrate them from mria
%% on init. Reads / writes do NOT go through this process.
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ?REGISTRY_TAB = ets:new(?REGISTRY_TAB, [
        ordered_set, named_table, public, {read_concurrency, true}
    ]),
    ?INDEX_TAB = ets:new(?INDEX_TAB, [
        ordered_set, named_table, public, {read_concurrency, true}
    ]),
    ok = mria:wait_for_tables([?MRIA_TAB]),
    ok = rehydrate(),
    {ok, #{}}.

handle_call(_Req, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal — local overlay management
%%--------------------------------------------------------------------

%% Rebuild the local ETS overlay from mria. Called once on init/1.
%% Each row gets a freshly-allocated counter_ref since counters are
%% local atomics and cannot survive a restart.
rehydrate() ->
    Names = mria_all_names(),
    lists:foreach(
        fun(Name) ->
            case mnesia:dirty_read(?MRIA_TAB, Name) of
                [#topic_metric{} = Rec] -> install_local(Rec);
                [] -> ok
            end
        end,
        Names
    ).

%% Install / refresh the local ETS overlay for one collection. If the
%% row already exists locally with a live counter_ref, the counter_ref
%% is preserved — re-registering or rehydrating must never zero a live
%% counter.
install_local(#topic_metric{} = Rec) ->
    #topic_metric{
        name = Name,
        topic_filter = TopicFilter,
        create_time = CreateTime
    } = Rec,
    CRef =
        case ets:lookup(?REGISTRY_TAB, Name) of
            [{Name, #{counter_ref := Existing}}] -> Existing;
            [] -> counters:new(length(?METRICS), [write_concurrency])
        end,
    LocalRec = #{
        topic_filter => TopicFilter,
        counter_ref => CRef,
        create_time => CreateTime
    },
    true = ets:insert(?REGISTRY_TAB, {Name, LocalRec}),
    true = emqx_topic_index:insert(TopicFilter, Name, CRef, ?INDEX_TAB),
    ok.

%% Idempotent: a missing local row is fine (e.g. a deregister on a
%% node that never had this collection installed).
uninstall_local(Name) ->
    case ets:lookup(?REGISTRY_TAB, Name) of
        [{Name, #{topic_filter := TF}}] ->
            true = emqx_topic_index:delete(TF, Name, ?INDEX_TAB),
            true = ets:delete(?REGISTRY_TAB, Name),
            ok;
        [] ->
            ok
    end.

clear_local_counters(Name) ->
    case ets:lookup(?REGISTRY_TAB, Name) of
        [{Name, #{counter_ref := CRef}}] ->
            lists:foreach(
                fun(Idx) -> counters:put(CRef, Idx, 0) end,
                lists:seq(1, length(?METRICS))
            );
        [] ->
            ok
    end,
    ok.

%% Lift a stored {Name, ETS-record} pair into the external record() shape:
%% adds the qualified `name', the user-facing `bin_name', and the
%% derived `owner_ns'.
expand({OwnerNs, BinName} = Name, Rec) ->
    Rec#{name => Name, bin_name => BinName, owner_ns => OwnerNs}.

%% Audit log emitted by every node when its counters are zeroed by a
%% cluster_rpc reset. The `kind' field comes from the cluster_rpc Ctx
%% and is either `initiate' (the node that handled the REST request)
%% or `replicate' (followers).
emit_reset_audit({OwnerNs, BinName}, Ctx) ->
    Kind = maps:get(kind, Ctx, undefined),
    ?AUDIT(info, #{
        cmd => topic_metrics_reset,
        args => [BinName, OwnerNs],
        from => cluster_rpc,
        kind => Kind,
        duration_ms => 0
    }),
    ok.

%%--------------------------------------------------------------------
%% Internal — mria scan helpers
%%--------------------------------------------------------------------

mria_all_names() ->
    mria:async_dirty(?COMMON_SHARD, fun() ->
        mnesia:foldl(
            fun(#topic_metric{name = N}, Acc) -> [N | Acc] end,
            [],
            ?MRIA_TAB
        )
    end).
