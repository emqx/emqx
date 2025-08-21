%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implelements the state management for multi-tenancy.
-module(emqx_mt_state).

-export([
    create_tables/0
]).

-export([
    list_ns/2,
    list_ns_details/2,
    is_known_ns/1,
    count_clients/1,
    evict_ccache/0,
    evict_ccache/1,
    list_clients/3,
    list_clients_no_check/3,
    is_known_client/2
]).

-export([
    add/3,
    del/1,
    clear_for_node/1,
    clear_self_node/0
]).

%% Managed namespaces
-export([
    list_managed_ns/2,
    list_managed_ns_details/2,
    create_managed_ns/1,
    delete_managed_ns/1,
    is_known_managed_ns/1,
    fold_managed_nss/2,

    get_root_configs/1,
    update_root_configs/2,

    bulk_import_configs/1,

    tombstoned_namespaces/0
]).

%% Limiter
-export([
    get_limiter_config/2
]).

%% In-transaction fns
-export([
    create_managed_ns_txn/1,
    delete_ns_txn/1,
    update_root_configs_txn/2,
    bulk_update_root_configs_txn/1
]).

%% Internal exports for `emqx_mt_config{,_janitor}`.
-export([
    tables_to_backup/0,
    ensure_ns_added/1,
    clear_tombstone/1,
    is_tombstoned/1
]).

-ifdef(TEST).
-export([update_ccache/1]).
-endif.

-include("emqx_mt.hrl").
-include("emqx_mt_internal.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(EMQX_MT_SHARD, emqx_mt_shard).

%% ets tables
-define(MONITOR_TAB, emqx_mt_monitor).
-define(CCACHE_TAB, emqx_mt_ccache).

-define(MAX_NUM_TNS_CONFIGS, 1_000).

%% Mria table (disc_copies) to store the namespace records.
%% Value is for future use.
-record(?NS_TAB, {
    ns :: tns(),
    value = #{} :: term()
}).

%% Mria table to store the client records.
%% Pid is used in the key to make sure the record is unique,
%% so there is no need for transaction to update the record.
-define(RECORD_KEY(Ns, ClientId, Pid), {Ns, ClientId, Pid}).
%% 0 is less '<' than any pid
-define(MIN_PID, 0).
-record(?RECORD_TAB, {
    key :: ?RECORD_KEY(tns(), clientid(), pid()),
    node :: node(),
    extra = #{} :: nil()
}).

%% Mria table to store the number of clients in each namespace.
%% The key has node in it to make sure the counter is unique,
%% so there is no need for transaction to update the counter.
-define(COUNTER_KEY(Ns, Node), {Ns, Node}).
-record(?COUNTER_TAB, {
    key :: ?COUNTER_KEY(tns(), node()),
    count :: non_neg_integer()
}).

%% Mria table to store various configurations for explicitly created namespaces.
%% They is simply the namespace name (a binary).
%% Currently, we limit the maximum number of configurable namespaces.
-record(?CONFIG_TAB, {
    key :: tns(),
    configs :: emqx_mt_config:root_config(),
    extra = #{} :: map()
}).

-doc """
Mria table to mark a namespace as deleted and needing cleanup.  Once we finish cleaning up
the namespace (deleting all of its resources), we may delete the corresponding entry here.
We must forbid creating a namespace if its name is an entry in this table.
""".
-record(?TOMBSTONE_TAB, {
    %% :: tns()
    key,
    %% :: map()
    extra = #{}
}).

%% ETS table to keep track of the session pid for each client.
-define(MONITOR(Pid, Key), {Pid, Key}).
-define(CCACHE(Ns, Ts, Cnt), {Ns, Ts, Cnt}).
-define(CCACHE_VALID_MS, 5000).

-define(LOCK(Node), {emqx_mt_clear_node_lock, Node}).

-define(limiter, limiter).

-type tns() :: emqx_mt:tns().
-type tns_details() :: emqx_mt:tns_details().
-type clientid() :: emqx_types:clientid().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?NS_TAB, [
        {type, ordered_set},
        {rlog_shard, ?EMQX_MT_SHARD},
        {storage, disc_copies},
        {attributes, record_info(fields, ?NS_TAB)}
    ]),
    ok = mria:create_table(?RECORD_TAB, [
        {type, ordered_set},
        {rlog_shard, ?EMQX_MT_SHARD},
        {storage, ram_copies},
        {attributes, record_info(fields, ?RECORD_TAB)}
    ]),
    ok = mria:create_table(?COUNTER_TAB, [
        {type, set},
        {rlog_shard, ?EMQX_MT_SHARD},
        {storage, ram_copies},
        {attributes, record_info(fields, ?COUNTER_TAB)}
    ]),
    ok = mria:create_table(?CONFIG_TAB, [
        {type, ordered_set},
        {rlog_shard, ?EMQX_MT_SHARD},
        {storage, disc_copies},
        {attributes, record_info(fields, ?CONFIG_TAB)}
    ]),
    ok = mria:create_table(?TOMBSTONE_TAB, [
        {type, ordered_set},
        {rlog_shard, ?EMQX_MT_SHARD},
        {storage, disc_copies},
        {attributes, record_info(fields, ?TOMBSTONE_TAB)}
    ]),
    ok = mria:wait_for_tables([
        ?NS_TAB,
        ?RECORD_TAB,
        ?COUNTER_TAB,
        ?CONFIG_TAB,
        ?TOMBSTONE_TAB
    ]),
    _ = ets:new(?MONITOR_TAB, [
        set,
        named_table,
        public
    ]),
    _ = ets:new(?CCACHE_TAB, [
        set,
        named_table,
        public
    ]),
    ok.

tables_to_backup() ->
    [?CONFIG_TAB].

%% @doc List namespaces.
%% The second argument is the last namespace from the previous page.
%% The third argument is the number of namespaces to return.
-spec list_ns(tns(), pos_integer()) -> [tns()].
list_ns(LastNs, Limit) ->
    do_list_ns(?NS_TAB, LastNs, Limit).

do_list_ns(_Table, _LastNs, 0) ->
    [];
do_list_ns(Table, LastNs, Limit) ->
    case ets:next(Table, LastNs) of
        '$end_of_table' ->
            [];
        Ns ->
            [Ns | do_list_ns(Table, Ns, Limit - 1)]
    end.

%% @doc List namespaces with extra details.
%% The second argument is the last namespace from the previous page.
%% The third argument is the number of namespaces to return.
-spec list_ns_details(tns(), pos_integer()) -> [tns_details()].
list_ns_details(LastNs, Limit) ->
    do_list_ns_details(?NS_TAB, LastNs, Limit).

do_list_ns_details(_Table, _LastNs, 0) ->
    [];
do_list_ns_details(Table, LastNs, Limit) ->
    case ets:next_lookup(Table, LastNs) of
        '$end_of_table' ->
            [];
        {Ns, [Rec]} ->
            Details = get_ns_details(Table, Rec),
            [Details | do_list_ns_details(Table, Ns, Limit - 1)]
    end.

get_ns_details(?NS_TAB, #?NS_TAB{ns = Ns, value = []}) ->
    %% Old record, since it's not a map.
    #{name => Ns, created_at => undefined};
get_ns_details(?NS_TAB, #?NS_TAB{ns = Ns, value = #{} = Extra}) ->
    CreatedAt = maps:get(created_at, Extra, undefined),
    #{name => Ns, created_at => CreatedAt};
get_ns_details(?CONFIG_TAB, #?CONFIG_TAB{key = Ns, extra = #{} = Extra}) ->
    CreatedAt = maps:get(created_at, Extra, undefined),
    #{name => Ns, created_at => CreatedAt}.

-doc """
List managed namespaces.

The second argument is the last namespace from the previous page.

The third argument is the number of namespaces to return.
""".
-spec list_managed_ns(tns(), pos_integer()) -> [tns()].
list_managed_ns(LastNs, Limit) ->
    do_list_ns(?CONFIG_TAB, LastNs, Limit).

-doc """
List managed namespaces with extra details.

The second argument is the last namespace from the previous page.

The third argument is the number of namespaces to return.
""".
-spec list_managed_ns_details(tns(), pos_integer()) -> [tns_details()].
list_managed_ns_details(LastNs, Limit) ->
    do_list_ns_details(?CONFIG_TAB, LastNs, Limit).

fold_managed_nss(Fn, Acc) ->
    do_fold_managed_nss(ets:first(?CONFIG_TAB), Fn, Acc).

do_fold_managed_nss('$end_of_table', _Fn, Acc) ->
    Acc;
do_fold_managed_nss(Ns, Fn, Acc) ->
    case ets:lookup(?CONFIG_TAB, Ns) of
        [#?CONFIG_TAB{configs = Configs}] ->
            NewAcc = Fn(#{ns => Ns, configs => Configs}, Acc),
            do_fold_managed_nss(ets:next(?CONFIG_TAB, Ns), Fn, NewAcc);
        [] ->
            %% Race?
            do_fold_managed_nss(ets:next(?CONFIG_TAB, Ns), Fn, Acc)
    end.

%% @doc count the number of clients for a given tns.
-spec count_clients(tns()) -> {ok, non_neg_integer()} | {error, not_found}.
count_clients(Ns) ->
    case is_known_ns(Ns) of
        true -> {ok, with_ccache(Ns)};
        false -> {error, not_found}
    end.

%% @doc Returns {true, node()} if the client is known.
-spec is_known_client(tns(), clientid()) -> {true, node()} | false.
is_known_client(Ns, ClientId) ->
    case ets:next(?RECORD_TAB, ?RECORD_KEY(Ns, ClientId, ?MIN_PID)) of
        ?RECORD_KEY(Ns, ClientId, Pid) -> {true, node(Pid)};
        _ -> false
    end.
now_ts() ->
    erlang:system_time(millisecond).

lookup_counter_from_cache(Ns) ->
    case ets:lookup(?CCACHE_TAB, Ns) of
        [] ->
            false;
        [?CCACHE(Ns, Ts, Cnt)] ->
            case now_ts() - Ts < ?CCACHE_VALID_MS of
                true -> Cnt;
                false -> false
            end
    end.

with_ccache(Ns) ->
    case lookup_counter_from_cache(Ns) of
        false ->
            update_ccache(Ns);
        Cnt ->
            Cnt
    end.

update_ccache(Ns) ->
    Cnt = do_count_clients(Ns),
    Ts = now_ts(),
    _ = ets:insert(?CCACHE_TAB, ?CCACHE(Ns, Ts, Cnt)),
    Cnt.

do_count_clients(Ns) ->
    MS = ets:fun2ms(fun(#?COUNTER_TAB{key = ?COUNTER_KEY(Ns0, _), count = Count}) when
        Ns0 =:= Ns
    ->
        Count
    end),
    lists:sum(ets:select(?COUNTER_TAB, MS)).

%% @doc Returns true if it is a known namespace.
-spec is_known_ns(tns()) -> boolean().
is_known_ns(Ns) ->
    [] =/= ets:lookup(?NS_TAB, Ns).

%% @doc list all clients for a given tns.
%% The second argument is the last clientid from the previous page.
%% The third argument is the number of clients to return.
-spec list_clients(tns(), clientid(), non_neg_integer()) ->
    {ok, [clientid()]} | {error, not_found}.
list_clients(Ns, LastClientId, Limit) ->
    case is_known_ns(Ns) of
        false ->
            {error, not_found};
        true ->
            {ok, list_clients_no_check(Ns, LastClientId, Limit)}
    end.

-spec list_clients_no_check(tns(), clientid(), non_neg_integer()) ->
    [clientid()].
list_clients_no_check(Ns, LastClientId, Limit) ->
    PrevKey = ?RECORD_KEY(Ns, LastClientId, ?MIN_PID),
    do_list_clients(PrevKey, Limit, []).

do_list_clients(_Key, 0, Acc) ->
    lists:reverse(Acc);
do_list_clients(?RECORD_KEY(Ns, ClientId, _) = Key, Limit, Acc) ->
    case ets:next(?RECORD_TAB, Key) of
        ?RECORD_KEY(Ns, ClientId, _) = Key1 ->
            %% skip the same clientid
            %% this is likely a transient state
            %% there should eventually be only one pid for a clientid
            do_list_clients(Key1, Limit, Acc);
        ?RECORD_KEY(Ns, ClientId1, _) = Key1 ->
            do_list_clients(Key1, Limit - 1, [ClientId1 | Acc]);
        _ ->
            %% end of table, or another namespace
            lists:reverse(Acc)
    end.

%% @doc insert a record into the table.
-spec add(tns(), clientid(), pid()) -> ok.
add(Ns, ClientId, Pid) ->
    Record = #?RECORD_TAB{
        key = ?RECORD_KEY(Ns, ClientId, Pid),
        node = node(),
        extra = []
    },
    Monitor = ?MONITOR(Pid, {Ns, ClientId}),
    ok = ensure_ns_added(Ns),
    _ = ets:insert(?MONITOR_TAB, Monitor),
    _ = mria:dirty_update_counter(?COUNTER_TAB, ?COUNTER_KEY(Ns, node()), 1),
    ok = mria:dirty_write(?RECORD_TAB, Record),
    ?tp(multi_tenant_client_added, #{tns => Ns, clientid => ClientId, proc => Pid}),
    ok.

-spec ensure_ns_added(tns()) -> ok.
ensure_ns_added(Ns) ->
    case is_known_ns(Ns) of
        true ->
            ok;
        false ->
            NowS = now_s(),
            Extra = #{created_at => NowS},
            ok = mria:dirty_write(?NS_TAB, #?NS_TAB{ns = Ns, value = Extra})
    end.

-spec clear_tombstone(tns()) -> ok.
clear_tombstone(Ns) ->
    ok = mria:dirty_delete(?TOMBSTONE_TAB, Ns).

-spec is_tombstoned(tns()) -> boolean().
is_tombstoned(Ns) ->
    case mnesia:dirty_read(?TOMBSTONE_TAB, Ns) of
        [] ->
            false;
        [_] ->
            true
    end.

%% @doc delete a client.
-spec del(pid()) -> ok.
del(Pid) ->
    case ets:lookup(?MONITOR_TAB, Pid) of
        [] ->
            %% some other process' DOWN message?
            ?tp(debug, multi_tenant_client_proc_not_found, #{proc => Pid}),
            ok;
        [?MONITOR(_, {Ns, ClientId})] ->
            _ = mria:dirty_update_counter(?COUNTER_TAB, ?COUNTER_KEY(Ns, node()), -1),
            ok = mria:dirty_delete(?RECORD_TAB, ?RECORD_KEY(Ns, ClientId, Pid)),
            ets:delete(?MONITOR_TAB, Pid),
            ?tp(multi_tenant_client_proc_deleted, #{
                tns => Ns, clientid => ClientId, proc => Pid
            })
    end.

%% @doc clear all clients from a node which was previously down.
clear_self_node() ->
    ?LOG(info, #{msg => "clear_multi_tenant_session_records_begin"}),
    T1 = erlang:system_time(millisecond),
    ok = do_clear_for_node(node()),
    T2 = erlang:system_time(millisecond),
    ?LOG(info, #{
        msg => "clear_multi_tenant_session_records_end",
        elapsed => T2 - T1
    }),
    ok.

%% @doc clear all clients from a node which is down.
-spec clear_for_node(node()) -> ok | aborted.
clear_for_node(Node) ->
    Fn = fun() -> do_clear_for_node(Node) end,
    global:trans({?LOCK(Node), self()}, Fn).

do_clear_for_node(Node) ->
    M1 = erlang:make_tuple(record_info(size, ?RECORD_TAB), '_', [{#?RECORD_TAB.node, Node}]),
    _ = mria:match_delete(?RECORD_TAB, M1),
    M2 = erlang:make_tuple(record_info(size, ?COUNTER_TAB), '_', [
        {#?COUNTER_TAB.key, ?COUNTER_KEY('_', Node)}
    ]),
    _ = mria:match_delete(?COUNTER_TAB, M2),
    ok.

%% @doc delete all counter cache entries.
-spec evict_ccache() -> ok.
evict_ccache() ->
    ets:delete_all_objects(?CCACHE_TAB),
    ok.

%% @doc delete counter cache for a given namespace.
-spec evict_ccache(tns()) -> ok.
evict_ccache(Ns) ->
    ets:delete(?CCACHE_TAB, Ns),
    ok.

-spec create_managed_ns(emqx_mt:tns()) ->
    ok
    | {error, {aborted, _}}
    | {error, table_is_full | already_exists | namespace_being_deleted}.
create_managed_ns(Ns) ->
    ensure_ns_added(Ns),
    transaction(fun create_managed_ns_txn/1, [Ns]).

-spec delete_managed_ns(emqx_mt:tns()) ->
    {ok, emqx_mt_config:root_config()} | {error, {aborted, _}}.
delete_managed_ns(Ns) ->
    %% Note: we may safely delete the limiter groups here: when clients attempt to consume
    %% from the now dangling limiters, `emqx_limiter_client' will log the error but don't
    %% do any limiting when it fails to fetch the missing limiter group configuration.
    %% The user may choose to later kick all clients from this namespace.
    transaction(fun delete_ns_txn/1, [Ns]).

-spec is_known_managed_ns(emqx_mt:tns()) -> boolean().
is_known_managed_ns(Ns) ->
    case mnesia:dirty_read(?CONFIG_TAB, Ns) of
        [] ->
            false;
        [_] ->
            true
    end.

-spec get_root_configs(emqx_mt:tns()) ->
    {ok, emqx_mt_config:root_config()} | {error, not_found}.
get_root_configs(Ns) ->
    case mnesia:dirty_read(?CONFIG_TAB, Ns) of
        [#?CONFIG_TAB{configs = Configs}] ->
            {ok, Configs};
        _ ->
            {error, not_found}
    end.

update_root_configs(Ns, NewConfig) ->
    transaction(fun ?MODULE:update_root_configs_txn/2, [Ns, NewConfig]).

get_limiter_config(Ns, Kind) ->
    case mnesia:dirty_read(?CONFIG_TAB, Ns) of
        [#?CONFIG_TAB{configs = #{?limiter := #{Kind := Config}}}] ->
            {ok, Config};
        _ ->
            {error, not_found}
    end.

bulk_import_configs(Entries) ->
    transaction(fun ?MODULE:bulk_update_root_configs_txn/1, [Entries]).

tombstoned_namespaces() ->
    MatchHead = #?TOMBSTONE_TAB{key = '$1', _ = '_'},
    MS = [{MatchHead, [], ['$1']}],
    mnesia:dirty_select(?TOMBSTONE_TAB, MS).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

transaction(Fn, Args) ->
    case mria:transaction(?EMQX_MT_SHARD, Fn, Args) of
        {atomic, Res} ->
            Res;
        {aborted, Reason} ->
            {error, {aborted, Reason}}
    end.

create_managed_ns_txn(Ns) ->
    maybe
        [] ?= mnesia:read(?TOMBSTONE_TAB, Ns, read),
        [] ?= mnesia:read(?CONFIG_TAB, Ns, write),
        Size = tns_config_size(),
        case Size >= ?MAX_NUM_TNS_CONFIGS of
            true ->
                {error, table_is_full};
            false ->
                Extra = #{created_at => now_s()},
                Rec = #?CONFIG_TAB{key = Ns, configs = #{}, extra = Extra},
                ok = mnesia:write(?CONFIG_TAB, Rec, write),
                ok
        end
    else
        [#?CONFIG_TAB{}] ->
            {error, already_exists};
        [#?TOMBSTONE_TAB{}] ->
            {error, namespace_being_deleted}
    end.

delete_ns_txn(Ns) ->
    ok = mnesia:delete(?NS_TAB, Ns, write),
    case mnesia:read(?CONFIG_TAB, Ns, write) of
        [] ->
            {ok, #{}};
        [#?CONFIG_TAB{configs = Configs}] ->
            ok = mnesia:delete(?CONFIG_TAB, Ns, write),
            ok = mnesia:write(?TOMBSTONE_TAB, #?TOMBSTONE_TAB{key = Ns}, write),
            {ok, Configs}
    end.

tns_config_size() ->
    mnesia:table_info(?CONFIG_TAB, size).

update_root_configs_txn(Ns, NewConfigs0) ->
    maybe
        [#?CONFIG_TAB{configs = OldConfigs} = Rec0] ?=
            mnesia:read(?CONFIG_TAB, Ns, write),
        Diffs = emqx_utils_maps:diff_maps(NewConfigs0, OldConfigs),
        NewConfigs = emqx_utils_maps:deep_merge(OldConfigs, NewConfigs0),
        Rec = Rec0#?CONFIG_TAB{configs = NewConfigs},
        ok = mnesia:write(?CONFIG_TAB, Rec, write),
        {ok, #{diffs => Diffs, configs => NewConfigs}}
    else
        [] ->
            {error, not_found}
    end.

bulk_update_root_configs_txn(Entries) ->
    Size0 = tns_config_size(),
    {Changes, _} = lists:foldl(
        fun(#{ns := Ns, config := NewConfigs0}, {Acc, SizeAcc}) ->
            case mnesia:read(?CONFIG_TAB, Ns, write) of
                [#?CONFIG_TAB{configs = OldConfigs} = Rec0] ->
                    Size = SizeAcc;
                [] ->
                    Size = SizeAcc + 1,
                    OldConfigs = #{},
                    Rec0 = #?CONFIG_TAB{key = Ns, configs = OldConfigs}
            end,
            maybe
                true ?= Size > ?MAX_NUM_TNS_CONFIGS,
                mnesia:abort(table_is_full)
            end,
            NewConfigs = emqx_utils_maps:deep_merge(OldConfigs, NewConfigs0),
            Rec = Rec0#?CONFIG_TAB{configs = NewConfigs},
            ok = mnesia:write(?CONFIG_TAB, Rec, write),
            {Acc#{Ns => #{old => OldConfigs, new => NewConfigs0}}, Size}
        end,
        {#{}, Size0},
        Entries
    ),
    {ok, Changes}.

now_s() ->
    erlang:system_time(second).
