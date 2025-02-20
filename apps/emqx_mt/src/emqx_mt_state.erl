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
    is_known_ns/1,
    count_clients/1,
    evict_ccache/0,
    evict_ccache/1,
    list_clients/3,
    is_known_client/2
]).

-export([
    add/3,
    del/1,
    clear_for_node/1,
    clear_self_node/0
]).

-include("emqx_mt.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(EMQX_MT_SHARD, emqx_mt_shard).

%% mria tables
-define(RECORD_TAB, emqx_mt_record).
-define(COUNTER_TAB, emqx_mt_counter).
-define(NS_TAB, emqx_mt_ns).

%% ets tables
-define(MONITOR_TAB, emqx_mt_monitor).
-define(CCACHE_TAB, emqx_mt_ccache).

%% Mria table (disc_copies) to store the namespace records.
%% Value is for future use.
-record(?NS_TAB, {
    ns :: tns(),
    value = [] :: term()
}).

%% Mria table to store the client records.
%% Pid is used in the key to make sure the record is unique,
%% so there is no need for transaction to update the record.
-define(RECORD_KEY(Ns, ClientId, Pid), {Ns, ClientId, Pid}).
%% 0 is less '<' than any pid
-define(MIN_PID, 0).
-define(MIN_RECORD_KEY(Ns), ?RECORD_KEY(Ns, ?MIN_CLIENTID, ?MIN_PID)).
-record(?RECORD_TAB, {
    key :: ?RECORD_KEY(tns(), clientid(), pid()),
    node :: node(),
    extra = [] :: nil()
}).

%% Mria table to store the number of clients in each namespace.
%% The key has node in it to make sure the counter is unique,
%% so there is no need for transaction to update the counter.
-define(COUNTER_KEY(Ns, Node), {Ns, Node}).
-record(?COUNTER_TAB, {
    key :: ?COUNTER_KEY(tns(), node()),
    count :: non_neg_integer()
}).

%% ETS table to keep track of the session pid for each client.
-define(MONITOR(Pid, Key), {Pid, Key}).
-define(CCACHE(Ns, Ts, Cnt), {Ns, Ts, Cnt}).
-define(CCACHE_VALID_MS, 5000).

-define(LOCK(Node), {emqx_mt_clear_node_lock, Node}).

-type tns() :: emqx_mt:tns().
-type clientid() :: emqx_types:clientid().

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

%% @doc List namespaces.
%% The second argument is the last namespace from the previous page.
%% The third argument is the number of namespaces to return.
-spec list_ns(tns(), pos_integer()) -> [tns()].
list_ns(LastNs, Limit) ->
    do_list_ns(LastNs, Limit).

do_list_ns(_LastNs, 0) ->
    [];
do_list_ns(LastNs, Limit) ->
    case ets:next(?NS_TAB, LastNs) of
        '$end_of_table' ->
            [];
        Ns ->
            [Ns | do_list_ns(Ns, Limit - 1)]
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
            Cnt = do_count_clients(Ns),
            Ts = now_ts(),
            _ = ets:insert(?CCACHE_TAB, ?CCACHE(Ns, Ts, Cnt)),
            Cnt;
        Cnt ->
            Cnt
    end.

do_count_clients(Ns) ->
    Ms = ets:fun2ms(fun(#?COUNTER_TAB{key = ?COUNTER_KEY(Ns0, _), count = Count}) when
        Ns0 =:= Ns
    ->
        Count
    end),
    lists:sum(ets:select(?COUNTER_TAB, Ms)).

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
            PrevKey = ?RECORD_KEY(Ns, LastClientId, ?MIN_PID),
            {ok, do_list_clients(PrevKey, Limit, [])}
    end.

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
            ok = mria:dirty_write(?NS_TAB, #?NS_TAB{ns = Ns})
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
