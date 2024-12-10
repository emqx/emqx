%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    clear_for_node/1
]).

-include("emqx_mt.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(EMQX_MT_SHARD, emqx_mt_shard).

-define(RECORD_TAB, emqx_mt_record).
-define(COUNTER_TAB, emqx_mt_counter).
-define(MONITOR_TAB, emqx_mt_monitor).
-define(CCACHE_TAB, emqx_mt_ccache).
-define(LOCK, emqx_mt_clear_node_lock).
-define(RECORD_KEY(Tns, ClientId, Pid), {Tns, ClientId, Pid}).
%% 0 is less '<' than any pid
-define(MIN_PID, 0).

-type tns() :: emqx_mt:tns().
-type clientid() :: emqx_types:clientid().

%% Mria table (disc_copies) to store the namespace records.
%% Value is for future use.
-define(NS_TAB, emqx_mt_ns).
-record(?NS_TAB, {
    ns :: tns(),
    value = [] :: term()
}).

%% Mria table to store the client records.
%% Pid is used in the key to make sure the record is unique,
%% so there is no need for transaction to update the record.
-define(MIN_RECORD_KEY(Tns), ?RECORD_KEY(Tns, ?MIN_CLIENTID, ?MIN_PID)).
-record(?RECORD_TAB, {
    key :: ?RECORD_KEY(tns(), clientid(), pid()),
    node :: node(),
    extra :: []
}).

%% Mria table to store the number of clients in each namespace.
%% The key has node in it to make sure the counter is unique,
%% so there is no need for transaction to update the counter.
-define(COUNTER_KEY(Tns, Node), {Tns, Node}).
-record(?COUNTER_TAB, {
    key :: ?COUNTER_KEY(tns(), node()),
    count :: non_neg_integer()
}).

%% ETS table to keep track of the session pid for each client.
-define(MONITOR(Pid, Key), {Pid, Key}).
-define(CCACHE(Ns, Ts, Cnt), {Ns, Ts, Cnt}).
-define(CCACHE_VALID_MS, 5000).

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
    Ms = ets:fun2ms(fun(#?NS_TAB{ns = Ns}) when Ns > LastNs -> Ns end),
    case ets:select(?NS_TAB, Ms, Limit) of
        '$end_of_table' -> [];
        {Nss, _Continuation} -> Nss
    end.

%% @doc count the number of clients for a given tns.
-spec count_clients(tns()) -> {ok, non_neg_integer()} | {error, not_found}.
count_clients(Tns) ->
    case is_known_ns(Tns) of
        true -> {ok, with_ccache(Tns)};
        false -> {error, not_found}
    end.

%% @doc Returns {true, node()} if the client is known.
-spec is_known_client(tns(), clientid()) -> {true, node()} | false.
is_known_client(Tns, ClientId) ->
    case ets:next(?RECORD_TAB, ?RECORD_KEY(Tns, ClientId, ?MIN_PID)) of
        ?RECORD_KEY(Tns, ClientId, Pid) -> {true, node(Pid)};
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

with_ccache(Tns) ->
    case lookup_counter_from_cache(Tns) of
        false ->
            Cnt = do_count_clients(Tns),
            Ts = now_ts(),
            _ = ets:insert(?CCACHE_TAB, ?CCACHE(Tns, Ts, Cnt)),
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
is_known_ns(Tns) ->
    [] =/= ets:lookup(?NS_TAB, Tns).

%% @doc list all clients for a given tns.
%% The second argument is the last clientid from the previous page.
%% The third argument is the number of clients to return.
-spec list_clients(tns(), clientid(), non_neg_integer()) ->
    {ok, [clientid()]} | {error, not_found}.
list_clients(Tns, LastClientId, Limit) ->
    case is_known_ns(Tns) of
        false -> {error, not_found};
        true -> {ok, do_list_clients(Tns, LastClientId, Limit)}
    end.

do_list_clients(Tns, LastClientId, Limit) ->
    Ms = ets:fun2ms(fun(#?RECORD_TAB{key = ?RECORD_KEY(Tns0, ClientId, _Pid)}) when
        Tns0 =:= Tns andalso ClientId > LastClientId
    ->
        ClientId
    end),
    case ets:select(?RECORD_TAB, Ms, Limit) of
        '$end_of_table' -> [];
        {Clients, _Continuation} -> Clients
    end.

%% @doc insert a record into the table.
-spec add(tns(), clientid(), pid()) -> ok.
add(Tns, ClientId, Pid) ->
    Record = #?RECORD_TAB{
        key = ?RECORD_KEY(Tns, ClientId, Pid),
        node = node(),
        extra = []
    },
    Monitor = ?MONITOR(Pid, {Tns, ClientId}),
    ok = ensure_ns_added(Tns),
    _ = ets:insert(?MONITOR_TAB, Monitor),
    _ = mria:dirty_update_counter(?COUNTER_TAB, ?COUNTER_KEY(Tns, node()), 1),
    ok = mria:dirty_write(?RECORD_TAB, Record),
    ?tp(multi_tenant_client_added, #{tns => Tns, clientid => ClientId, proc => Pid}),
    ok.

-spec ensure_ns_added(tns()) -> ok.
ensure_ns_added(Tns) ->
    case is_known_ns(Tns) of
        true ->
            ok;
        false ->
            ok = mria:dirty_write(?NS_TAB, #?NS_TAB{ns = Tns})
    end.

%% @doc delete a client.
-spec del(pid()) -> ok.
del(Pid) ->
    case ets:lookup(?MONITOR_TAB, Pid) of
        [] ->
            %% some other process' DOWN message?
            ?tp(debug, multi_tenant_client_proc_not_found, #{proc => Pid}),
            ok;
        [?MONITOR(_, {Tns, ClientId})] ->
            _ = mria:dirty_update_counter(?COUNTER_TAB, ?COUNTER_KEY(Tns, node()), -1),
            ok = mria:dirty_delete(?RECORD_TAB, ?RECORD_KEY(Tns, ClientId, Pid)),
            ets:delete(?MONITOR_TAB, Pid),
            ?tp(multi_tenant_client_proc_deleted, #{
                tns => Tns, clientid => ClientId, proc => Pid
            })
    end.

%% @doc clear all clients from a node which is down.
-spec clear_for_node(node()) -> ok.
clear_for_node(Node) ->
    Fn = fun() -> do_clear_for_node(Node) end,
    T1 = erlang:system_time(),
    Res = global:trans({?LOCK, self()}, Fn),
    T2 = erlang:system_time(),
    Level =
        case Res of
            ok -> debug;
            _ -> error
        end,
    ?tp(Level, multi_tenant_node_clear_done, #{node => Node, result => Res, took => T2 - T1}),
    ok.

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
