%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implelements the state management for multi-tenancy.
-module(emqx_mt_state).

-export([
    create_tables/0,
    count_clients/1,
    list_clients/3,
    list_tenants/0
]).
-export([
    add/3,
    del/1,
    clear_for_node/1
]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(EMQX_MT_SHARD, emqx_mt_shard).

-define(RECORD_TAB, emqx_mt_record).
-define(COUNTER_TAB, emqx_mt_counter).
-define(MONITOR_TAB, emqx_mt_monitor).
-define(LOCK, emqx_mt_clear_node_lock).

-type tns() :: emqx_mt:tns().
-type clientid() :: emqx_types:clientid().

%% mria
-record(?RECORD_TAB, {key :: {tns(), clientid(), pid()}, value :: node()}).
-record(?COUNTER_TAB, {key :: {tns(), node()}, count :: non_neg_integer()}).

%% ets
-record(?MONITOR_TAB, {pid :: pid(), key :: {tns(), clientid()}}).

create_tables() ->
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
        public,
        {keypos, #?MONITOR_TAB.pid},
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    ok.

%% @doc List all tenants.
-spec list_tenants() -> [tns()].
list_tenants() ->
    Ms = ets:fun2ms(fun(#?COUNTER_TAB{key = {Tns, _}}) -> Tns end),
    ets:select(?COUNTER_TAB, Ms).

%% @doc count the number of clients for a given tns.
-spec count_clients(tns()) -> non_neg_integer().
count_clients(Tns) ->
    Ms = ets:fun2ms(fun(#?COUNTER_TAB{key = {Tns0, _}, count = Count}) when Tns0 =:= Tns -> Count
    end),
    Counts = ets:select(?COUNTER_TAB, Ms),
    lists:sum(Counts).

%% @doc list all clients for a given tns.
%% The second argument is the last clientid from the previous page.
%% The third argument is the number of clients to return.
-spec list_clients(tns(), clientid(), non_neg_integer()) -> [{tns(), clientid()}].
list_clients(Tns, LastClientId, Limit) ->
    Ms = ets:fun2ms(fun(#?RECORD_TAB{key = {Tns0, ClientId, _}, value = _}) when
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
    Record = #?RECORD_TAB{key = {Tns, ClientId, Pid}, value = node()},
    Monitor = #?MONITOR_TAB{pid = Pid, key = {Tns, ClientId}},
    _ = ets:insert(?MONITOR_TAB, Monitor),
    _ = mria:dirty_update_counter(?COUNTER_TAB, {Tns, node()}, 1),
    ok = mria:dirty_write(?RECORD_TAB, Record),
    ?tp(debug, multi_tenant_client_added, #{tns => Tns, clientid => ClientId, pid => Pid}),
    ok.

%% @doc delete a client.
-spec del(pid()) -> ok.
del(Pid) ->
    case ets:lookup(?MONITOR_TAB, Pid) of
        [] ->
            %% already deleted
            ?tp(debug, multi_tenant_client_not_found, #{pid => Pid}),
            ok;
        [#?MONITOR_TAB{key = {Tns, ClientId}}] ->
            _ = mria:dirty_update_counter(?COUNTER_TAB, {Tns, node()}, -1),
            ok = mria:dirty_delete(?RECORD_TAB, {Tns, ClientId, Pid}),
            ets:delete(?MONITOR_TAB, Pid),
            ?tp(debug, multi_tenant_client_deleted, #{tns => Tns, clientid => ClientId, pid => Pid})
    end,
    ok.

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
    M1 = erlang:make_tuple(record_info(size, ?RECORD_TAB), '_', [{#?RECORD_TAB.value, Node}]),
    _ = mria:match_delete(?RECORD_TAB, M1),
    M2 = erlang:make_tuple(record_info(size, ?COUNTER_TAB), '_', [{#?COUNTER_TAB.key, {'_', Node}}]),
    _ = mria:match_delete(?COUNTER_TAB, M2),
    ok.
