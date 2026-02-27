%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_state).

-export([
    create_tables/0,
    add/3,
    del/1,
    del_client/2,
    count/1,
    is_known_client/2,
    list_usernames/5,
    get_username/1,
    kick_username/1,
    clear_for_node/1,
    clear_self_node/0,
    reset/0,
    fold_username_counts/2,
    build_snapshot_into/3,
    set_overrides/1,
    get_override/1,
    delete_overrides/1,
    list_overrides/0,
    get_effective_limit/1
]).

-include("emqx_username_quota.hrl").

-doc """
Create the two core Mria tables used by username quota state.
?RECORD_TAB stores one row per active session, keyed by {Username, ClientId, Pid}.
?COUNTER_TAB stores per-node counters, keyed by {Username, Node};
global count per username is derived by summing all node counters.
""".
create_tables() ->
    ok = mria:create_table(?RECORD_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, ram_copies},
        {attributes, record_info(fields, ?RECORD_TAB)}
    ]),
    ok = mria:create_table(?COUNTER_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, ram_copies},
        {attributes, record_info(fields, ?COUNTER_TAB)}
    ]),
    ok = mria:create_table(?OVERRIDE_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {attributes, record_info(fields, ?OVERRIDE_TAB)}
    ]),
    ok = mria:wait_for_tables([?RECORD_TAB, ?COUNTER_TAB, ?OVERRIDE_TAB]),
    ok = emqx_utils_ets:new(?MONITOR_TAB, [bag, public]),
    ok = emqx_utils_ets:new(?CCACHE_TAB, [ordered_set, public]),
    ok.

add(Username, ClientId, Pid) ->
    Key = ?RECORD_KEY(Username, ClientId, Pid),
    case monitor_exists(Pid, Username, ClientId) of
        true ->
            existing;
        false ->
            Record = #?RECORD_TAB{key = Key, node = node(), extra = #{}},
            _ = ets:insert(?MONITOR_TAB, ?MONITOR(Pid, Username, ClientId)),
            _ = mria:dirty_update_counter(?COUNTER_TAB, ?COUNTER_KEY(Username, node()), 1),
            ok = mria:dirty_write(?RECORD_TAB, Record),
            evict_ccache(Username),
            new
    end.

del(Pid) ->
    case ets:lookup(?MONITOR_TAB, Pid) of
        [] ->
            ok;
        Monitors ->
            lists:foreach(
                fun(?MONITOR(_, Username, ClientId)) ->
                    dec_counter(Username),
                    ok = mria:dirty_delete(?RECORD_TAB, ?RECORD_KEY(Username, ClientId, Pid)),
                    evict_ccache(Username)
                end,
                Monitors
            ),
            _ = ets:delete(?MONITOR_TAB, Pid),
            ok
    end.

del_client(Username, ClientId) ->
    Start = ?RECORD_KEY(Username, ClientId, ?MIN_PID),
    delete_client_records(Username, ClientId, ets:next(?RECORD_TAB, Start)).

count(Username) ->
    with_ccache(Username).

is_known_client(Username, ClientId) ->
    case ets:next(?RECORD_TAB, ?RECORD_KEY(Username, ClientId, ?MIN_PID)) of
        ?RECORD_KEY(Username, ClientId, Pid) -> {true, node(Pid)};
        _ -> false
    end.

list_usernames(RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) ->
    case
        emqx_username_quota_snapshot:request_page(RequesterPid, DeadlineMs, Cursor, Limit, UsedGte)
    of
        {ok, PageResult0} ->
            SnapshotRows = maps:get(data, PageResult0, []),
            Data = [
                build_page_item(Username, SnapshotUsed)
             || {{_Counter, Username}, SnapshotUsed} <- SnapshotRows
            ],
            {ok, PageResult0#{data => Data}};
        {error, Reason} ->
            {error, Reason}
    end.

get_username(Username) ->
    case count(Username) of
        0 ->
            {error, not_found};
        Used ->
            {ok, #{
                username => Username,
                used => Used,
                limit => get_effective_limit(Username),
                clientids => list_clientids(Username)
            }}
    end.

kick_username(Username) ->
    ClientIds = list_clientids(Username),
    case ClientIds of
        [] ->
            {error, not_found};
        _ ->
            lists:foreach(fun(ClientId) -> _ = emqx_cm:kick_session(ClientId) end, ClientIds),
            {ok, length(ClientIds)}
    end.

clear_self_node() ->
    do_clear_for_node(node()).

clear_for_node(Node) ->
    Fn = fun() -> do_clear_for_node(Node) end,
    global:trans({?LOCK(Node), self()}, Fn).

reset() ->
    create_tables(),
    _ = mria:clear_table(?RECORD_TAB),
    _ = mria:clear_table(?COUNTER_TAB),
    _ = mria:clear_table(?OVERRIDE_TAB),
    true = ets:delete_all_objects(?MONITOR_TAB),
    true = ets:delete_all_objects(?CCACHE_TAB),
    _ =
        try emqx_username_quota_snapshot:reset() of
            ok -> ok
        catch
            _:_ -> ok
        end,
    ok.

-doc """
Batch upsert per-username quota overrides.
Each entry: #{<<"username">> => binary(), <<"quota">> => non_neg_integer() | <<"nolimit">>}
""".
set_overrides(List) when is_list(List) ->
    Records = lists:map(fun parse_override_entry/1, List),
    {atomic, ok} = mria:transaction(?DB_SHARD, fun() ->
        lists:foreach(fun(Rec) -> mnesia:write(Rec) end, Records)
    end),
    {ok, length(Records)}.

-doc "Look up a single override.".
get_override(Username) ->
    case mnesia:dirty_read(?OVERRIDE_TAB, Username) of
        [#?OVERRIDE_TAB{quota = Quota}] -> {ok, Quota};
        [] -> undefined
    end.

-doc "Batch delete overrides by username list.".
delete_overrides(Usernames) when is_list(Usernames) ->
    N = length(Usernames),
    {atomic, ok} = mria:transaction(?DB_SHARD, fun() ->
        lists:foreach(fun(U) -> mnesia:delete({?OVERRIDE_TAB, U}) end, Usernames)
    end),
    {ok, N}.

-doc "List all overrides.".
list_overrides() ->
    Records = mnesia:dirty_select(?OVERRIDE_TAB, [{'_', [], ['$_']}]),
    [#{username => U, quota => Q} || #?OVERRIDE_TAB{username = U, quota = Q} <- Records].

-doc "Get the effective limit for a username: override if present, else global config.".
get_effective_limit(Username) ->
    case get_override(Username) of
        {ok, Quota} -> Quota;
        undefined -> emqx_username_quota_config:max_sessions_per_username()
    end.

fold_username_counts(Fun, Acc0) when is_function(Fun, 3) ->
    TmpTab = ets:new(?MODULE, [set]),
    ok = fold_counter_rows(ets:first(?COUNTER_TAB), TmpTab),
    Acc = fold_counter_sums(ets:first(TmpTab), TmpTab, Fun, Acc0),
    true = ets:delete(TmpTab),
    Acc.

-doc "Build snapshot into TargetTab, filtering by UsedGte, yielding every YieldInterval inserts.".
build_snapshot_into(TargetTab, UsedGte, YieldInterval) ->
    TmpTab = ets:new(snapshot_build_tmp, [set]),
    try
        ok = fold_counter_rows(ets:first(?COUNTER_TAB), TmpTab),
        insert_snapshot_rows(ets:first(TmpTab), TmpTab, TargetTab, UsedGte, YieldInterval, 0)
    after
        ets:delete(TmpTab)
    end.

insert_snapshot_rows('$end_of_table', _TmpTab, _TargetTab, _UsedGte, _YieldInterval, _N) ->
    ok;
insert_snapshot_rows(Username, TmpTab, TargetTab, UsedGte, YieldInterval, N) ->
    N1 =
        case ets:lookup(TmpTab, Username) of
            [{Username, Counter}] when Counter >= UsedGte ->
                ets:insert(TargetTab, {{Counter, Username}, Counter}),
                case (N + 1) rem YieldInterval of
                    0 -> erlang:yield();
                    _ -> ok
                end,
                N + 1;
            _ ->
                N
        end,
    insert_snapshot_rows(ets:next(TmpTab, Username), TmpTab, TargetTab, UsedGte, YieldInterval, N1).

list_clientids(Username) ->
    Start = ?RECORD_KEY(Username, ?MIN_CLIENTID, ?MIN_PID),
    list_clientids(Username, ets:next(?RECORD_TAB, Start), []).

list_clientids(Username, ?RECORD_KEY(Username, ClientId, _Pid) = Key, [ClientId | _] = Acc) ->
    %% Skip duplicate clientids produced by transient reconnect states.
    list_clientids(Username, ets:next(?RECORD_TAB, Key), Acc);
list_clientids(Username, ?RECORD_KEY(Username, ClientId, _Pid) = Key, Acc) ->
    list_clientids(Username, ets:next(?RECORD_TAB, Key), [ClientId | Acc]);
list_clientids(_Username, _OtherKey, Acc) ->
    lists:reverse(Acc).

with_ccache(Username) ->
    case lookup_ccache(Username) of
        false -> update_ccache(Username);
        Cnt -> Cnt
    end.

build_page_item(Username, SnapshotUsed) ->
    ClientIds = list_clientids(Username),
    Used = length(ClientIds),
    Base = #{
        username => Username,
        used => Used,
        limit => get_effective_limit(Username),
        clientids => ClientIds
    },
    case Used =:= SnapshotUsed of
        true -> Base;
        false -> Base#{snapshot_used => SnapshotUsed}
    end.

lookup_ccache(Username) ->
    case ets:lookup(?CCACHE_TAB, Username) of
        [] ->
            false;
        [?CCACHE(Username, Ts, Cnt)] ->
            case erlang:system_time(millisecond) - Ts < ?CCACHE_VALID_MS of
                true -> Cnt;
                false -> false
            end
    end.

update_ccache(Username) ->
    Cnt = sum_username_counters(Username),
    _ = ets:insert(?CCACHE_TAB, ?CCACHE(Username, erlang:system_time(millisecond), Cnt)),
    Cnt.

sum_username_counters(Username) ->
    StartKey = ?COUNTER_KEY(Username, 0),
    sum_username_counters(Username, ets:next(?COUNTER_TAB, StartKey), 0).

sum_username_counters(Username, ?COUNTER_KEY(Username, _Node) = Key, Acc) ->
    Acc1 =
        case ets:lookup(?COUNTER_TAB, Key) of
            [#?COUNTER_TAB{count = Count}] when is_integer(Count), Count > 0 ->
                Acc + Count;
            _ ->
                Acc
        end,
    sum_username_counters(Username, ets:next(?COUNTER_TAB, Key), Acc1);
sum_username_counters(_Username, _Key, Acc) ->
    Acc.

evict_ccache(Username) ->
    ets:delete(?CCACHE_TAB, Username),
    ok.

delete_client_records(Username, ClientId, ?RECORD_KEY(Username, ClientId, Pid) = Key) ->
    dec_counter(Username),
    ok = mria:dirty_delete(?RECORD_TAB, Key),
    _ = ets:delete_object(?MONITOR_TAB, ?MONITOR(Pid, Username, ClientId)),
    evict_ccache(Username),
    delete_client_records(Username, ClientId, ets:next(?RECORD_TAB, Key));
delete_client_records(_Username, _ClientId, _Key) ->
    ok.

do_clear_for_node(Node) ->
    M1 = #?RECORD_TAB{node = Node, _ = '_'},
    _ = mria:match_delete(?RECORD_TAB, M1),
    M2 = #?COUNTER_TAB{key = ?COUNTER_KEY('_', Node), _ = '_'},
    _ = mria:match_delete(?COUNTER_TAB, M2),
    true = ets:delete_all_objects(?CCACHE_TAB),
    ok.

dec_counter(Username) ->
    Key = ?COUNTER_KEY(Username, node()),
    case mria:dirty_update_counter(?COUNTER_TAB, Key, -1) of
        NewCount when is_integer(NewCount), NewCount =< 0 ->
            _ = mria:dirty_delete(?COUNTER_TAB, Key),
            ok;
        _ ->
            ok
    end.

monitor_exists(Pid, Username, ClientId) ->
    lists:any(
        fun(?MONITOR(_, U, C)) ->
            U =:= Username andalso C =:= ClientId
        end,
        ets:lookup(?MONITOR_TAB, Pid)
    ).

fold_counter_rows('$end_of_table', _TmpTab) ->
    ok;
fold_counter_rows(Key, TmpTab) ->
    case ets:lookup(?COUNTER_TAB, Key) of
        [#?COUNTER_TAB{key = ?COUNTER_KEY(Username, _Node), count = Counter}] when Counter > 0 ->
            _ = ets:update_counter(TmpTab, Username, {2, Counter}, {Username, 0}),
            ok;
        _ ->
            ok
    end,
    fold_counter_rows(ets:next(?COUNTER_TAB, Key), TmpTab).

fold_counter_sums('$end_of_table', _TmpTab, _Fun, Acc) ->
    Acc;
fold_counter_sums(Username, TmpTab, Fun, Acc0) ->
    Acc =
        case ets:lookup(TmpTab, Username) of
            [{Username, Counter}] when Counter > 0 ->
                Fun(Username, Counter, Acc0);
            _ ->
                Acc0
        end,
    fold_counter_sums(ets:next(TmpTab, Username), TmpTab, Fun, Acc).

parse_override_entry(#{<<"username">> := Username, <<"quota">> := <<"nolimit">>}) ->
    #?OVERRIDE_TAB{username = Username, quota = nolimit};
parse_override_entry(#{<<"username">> := Username, <<"quota">> := Quota}) when
    is_integer(Quota), Quota >= 0
->
    #?OVERRIDE_TAB{username = Username, quota = Quota}.
