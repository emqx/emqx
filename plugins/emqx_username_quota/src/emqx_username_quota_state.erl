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
    list_usernames/1,
    get_username/1,
    kick_username/1,
    clear_for_node/1,
    clear_self_node/0,
    reset/0
]).

-include("emqx_username_quota.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% @doc Create the two core Mria tables used by username quota state.
%% ?RECORD_TAB stores one row per active session, keyed by {Username, ClientId, Pid}.
%% ?COUNTER_TAB stores per-node counters, keyed by {Username, Node};
%% global count per username is derived by summing all node counters.
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
    ok = mria:wait_for_tables([?RECORD_TAB, ?COUNTER_TAB]),
    ensure_ets_table(?MONITOR_TAB),
    ensure_ets_table(?CCACHE_TAB),
    ok.

add(Username, ClientId, Pid) ->
    Key = ?RECORD_KEY(Username, ClientId, Pid),
    case monitor_exists(Pid, Username, ClientId) of
        true ->
            ok;
        false ->
            Record = #?RECORD_TAB{key = Key, node = node(), extra = #{}},
            _ = ets:insert(?MONITOR_TAB, ?MONITOR(Pid, Username, ClientId)),
            _ = mria:dirty_update_counter(?COUNTER_TAB, ?COUNTER_KEY(Username, node()), 1),
            ok = mria:dirty_write(?RECORD_TAB, Record),
            evict_ccache(Username),
            ok
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

list_usernames(SortByUsedDesc) ->
    CountMap = count_map(),
    Usernames = maps:keys(CountMap),
    Details = [
        #{
            username => Username,
            used => maps:get(Username, CountMap),
            clientids => list_clientids(Username)
        }
     || Username <- Usernames, maps:get(Username, CountMap) > 0
    ],
    sort_usernames(SortByUsedDesc, Details).

get_username(Username) ->
    case count(Username) of
        0 ->
            {error, not_found};
        Used ->
            {ok, #{username => Username, used => Used, clientids => list_clientids(Username)}}
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
    true = ets:delete_all_objects(?MONITOR_TAB),
    true = ets:delete_all_objects(?CCACHE_TAB),
    ok.

sort_usernames(true, Details) ->
    lists:sort(
        fun(#{username := U1, used := C1}, #{username := U2, used := C2}) ->
            case C1 =:= C2 of
                true -> U1 =< U2;
                false -> C1 > C2
            end
        end,
        Details
    );
sort_usernames(_Sort, Details) ->
    lists:sort(fun(#{username := U1}, #{username := U2}) -> U1 =< U2 end, Details).

count_map() ->
    lists:foldl(
        fun(#?COUNTER_TAB{key = ?COUNTER_KEY(Username, _Node), count = Cnt}, Acc) ->
            maps:update_with(Username, fun(V) -> V + Cnt end, Cnt, Acc)
        end,
        #{},
        ets:tab2list(?COUNTER_TAB)
    ).

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
    MS = ets:fun2ms(fun(#?COUNTER_TAB{key = ?COUNTER_KEY(U0, _), count = Count}) when
        U0 =:= Username
    ->
        Count
    end),
    Cnt = lists:sum(ets:select(?COUNTER_TAB, MS)),
    _ = ets:insert(?CCACHE_TAB, ?CCACHE(Username, erlang:system_time(millisecond), Cnt)),
    Cnt.

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
    M1 = erlang:make_tuple(record_info(size, ?RECORD_TAB), '_', [{#?RECORD_TAB.node, Node}]),
    _ = mria:match_delete(?RECORD_TAB, M1),
    M2 = erlang:make_tuple(record_info(size, ?COUNTER_TAB), '_', [
        {#?COUNTER_TAB.key, ?COUNTER_KEY('_', Node)}
    ]),
    _ = mria:match_delete(?COUNTER_TAB, M2),
    true = ets:delete_all_objects(?CCACHE_TAB),
    ok.

ensure_ets_table(Tab) ->
    case ets:info(Tab) of
        undefined ->
            Opts =
                case Tab of
                    ?MONITOR_TAB -> [bag, named_table, public];
                    _ -> [ordered_set, named_table, public]
                end,
            _ = ets:new(Tab, Opts),
            ok;
        _ ->
            ok
    end.

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
