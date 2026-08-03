%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_leaderboard).
-moduledoc """
This module is responsible for maintaining `emqx_ds_shards` table,
which is a "merge" mria table,
where optimistic_transaction leader processes register themselves.

It monitors local processes and automatically unregisters them if they terminate.
""".

-behaviour(gen_server).

-elvis([{elvis_style, no_spec_with_records, disable}]).

%% API:
-export([overview/1]).
-export([register_leader/3, unregister_leader/3, whereis_leader/2, leaders/1]).
-export([register_replica/3, unregister_replica/3, replicas/2, replicas/1]).

%% Internal exports:
-export([start_link/0]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-define(tab, emqx_ds_leaderboard).
-define(mria_shard, emqx_ds_mria_shard).

-define(otx_leader, otx_leader).
-define(replica, replica).

-type key() :: ?otx_leader | ?replica.

-define(table_key(DB, SHARD, KEY, NODE, PID), {DB, SHARD, KEY, NODE, PID}).

-type table_key() :: ?table_key(emqx_ds:db(), emqx_ds:shard(), key(), node(), pid()).

-record(?tab, {key, reserved}).

-record(call_reg, {
    db :: emqx_ds:db(),
    shard :: emqx_ds:shard(),
    key :: key(),
    pid :: pid()
}).

-record(call_unreg, {
    db :: emqx_ds:db(),
    shard :: emqx_ds:shard(),
    key :: key(),
    pid :: pid()
}).

-type replica_info() :: {node(), classy:site() | undefined}.

-type shard_info() :: [{_Leaders :: [replica_info()], _NonLeaders :: [replica_info()]}].

%%================================================================================
%% API functions
%%================================================================================

-doc """
This function is meant for human interaction and debugging.

Do not rely on it as an API.
""".
-spec overview(emqx_ds:db()) -> #{emqx_ds:shard() => shard_info()}.
overview(DB) ->
    {ok, Sites} = classy:node_to_site(),
    Leaders = maps:groups_from_list(
        fun({Shard, _}) -> Shard end,
        fun({_Shard, Pid}) -> node(Pid) end,
        leaders(DB)
    ),
    Replicas = maps:groups_from_list(
        fun({Shard, _}) -> Shard end,
        fun({_Shard, Pid}) ->
            Node = node(Pid),
            {Node, maps:get(Node, Sites, undefined)}
        end,
        replicas(DB)
    ),
    maps:map(
        fun(Shard, ShardReplicas) ->
            lists:partition(
                fun({Node, _Site}) ->
                    lists:member(Node, maps:get(Shard, Leaders, []))
                end,
                ShardReplicas
            )
        end,
        Replicas
    ).

-spec register_leader(emqx_ds:db(), emqx_ds:shard(), pid()) -> ok.
register_leader(DB, Shard, Pid) ->
    gen_server:call(?SERVER, #call_reg{db = DB, shard = Shard, key = ?otx_leader, pid = Pid}).

-spec unregister_leader(emqx_ds:db(), emqx_ds:shard(), pid()) -> ok.
unregister_leader(DB, Shard, Pid) ->
    gen_server:call(?SERVER, #call_unreg{db = DB, shard = Shard, key = ?otx_leader, pid = Pid}).

-spec whereis_leader(emqx_ds:db(), emqx_ds:shard()) -> [pid()].
whereis_leader(DB, Shard) ->
    select(DB, Shard, ?otx_leader).

-spec leaders(emqx_ds:db()) -> [{emqx_ds:shard(), pid()}].
leaders(DB) ->
    select(DB, ?otx_leader).

-spec register_replica(emqx_ds:db(), emqx_ds:shard(), pid()) -> ok.
register_replica(DB, Shard, Pid) ->
    gen_server:call(?SERVER, #call_reg{db = DB, shard = Shard, key = ?replica, pid = Pid}).

-spec unregister_replica(emqx_ds:db(), emqx_ds:shard(), pid()) -> ok.
unregister_replica(DB, Shard, Pid) ->
    gen_server:call(?SERVER, #call_unreg{db = DB, shard = Shard, key = ?replica, pid = Pid}).

-spec replicas(emqx_ds:db(), emqx_ds:shard()) -> [pid()].
replicas(DB, Shard) ->
    select(DB, Shard, ?replica).

-spec replicas(emqx_ds:db()) -> [{emqx_ds:shard(), pid()}].
replicas(DB) ->
    select(DB, ?replica).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {monitors = #{} :: #{reference() => table_key()}}).

init(_) ->
    process_flag(trap_exit, true),
    Pattern = #?tab{
        key = ?table_key('_', '_', '_', '$1', '_'),
        _ = '_'
    },
    ok = mria:create_table(
        ?tab,
        [
            {merge_table, true},
            {auto_clean, true},
            {node_pattern, Pattern},
            {rlog_shard, ?mria_shard},
            {type, ordered_set},
            {storage, ram_copies}
        ]
    ),
    case mria:wait_for_tables([?tab]) of
        ok ->
            S = #s{},
            {ok, S};
        {error, stopping} ->
            {stop, normal, undefined}
    end.

handle_call(#call_reg{} = C, _From, S) ->
    {reply, ok, handle_reg(C, S)};
handle_call(#call_unreg{} = C, _From, S) ->
    {reply, ok, handle_unreg(C, S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'DOWN', MRef, _, _, _}, S = #s{monitors = M0}) ->
    case maps:take(MRef, M0) of
        {TableKey, M} ->
            ok = mria:dirty_delete(?tab, TableKey);
        error ->
            M = M0
    end,
    {noreply, S#s{monitors = M}};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, undefined) ->
    ok;
terminate(_Reason, #s{}) ->
    mria:clear_table(?tab),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

-spec handle_reg(#call_reg{}, #s{}) -> #s{}.
handle_reg(#call_reg{db = DB, shard = Shard, key = Key, pid = Pid}, S = #s{monitors = M0}) ->
    TableKey = make_key(DB, Shard, Key, node(), Pid),
    MRef = monitor(process, Pid),
    M = M0#{MRef => TableKey},
    ok = mria:dirty_write(?tab, #?tab{key = TableKey}),
    S#s{monitors = M}.

-spec handle_unreg(#call_unreg{}, #s{}) -> #s{}.
handle_unreg(#call_unreg{db = DB, shard = Shard, key = Key, pid = Pid}, S = #s{monitors = M0}) ->
    TableKey = make_key(DB, Shard, Key, node(), Pid),
    M =
        case emqx_utils_maps:find_key(TableKey, M0) of
            {ok, MRef} ->
                demonitor(MRef),
                maps:remove(TableKey, M0);
            undefined ->
                M0
        end,
    ok = mria:dirty_delete(?tab, TableKey),
    S#s{monitors = M}.

-spec make_key(emqx_ds:db(), emqx_ds:shard(), key(), node(), pid()) -> table_key().
make_key(DB, Shard, Key, Node, Pid) ->
    ?table_key(DB, Shard, Key, Node, Pid).

-spec select(emqx_ds:db(), emqx_ds:shard(), key()) -> [pid()].
select(DB, Shard, Key) ->
    Spec = {#?tab{key = ?table_key(DB, Shard, Key, '_', '$1')}, [], ['$1']},
    local_first(ets:select(?tab, [Spec])).

-spec select(emqx_ds:db(), key()) -> [{emqx_ds:shard(), pid()}].
select(DB, Key) ->
    Spec = {#?tab{key = ?table_key(DB, '$1', Key, '_', '$2')}, [], [{{'$1', '$2'}}]},
    ets:select(?tab, [Spec]).

-spec local_first([pid()]) -> [pid()].
local_first([_] = L) ->
    L;
local_first(Pids) ->
    case find_local(Pids, []) of
        {Local, Head, Tail} ->
            [Local | Head ++ Tail];
        undefined ->
            Pids
    end.

-spec find_local([pid()], [pid()]) -> {pid(), [pid()], [pid()]} | undefined.
find_local([], _) ->
    undefined;
find_local([Pid | Head], Tail) when node(Pid) =:= node() ->
    {Pid, Head, Tail};
find_local([Pid | Head], Tail) ->
    find_local(Head, [Pid | Tail]).
