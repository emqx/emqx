%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_server).

-moduledoc """
Owner of the in-memory mapping-table caches.

Every table lives in its own ETS table; a cache rebuild creates a
fresh ETS table and swaps it into the registry with a single insert,
so readers see either the old or the new version, never a partial
one. All cache mutations serialize through this gen_server; the read
path (`emqx_maptabs:lookup`) never calls it.

The caches follow the replicated storage (`emqx_maptabs_store`): at
start the server rebuilds them from storage, and afterwards it
reconciles on every mnesia table event (debounced, so a burst of
replicated updates triggers one rebuild pass).
""".

-behaviour(gen_server).

-include("emqx_maptabs.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    health_check/0
]).

%% Cache operations (any node, including the local one)
-export([
    reconcile/1,
    list_tables/1
]).

%% Parallel cache operations on several nodes, bounded by one deadline
-export([
    confirm_on_nodes/4,
    reconcile_on_nodes/2,
    list_tables_on_nodes/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 15000).
-define(RECONCILE_DEBOUNCE, 100).

-record(reconcile, {}).
-record(list_tables, {}).
-record(do_reconcile, {}).
%% `expect' is the content version the cache must hold, or `absent'
-record(confirm, {name, expect, timeout}).
-record(confirm_expired, {}).

-type expect() :: binary() | absent.
-type deadline() :: integer().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec health_check() -> ok | {error, binary()}.
health_check() ->
    case whereis(?SERVER) of
        undefined -> {error, <<"Plugin is not running">>};
        Pid when is_pid(Pid) -> ok
    end.

%% Rebuilds the target node's cache from storage (synchronous).
-spec reconcile(node()) -> ok | {error, term()}.
reconcile(Node) ->
    call_node(Node, #reconcile{}).

-spec list_tables(node()) -> [map()] | {error, term()}.
list_tables(Node) ->
    call_node(Node, #list_tables{}).

-doc """
Asks every node to reconcile its cache and to reply `ok` once the
cache of `Name` holds `Expect`. A node whose storage replica has not
received the write yet keeps the call open and re-checks after each
reconcile, until shortly before `Deadline` (`erlang:monotonic_time`
in milliseconds).
""".
-spec confirm_on_nodes([node()], binary(), expect(), deadline()) ->
    [{node(), ok | {error, term()}}].
confirm_on_nodes(Nodes, Name, Expect, Deadline) ->
    multicall(
        Nodes,
        fun(Remaining) ->
            %% leave time for the node's answer to come back before the
            %% caller gives up, so a lagging node reports what it holds
            #confirm{name = Name, expect = Expect, timeout = Remaining * 3 div 4}
        end,
        Deadline
    ).

-spec reconcile_on_nodes([node()], deadline()) -> [{node(), ok | {error, term()}}].
reconcile_on_nodes(Nodes, Deadline) ->
    multicall(Nodes, fun(_) -> #reconcile{} end, Deadline).

-spec list_tables_on_nodes([node()], deadline()) -> [{node(), [map()] | {error, term()}}].
list_tables_on_nodes(Nodes, Deadline) ->
    multicall(Nodes, fun(_) -> #list_tables{} end, Deadline).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_maptabs_store:create_tables(),
    _ = ets:new(?MAPTABS_REGISTRY, [named_table, set, protected, {read_concurrency, true}]),
    %% every storage write and delete touches the index table
    {ok, _} = mnesia:subscribe({table, ?MAPTABS_INDEX_TAB, simple}),
    ok = do_reconcile(),
    {ok, #{reconcile_timer => undefined, waiters => #{}}}.

handle_call(#reconcile{}, _From, State) ->
    {reply, ok, reconcile_now(cancel_debounce(State))};
handle_call(#confirm{name = Name, expect = Expect, timeout = Timeout}, From, State0) ->
    State = reconcile_now(cancel_debounce(State0)),
    case cached_version(Name) of
        Expect ->
            {reply, ok, State};
        _ ->
            %% the write has not reached this node's storage replica
            %% yet; answer when a later reconcile catches up, or on
            %% expiry
            TRef = erlang:start_timer(max(Timeout, 0), self(), #confirm_expired{}),
            #{waiters := Waiters} = State,
            {noreply, State#{waiters := Waiters#{TRef => {From, Name, Expect}}}}
    end;
handle_call(#list_tables{}, _From, State) ->
    {reply, emqx_maptabs:list_local(), State};
handle_call(Request, From, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_call", request => Request, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_cast", request => Request}),
    {noreply, State}.

%% Storage changed (locally initiated or replicated from another
%% node): schedule a debounced reconcile.
handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, ensure_debounce(State)};
handle_info(#do_reconcile{}, State) ->
    {noreply, reconcile_now(State#{reconcile_timer := undefined})};
handle_info({timeout, TRef, #confirm_expired{}}, #{waiters := Waiters} = State) ->
    case maps:take(TRef, Waiters) of
        {{From, Name, Expect}, Waiters1} ->
            gen_server:reply(
                From,
                {error, #{
                    reason => not_confirmed, expected => Expect, cached => cached_version(Name)
                }}
            ),
            {noreply, State#{waiters := Waiters1}};
        error ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    %% the registry and all data tables are owned by this process and
    %% are deleted with it
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

call_node(Node, Request) ->
    try
        gen_server:call({?SERVER, Node}, Request, ?TIMEOUT)
    catch
        exit:{noproc, _} -> {error, plugin_not_running};
        exit:{{nodedown, N}, _} -> {error, {nodedown, N}};
        exit:{timeout, _} -> {error, timeout};
        exit:Reason -> {error, Reason}
    end.

%% Sends `MkRequest(RemainingMs)' to the server on every node at once
%% and collects the replies until `Deadline'. A node that has not
%% answered by then is reported as `{error, timeout}'.
multicall(Nodes, MkRequest, Deadline) ->
    Request = MkRequest(max(Deadline - now_ms(), 0)),
    ReqIds = lists:foldl(
        fun(Node, Acc) ->
            gen_server:send_request({?SERVER, Node}, Request, Node, Acc)
        end,
        gen_server:reqids_new(),
        Nodes
    ),
    Replies = collect(ReqIds, Deadline, #{}),
    [{Node, maps:get(Node, Replies)} || Node <- Nodes].

collect(ReqIds, Deadline, Acc) ->
    case gen_server:receive_response(ReqIds, {abs, Deadline}, true) of
        no_request ->
            Acc;
        timeout ->
            lists:foldl(
                fun({_ReqId, Node}, Acc1) -> Acc1#{Node => {error, timeout}} end,
                Acc,
                gen_server:reqids_to_list(ReqIds)
            );
        {{reply, Reply}, Node, ReqIds1} ->
            collect(ReqIds1, Deadline, Acc#{Node => Reply});
        {{error, {Reason, _ServerRef}}, Node, ReqIds1} ->
            collect(ReqIds1, Deadline, Acc#{Node => {error, call_error(Reason)}})
    end.

call_error(noproc) -> plugin_not_running;
call_error(noconnection) -> nodedown;
call_error(Reason) -> Reason.

now_ms() ->
    erlang:monotonic_time(millisecond).

ensure_debounce(#{reconcile_timer := undefined} = State) ->
    Ref = erlang:send_after(?RECONCILE_DEBOUNCE, self(), #do_reconcile{}),
    State#{reconcile_timer := Ref};
ensure_debounce(State) ->
    State.

cancel_debounce(#{reconcile_timer := undefined} = State) ->
    State;
cancel_debounce(#{reconcile_timer := Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    %% a #do_reconcile{} may already be in the mailbox; reconciling
    %% twice is a cheap no-op
    State#{reconcile_timer := undefined}.

%%--------------------------------------------------------------------
%% Cache operations (run inside the server process)
%%--------------------------------------------------------------------

%% Reconciles, then answers every waiting confirm the cache now satisfies.
reconcile_now(#{waiters := Waiters} = State) ->
    ok = do_reconcile(),
    Done = maps:filter(
        fun(_TRef, {_From, Name, Expect}) -> cached_version(Name) =:= Expect end,
        Waiters
    ),
    ok = maps:foreach(
        fun(TRef, {From, _Name, _Expect}) ->
            _ = erlang:cancel_timer(TRef),
            gen_server:reply(From, ok)
        end,
        Done
    ),
    State#{waiters := maps:without(maps:keys(Done), Waiters)}.

cached_version(Name) ->
    case ets:lookup(?MAPTABS_REGISTRY, Name) of
        [{_, _Tid, #{version := Version}}] -> Version;
        [] -> absent
    end.

%% Diffs the cache against storage by content version: (re)builds the
%% cache of every stored table whose version differs, drops cached
%% tables that are no longer stored.
do_reconcile() ->
    Stored = emqx_maptabs_store:versions(),
    Cached = maps:from_list(
        [{Name, maps:get(version, Meta)} || {Name, _Tid, Meta} <- ets:tab2list(?MAPTABS_REGISTRY)]
    ),
    ok = maps:foreach(
        fun(Name, _Version) ->
            is_map_key(Name, Stored) orelse do_drop(Name)
        end,
        Cached
    ),
    ok = maps:foreach(
        fun(Name, Version) ->
            case maps:get(Name, Cached, undefined) of
                Version -> ok;
                _ -> load_into_cache(Name)
            end
        end,
        Stored
    ).

load_into_cache(Name) ->
    Result =
        maybe
            {ok, #maptab{json = Json}} ?= emqx_maptabs_store:get(Name),
            {ok, Parsed} ?= emqx_maptabs_loader:parse(Json),
            do_commit(Name, Parsed)
        end,
    case Result of
        ok ->
            ok;
        {error, Reason} ->
            %% storage content is validated before it is written; this
            %% is either a concurrent delete or corruption
            ?SLOG(warning, #{msg => "maptab_cache_rebuild_failed", name => Name, reason => Reason})
    end.

do_commit(Name, #{rows := Rows, row_count := RowCount, version := Version}) ->
    OldTid =
        case ets:lookup(?MAPTABS_REGISTRY, Name) of
            [{_, Tid0, _}] -> Tid0;
            [] -> undefined
        end,
    NewTid = ets:new(emqx_maptabs_table, [set, protected, {read_concurrency, true}]),
    true = ets:insert(NewTid, Rows),
    Meta = #{
        row_count => RowCount,
        version => Version,
        loaded_at => erlang:system_time(second)
    },
    %% single insert = atomic swap for readers
    true = ets:insert(?MAPTABS_REGISTRY, {Name, NewTid, Meta}),
    ok = delete_old_tid(OldTid),
    ?SLOG(info, #{
        msg => "maptab_loaded",
        name => Name,
        row_count => RowCount,
        version => Version
    }),
    ok.

do_drop(Name) ->
    case ets:lookup(?MAPTABS_REGISTRY, Name) of
        [{_, OldTid, _}] ->
            true = ets:delete(?MAPTABS_REGISTRY, Name),
            ok = delete_old_tid(OldTid),
            ?SLOG(info, #{msg => "maptab_deleted", name => Name});
        [] ->
            ok
    end,
    ok.

delete_old_tid(undefined) ->
    ok;
delete_old_tid(Tid) ->
    true = ets:delete(Tid),
    ok.
