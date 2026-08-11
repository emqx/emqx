%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Owner of the in-memory mapping-table caches.
%%
%% Every table lives in its own ETS table; reloads build a fresh ETS
%% table and swap it into the registry with a single insert, so readers
%% see either the old or the new version, never a partial one. All
%% cache mutations serialize through this gen_server; the read path
%% (emqx_maptabs:lookup) never calls it.
-module(emqx_maptabs_server).

-behaviour(gen_server).

-include("emqx_maptabs.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    health_check/0
]).

%% Cache operations (local node)
-export([
    commit/2,
    drop/1
]).

%% Cache operations (any node, including the local one)
-export([
    reload/2,
    list_tables/1,
    read_table_file/2,
    preflight/1
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
-define(SYNC_RETRY_DELAY, 10000).
-define(SYNC_ATTEMPTS, 6).

-record(commit, {name :: binary(), parsed :: map()}).
-record(drop, {name :: binary()}).
-record(reload, {name :: all | binary()}).
-record(list_tables, {}).
-record(read_table_file, {name :: binary()}).
-record(adopt, {name :: binary(), bin :: binary()}).
-record(preflight, {}).
-record(sync_from_peers, {attempts_left :: non_neg_integer()}).
-record(sync_result, {result :: ok | no_peer, attempts_left :: non_neg_integer()}).

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

%% Swaps a parsed table into the local cache. When the plugin app is
%% stopped on this node the call degrades to `ok': the table file is
%% already on disk and the cache is rebuilt from it on the next start.
-spec commit(binary(), map()) -> ok | {error, term()}.
commit(Name, Parsed) ->
    call_replicated(#commit{name = Name, parsed = Parsed}).

-spec drop(binary()) -> ok | {error, term()}.
drop(Name) ->
    call_replicated(#drop{name = Name}).

%% Re-reads table files from the target node's local disk.
-spec reload(node(), all | binary()) -> term().
reload(Node, Name) ->
    call_node(Node, #reload{name = Name}).

-spec list_tables(node()) -> [map()] | {error, term()}.
list_tables(Node) ->
    call_node(Node, #list_tables{}).

-spec read_table_file(node(), binary()) -> {ok, binary()} | {error, term()}.
read_table_file(Node, Name) ->
    call_node(Node, #read_table_file{name = Name}).

%% Checks that the plugin is running with a writable tables dir on the
%% target node, so a replicated update is known to be applicable there.
-spec preflight(node()) -> ok | {error, term()}.
preflight(Node) ->
    call_node(Node, #preflight{}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?MAPTABS_REGISTRY, [named_table, set, protected, {read_concurrency, true}]),
    _ = do_reload_all(),
    %% deferred: a booting node must not block its own plugin start on
    %% peers (the plugin manager runs app start under a short timeout)
    self() ! #sync_from_peers{attempts_left = ?SYNC_ATTEMPTS},
    {ok, #{}}.

handle_call(#commit{name = Name, parsed = Parsed}, _From, State) ->
    {reply, do_commit(Name, Parsed), State};
handle_call(#drop{name = Name}, _From, State) ->
    {reply, do_drop(Name), State};
handle_call(#reload{name = all}, _From, State) ->
    {reply, do_reload_all(), State};
handle_call(#reload{name = Name}, _From, State) ->
    {reply, do_reload_one(Name), State};
handle_call(#list_tables{}, _From, State) ->
    {reply, emqx_maptabs:list_local(), State};
handle_call(#read_table_file{name = Name}, _From, State) ->
    {reply, emqx_maptabs:read_table_file(Name), State};
handle_call(#adopt{name = Name, bin = Bin}, _From, State) ->
    {reply, do_adopt(Name, Bin), State};
handle_call(#preflight{}, _From, State) ->
    {reply, check_dir_writable(), State};
handle_call(Request, From, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_call", request => Request, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_cast", request => Request}),
    {noreply, State}.

handle_info(#sync_from_peers{attempts_left = Left}, State) ->
    %% run in a worker: two servers syncing from each other at the same
    %% time must not block in calls to one another
    Server = self(),
    _ = spawn(fun() ->
        Result =
            try
                do_sync_from_peers()
            catch
                _:_ -> ok
            end,
        Server ! #sync_result{result = Result, attempts_left = Left}
    end),
    {noreply, State};
handle_info(#sync_result{result = ok}, State) ->
    {noreply, State};
handle_info(#sync_result{result = no_peer, attempts_left = Left}, State) when Left > 1 ->
    %% peers (or their plugin) may still be booting
    _ = erlang:send_after(?SYNC_RETRY_DELAY, self(), #sync_from_peers{attempts_left = Left - 1}),
    {noreply, State};
handle_info(#sync_result{result = no_peer}, State) ->
    ?SLOG(info, #{msg => "maptabs_peer_sync_skipped", reason => no_usable_peer}),
    {noreply, State};
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

call_replicated(Request) ->
    try
        gen_server:call(?SERVER, Request, ?TIMEOUT)
    catch
        %% plugin app stopped on this node: for replicated updates the
        %% file is already on disk and the cache is rebuilt from it on
        %% the next plugin start
        exit:{noproc, _} -> ok
    end.

call_node(Node, Request) ->
    try
        gen_server:call({?SERVER, Node}, Request, ?TIMEOUT)
    catch
        exit:{noproc, _} -> {error, plugin_not_running};
        exit:{{nodedown, N}, _} -> {error, {nodedown, N}};
        exit:{timeout, _} -> {error, timeout};
        exit:Reason -> {error, Reason}
    end.

check_dir_writable() ->
    Dir = emqx_maptabs:tables_dir(),
    Probe = filename:join(Dir, ".write_probe"),
    maybe
        ok ?= ensure_dir(Dir),
        ok ?= file:write_file(Probe, <<>>),
        _ = file:delete(Probe),
        ok
    else
        {error, Reason} ->
            {error, #{reason => tables_dir_not_writable, dir => Dir, detail => Reason}}
    end.

ensure_dir(Dir) ->
    case filelib:ensure_path(Dir) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Cache operations (run inside the server process)
%%--------------------------------------------------------------------

load_file_into_cache(File) ->
    Result =
        maybe
            {ok, Name} ?= emqx_maptabs_loader:table_name_from_path(File),
            {ok, Bin} ?= emqx_maptabs:read_source_file(File),
            {ok, Parsed} ?= emqx_maptabs_loader:parse(Bin),
            ok ?= emqx_maptabs:check_row_limit(Parsed),
            do_commit(Name, Parsed)
        end,
    case Result of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "maptab_file_rejected",
                file => unicode:characters_to_binary(File),
                reason => Reason
            }),
            {error, Reason}
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

do_adopt(Name, Bin) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        {ok, Parsed} ?= emqx_maptabs_loader:parse(Bin),
        ok ?= emqx_maptabs:write_table_file(Name, Bin),
        do_commit(Name, Parsed)
    end.

%%--------------------------------------------------------------------
%% Peer sync (runs in a short-lived worker process)
%%--------------------------------------------------------------------

%% Pulls tables this node is missing (or holds a different version of)
%% from the first running peer whose plugin answers. A rejoining node
%% may have missed replicated updates: the boot-time cluster-config
%% sync commits past unapplied cluster_rpc entries, so the transaction
%% log is not replayed for this plugin; the peers hold the cluster
%% state and win on any version mismatch. Local tables absent on the
%% peer are kept: a missed delete needs `maptabs delete' again, and
%% `maptabs status' surfaces such drift.
do_sync_from_peers() ->
    Peers = lists:sort(emqx:running_nodes() -- [node()]),
    case first_usable_peer(Peers) of
        {ok, Peer, PeerTables} ->
            sync_tables_from_peer(Peer, PeerTables);
        undefined when Peers =:= [] ->
            ok;
        undefined ->
            no_peer
    end.

first_usable_peer([]) ->
    undefined;
first_usable_peer([Peer | Rest]) ->
    case list_tables(Peer) of
        Tables when is_list(Tables) -> {ok, Peer, Tables};
        {error, _} -> first_usable_peer(Rest)
    end.

sync_tables_from_peer(Peer, PeerTables) ->
    Local = maps:from_list([
        {maps:get(name, T), maps:get(version, T)}
     || T <- emqx_maptabs:list_local()
    ]),
    Results = lists:filtermap(
        fun(T) ->
            Name = maps:get(name, T),
            case maps:get(Name, Local, undefined) =:= maps:get(version, T) of
                true -> false;
                false -> {true, {Name, sync_one_table(Peer, Name)}}
            end
        end,
        PeerTables
    ),
    Results =/= [] andalso
        ?SLOG(info, #{msg => "maptabs_synced_from_peer", peer => Peer, results => Results}),
    ok.

sync_one_table(Peer, Name) ->
    maybe
        {ok, Bin} ?= read_table_file(Peer, Name),
        call_local(#adopt{name = Name, bin = Bin})
    end.

%% adoption is serialized through the server so a concurrently
%% replicated update cannot interleave with the file write + cache swap
call_local(Request) ->
    try
        gen_server:call(?SERVER, Request, ?TIMEOUT)
    catch
        exit:{noproc, _} -> {error, plugin_not_running};
        exit:{timeout, _} -> {error, timeout}
    end.

do_reload_all() ->
    OnDisk = emqx_maptabs:table_files(),
    Pairs0 = [{F, N} || F <- OnDisk, {ok, N} <- [emqx_maptabs_loader:table_name_from_path(F)]],
    %% hand-copied files beyond the table limit are not loaded
    {Pairs, Beyond} = split_at_table_limit(Pairs0),
    Beyond =/= [] andalso
        ?SLOG(warning, #{
            msg => "maptab_files_beyond_table_limit",
            max_tables => emqx_maptabs:max_tables(),
            skipped => [N || {_, N} <- Beyond]
        }),
    OnDiskNames = [N || {_, N} <- Pairs],
    %% drop cached tables whose file is gone, then (re)load the rest
    Cached = [Name || {Name, _, _} <- ets:tab2list(?MAPTABS_REGISTRY)],
    lists:foreach(
        fun(Name) -> do_drop(Name) end,
        Cached -- OnDiskNames
    ),
    [{Name, load_file_into_cache(File)} || {File, Name} <- Pairs].

split_at_table_limit(Pairs) ->
    Max = emqx_maptabs:max_tables(),
    case length(Pairs) > Max of
        true -> lists:split(Max, Pairs);
        false -> {Pairs, []}
    end.

do_reload_one(Name) ->
    Path = emqx_maptabs:table_file_path(Name),
    case {filelib:is_regular(Path), ets:member(?MAPTABS_REGISTRY, Name)} of
        {true, true} ->
            load_file_into_cache(Path);
        {true, false} ->
            %% new to the cache: counts against the table limit
            maybe
                ok ?= emqx_maptabs:check_table_count(ets:info(?MAPTABS_REGISTRY, size)),
                load_file_into_cache(Path)
            end;
        {false, _} ->
            %% file gone: the cache follows the disk
            do_drop(Name)
    end.
