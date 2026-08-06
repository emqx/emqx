%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Owner of the in-memory mapping-table caches.
%%
%% The on-disk JSON files (one per table, under `tables_dir/0') are the
%% source of truth; ETS is a read cache derived from them. Every table
%% lives in its own ETS table; reloads build a fresh ETS table and swap
%% it into the registry with a single insert, so readers see either the
%% old or the new version, never a partial one.
%%
%% Updates are replicated with `emqx_cluster_rpc' (see `load_file/1' and
%% `delete/1'): each node re-validates the content, writes the file
%% atomically (temp file + rename) and reloads its cache. Nodes that were
%% down during an update replay the transaction on rejoin. `reload/1' is
%% the per-node reconcile fallback that re-reads the local files.
-module(emqx_maptabs).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% API
-export([
    start_link/0,
    child_spec/0
]).

%% Hot-path lookups (called from rule SQL via emqx_maptabs_rule_funcs)
-export([
    lookup/2,
    lookup/3,
    lookup/4
]).

%% Management API (CLI)
-export([
    load_file/1,
    delete/1,
    reload_cluster/1,
    reload_local/1,
    list_local/0,
    read_table_file/1,
    tables_dir/0,
    health_check/0
]).

%% Config accessors
-export([
    max_tables/0,
    max_rows_per_table/0
]).

%% cluster_rpc entrypoints: replayed on every node (also on rejoin);
%% argument shapes must stay backward compatible. cluster_rpc appends
%% an opts map (`#{kind => ...}') to the declared argument list.
-export([
    do_load_v1/3,
    do_delete_v1/2,
    preflight_v1/1
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
-define(REGISTRY, emqx_maptabs_registry).
-define(TIMEOUT, 15000).
-define(DEFAULT_MAX_TABLES, 100).
-define(DEFAULT_MAX_ROWS_PER_TABLE, 10000).

-record(commit, {name :: binary(), parsed :: map()}).
-record(drop, {name :: binary()}).
-record(reload, {name :: all | binary()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []},
        type => worker,
        modules => [?MODULE],
        restart => permanent,
        shutdown => 5000
    }.

%%--------------------------------------------------------------------
%% Lookups
%%--------------------------------------------------------------------

%% Exact-term key matching, no type coercion: an integer key only
%% matches an integer, a string key only matches a string. Any miss,
%% unknown table, or in-flight table swap yields `undefined'.
-spec lookup(binary(), term()) -> map() | undefined.
lookup(Table, Key) when is_binary(Table) ->
    try ets:lookup(?REGISTRY, Table) of
        [{_, Tid, _Meta}] ->
            lookup_key(Tid, Key);
        [] ->
            undefined
    catch
        %% plugin not running
        error:badarg -> undefined
    end;
lookup(_Table, _Key) ->
    undefined.

-spec lookup(binary(), term(), binary()) -> term() | undefined.
lookup(Table, Key, Field) ->
    case lookup(Table, Key) of
        Values when is_map(Values) -> maps:get(Field, Values, undefined);
        _ -> undefined
    end.

-spec lookup(binary(), term(), binary(), term()) -> term().
lookup(Table, Key, Field, Default) ->
    case lookup(Table, Key, Field) of
        undefined -> Default;
        Value -> Value
    end.

lookup_key(Tid, Key) ->
    try ets:lookup(Tid, Key) of
        [{_, Values}] ->
            Values;
        [] ->
            undefined
    catch
        %% table deleted by a concurrent swap; the registry already
        %% points at the new version, treat as a transient miss
        error:badarg -> undefined
    end.

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

%% Loads a table file from a local path and replicates it to all nodes.
%% The content is validated and all nodes are preflight-checked (plugin
%% running, tables dir writable) before the cluster_rpc transaction, so
%% a broken node fails the request fast instead of lagging forever.
-spec load_file(file:filename_all()) -> ok | {error, term()}.
load_file(Path) ->
    maybe
        {ok, Name} ?= emqx_maptabs_loader:table_name_from_path(Path),
        {ok, Bin} ?= read_source_file(Path),
        {ok, _Parsed} ?= emqx_maptabs_loader:parse(Bin),
        ok ?= preflight_cluster(Name),
        multicall(do_load_v1, [Name, Bin])
    end.

%% Deletes a table (file + cache) on all nodes.
-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        ok ?= preflight_cluster(Name),
        multicall(do_delete_v1, [Name])
    end.

%% Re-reads table files from the local disk of every running node.
%% This is the reconcile fallback (e.g. after the cluster_rpc log was
%% trimmed before a node caught up, or files were copied by hand).
-spec reload_cluster(all | binary()) -> [{node(), term()}].
reload_cluster(Name) ->
    [{Node, rpc_reload(Node, Name)} || Node <- emqx:running_nodes()].

-spec reload_local(all | binary()) -> term().
reload_local(Name) ->
    try
        gen_server:call(?SERVER, #reload{name = Name}, ?TIMEOUT)
    catch
        exit:{noproc, _} -> {error, plugin_not_running}
    end.

%% Metadata of the tables cached on this node.
-spec list_local() -> [map()].
list_local() ->
    try ets:tab2list(?REGISTRY) of
        Entries ->
            lists:sort([Meta#{name => Name} || {Name, _Tid, Meta} <- Entries])
    catch
        error:badarg -> []
    end.

-spec read_table_file(binary()) -> {ok, binary()} | {error, term()}.
read_table_file(Name) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        read_source_file(table_file_path(Name))
    end.

-spec tables_dir() -> file:filename().
tables_dir() ->
    filename:join([emqx:data_dir(), "plugins", "emqx_maptabs", "tables"]).

-spec health_check() -> ok | {error, binary()}.
health_check() ->
    case whereis(?SERVER) of
        undefined -> {error, <<"Plugin is not running">>};
        Pid when is_pid(Pid) -> ok
    end.

%%--------------------------------------------------------------------
%% Config
%%--------------------------------------------------------------------

%% Limits are read fresh on every load/reload, so a config update takes
%% effect on the next load; already-loaded tables are never dropped by
%% a limit change.
-spec max_tables() -> pos_integer().
max_tables() ->
    config_pos_int(<<"max_tables">>, ?DEFAULT_MAX_TABLES).

-spec max_rows_per_table() -> pos_integer().
max_rows_per_table() ->
    config_pos_int(<<"max_rows_per_table">>, ?DEFAULT_MAX_ROWS_PER_TABLE).

config_pos_int(Key, Default) ->
    try emqx_plugins:get_config(name_vsn(), #{}) of
        Conf when is_map(Conf) ->
            case maps:get(Key, Conf, Default) of
                I when is_integer(I), I > 0 -> I;
                _ -> Default
            end;
        _ ->
            Default
    catch
        %% plugin app not (yet) fully set up, e.g. during cluster_rpc replay
        _:_ -> Default
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_maptabs, vsn),
    iolist_to_binary([<<"emqx_maptabs-">>, Vsn]).

%%--------------------------------------------------------------------
%% cluster_rpc entrypoints
%%--------------------------------------------------------------------

%% Runs on every node (including replay on rejoin): re-validate, write
%% the file atomically, reload the cache. If the plugin app is stopped
%% on this node the file is still written; the cache catches up from
%% disk on the next plugin start.
-spec do_load_v1(binary(), binary(), emqx_config:cluster_rpc_opts()) -> ok | {error, term()}.
do_load_v1(Name, Bin, _ClusterRpcOpts) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        {ok, Parsed} ?= emqx_maptabs_loader:parse(Bin),
        ok ?= check_row_limit(Parsed),
        ok ?= check_table_limit_on_disk(Name),
        ok ?= write_table_file(Name, Bin),
        call(#commit{name = Name, parsed = Parsed})
    end.

-spec do_delete_v1(binary(), emqx_config:cluster_rpc_opts()) -> ok | {error, term()}.
do_delete_v1(Name, _ClusterRpcOpts) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        ok ?= delete_table_file(Name),
        call(#drop{name = Name})
    end.

%% Fails fast on nodes where a replicated update could not be applied.
-spec preflight_v1(binary()) -> ok | {error, term()}.
preflight_v1(_Name) ->
    maybe
        ok ?= health_check(),
        check_dir_writable()
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?REGISTRY, [named_table, set, protected, {read_concurrency, true}]),
    ok = load_all_from_disk(),
    {ok, #{}}.

handle_call(#commit{name = Name, parsed = Parsed}, _From, State) ->
    {reply, commit(Name, Parsed), State};
handle_call(#drop{name = Name}, _From, State) ->
    {reply, drop(Name), State};
handle_call(#reload{name = all}, _From, State) ->
    {reply, reload_all(), State};
handle_call(#reload{name = Name}, _From, State) ->
    {reply, reload_one(Name), State};
handle_call(Request, From, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_call", request => Request, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?SLOG(error, #{msg => "maptabs_unexpected_cast", request => Request}),
    {noreply, State}.

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

call(Request) ->
    try
        gen_server:call(?SERVER, Request, ?TIMEOUT)
    catch
        %% plugin app stopped on this node: for replicated updates the
        %% file is already on disk and the cache is rebuilt from it on
        %% the next plugin start
        exit:{noproc, _} -> ok
    end.

multicall(Fun, Args) ->
    case emqx_cluster_rpc:multicall(?MODULE, Fun, Args) of
        ok -> ok;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

preflight_cluster(Name) ->
    Errors = lists:filtermap(
        fun(Node) ->
            case rpc_preflight(Node, Name) of
                ok -> false;
                {error, Reason} -> {true, {Node, Reason}}
            end
        end,
        emqx:running_nodes()
    ),
    case Errors of
        [] -> ok;
        _ -> {error, #{reason => preflight_failed, errors => Errors}}
    end.

rpc_preflight(Node, Name) ->
    try
        erpc:call(Node, ?MODULE, preflight_v1, [Name], ?TIMEOUT)
    catch
        Class:Reason ->
            {error, {Class, Reason}}
    end.

rpc_reload(Node, Name) ->
    try
        erpc:call(Node, ?MODULE, reload_local, [Name], ?TIMEOUT)
    catch
        Class:Reason ->
            {error, {Class, Reason}}
    end.

check_dir_writable() ->
    Dir = tables_dir(),
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

read_source_file(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            {ok, Bin};
        {error, Reason} ->
            {error, #{
                reason => failed_to_read_file,
                path => unicode:characters_to_binary(Path),
                detail => Reason
            }}
    end.

write_table_file(Name, Bin) ->
    Path = table_file_path(Name),
    Tmp = Path ++ ".tmp",
    maybe
        ok ?= ensure_dir(tables_dir()),
        ok ?= file:write_file(Tmp, Bin),
        ok ?= file:rename(Tmp, Path)
    else
        {error, Reason} ->
            {error, #{
                reason => failed_to_write_table_file,
                path => unicode:characters_to_binary(Path),
                detail => Reason
            }}
    end.

delete_table_file(Name) ->
    Path = table_file_path(Name),
    case file:delete(Path) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            {error, #{
                reason => failed_to_delete_table_file,
                path => unicode:characters_to_binary(Path),
                detail => Reason
            }}
    end.

table_file_path(Name) ->
    filename:join(tables_dir(), binary_to_list(Name) ++ ".json").

check_row_limit(#{row_count := RowCount}) ->
    Max = max_rows_per_table(),
    case RowCount =< Max of
        true ->
            ok;
        false ->
            {error, #{reason => too_many_rows, row_count => RowCount, max_rows_per_table => Max}}
    end.

%% Replacing an existing table is always allowed; only a new table
%% counts against the limit. The on-disk files are the count base so
%% the check also works while the plugin app is stopped (replay).
check_table_limit_on_disk(Name) ->
    case filelib:is_regular(table_file_path(Name)) of
        true ->
            ok;
        false ->
            check_table_count(length(table_files()))
    end.

check_table_count(Count) ->
    Max = max_tables(),
    case Count < Max of
        true -> ok;
        false -> {error, #{reason => too_many_tables, table_count => Count, max_tables => Max}}
    end.

table_files() ->
    lists:sort(filelib:wildcard(filename:join(tables_dir(), "*.json"))).

%%--------------------------------------------------------------------
%% Server-side cache operations
%%--------------------------------------------------------------------

load_all_from_disk() ->
    _ = reload_all(),
    ok.

load_file_into_cache(File) ->
    Result =
        maybe
            {ok, Name} ?= emqx_maptabs_loader:table_name_from_path(File),
            {ok, Bin} ?= read_source_file(File),
            {ok, Parsed} ?= emqx_maptabs_loader:parse(Bin),
            ok ?= check_row_limit(Parsed),
            commit(Name, Parsed)
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

commit(Name, #{rows := Rows, row_count := RowCount, version := Version}) ->
    OldTid =
        case ets:lookup(?REGISTRY, Name) of
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
    true = ets:insert(?REGISTRY, {Name, NewTid, Meta}),
    ok = delete_old_tid(OldTid),
    ?SLOG(info, #{
        msg => "maptab_loaded",
        name => Name,
        row_count => RowCount,
        version => Version
    }),
    ok.

drop(Name) ->
    case ets:lookup(?REGISTRY, Name) of
        [{_, OldTid, _}] ->
            true = ets:delete(?REGISTRY, Name),
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

reload_all() ->
    OnDisk = table_files(),
    Pairs0 = [{F, N} || F <- OnDisk, {ok, N} <- [emqx_maptabs_loader:table_name_from_path(F)]],
    %% hand-copied files beyond the table limit are not loaded
    {Pairs, Beyond} = split_at_table_limit(Pairs0),
    Beyond =/= [] andalso
        ?SLOG(warning, #{
            msg => "maptab_files_beyond_table_limit",
            max_tables => max_tables(),
            skipped => [N || {_, N} <- Beyond]
        }),
    OnDiskNames = [N || {_, N} <- Pairs],
    %% drop cached tables whose file is gone, then (re)load the rest
    Cached = [Name || {Name, _, _} <- ets:tab2list(?REGISTRY)],
    lists:foreach(
        fun(Name) -> drop(Name) end,
        Cached -- OnDiskNames
    ),
    [{Name, load_file_into_cache(File)} || {File, Name} <- Pairs].

split_at_table_limit(Pairs) ->
    Max = max_tables(),
    case length(Pairs) > Max of
        true -> lists:split(Max, Pairs);
        false -> {Pairs, []}
    end.

reload_one(Name) ->
    case {filelib:is_regular(table_file_path(Name)), ets:member(?REGISTRY, Name)} of
        {true, true} ->
            load_file_into_cache(table_file_path(Name));
        {true, false} ->
            %% new to the cache: counts against the table limit
            maybe
                ok ?= check_table_count(ets:info(?REGISTRY, size)),
                load_file_into_cache(table_file_path(Name))
            end;
        {false, _} ->
            %% file gone: the cache follows the disk
            drop(Name)
    end.
