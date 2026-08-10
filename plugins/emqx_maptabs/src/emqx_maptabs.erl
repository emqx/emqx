%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Public API of the emqx_maptabs plugin.
%%
%% The on-disk JSON files (one per table, under `tables_dir/0') are the
%% source of truth; ETS is a read cache derived from them, owned by
%% emqx_maptabs_server. Lookups read the cache directly and never call
%% the server.
%%
%% Updates are replicated with `emqx_cluster_rpc' (see `load_file/1' and
%% `delete/1'): each node re-validates the content, writes the file
%% atomically (temp file + rename) and reloads its cache. Nodes that were
%% down during an update replay the transaction on rejoin. `reload_cluster/1'
%% is the per-node reconcile fallback that re-reads the local files.
-module(emqx_maptabs).

-include("emqx_maptabs.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("kernel/include/file.hrl").

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
    list_on_node/1,
    read_table_file/1,
    tables_dir/0,
    health_check/0
]).

%% Config accessors
-export([
    max_tables/0,
    max_rows_per_table/0,
    max_table_file_bytes/0
]).

%% cluster_rpc entrypoints: replayed on every node (also on rejoin);
%% argument shapes must stay backward compatible. cluster_rpc appends
%% an opts map (`#{kind => ...}') to the declared argument list.
-export([
    do_load_v1/3,
    do_delete_v1/2
]).

%% Internal exports for emqx_maptabs_server
-export([
    read_source_file/1,
    table_file_path/1,
    table_files/0,
    check_row_limit/1,
    check_table_count/1
]).

-define(DEFAULT_MAX_TABLES, 100).
-define(DEFAULT_MAX_ROWS_PER_TABLE, 10000).
-define(DEFAULT_MAX_TABLE_FILE_BYTES, 10000000).

%%--------------------------------------------------------------------
%% Lookups
%%--------------------------------------------------------------------

%% Exact-term key matching, no type coercion: an integer key only
%% matches an integer, a string key only matches a string. Any miss,
%% unknown table, or in-flight table swap yields `undefined'.
-spec lookup(binary(), term()) -> map() | undefined.
lookup(Table, Key) when is_binary(Table) ->
    try ets:lookup(?MAPTABS_REGISTRY, Table) of
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
        %% size is checked before reading: a mistakenly huge file must
        %% not be pulled into memory at all
        ok ?= check_source_file_size(Path),
        {ok, Bin} ?= read_source_file(Path),
        {ok, _Parsed} ?= emqx_maptabs_loader:parse(Bin),
        ok ?= preflight_cluster(),
        multicall(do_load_v1, [Name, Bin])
    end.

%% Deletes a table (file + cache) on all nodes.
-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        ok ?= preflight_cluster(),
        multicall(do_delete_v1, [Name])
    end.

%% Re-reads table files from the local disk of every running node.
%% This is the reconcile fallback (e.g. after the cluster_rpc log was
%% trimmed before a node caught up, or files were copied by hand).
-spec reload_cluster(all | binary()) -> [{node(), term()}].
reload_cluster(Name) ->
    [{Node, emqx_maptabs_server:reload(Node, Name)} || Node <- emqx:running_nodes()].

-spec reload_local(all | binary()) -> term().
reload_local(Name) ->
    emqx_maptabs_server:reload(node(), Name).

%% Metadata of the tables cached on this node.
-spec list_local() -> [map()].
list_local() ->
    try ets:tab2list(?MAPTABS_REGISTRY) of
        Entries ->
            lists:sort([Meta#{name => Name} || {Name, _Tid, Meta} <- Entries])
    catch
        error:badarg -> []
    end.

-spec list_on_node(node()) -> [map()] | {error, term()}.
list_on_node(Node) when Node =:= node() ->
    list_local();
list_on_node(Node) ->
    emqx_maptabs_server:list_tables(Node).

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
    emqx_maptabs_server:health_check().

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

-spec max_table_file_bytes() -> pos_integer().
max_table_file_bytes() ->
    config_pos_int(<<"max_table_file_bytes">>, ?DEFAULT_MAX_TABLE_FILE_BYTES).

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
do_load_v1(Name, Bin, ClusterRpcOpts) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        {ok, Parsed} ?= emqx_maptabs_loader:parse(Bin),
        ok ?= check_limits(Name, Bin, Parsed, ClusterRpcOpts),
        ok ?= write_table_file(Name, Bin),
        emqx_maptabs_server:commit(Name, Parsed)
    end.

%% Limits guard the initiating node only: replicating (and replaying)
%% nodes must apply an already-committed transaction even if the limits
%% were lowered in the meantime.
check_limits(Name, Bin, Parsed, #{kind := ?KIND_INITIATE}) ->
    maybe
        ok ?= check_file_size(byte_size(Bin)),
        ok ?= check_row_limit(Parsed),
        check_table_limit_on_disk(Name)
    end;
check_limits(_Name, _Bin, _Parsed, _ClusterRpcOpts) ->
    ok.

-spec do_delete_v1(binary(), emqx_config:cluster_rpc_opts()) -> ok | {error, term()}.
do_delete_v1(Name, _ClusterRpcOpts) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        ok ?= delete_table_file(Name),
        emqx_maptabs_server:drop(Name)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

multicall(Fun, Args) ->
    case emqx_cluster_rpc:multicall(?MODULE, Fun, Args) of
        ok -> ok;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

%% Fails fast when a replicated update could not be applied on some
%% node: the plugin must be running with a writable tables dir on all
%% of them before the cluster_rpc transaction is initiated.
preflight_cluster() ->
    Errors = lists:filtermap(
        fun(Node) ->
            case emqx_maptabs_server:preflight(Node) of
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

ensure_dir(Dir) ->
    case filelib:ensure_path(Dir) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
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

table_files() ->
    lists:sort(filelib:wildcard(filename:join(tables_dir(), "*.json"))).

check_source_file_size(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            check_file_size(Size);
        {error, Reason} ->
            {error, #{
                reason => failed_to_read_file,
                path => unicode:characters_to_binary(Path),
                detail => Reason
            }}
    end.

check_file_size(Size) ->
    Max = max_table_file_bytes(),
    case Size =< Max of
        true ->
            ok;
        false ->
            {error, #{
                reason => table_file_too_large,
                file_bytes => Size,
                max_table_file_bytes => Max
            }}
    end.

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
