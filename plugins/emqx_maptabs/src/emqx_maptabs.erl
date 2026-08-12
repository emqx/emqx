%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Public API of the emqx_maptabs plugin.
%%
%% Mapping tables are stored in a replicated mria table (see
%% emqx_maptabs_store), one record per table holding the validated JSON
%% source. ETS is a per-node read cache derived from it, owned by
%% emqx_maptabs_server; lookups read the cache directly and never call
%% the server.
%%
%% An update is a single-record transaction: mria replicates it to
%% every node (a node that was down catches up on restart), and each
%% node's server follows the storage through mnesia table events. The
%% local node's cache is brought up to date synchronously before a
%% load/delete returns.
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
    reconcile_cluster/0,
    list_local/0,
    list_on_node/1,
    read_table/1,
    health_check/0
]).

%% Config accessors
-export([
    max_tables/0,
    max_rows_per_table/0,
    max_table_file_bytes/0
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

%% Loads a table file from a local path: validate, store the JSON in
%% the replicated table, and bring the local cache up to date. Other
%% nodes follow the storage through table events.
-spec load_file(file:filename_all()) -> ok | {error, term()}.
load_file(Path) ->
    maybe
        ok ?= health_check(),
        {ok, Name} ?= emqx_maptabs_loader:table_name_from_path(Path),
        %% size is checked before reading: a mistakenly huge file must
        %% not be pulled into memory at all
        ok ?= check_source_file_size(Path),
        {ok, Bin} ?= read_source_file(Path),
        {ok, Parsed} ?= emqx_maptabs_loader:parse(Bin),
        ok ?= check_row_limit(Parsed),
        ok ?= check_table_limit(Name),
        ok ?= emqx_maptabs_store:put(Name, Bin),
        emqx_maptabs_server:reconcile(node())
    end.

%% Deletes a table on all nodes.
-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    maybe
        ok ?= health_check(),
        ok ?= emqx_maptabs_loader:validate_name(Name),
        ok ?= emqx_maptabs_store:delete(Name),
        emqx_maptabs_server:reconcile(node())
    end.

%% Rebuilds the cache from storage on every running node. Normally a
%% no-op (caches follow storage through table events); this is the
%% operator fallback.
-spec reconcile_cluster() -> [{node(), term()}].
reconcile_cluster() ->
    [{Node, emqx_maptabs_server:reconcile(Node)} || Node <- emqx:running_nodes()].

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

%% The stored JSON source of a table.
-spec read_table(binary()) -> {ok, binary()} | {error, term()}.
read_table(Name) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        {ok, #maptab{json = Json}} ?= emqx_maptabs_store:get(Name),
        {ok, Json}
    end.

-spec health_check() -> ok | {error, binary()}.
health_check() ->
    emqx_maptabs_server:health_check().

%%--------------------------------------------------------------------
%% Config
%%--------------------------------------------------------------------

%% Limits are read fresh on every load, so a config update takes
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
        %% plugin app not (yet) fully set up
        _:_ -> Default
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_maptabs, vsn),
    iolist_to_binary([<<"emqx_maptabs-">>, Vsn]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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
%% counts against the limit.
check_table_limit(Name) ->
    case emqx_maptabs_store:get(Name) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            Count = emqx_maptabs_store:count(),
            Max = max_tables(),
            case Count < Max of
                true ->
                    ok;
                false ->
                    {error, #{reason => too_many_tables, table_count => Count, max_tables => Max}}
            end
    end.
