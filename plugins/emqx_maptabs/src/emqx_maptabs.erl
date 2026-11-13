%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs).

-moduledoc """
Public API of the emqx_maptabs plugin.

Mapping tables are stored in a replicated mria table (see
`emqx_maptabs_store`), one record per table holding the validated JSON
source. ETS is a per-node read cache derived from it, owned by
`emqx_maptabs_server`; lookups read the cache directly and never call
the server.

An update is a single-record transaction: mria replicates it to
every node (a node that was down catches up on restart), and each
node's server follows the storage through mnesia table events. The
local node's cache is brought up to date synchronously before a
load/delete returns.
""".

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

%% Management API (REST): every running node confirms its cache
-export([
    put_table/3,
    delete_table/2,
    reconcile_cluster/1,
    list_cluster/1
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
        {ok, _} ?= store_table(Name, Bin),
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

-doc """
Validates and stores a table, then asks every running node to confirm
its cache holds the new version before `Deadline`
(`erlang:monotonic_time` in milliseconds). A node that does not
confirm makes the result `{error, {partial, Info, Failed}}`; the
stored table is kept, and a repeat of the same content completes the
update, because the version is the hash of the content.
""".
-spec put_table(binary(), binary(), integer()) ->
    {ok, map()} | {error, {partial, map(), [{node(), term()}]}} | {error, term()}.
put_table(Name, Json, Deadline) ->
    maybe
        ok ?= health_check(),
        {ok, #{version := Version} = Info} ?= store_table(Name, Json),
        confirm_cluster(Name, Version, Info#{name => Name}, Deadline)
    end.

-doc """
Deletes a table, then asks every running node to confirm its cache
has dropped it, as `put_table/3`. Idempotent: an unknown table is
`ok` once every node confirms none is cached.
""".
-spec delete_table(binary(), integer()) ->
    ok
    | {error, {partial, map(), [{node(), term()}]}}
    | {error, term()}.
delete_table(Name, Deadline) ->
    maybe
        ok ?= health_check(),
        ok ?= emqx_maptabs_loader:validate_name(Name),
        %% an unknown table still goes to confirm: a retry after a
        %% partial delete must reach the nodes that missed it
        ok ?=
            case emqx_maptabs_store:get(Name) of
                {ok, _} -> emqx_maptabs_store:delete(Name);
                {error, not_found} -> ok
            end,
        {ok, _} ?= confirm_cluster(Name, absent, #{name => Name}, Deadline),
        ok
    end.

-doc "Rebuilds the cache from storage on every running node, in parallel.".
-spec reconcile_cluster(integer()) -> [{node(), ok | {error, term()}}].
reconcile_cluster(Deadline) ->
    emqx_maptabs_server:reconcile_on_nodes(emqx:running_nodes(), Deadline).

-doc "Metadata of the tables cached on every running node, in parallel.".
-spec list_cluster(integer()) -> [{node(), [map()] | {error, term()}}].
list_cluster(Deadline) ->
    emqx_maptabs_server:list_tables_on_nodes(emqx:running_nodes(), Deadline).

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

%% Validates and stores a table; caches follow on their own.
store_table(Name, Json) ->
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        ok ?= check_file_size(byte_size(Json)),
        {ok, #{row_count := RowCount, version := Version} = Parsed} ?=
            emqx_maptabs_loader:parse(Json),
        ok ?= check_row_limit(Parsed),
        ok ?= check_table_limit(Name),
        ok ?= emqx_maptabs_store:put(Name, Json),
        {ok, #{row_count => RowCount, version => Version}}
    end.

confirm_cluster(Name, Expect, Info, Deadline) ->
    Results = emqx_maptabs_server:confirm_on_nodes(
        emqx:running_nodes(), Name, Expect, Deadline
    ),
    case [{Node, failure_reason(Result)} || {Node, Result} <- Results, Result =/= ok] of
        [] ->
            {ok, Info#{nodes => [Node || {Node, _} <- Results]}};
        Failed ->
            {error, {partial, Info#{nodes => [Node || {Node, ok} <- Results]}, Failed}}
    end.

failure_reason({error, Reason}) -> Reason;
failure_reason(Other) -> Other.

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
